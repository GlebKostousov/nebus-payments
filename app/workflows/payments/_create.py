"""Приём платежа. Платёж и первая строка outbox пишутся одним commit."""

from uuid import UUID, uuid4

from sqlalchemy import text

from app.core.db import SessionFactory
from app.core.errors import (
    IdempotencyConflictError,
    InvariantError,
    PaymentNotFoundError,
)
from app.core.types import PaymentView
from app.core.webhook_policy import WebhookPolicy
from app.entities.outbox.model import Outbox
from app.entities.outbox.repository import OutboxRepository
from app.entities.payment.repository import PaymentRepository
from app.entities.payment.schema import CreatePayment, PaymentAccepted


class PaymentService:
    """Принимает платёж без обращения к RabbitMQ в HTTP-запросе."""

    def __init__(
        self, sessions: SessionFactory, *, webhook_policy: WebhookPolicy
    ) -> None:
        """Передать фабрику коротких транзакций и правила получателей."""
        self._sessions = sessions
        self._webhook_policy = webhook_policy

    async def create(
        self, request: CreatePayment, *, idempotency_key: str
    ) -> PaymentAccepted:
        """Сохранить платёж и событие вместе либо вернуть исходную квитанцию."""
        fingerprint = request.fingerprint()
        async with self._sessions() as session:
            repository = PaymentRepository(session)
            # Быстрый повтор остаётся доступен даже после изменения allowlist.
            existing = await repository.by_key(idempotency_key)
            if existing is not None:
                return self._receipt(
                    existing.view(), existing.request_fingerprint, fingerprint
                )
            self._webhook_policy.check(str(request.webhook_url))
            payment = await repository.insert_once(
                request,
                payment_id=uuid4(),
                message_id=uuid4(),
                idempotency_key=idempotency_key,
                fingerprint=fingerprint,
            )
            if payment is None:
                # READ COMMITTED: отдельный SELECT видит победившую вставку.
                return await self._reload_winner(
                    repository, idempotency_key, fingerprint
                )
            OutboxRepository(session).add(
                Outbox.processing(
                    payment_id=payment.id,
                    message_id=payment.expected_message_id,
                    attempt=1,
                    available_at=payment.created_at,
                )
            )
            await session.commit()
            return PaymentAccepted(payment_id=payment.id, created_at=payment.created_at)

    async def _reload_winner(
        self,
        repository: PaymentRepository,
        idempotency_key: str,
        fingerprint: str,
    ) -> PaymentAccepted:
        """Вернуть квитанцию вставки, победившей в гонке уникального ключа."""
        existing = await repository.by_key(idempotency_key)
        if existing is None:
            message = "После конфликта исчез idempotency key"
            raise InvariantError(message)
        return self._receipt(existing.view(), existing.request_fingerprint, fingerprint)

    @staticmethod
    def _receipt(
        payment: PaymentView, saved_fingerprint: str, fingerprint: str
    ) -> PaymentAccepted:
        """Вернуть первоначальное принятие, а не текущее состояние обработки."""
        if saved_fingerprint != fingerprint:
            raise IdempotencyConflictError
        return PaymentAccepted(payment_id=payment.id, created_at=payment.created_at)

    async def get(self, payment_id: UUID) -> PaymentView:
        """Получить актуальный снимок платежа."""
        async with self._sessions() as session:
            payment = await PaymentRepository(session).by_id(payment_id)
            if payment is None:
                raise PaymentNotFoundError
            return payment.view()

    async def ping(self) -> None:
        """Проверить только зависимость, необходимую для принятия платежа."""
        async with self._sessions() as session:
            await session.execute(text("SELECT 1"))

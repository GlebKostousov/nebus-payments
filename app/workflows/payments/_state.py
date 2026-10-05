"""Короткие транзакции вокруг внешних действий, а не вместо них."""

from datetime import datetime, timedelta
from typing import TypeGuard
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.db import SessionFactory, database_now
from app.core.errors import InvariantError, MessageContractError
from app.core.messages import PaymentJob, WebhookEvent
from app.core.retry import Backoff
from app.core.types import (
    MAX_ATTEMPTS,
    Currency,
    Failure,
    FailureStage,
    PaymentResult,
    PaymentStatus,
    ProcessingState,
    Work,
)
from app.entities.outbox.repository import OutboxRepository
from app.entities.payment.model import Payment
from app.entities.payment.repository import PaymentRepository

from ._transitions import schedule_after_failure


class PaymentState:
    """Владелец commit для переходов обработки платежа."""

    def __init__(
        self,
        sessions: SessionFactory,
        *,
        lease_seconds: float,
        backoff: Backoff,
    ) -> None:
        """Передать фабрику сессий и неизменяемые правила обработки."""
        self._sessions = sessions
        self._lease = timedelta(seconds=lease_seconds)
        self._backoff = backoff

    async def claim(self, job: PaymentJob) -> Work | None:
        """Начать только ожидаемую, доступную и ещё не захваченную работу."""
        async with self._sessions() as session:
            repo = PaymentRepository(session)
            payment = await _locked_payment(repo, job)
            if not self._can_claim(payment, job):
                return None
            return await self._start(session, repo, payment, job)

    async def _start(
        self,
        session: AsyncSession,
        repo: PaymentRepository,
        payment: Payment,
        job: PaymentJob,
    ) -> Work | None:
        """Зафиксировать аренду, если срок сообщения уже наступил."""
        now = await database_now(session)
        if not await repo.due(job.message_id, now=now):
            return None
        token = uuid4()
        payment.processing_state = ProcessingState.RUNNING.value
        payment.attempt_count += 1
        payment.lease_token = token
        payment.lease_until = now + self._lease
        await session.commit()
        return Work(payment.view(), token)

    @staticmethod
    def _can_claim(payment: Payment, job: PaymentJob) -> bool:
        """Дубли не расходуют попытки, даже когда другая доставка ещё выполняется."""
        if not _is_expected_ready(payment, job):
            return False
        _require_attempt_budget(payment)
        return True

    async def save_result(self, work: Work, result: PaymentResult) -> Work | None:
        """Сохранить результат и точные байты уведомления одной транзакцией."""
        async with self._sessions() as session:
            payment = await PaymentRepository(session).lock(work.payment.id)
            now = await database_now(session)
            if not _owns(payment, work, now=now):
                return None
            if payment.status != PaymentStatus.PENDING.value:
                message = "Повторное сохранение окончательного результата"
                raise InvariantError(message)
            event_id = uuid4()
            event = WebhookEvent(
                event_id=event_id,
                payment_id=payment.id,
                status=result.status,
                amount=payment.amount,
                currency=Currency(payment.currency),
                processed_at=now,
            )
            payment.status = result.status.value
            payment.provider_reference = result.provider_reference
            payment.processed_at = now
            payment.webhook_event_id = event_id
            payment.webhook_payload = event.model_dump_json()
            await session.commit()
            return Work(payment.view(), work.lease_token)

    async def complete(self, work: Work) -> bool:
        """Зафиксировать доставку до ACK сообщения, пока аренда ещё действует."""
        async with self._sessions() as session:
            payment = await PaymentRepository(session).lock(work.payment.id)
            now = await database_now(session)
            if not _owns(payment, work, now=now):
                return False
            payment.webhook_delivered_at = now
            payment.processing_state = ProcessingState.COMPLETED.value
            payment.lease_token = None
            payment.lease_until = None
            payment.last_error_code = None
            payment.last_error_stage = None
            await session.commit()
            return True

    async def fail(self, work: Work, failure: Failure) -> bool:
        """Записать исход попытки вместе с продолжением или DLQ."""
        async with self._sessions() as session:
            payment = await PaymentRepository(session).lock(work.payment.id)
            now = await database_now(session)
            if not _owns(payment, work, now=now):
                return False
            event = schedule_after_failure(
                payment, failure, now=now, backoff=self._backoff
            )
            OutboxRepository(session).add(event)
            await session.commit()
            return True

    async def recover(self, *, limit: int = 100) -> int:
        """Продолжить оборванные попытки, включая падение до первого внешнего вызова."""
        async with self._sessions() as session:
            now = await database_now(session)
            rows = await PaymentRepository(session).expired(now=now, limit=limit)
            outbox = OutboxRepository(session)
            for payment in rows:
                outbox.add(
                    schedule_after_failure(
                        payment,
                        Failure("attempt_interrupted", FailureStage.WORKER),
                        now=now,
                        backoff=self._backoff,
                    )
                )
            await session.commit()
            return len(rows)


async def _locked_payment(repo: PaymentRepository, job: PaymentJob) -> Payment:
    """Найти платёж сообщения или отвергнуть чужой идентификатор."""
    payment = await repo.lock(job.payment_id)
    if payment is None:
        message = "Сообщение ссылается на неизвестный платёж"
        raise MessageContractError(message)
    return payment


def _is_expected_ready(payment: Payment, job: PaymentJob) -> bool:
    """Проверить, что сообщение относится к свободной попытке."""
    if payment.expected_message_id != job.message_id:
        return False
    return payment.processing_state == ProcessingState.READY.value


def _require_attempt_budget(payment: Payment) -> None:
    """Не продолжать готовую работу после исчерпания бюджета."""
    if payment.attempt_count >= MAX_ATTEMPTS:
        message = "READY после исчерпания попыток"
        raise InvariantError(message)


def _owns(payment: Payment | None, work: Work, *, now: datetime) -> TypeGuard[Payment]:
    """Поздний ответ старого процесса не имеет права завершать новую попытку."""
    return (
        payment is not None
        and payment.processing_state == ProcessingState.RUNNING.value
        and payment.expected_message_id == work.payment.expected_message_id
        and payment.lease_token == work.lease_token
        and payment.lease_until is not None
        and payment.lease_until > now
    )

"""SQL платежей. Репозиторий не фиксирует транзакцию и не вызывает сеть."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.types import ProcessingState
from app.entities.outbox.model import Outbox

from .model import Payment
from .schema import CreatePayment


class PaymentRepository:
    """Запросы одного агрегата на переданной сессии."""

    def __init__(self, session: AsyncSession) -> None:
        """Использовать сессию владельца сценария."""
        self._session = session

    async def insert_once(
        self,
        request: CreatePayment,
        *,
        payment_id: UUID,
        message_id: UUID,
        idempotency_key: str,
        fingerprint: str,
    ) -> Payment | None:
        """Атомарно вставить платёж или уступить уже существующему ключу."""
        statement = (
            insert(Payment)
            .values(
                id=payment_id,
                amount=request.amount,
                currency=request.currency.value,
                description=request.description,
                extra_metadata=request.metadata,
                idempotency_key=idempotency_key,
                request_fingerprint=fingerprint,
                webhook_url=str(request.webhook_url),
                expected_message_id=message_id,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return (await self._session.scalars(statement)).one_or_none()

    async def by_key(self, idempotency_key: str) -> Payment | None:
        """Прочитать строку после разрешения конкурентного INSERT."""
        return await self._session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )

    async def by_id(self, payment_id: UUID) -> Payment | None:
        """Прочитать платёж без блокировки и изменений."""
        return await self._session.get(Payment, payment_id)

    async def lock(self, payment_id: UUID) -> Payment | None:
        """Сериализовать короткий переход одного платежа."""
        return await self._session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )

    async def due(self, message_id: UUID, *, now: datetime) -> bool:
        """Не начать попытку раньше её единственного расписания в outbox."""
        return (
            await self._session.scalar(
                select(Outbox.id).where(
                    Outbox.id == message_id, Outbox.available_at <= now
                )
            )
            is not None
        )

    async def expired(self, *, now: datetime, limit: int) -> list[Payment]:
        """Захватить оборванные попытки, не ожидая чужих блокировок."""
        rows = await self._session.scalars(
            select(Payment)
            .where(
                Payment.processing_state == ProcessingState.RUNNING.value,
                Payment.lease_until <= now,
            )
            .order_by(Payment.lease_until, Payment.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(rows)

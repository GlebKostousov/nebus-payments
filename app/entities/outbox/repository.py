"""Захват outbox и защита от завершения чужой аренды."""

from datetime import datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.types import Destination, Publication

from .model import Outbox


class OutboxRepository:
    """Работает только в транзакции вызывающего сценария."""

    def __init__(self, session: AsyncSession) -> None:
        """Сохранить сессию, не создавая собственного пула."""
        self._session = session

    def add(self, event: Outbox) -> None:
        """Включить событие в текущую транзакцию."""
        self._session.add(event)

    async def claim(
        self, *, now: datetime, lease: timedelta, limit: int
    ) -> list[Publication]:
        """Захватить доступный батч без удержания блокировки на время публикации."""
        rows = await self._session.scalars(
            select(Outbox)
            .where(
                Outbox.published_at.is_(None),
                Outbox.available_at <= now,
                or_(Outbox.lease_until.is_(None), Outbox.lease_until <= now),
            )
            .order_by(Outbox.available_at, Outbox.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        claimed: list[Publication] = []
        for row in rows:
            row.lease_token = uuid4()
            row.lease_until = now + lease
            row.publish_attempt_count += 1
            claimed.append(
                Publication(
                    id=row.id,
                    payment_id=row.payment_id,
                    destination=Destination(row.destination),
                    payload=row.payload,
                    lease_token=row.lease_token,
                    attempt_count=row.publish_attempt_count,
                )
            )
        return claimed

    async def owned(self, publication: Publication, *, now: datetime) -> Outbox | None:
        """Вернуть строку только действующему владельцу аренды."""
        return await self._session.scalar(
            select(Outbox)
            .where(
                Outbox.id == publication.id,
                Outbox.lease_token == publication.lease_token,
                Outbox.lease_until > now,
                Outbox.published_at.is_(None),
            )
            .with_for_update()
        )

    async def by_id(self, message_id: UUID) -> Outbox | None:
        """Прочитать сообщение для диагностики и теста сохранённого расписания."""
        return await self._session.get(Outbox, message_id)

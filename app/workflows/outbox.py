"""Публикация в RabbitMQ. Транзакция закрывается до сети и открывается снова после подтверждения."""

import asyncio
from datetime import timedelta

from app.core.db import SessionFactory, database_now
from app.core.errors import PublicationError
from app.core.ports import Publisher
from app.core.retry import Backoff
from app.core.types import Publication
from app.entities.outbox.repository import OutboxRepository


class OutboxRelay:
    """Счётчик публикации не расходует бюджет обработки платежа."""

    def __init__(
        self,
        sessions: SessionFactory,
        publisher: Publisher,
        *,
        backoff: Backoff,
        batch_size: int = 10,
        lease_seconds: float = 30,
    ) -> None:
        """Ограничить батч; все его сообщения отправляются конкурентно."""
        self._sessions = sessions
        self._publisher = publisher
        self._backoff = backoff
        self._batch_size = batch_size
        self._lease = timedelta(seconds=lease_seconds)

    async def tick(self) -> int:
        """Выполнить один проход: захват, сеть, сохранение результата."""
        async with self._sessions() as session:
            rows = await OutboxRepository(session).claim(
                now=await database_now(session),
                lease=self._lease,
                limit=self._batch_size,
            )
            await session.commit()
        async with asyncio.TaskGroup() as group:
            for publication in rows:
                group.create_task(self._publish(publication))
        return len(rows)

    async def _publish(self, publication: Publication) -> None:
        """После неоднозначного результата оставляем возможность повторной отправки."""
        try:
            await self._publisher.publish(publication)
        except PublicationError:
            await self._finish(publication, success=False)
        else:
            await self._finish(publication, success=True)

    async def _finish(self, publication: Publication, *, success: bool) -> None:
        """Обновить запись только при совпадении ещё действующего токена аренды."""
        async with self._sessions() as session:
            now = await database_now(session)
            row = await OutboxRepository(session).owned(publication, now=now)
            if row is None:
                return
            row.lease_token = None
            row.lease_until = None
            if success:
                row.published_at = now
                row.last_error_code = None
            else:
                row.available_at = now + self._backoff.delay(publication.attempt_count)
                row.last_error_code = "broker_publication_failed"
            await session.commit()

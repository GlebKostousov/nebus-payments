"""Публикация считается успешной только после подтверждения брокера."""

import asyncio

from aio_pika.exceptions import AMQPError
from faststream.rabbit import RabbitBroker, RabbitExchange
from pamqp.commands import Basic

from app.core.errors import PublicationError
from app.core.types import Destination, Publication

from ._topology import Topology


class RabbitPublisher:
    """Persistent + mandatory + confirms для обеих destinations."""

    def __init__(
        self, broker: RabbitBroker, topology: Topology, *, timeout_seconds: float = 5
    ) -> None:
        """Передать брокер с включённым on_return_raises."""
        self._broker = broker
        self._topology = topology
        self._timeout = timeout_seconds

    async def publish(self, publication: Publication) -> None:
        """Подтвердить только ACK брокера, а не отсутствие исключения клиента."""
        exchange, routing_key = self._route(publication)
        try:
            async with asyncio.timeout(self._timeout):
                confirmation = await self._broker.publish(
                    publication.payload.encode("utf-8"),
                    exchange=exchange,
                    routing_key=routing_key,
                    message_id=str(publication.id),
                    correlation_id=str(publication.payment_id),
                    content_type="application/json",
                    persist=True,
                    mandatory=True,
                    timeout=self._timeout,
                )
        except (AMQPError, OSError, TimeoutError) as error:
            message = "RabbitMQ не подтвердил публикацию"
            raise PublicationError(message) from error
        _require_ack(confirmation)

    def _route(self, publication: Publication) -> tuple[RabbitExchange, str]:
        """Выбрать exchange и routing key по назначению записи."""
        topology = self._topology
        if publication.destination is Destination.DEAD_LETTER:
            return topology.dead_exchange, topology.dead_queue.name
        return topology.exchange, topology.queue.name


def _require_ack(confirmation: object) -> None:
    """Считать успехом только положительный confirm брокера."""
    if not isinstance(confirmation, Basic.Ack):
        message = "Нет положительного publisher confirm"
        raise PublicationError(message)

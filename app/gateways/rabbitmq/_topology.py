"""Рабочая очередь и DLQ. Когда публиковать, записано в outbox.available_at."""

from dataclasses import dataclass
from typing import Self

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import Channel
from faststream.rabbit.schemas.queue import QueueType

from app.core.config import Settings

# pamqp 3.3.0 кодирует диапазон -128..-1 беззнаковым байтом и падает.
# В RabbitMQ 4.1 любое отрицательное значение снимает лимит передоставок.
_UNLIMITED_DELIVERY_LIMIT = -129


@dataclass(frozen=True, slots=True)
class Topology:
    """Объекты одной топологии без глобального изменяемого состояния."""

    exchange: RabbitExchange
    dead_exchange: RabbitExchange
    queue: RabbitQueue
    dead_queue: RabbitQueue

    @classmethod
    def build(cls, *, prefix: str = "payments") -> Self:
        """В тестах prefix изолирует очереди, не меняя их свойства."""
        dead_name = f"{prefix}.dead"
        dead_queue_name = f"{prefix}.dlq"
        return cls(
            exchange=RabbitExchange(prefix, durable=True),
            dead_exchange=RabbitExchange(dead_name, durable=True),
            queue=RabbitQueue(
                f"{prefix}.new",
                queue_type=QueueType.QUORUM,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": dead_name,
                    "x-dead-letter-routing-key": dead_queue_name,
                    "x-dead-letter-strategy": "at-least-once",
                    "x-overflow": "reject-publish",
                    # Сетевые переподключения не должны съедать бизнес-бюджет.
                    "x-delivery-limit": _UNLIMITED_DELIVERY_LIMIT,
                },
            ),
            dead_queue=RabbitQueue(
                dead_queue_name,
                queue_type=QueueType.QUORUM,
                durable=True,
                arguments={"x-overflow": "reject-publish"},
            ),
        )

    async def declare(self, broker: RabbitBroker) -> None:
        """Создать и связать DLQ до запуска доставки рабочих сообщений."""
        dead_exchange = await broker.declare_exchange(self.dead_exchange)
        dead_queue = await broker.declare_queue(self.dead_queue)
        await dead_queue.bind(dead_exchange, routing_key=self.dead_queue.name)
        exchange = await broker.declare_exchange(self.exchange)
        queue = await broker.declare_queue(self.queue)
        await queue.bind(exchange, routing_key=self.queue.name)


def build_broker(settings: Settings) -> RabbitBroker:
    """Включить подтверждения и ошибку при возврате unroutable сообщения."""
    return RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        timeout=5,
        reconnect_interval=5,
        graceful_timeout=settings.attempt_timeout_seconds + 5,
        default_channel=Channel(
            prefetch_count=settings.consumer_prefetch,
            publisher_confirms=True,
            on_return_raises=True,
        ),
        logger=None,
    )

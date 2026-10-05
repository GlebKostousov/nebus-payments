"""Проверки relay на настоящем PostgreSQL и подтверждений настоящего RabbitMQ."""

import asyncio
from uuid import uuid4

import pytest
from app.core.db import SessionFactory
from app.core.errors import PublicationError
from app.core.retry import Backoff
from app.core.types import Destination, Publication
from app.entities.outbox.model import Outbox
from app.gateways.rabbitmq.main import RabbitPublisher
from app.workflows.outbox import OutboxRelay
from app.workflows.payments.main import PaymentService
from sqlalchemy import func, select, text, update

from tests.factories import request
from tests.integration.conftest import RabbitEnvironment
from tests.integration.helpers import receive

pytestmark = pytest.mark.integration


def publication() -> Publication:
    """Создать техническое сообщение без обращения к внешнему HTTP."""
    return Publication(
        id=uuid4(),
        payment_id=uuid4(),
        destination=Destination.PROCESSING,
        payload='{"test":true}',
        lease_token=uuid4(),
        attempt_count=1,
    )


async def test_persistent_publish_and_message_id(rabbit: RabbitEnvironment) -> None:
    """Проверить реальное сообщение, а не только аргументы вызова publisher."""
    value = publication()
    await RabbitPublisher(rabbit.broker, rabbit.topology).publish(value)
    queue = await rabbit.broker.declare_queue(rabbit.topology.queue)
    message = await receive(queue)
    assert message.message_id == str(value.id)
    assert message.body == value.payload.encode()
    assert message.delivery_mode.value == 2
    await message.ack()


async def test_unroutable_message_is_failure(rabbit: RabbitEnvironment) -> None:
    """Положительный confirm не должен скрыть basic.return."""
    queue = await rabbit.broker.declare_queue(rabbit.topology.queue)
    exchange = await rabbit.broker.declare_exchange(rabbit.topology.exchange)
    await queue.unbind(exchange, routing_key=rabbit.topology.queue.name)
    with pytest.raises(PublicationError):
        await RabbitPublisher(rabbit.broker, rabbit.topology).publish(publication())


class FailingPublisher:
    """Управляемый отказ публикации для проверки отдельного бюджета outbox."""

    async def publish(self, value: Publication) -> None:
        """Эмулировать недоступный брокер до факта обработки платежа."""
        del value
        message = "injected broker failure"
        raise PublicationError(message)


class RecordingPublisher:
    """Запоминает идентификаторы реально отданных публикаций."""

    def __init__(self) -> None:
        """Начать с пустого списка подтверждённых попыток."""
        self.ids: list[str] = []

    async def publish(self, value: Publication) -> None:
        """Сохранить идентификатор без обращения к брокеру."""
        self.ids.append(str(value.id))


async def test_concurrent_relays_publish_one_message(
    sessions: SessionFactory,
    service: PaymentService,
) -> None:
    """Два relay не публикуют одну строку outbox дважды."""
    await service.create(request(), idempotency_key=str(uuid4()))
    publishers = [RecordingPublisher(), RecordingPublisher()]
    relays = [
        OutboxRelay(sessions, publisher, backoff=Backoff(lambda: 0))
        for publisher in publishers
    ]
    counts = await asyncio.gather(*(relay.tick() for relay in relays))
    published = [item for publisher in publishers for item in publisher.ids]
    assert sum(counts) == len(published) == 1


async def test_publication_failure_does_not_spend_payment_attempt(
    sessions: SessionFactory,
    service: PaymentService,
) -> None:
    """При недоступном RabbitMQ платёж остаётся принятым, а outbox — неопубликованным."""
    receipt = await service.create(request(), idempotency_key=str(uuid4()))
    relay = OutboxRelay(sessions, FailingPublisher(), backoff=Backoff(lambda: 0))
    assert await relay.tick() == 1
    payment = await service.get(receipt.payment_id)
    assert payment.attempt_count == 0
    async with sessions() as session:
        row = await session.scalar(
            select(Outbox).where(Outbox.payment_id == receipt.payment_id)
        )
        assert row is not None
        assert row.published_at is None
        assert row.publish_attempt_count == 1
        assert row.lease_token is None
    assert await relay.tick() == 0


async def test_crash_after_publish_repeats_same_message_id(
    sessions: SessionFactory,
    service: PaymentService,
    rabbit: RabbitEnvironment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Убить фиксацию после publisher confirm и проверить реальный дубль в очереди."""
    receipt = await service.create(request(), idempotency_key=str(uuid4()))
    publisher = RabbitPublisher(rabbit.broker, rabbit.topology)
    relay = OutboxRelay(sessions, publisher, backoff=Backoff(lambda: 0))

    async def fail_finish(_value: Publication, *, success: bool) -> None:
        """Эмулировать остановку между RabbitMQ ACK и published_at."""
        del success
        message = "injected crash after confirm"
        raise RuntimeError(message)

    monkeypatch.setattr(relay, "_finish", fail_finish)
    with pytest.raises(ExceptionGroup):
        await relay.tick()
    async with sessions() as session:
        row = await session.scalar(
            select(Outbox).where(Outbox.payment_id == receipt.payment_id)
        )
        assert row is not None
        assert row.published_at is None
        await session.execute(
            update(Outbox)
            .where(Outbox.id == row.id)
            .values(lease_until=func.clock_timestamp() - text("interval '1 second'"))
        )
        await session.commit()
    following = OutboxRelay(sessions, publisher, backoff=Backoff(lambda: 0))
    assert await following.tick() == 1
    queue = await rabbit.broker.declare_queue(rabbit.topology.queue)
    first, second = await receive(queue), await receive(queue)
    assert first.message_id == second.message_id
    assert first.body == second.body
    await first.ack()
    await second.ack()

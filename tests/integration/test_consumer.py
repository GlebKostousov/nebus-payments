"""Настоящий FastStream handler: raw payload, manual ACK и broker-side DLX."""

import asyncio
from uuid import uuid4

import pytest
from app.core.db import SessionFactory
from app.core.messages import DeadLetter
from app.core.retry import Backoff
from app.core.types import PaymentStatus, ProcessingState
from app.gateways.rabbitmq.main import RabbitPublisher
from app.workers.payment_consumer import register_consumer
from app.workflows.outbox import OutboxRelay
from app.workflows.payments.main import PaymentProcessor, PaymentService, PaymentState

from tests.factories import request
from tests.fakes import CountingPaymentGateway, RecordingWebhook
from tests.integration.conftest import RabbitEnvironment
from tests.integration.helpers import receive, wait_completed

pytestmark = pytest.mark.integration


async def test_one_handler_processes_and_delivers(
    sessions: SessionFactory,
    service: PaymentService,
    rabbit: RabbitEnvironment,
) -> None:
    """Пройти API-сценарий, outbox, реальный брокер и тот же production handler."""
    state = PaymentState(sessions, lease_seconds=60, backoff=Backoff(lambda: 0))
    gateway = CountingPaymentGateway()
    webhook = RecordingWebhook()
    processor = PaymentProcessor(
        state, payment_gateway=gateway, webhook_gateway=webhook
    )
    fatal = asyncio.Event()
    register_consumer(rabbit.broker, rabbit.topology, processor, fatal=fatal)
    await rabbit.broker.start()
    receipt = await service.create(request(), idempotency_key=str(uuid4()))
    relay = OutboxRelay(
        sessions,
        RabbitPublisher(rabbit.broker, rabbit.topology),
        backoff=Backoff(lambda: 0),
    )
    await relay.tick()
    payment = await wait_completed(service, receipt.payment_id)
    assert payment.attempt_count == 1
    assert gateway.calls == 1
    assert len(webhook.sent) == 1
    assert not fatal.is_set()


async def test_invalid_json_reaches_dlq_with_manual_ack(
    sessions: SessionFactory,
    rabbit: RabbitEnvironment,
) -> None:
    """Валидация не должна упасть до handler и оставить unacked poison message."""
    state = PaymentState(sessions, lease_seconds=60, backoff=Backoff(lambda: 0))
    fatal = asyncio.Event()
    processor = PaymentProcessor(
        state,
        payment_gateway=CountingPaymentGateway(),
        webhook_gateway=RecordingWebhook(),
    )
    register_consumer(rabbit.broker, rabbit.topology, processor, fatal=fatal)
    await rabbit.broker.start()
    await rabbit.broker.publish(
        b"not-json",
        exchange=rabbit.topology.exchange,
        routing_key=rabbit.topology.queue.name,
        message_id=str(uuid4()),
        content_type="application/json",
        persist=True,
        mandatory=True,
    )
    queue = await rabbit.broker.declare_queue(rabbit.topology.dead_queue)
    rejected = await receive(queue)
    assert rejected.body == b"not-json"
    assert "x-death" in rejected.headers
    assert not fatal.is_set()
    await rejected.ack()


async def test_third_webhook_failure_publishes_durable_dead_letter(
    sessions: SessionFactory,
    service: PaymentService,
    rabbit: RabbitEnvironment,
) -> None:
    """Реальный consumer проходит три попытки, relay отправляет сохранённый DLQ."""
    state = PaymentState(sessions, lease_seconds=60, backoff=Backoff(lambda: 0))
    gateway = CountingPaymentGateway()
    webhook = RecordingWebhook(fail=True)
    processor = PaymentProcessor(
        state, payment_gateway=gateway, webhook_gateway=webhook
    )
    fatal = asyncio.Event()
    register_consumer(rabbit.broker, rabbit.topology, processor, fatal=fatal)
    await rabbit.broker.start()
    receipt = await service.create(request(), idempotency_key=str(uuid4()))
    relay = OutboxRelay(
        sessions,
        RabbitPublisher(rabbit.broker, rabbit.topology),
        backoff=Backoff(lambda: 0),
    )
    async with asyncio.timeout(20):
        while True:
            await relay.tick()
            payment = await service.get(receipt.payment_id)
            if payment.processing_state is ProcessingState.DEAD_LETTERED:
                break
            await asyncio.sleep(0.05)
    await relay.tick()
    queue = await rabbit.broker.declare_queue(rabbit.topology.dead_queue)
    message = await receive(queue)
    letter = DeadLetter.model_validate_json(message.body)
    assert letter.payment_id == receipt.payment_id
    assert letter.attempt_count == 3
    assert letter.error_code == "webhook_http_503"
    assert payment.status is PaymentStatus.SUCCEEDED
    assert gateway.calls == 1
    assert len(webhook.sent) == 3
    assert len(set(webhook.sent)) == 1
    assert not fatal.is_set()
    await message.ack()

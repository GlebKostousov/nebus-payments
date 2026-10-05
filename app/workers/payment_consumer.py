"""Подписка на платежи. Сообщение подтверждаем после записи в базу."""

import asyncio

from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from faststream.rabbit.message import RabbitMessage as RawRabbitMessage
from pydantic import ValidationError

from app.core.errors import MessageContractError
from app.core.logging import get_logger
from app.core.messages import PaymentJob
from app.gateways.rabbitmq.main import Topology
from app.workflows.payments.main import PaymentProcessor

logger = get_logger(__name__)
_MAX_MESSAGE_BYTES = 16384


async def raw_decoder(message: RawRabbitMessage) -> bytes:
    """Отдать тело сообщения байтами, без разбора внутри брокера."""
    return message.raw_message.body


def register_consumer(
    broker: RabbitBroker,
    topology: Topology,
    processor: PaymentProcessor,
    *,
    fatal: asyncio.Event,
) -> None:
    """Подписать один обработчик. Сбой инфраструктуры останавливает процесс."""

    @broker.subscriber(
        topology.queue,
        exchange=topology.exchange,
        ack_policy=AckPolicy.MANUAL,
        decoder=raw_decoder,
        no_reply=True,
    )
    async def process_payment(message: RabbitMessage) -> None:
        """Обработать доставку. Если подтвердить её не вышло, остановить процесс."""
        try:
            await _handle_delivery(message, processor)
        except Exception as error:  # noqa: BLE001 — в тексте исключения бывают URL и SQL
            logger.error(  # noqa: TRY400
                "consumer.failed", error_type=type(error).__name__
            )
            fatal.set()


def _job_from_message(message: RawRabbitMessage) -> PaymentJob:
    """Отклонить тело, которое нельзя безопасно разобрать."""
    body = message.raw_message.body
    if len(body) > _MAX_MESSAGE_BYTES:
        error = "Сообщение превышает лимит 16 КиБ"
        raise MessageContractError(error)
    job = PaymentJob.model_validate_json(body)
    if message.raw_message.message_id != str(job.message_id):
        error = "AMQP message_id не совпадает с payload"
        raise MessageContractError(error)
    return job


async def _handle_delivery(
    message: RawRabbitMessage, processor: PaymentProcessor
) -> None:
    """Разобрать сообщение, обработать платёж и подтвердить доставку."""
    try:
        job = _job_from_message(message)
        outcome = await processor.process(job)
    except (ValidationError, MessageContractError):
        logger.warning(
            "payment.message_rejected",
            reason="invalid_contract",
            message_id=message.raw_message.message_id,
        )
        await message.reject(requeue=False)
        return
    logger.info(
        "payment.processed",
        payment_id=str(job.payment_id),
        message_id=str(job.message_id),
        action=outcome.action,
        attempt=outcome.attempt,
        error_code=outcome.error_code,
    )
    await message.ack()

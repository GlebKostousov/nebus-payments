"""Ожидание наблюдаемого состояния и управляемые точки отказа в тестовой БД."""

import asyncio
from uuid import UUID

from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from app.core.db import SessionFactory
from app.core.messages import PaymentJob
from app.core.types import PaymentView, ProcessingState
from app.entities.outbox.model import Outbox
from app.entities.payment.model import Payment
from app.workflows.payments.main import PaymentService
from sqlalchemy import func, select, text, update


async def current_job(sessions: SessionFactory, payment_id: UUID) -> PaymentJob:
    """Прочитать именно текущее поколение работы из сохранённого outbox."""
    async with sessions() as session:
        payload = await session.scalar(
            select(Outbox.payload)
            .join(Payment, Payment.expected_message_id == Outbox.id)
            .where(Payment.id == payment_id)
        )
        assert payload is not None
        return PaymentJob.model_validate_json(payload)


async def make_due(sessions: SessionFactory, message_id: UUID) -> None:
    """Передвинуть часы задания, а не ждать несколько секунд в интеграционном тесте."""
    async with sessions() as session:
        await session.execute(
            update(Outbox)
            .where(Outbox.id == message_id)
            .values(available_at=func.clock_timestamp() - text("interval '1 second'"))
        )
        await session.commit()


async def receive(queue: AbstractQueue) -> AbstractIncomingMessage:
    """Дождаться broker-side события без фиксированного многосекундного sleep."""
    async with asyncio.timeout(10):
        while True:
            message = await queue.get(fail=False)
            if message is not None:
                return message
            await asyncio.sleep(0.025)


async def wait_completed(service: PaymentService, payment_id: UUID) -> PaymentView:
    """Опросить observable state; дедлайн ограничивает зависший тест."""
    async with asyncio.timeout(10):
        while True:
            payment = await service.get(payment_id)
            if payment.processing_state is ProcessingState.COMPLETED:
                return payment
            await asyncio.sleep(0.025)

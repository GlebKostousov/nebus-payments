"""Проверки оркестрации без подмены PostgreSQL in-memory базой."""

import asyncio

import pytest
from app.core.messages import PaymentJob
from app.core.types import PaymentStatus
from app.workflows.payments.main import PaymentProcessor

from tests.fakes import (
    BlockingWebhook,
    CountingPaymentGateway,
    RecordingState,
    RecordingWebhook,
)


def job(state: RecordingState) -> PaymentJob:
    """Создать поручение к подготовленному снимку."""
    return PaymentJob(
        payment_id=state.work.payment.id,
        message_id=state.work.payment.expected_message_id,
    )


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_final_result_is_saved_before_webhook(status: PaymentStatus) -> None:
    """Окончательный отказ — такой же доставляемый результат, не ошибка retry."""
    state = RecordingState()
    gateway = CountingPaymentGateway(status)
    webhook = RecordingWebhook()
    processor = PaymentProcessor(
        state, payment_gateway=gateway, webhook_gateway=webhook
    )
    result = await processor.process(job(state))
    assert result.action == "completed"
    assert state.saved == state.completed == gateway.calls == 1
    assert state.work.payment.status is status
    assert not state.failures


async def test_webhook_retry_does_not_charge_again() -> None:
    """Даже после потери ответа повторяется уведомление, а не оплата."""
    state = RecordingState()
    gateway = CountingPaymentGateway()
    webhook = RecordingWebhook(fail=True)
    processor = PaymentProcessor(
        state, payment_gateway=gateway, webhook_gateway=webhook
    )
    await processor.process(job(state))
    webhook.fail = False
    await processor.process(job(state))
    assert gateway.calls == 1
    assert state.saved == 1
    assert state.completed == 1
    assert len(state.failures) == 1
    assert len(webhook.sent) == 2
    assert webhook.sent[0] == webhook.sent[1]


async def test_duplicate_has_no_external_effects() -> None:
    """Пропущенное state-слоем сообщение не доходит до внешних клиентов."""
    state = RecordingState()
    state.skip = True
    gateway = CountingPaymentGateway()
    webhook = RecordingWebhook()
    outcome = await PaymentProcessor(
        state, payment_gateway=gateway, webhook_gateway=webhook
    ).process(job(state))
    assert outcome.action == "ignored"
    assert gateway.calls == 0
    assert not webhook.sent


async def test_expired_owner_does_not_send_webhook() -> None:
    """Если результат не сохранён из-за смены владельца, обработчик останавливается."""
    state = RecordingState()
    state.lose_result_lease = True
    webhook = RecordingWebhook()
    result = await PaymentProcessor(
        state, payment_gateway=CountingPaymentGateway(), webhook_gateway=webhook
    ).process(job(state))
    assert result.action == "lease_lost"
    assert not webhook.sent


async def test_task_cancellation_is_not_a_business_failure() -> None:
    """Отмена выходит наружу: новый переход выполнит процедура восстановления."""
    state = RecordingState()
    webhook = BlockingWebhook()
    processor = PaymentProcessor(
        state, payment_gateway=CountingPaymentGateway(), webhook_gateway=webhook
    )
    task = asyncio.create_task(processor.process(job(state)))
    await asyncio.wait_for(webhook.entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert webhook.cleaned_up
    assert state.saved == 1
    assert state.completed == 0
    assert not state.failures


async def test_attempt_deadline_is_recorded_once() -> None:
    """Общий дедлайн заканчивает одну попытку и освобождает отменённый ресурс."""
    state = RecordingState(status=PaymentStatus.SUCCEEDED)
    webhook = BlockingWebhook()
    result = await PaymentProcessor(
        state,
        payment_gateway=CountingPaymentGateway(),
        webhook_gateway=webhook,
        timeout_seconds=0.01,
    ).process(job(state))
    assert result.error_code == "attempt_deadline_exceeded"
    assert len(state.failures) == 1
    assert webhook.cleaned_up

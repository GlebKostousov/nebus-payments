"""Один переход ошибки используется обычной попыткой и восстановлением."""

from datetime import timedelta

import pytest
from app.core.messages import DeadLetter, PaymentJob
from app.core.retry import Backoff
from app.core.types import Destination, Failure, FailureStage, PaymentStatus, Work
from app.workflows.payments._state import _owns
from app.workflows.payments._transitions import schedule_after_failure

from tests.factories import NOW, payment_row


@pytest.mark.parametrize(("attempt", "delay"), [(1, 2), (2, 4)])
def test_failure_schedules_next_attempt(attempt: int, delay: int) -> None:
    """У расписания один владелец: время находится только в outbox."""
    payment = payment_row(attempt=attempt)
    previous_message = payment.expected_message_id
    event = schedule_after_failure(
        payment,
        Failure("webhook_http_503", FailureStage.WEBHOOK),
        now=NOW,
        backoff=Backoff(lambda: 0),
    )
    assert event.available_at == NOW + timedelta(seconds=delay)
    assert event.destination == Destination.PROCESSING.value
    assert payment.processing_state == "ready"
    assert payment.attempt_count == attempt
    assert payment.lease_token is None
    assert payment.lease_until is None
    assert payment.expected_message_id != previous_message
    job = PaymentJob.model_validate_json(event.payload)
    assert job.message_id == event.id == payment.expected_message_id
    assert event.deduplication_key == f"process:{payment.id}:{attempt + 1}"


@pytest.mark.parametrize("status", list(PaymentStatus))
def test_third_failure_does_not_change_financial_result(status: PaymentStatus) -> None:
    """DLQ не делает успешный платёж неуспешным, неизвестный — известным."""
    payment = payment_row(attempt=3, status=status)
    previous_message = payment.expected_message_id
    event = schedule_after_failure(
        payment,
        Failure("webhook_http_503", FailureStage.WEBHOOK),
        now=NOW,
        backoff=Backoff(lambda: 0),
    )
    assert payment.status == status.value
    assert payment.processing_state == "dead_lettered"
    assert event.destination == Destination.DEAD_LETTER.value
    assert event.available_at == NOW
    letter = DeadLetter.model_validate_json(event.payload)
    assert letter.attempt_count == 3
    assert letter.original_message_id == previous_message
    assert letter.error_code == "webhook_http_503"


def test_security_failure_is_not_retried() -> None:
    """Отозванный получатель не вызывается ещё дважды ради счётчика."""
    payment = payment_row()
    event = schedule_after_failure(
        payment,
        Failure("webhook_destination_forbidden", FailureStage.WEBHOOK, permanent=True),
        now=NOW,
        backoff=Backoff(lambda: 0),
    )
    assert event.destination == "dead_letter"
    assert payment.attempt_count == 1


def test_retry_after_is_a_lower_bound() -> None:
    """Обещанный внешним сервером срок нельзя сократить backoff-формулой."""
    payment = payment_row()
    resume = NOW + timedelta(minutes=5)
    event = schedule_after_failure(
        payment,
        Failure("webhook_http_429", FailureStage.WEBHOOK, retry_after=resume),
        now=NOW,
        backoff=Backoff(lambda: 0),
    )
    assert event.available_at == resume


def test_old_owner_cannot_write_after_lease_expiry() -> None:
    """Совпадения токена недостаточно: истёкшая аренда уже не даёт право записи."""
    payment = payment_row()
    assert payment.lease_token is not None
    work = Work(payment.view(), payment.lease_token)
    assert not _owns(payment, work, now=NOW)
    payment.lease_until = NOW + timedelta(seconds=60)
    assert _owns(payment, work, now=NOW)


def test_cancelled_generation_cannot_finish_new_attempt() -> None:
    """Новое сообщение имеет новый токен; старый ответ не завершает чужую работу."""
    payment = payment_row()
    assert payment.lease_token is not None
    old_work = Work(payment.view(), payment.lease_token)
    schedule_after_failure(
        payment,
        Failure("attempt_interrupted", FailureStage.WORKER),
        now=NOW,
        backoff=Backoff(lambda: 0),
    )
    assert not _owns(payment, old_work, now=NOW)

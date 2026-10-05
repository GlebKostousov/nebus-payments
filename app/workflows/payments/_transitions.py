"""Общий переход после ошибки для обычного обработчика и восстановления."""

from datetime import datetime
from uuid import uuid4

from app.core.messages import DeadLetter
from app.core.retry import Backoff
from app.core.types import MAX_ATTEMPTS, Destination, Failure, ProcessingState
from app.entities.outbox.model import Outbox
from app.entities.payment.model import Payment


def schedule_after_failure(
    payment: Payment, failure: Failure, *, now: datetime, backoff: Backoff
) -> Outbox:
    """Изменить захваченную строку и вернуть событие для той же транзакции."""
    payment.lease_token = None
    payment.lease_until = None
    payment.last_error_code = failure.code
    payment.last_error_stage = failure.stage.value
    if payment.attempt_count >= MAX_ATTEMPTS or failure.permanent:
        return _dead_letter(payment, failure, now=now)
    available_at = now + backoff.delay(payment.attempt_count)
    if failure.retry_after is not None:
        available_at = max(available_at, failure.retry_after)
    message_id = uuid4()
    payment.processing_state = ProcessingState.READY.value
    payment.expected_message_id = message_id
    return Outbox.processing(
        payment_id=payment.id,
        message_id=message_id,
        attempt=payment.attempt_count + 1,
        available_at=available_at,
    )


def _dead_letter(payment: Payment, failure: Failure, *, now: datetime) -> Outbox:
    """Закончить исполнение, не подменяя финансовый результат техническим сбоем."""
    message_id = uuid4()
    event = DeadLetter(
        message_id=message_id,
        original_message_id=payment.expected_message_id,
        payment_id=payment.id,
        attempt_count=payment.attempt_count,
        error_code=failure.code,
        error_stage=failure.stage,
        occurred_at=now,
    )
    payment.processing_state = ProcessingState.DEAD_LETTERED.value
    return Outbox(
        id=message_id,
        payment_id=payment.id,
        destination=Destination.DEAD_LETTER.value,
        payload=event.model_dump_json(),
        deduplication_key=f"dead:{payment.id}",
        available_at=now,
    )

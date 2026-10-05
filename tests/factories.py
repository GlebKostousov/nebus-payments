"""Заготовки платежей и outbox для тестов."""

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from app.core.types import Currency, PaymentStatus, PaymentView, ProcessingState
from app.entities.payment.model import Payment
from app.entities.payment.schema import CreatePayment

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


def request(
    *, amount: str = "1500.00", url: str = "https://receiver.example/hook"
) -> CreatePayment:
    """Создать валидный запрос без зависимости от реальной сети."""
    return CreatePayment.model_validate(
        {
            "amount": amount,
            "currency": "RUB",
            "description": "Заказ 123",
            "metadata": {"order_id": "123"},
            "webhook_url": url,
        }
    )


def view(*, status: PaymentStatus = PaymentStatus.PENDING) -> PaymentView:
    """Создать снимок платежа без ORM-сессии."""
    value = PaymentView(
        id=uuid4(),
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        description="Заказ 123",
        metadata={"order_id": "123"},
        status=PaymentStatus.PENDING,
        webhook_url="https://receiver.example/hook",
        created_at=NOW,
        processed_at=None,
        provider_reference=None,
        webhook_event_id=None,
        webhook_payload=None,
        webhook_delivered_at=None,
        processing_state=ProcessingState.READY,
        attempt_count=0,
        expected_message_id=uuid4(),
        last_error_code=None,
        last_error_stage=None,
    )
    if status is PaymentStatus.PENDING:
        return value
    return replace(
        value,
        status=status,
        processed_at=NOW,
        provider_reference="sim_test",
        webhook_event_id=uuid4(),
        webhook_payload='{"event_type":"payment.completed"}',
    )


def payment_row(
    *, attempt: int = 1, status: PaymentStatus = PaymentStatus.PENDING
) -> Payment:
    """Создать полностью заполненный ORM-объект для чистых переходов без БД."""
    snapshot = view(status=status)
    return Payment(
        id=snapshot.id,
        amount=snapshot.amount,
        currency=snapshot.currency.value,
        description=snapshot.description,
        extra_metadata=snapshot.metadata,
        status=snapshot.status.value,
        idempotency_key=f"test-{uuid4()}",
        request_fingerprint="a" * 64,
        webhook_url=snapshot.webhook_url,
        created_at=NOW,
        processed_at=snapshot.processed_at,
        provider_reference=snapshot.provider_reference,
        webhook_event_id=snapshot.webhook_event_id,
        webhook_payload=snapshot.webhook_payload,
        webhook_delivered_at=None,
        processing_state=ProcessingState.RUNNING.value,
        attempt_count=attempt,
        expected_message_id=snapshot.expected_message_id,
        lease_token=uuid4(),
        lease_until=NOW,
        last_error_code=None,
        last_error_stage=None,
    )

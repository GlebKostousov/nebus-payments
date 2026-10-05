"""Платёж и минимальное состояние возобновляемой обработки."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import JsonValue
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Numeric,
    SmallInteger,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base
from app.core.types import Currency, PaymentStatus, PaymentView, ProcessingState


class Payment(Base):
    """Финансовый результат не меняется из-за недоставленного webhook."""

    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="positive_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="currency_values"),
        CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name="status_values"
        ),
        CheckConstraint(
            "processing_state IN ('ready', 'running', 'completed', 'dead_lettered')",
            name="processing_state_values",
        ),
        CheckConstraint("attempt_count BETWEEN 0 AND 3", name="attempt_budget"),
        CheckConstraint(
            "processing_state != 'ready' OR attempt_count < 3", name="ready_budget"
        ),
        CheckConstraint(
            "(processing_state = 'running' AND lease_token IS NOT NULL "
            "AND lease_until IS NOT NULL) OR "
            "(processing_state != 'running' AND lease_token IS NULL "
            "AND lease_until IS NULL)",
            name="lease_pair",
        ),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL "
            "AND webhook_payload IS NULL AND webhook_event_id IS NULL "
            "AND provider_reference IS NULL) OR "
            "(status != 'pending' AND processed_at IS NOT NULL "
            "AND webhook_payload IS NOT NULL AND webhook_event_id IS NOT NULL "
            "AND provider_reference IS NOT NULL)",
            name="result_complete",
        ),
        CheckConstraint(
            "(processing_state = 'completed' AND webhook_delivered_at IS NOT NULL "
            "AND status != 'pending') OR "
            "(processing_state != 'completed' AND webhook_delivered_at IS NULL)",
            name="delivery_complete",
        ),
        Index(
            "ix_payments_expired_work",
            "lease_until",
            "id",
            postgresql_where=text("processing_state = 'running'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text)
    extra_metadata: Mapped[dict[str, JsonValue]] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(Text, server_default="pending")
    idempotency_key: Mapped[str] = mapped_column(Text, unique=True)
    request_fingerprint: Mapped[str] = mapped_column(Text)
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_reference: Mapped[str | None] = mapped_column(Text)
    webhook_event_id: Mapped[UUID | None] = mapped_column(unique=True)
    webhook_payload: Mapped[str | None] = mapped_column(Text)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    processing_state: Mapped[str] = mapped_column(Text, server_default="ready")
    attempt_count: Mapped[int] = mapped_column(SmallInteger, server_default="0")
    expected_message_id: Mapped[UUID] = mapped_column()
    lease_token: Mapped[UUID | None] = mapped_column()
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(Text)
    last_error_stage: Mapped[str | None] = mapped_column(Text)

    def view(self) -> PaymentView:
        """Отделить возвращаемые данные от времени жизни сессии."""
        return PaymentView(
            id=self.id,
            amount=self.amount,
            currency=Currency(self.currency),
            description=self.description,
            metadata=dict(self.extra_metadata),
            status=PaymentStatus(self.status),
            webhook_url=self.webhook_url,
            created_at=self.created_at,
            processed_at=self.processed_at,
            provider_reference=self.provider_reference,
            webhook_event_id=self.webhook_event_id,
            webhook_payload=self.webhook_payload,
            webhook_delivered_at=self.webhook_delivered_at,
            processing_state=ProcessingState(self.processing_state),
            attempt_count=self.attempt_count,
            expected_message_id=self.expected_message_id,
            last_error_code=self.last_error_code,
            last_error_stage=self.last_error_stage,
        )

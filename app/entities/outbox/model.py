"""Первичная публикация, продолжения и DLQ используют одну durable-таблицу."""

from datetime import datetime
from typing import Self
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.db import Base
from app.core.messages import PaymentJob
from app.core.types import Destination


class Outbox(Base):
    """Одна строка — одно сообщение с постоянным id при перепубликации."""

    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint(
            "destination IN ('processing', 'dead_letter')", name="destination_values"
        ),
        CheckConstraint("publish_attempt_count >= 0", name="attempts_nonnegative"),
        CheckConstraint(
            "(lease_token IS NULL AND lease_until IS NULL) OR "
            "(lease_token IS NOT NULL AND lease_until IS NOT NULL)",
            name="lease_pair",
        ),
        CheckConstraint(
            "published_at IS NULL OR lease_token IS NULL", name="published_unlocked"
        ),
        Index(
            "ix_outbox_available",
            "available_at",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index("ix_outbox_payment_id", "payment_id"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    destination: Mapped[str] = mapped_column(Text)
    payload: Mapped[str] = mapped_column(Text)
    deduplication_key: Mapped[str] = mapped_column(Text, unique=True)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempt_count: Mapped[int] = mapped_column(Integer, server_default="0")
    lease_token: Mapped[UUID | None] = mapped_column()
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )

    @classmethod
    def processing(
        cls,
        *,
        payment_id: UUID,
        message_id: UUID,
        attempt: int,
        available_at: datetime,
    ) -> Self:
        """Создать поручение конкретной попытке, не дублируя номер в Rabbit payload."""
        job = PaymentJob(message_id=message_id, payment_id=payment_id)
        return cls(
            id=message_id,
            payment_id=payment_id,
            destination=Destination.PROCESSING.value,
            payload=job.model_dump_json(),
            deduplication_key=f"process:{payment_id}:{attempt}",
            available_at=available_at,
        )

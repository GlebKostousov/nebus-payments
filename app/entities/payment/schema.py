"""Тело запроса на оплату и отпечаток для повторного POST."""

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    JsonValue,
    field_validator,
)

from app.core.types import Currency, PaymentStatus, PaymentView, ProcessingState

_MAX_METADATA_BYTES = 8192


class CreatePayment(BaseModel):
    """Запрос на оплату; деньги передаются десятичной строкой."""

    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)
    currency: Currency
    description: str = Field(min_length=1, max_length=1024)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("amount", mode="before", json_schema_input_type=str)
    @classmethod
    def require_decimal_string(cls, value: object) -> object:
        """Не принимать двоичный float под видом точной денежной суммы."""
        if not isinstance(value, str | Decimal):
            message = "Передайте сумму десятичной строкой, например 1500.00"
            # Pydantic превращает ValueError валидатора в 422. TypeError выходит наружу.
            raise ValueError(message)  # noqa: TRY004
        return value

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        """Привести уже проверенную точную сумму к двум знакам без округления."""
        return value.quantize(Decimal("0.01"))

    @field_validator("metadata")
    @classmethod
    def bound_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        """Ограничить JSON и запретить нечисловые NaN/Infinity."""
        encoded = json.dumps(value, allow_nan=False, ensure_ascii=False)
        if len(encoded.encode("utf-8")) > _MAX_METADATA_BYTES:
            message = "Метаданные не должны превышать 8 КиБ"
            raise ValueError(message)
        return value

    def fingerprint(self) -> str:
        """Сравнивать содержимое, а не порядок JSON-ключей и запись суммы."""
        canonical = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class PaymentAccepted(BaseModel):
    """Стабильная квитанция принятия, одинаковая при повторе POST."""

    payment_id: UUID
    status: PaymentStatus = PaymentStatus.PENDING
    created_at: datetime


class PaymentRead(BaseModel):
    """Финансовый результат и состояние обработки без внутренних ключей."""

    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    processing_state: ProcessingState
    attempt_count: int
    last_error_code: str | None
    last_error_stage: str | None

    @classmethod
    def from_view(cls, payment: PaymentView) -> Self:
        """Сформировать публичное представление без обращения к ORM."""
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
            processing_state=payment.processing_state,
            attempt_count=payment.attempt_count,
            last_error_code=payment.last_error_code,
            last_error_stage=payment.last_error_stage,
        )

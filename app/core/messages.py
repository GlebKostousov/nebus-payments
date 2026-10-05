"""Тела сообщения в очередь и исходящего webhook."""

from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from .types import Currency, FailureStage, PaymentStatus


class PaymentJob(BaseModel):
    """Поручение продолжить платёж; счётчик попыток берётся только из БД."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    event_type: Literal["payment.process"] = "payment.process"
    message_id: UUID
    payment_id: UUID


class DeadLetter(BaseModel):
    """Причина окончания автоматических попыток."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    event_type: Literal["payment.dead_lettered"] = "payment.dead_lettered"
    message_id: UUID
    original_message_id: UUID
    payment_id: UUID
    attempt_count: int = Field(ge=1, le=3)
    error_code: str
    error_stage: FailureStage
    occurred_at: AwareDatetime


class WebhookEvent(BaseModel):
    """Неизменяемый бизнес-payload для всех повторов одного уведомления."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    event_type: Literal["payment.completed"] = "payment.completed"
    event_id: UUID
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: AwareDatetime

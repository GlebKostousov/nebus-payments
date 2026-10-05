"""Значения, которые не зависят от HTTP, брокера и ORM."""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID

from pydantic import JsonValue

MAX_ATTEMPTS = 3


class Currency(StrEnum):
    """Поддерживаемые валюты платежа."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    """Финансовый результат, не состояние доставки webhook."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ProcessingState(StrEnum):
    """Состояние исполнения; время ожидания хранится только в outbox."""

    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    DEAD_LETTERED = "dead_lettered"


class Destination(StrEnum):
    """Назначение сообщения без привязки к именам exchange."""

    PROCESSING = "processing"
    DEAD_LETTER = "dead_letter"


class FailureStage(StrEnum):
    """Этап последней неудачной попытки."""

    PAYMENT = "payment"
    WEBHOOK = "webhook"
    WORKER = "worker"


@dataclass(frozen=True, slots=True)
class Failure:
    """Безопасное описание сбоя для сохранения и DLQ."""

    code: str
    stage: FailureStage
    retry_after: datetime | None = None
    permanent: bool = False


@dataclass(frozen=True, slots=True)
class PaymentResult:
    """Окончательный ответ шлюза, в том числе отказ в оплате."""

    status: PaymentStatus
    provider_reference: str

    def __post_init__(self) -> None:
        """Не позволить шлюзу представить неизвестный исход как результат."""
        if self.status is PaymentStatus.PENDING:
            message = "Результат шлюза не может быть pending"
            raise ValueError(message)


@dataclass(frozen=True, slots=True)
class PaymentView:
    """Снимок платежа без сессии ORM и ленивых загрузок."""

    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    provider_reference: str | None
    webhook_event_id: UUID | None
    webhook_payload: str | None
    webhook_delivered_at: datetime | None
    processing_state: ProcessingState
    attempt_count: int
    expected_message_id: UUID
    last_error_code: str | None
    last_error_stage: str | None


@dataclass(frozen=True, slots=True)
class Work:
    """Снимок захваченной попытки с токеном её владельца."""

    payment: PaymentView
    lease_token: UUID


@dataclass(frozen=True, slots=True)
class Publication:
    """Захваченная запись outbox, готовая к одной отправке."""

    id: UUID
    payment_id: UUID
    destination: Destination
    payload: str
    lease_token: UUID
    attempt_count: int


def utcnow() -> datetime:
    """Вернуть текущее время UTC для внешних протоколов."""
    return datetime.now(UTC)

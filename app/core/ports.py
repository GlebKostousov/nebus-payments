"""Протоколы шлюза, публикации и доставки webhook."""

from typing import Protocol
from uuid import UUID

from .messages import PaymentJob
from .types import Failure, PaymentResult, Publication, Work


class PaymentGateway(Protocol):
    """Один идемпотентный вызов платёжного провайдера."""

    async def charge(self, payment_id: UUID) -> PaymentResult:
        """Вернуть окончательный результат или поднять AttemptError."""
        ...


class WebhookGateway(Protocol):
    """Одна попытка доставки уже сформированного уведомления."""

    async def send(self, *, url: str, payload: str, event_id: UUID) -> None:
        """Доставить payload или поднять AttemptError."""
        ...


class PaymentStatePort(Protocol):
    """Атомарные переходы между сохранёнными этапами платежа."""

    async def claim(self, job: PaymentJob) -> Work | None:
        """Начать попытку; дубль или устаревшее сообщение возвращает None."""
        ...

    async def save_result(self, work: Work, result: PaymentResult) -> Work | None:
        """Сохранить финансовый результат и webhook, пока захват принадлежит нам."""
        ...

    async def complete(self, work: Work) -> bool:
        """Зафиксировать доставку webhook и завершение обработки."""
        ...

    async def fail(self, work: Work, failure: Failure) -> bool:
        """Атомарно записать продолжение или DLQ; False означает потерю захвата."""
        ...


class Publisher(Protocol):
    """Подтверждённая публикация одной записи outbox."""

    async def publish(self, publication: Publication) -> None:
        """Завершиться только после подтверждения брокера и проверки маршрутизации."""
        ...

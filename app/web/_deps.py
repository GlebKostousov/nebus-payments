"""Зависимости маршрута: сервис платежей и ключ идемпотентности."""

import re
from dataclasses import dataclass
from typing import Annotated, Protocol
from uuid import UUID

from fastapi import Depends, Request, status

from app.core.errors import InvariantError, ServiceError
from app.core.types import PaymentView
from app.entities.payment.schema import CreatePayment, PaymentAccepted

KEY_PATTERN = re.compile(r"[!-~]{1,128}")


class PaymentApi(Protocol):
    """Методы сервиса, которые вызывают маршруты."""

    async def create(
        self, request: CreatePayment, *, idempotency_key: str
    ) -> PaymentAccepted:
        """Принять запрос или вернуть первоначальную квитанцию."""
        ...

    async def get(self, payment_id: UUID) -> PaymentView:
        """Прочитать актуальное состояние."""
        ...

    async def ping(self) -> None:
        """Проверить готовность хранилища."""
        ...


@dataclass(frozen=True, slots=True)
class ApplicationContext:
    """Зависимости одного экземпляра приложения."""

    payments: PaymentApi


class InvalidIdempotencyKeyError(ServiceError):
    """Отсутствующий, повторённый или неоднозначный заголовок ключа."""

    code = "invalid_idempotency_key"
    http_status = status.HTTP_422_UNPROCESSABLE_CONTENT
    public_message = "Нужен один Idempotency-Key: от 1 до 128 символов, без пробелов"


def get_payments(request: Request) -> PaymentApi:
    """Проверить тип на единственной динамической границе Starlette state."""
    context: object = request.app.state.context
    if not isinstance(context, ApplicationContext):
        message = "Приложение не получило ApplicationContext"
        raise InvariantError(message)
    return context.payments


def get_idempotency_key(request: Request) -> str:
    """Заголовок должен прийти один раз."""
    values = request.headers.getlist("Idempotency-Key")
    if len(values) != 1 or KEY_PATTERN.fullmatch(values[0]) is None:
        raise InvalidIdempotencyKeyError
    return values[0]


type PaymentsDependency = Annotated[PaymentApi, Depends(get_payments)]
type IdempotencyDependency = Annotated[str, Depends(get_idempotency_key)]

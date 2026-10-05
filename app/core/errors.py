"""Ошибки для клиента и внутренние сбои."""

from typing import ClassVar

from fastapi import status

from .types import Failure


class ServiceError(Exception):
    """Ошибка с текстом, который можно отдать клиенту."""

    code: ClassVar[str] = "internal_error"
    http_status: ClassVar[int] = status.HTTP_500_INTERNAL_SERVER_ERROR
    public_message: ClassVar[str] = "Не получилось обработать запрос"

    def __init__(self, internal_message: str = "") -> None:
        """Сохранить диагностику отдельно от публичного сообщения."""
        self.internal_message = internal_message
        super().__init__(internal_message or self.code)


class IdempotencyConflictError(ServiceError):
    """Ключ уже использован с другим содержимым запроса."""

    code = "idempotency_conflict"
    http_status = status.HTTP_409_CONFLICT
    public_message = "Этот Idempotency-Key уже использован с другим запросом"


class PaymentNotFoundError(ServiceError):
    """Платёж не найден."""

    code = "payment_not_found"
    http_status = status.HTTP_404_NOT_FOUND
    public_message = "Платёж не найден"


class WebhookDestinationError(ServiceError):
    """Адрес не входит в разрешённые получатели webhook."""

    code = "webhook_destination_forbidden"
    http_status = status.HTTP_422_UNPROCESSABLE_CONTENT
    public_message = "Этот адрес webhook не разрешён"


class MessageContractError(ServiceError):
    """Сообщение брокера не соответствует известной работе."""

    code = "message_contract_invalid"


class InvariantError(ServiceError):
    """Строка в базе не сходится с правилами платежа."""

    code = "state_invariant_violated"


class AttemptError(Exception):
    """Сбой шлюза или webhook. Повторять ли попытку, решает сценарий."""

    def __init__(self, failure: Failure) -> None:
        """Сохранить код и этап сбоя, без тела чужого ответа."""
        self.failure = failure
        super().__init__(failure.code)


class PublicationError(Exception):
    """Брокер не подтвердил успешно маршрутизированную публикацию."""

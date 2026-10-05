"""Локальный получатель для проверки подписи, повторов и дедупликации."""

import hashlib
import hmac
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from app.core.config import Settings
from app.core.messages import WebhookEvent
from fastapi import FastAPI, Request, Response, status
from pydantic import BaseModel, ConfigDict, ValidationError

_MAX_RECEIPTS = 1000
_FLAKY_FAILURES_BEFORE_SUCCESS = 3
_TIMESTAMP_MAX_DIGITS = 12
_SIGNATURE_WINDOW_SECONDS = 300
_MAX_BODY_BYTES = 16384


class ReceivedEvent(BaseModel):
    """Наблюдаемый результат доставки, без полного тела и секретов."""

    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    payment_id: UUID
    requests: int
    applied: bool
    last_status_code: int


class ReceivedEvents(BaseModel):
    """Ответ диагностического endpoint демонстрационного сервера."""

    model_config = ConfigDict(extra="forbid")
    items: list[ReceivedEvent]


@dataclass(slots=True)
class Receipt:
    """Состояние одного события только на время демонстрации."""

    event: WebhookEvent
    body: bytes
    requests: int = 0
    applied: bool = False
    last_status_code: int = status.HTTP_503_SERVICE_UNAVAILABLE

    def snapshot(self) -> ReceivedEvent:
        """Не отдавать клиенту изменяемое внутреннее состояние."""
        return ReceivedEvent(
            event_id=self.event.event_id,
            payment_id=self.event.payment_id,
            requests=self.requests,
            applied=self.applied,
            last_status_code=self.last_status_code,
        )


@dataclass(slots=True)
class Receiver:
    """Небольшой in-memory стенд, не пример production-хранилища."""

    receipts: dict[UUID, Receipt] = field(default_factory=dict)

    def accept(
        self,
        event: WebhookEvent,
        body: bytes,
        mode: Literal["success", "flaky", "fail"],
    ) -> int:
        """Повторно не применять событие и не принимать другой payload под его ID."""
        receipt = self.receipts.get(event.event_id)
        if receipt is None:
            return self._store_or_reject(event, body, mode)
        return self._apply(receipt, body, mode)

    def _store_or_reject(
        self,
        event: WebhookEvent,
        body: bytes,
        mode: Literal["success", "flaky", "fail"],
    ) -> int:
        """Сохранить первое тело события, пока стенд не переполнен."""
        if len(self.receipts) >= _MAX_RECEIPTS:
            return status.HTTP_503_SERVICE_UNAVAILABLE
        receipt = Receipt(event, body)
        self.receipts[event.event_id] = receipt
        return self._apply(receipt, body, mode)

    def _apply(
        self, receipt: Receipt, body: bytes, mode: Literal["success", "flaky", "fail"]
    ) -> int:
        """Повторно не применять событие и не менять его тело."""
        if receipt.body != body:
            return status.HTTP_409_CONFLICT
        receipt.requests += 1
        if _should_fail(mode, receipt):
            receipt.last_status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return receipt.last_status_code
        receipt.applied = True
        receipt.last_status_code = status.HTTP_204_NO_CONTENT
        return receipt.last_status_code


def valid_signature(request: Request, body: bytes, *, secret: str) -> bool:
    """Проверить подпись точных байтов и окно в пять минут до разбора JSON."""
    timestamp = request.headers.get("Webhook-Timestamp", "")
    signature = request.headers.get("Webhook-Signature", "")
    if not _timestamp_ok(timestamp):
        return False
    if abs(datetime.now(UTC).timestamp() - int(timestamp)) > _SIGNATURE_WINDOW_SECONDS:
        return False
    expected = hmac.new(
        secret.encode("utf-8"),
        timestamp.encode("ascii") + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature.encode("utf-8"), f"v1={expected}".encode())


def _timestamp_ok(timestamp: str) -> bool:
    """Принять только короткую десятичную метку времени."""
    if not timestamp.isascii() or not timestamp.isdecimal():
        return False
    return len(timestamp) <= _TIMESTAMP_MAX_DIGITS


def create_app(settings: Settings | None = None) -> FastAPI:
    """Создать изолированный стенд; его состояние пропадает после перезапуска."""
    resolved = settings or Settings()
    receiver = Receiver()
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    _mount(app, receiver, resolved)
    return app


def _mount(app: FastAPI, receiver: Receiver, settings: Settings) -> None:
    """Подключить приём webhook и просмотр попыток."""
    secret = settings.webhook_signing_secret.get_secret_value()
    api_key = settings.api_key.get_secret_value().encode("utf-8")
    app.add_api_route(
        "/hooks/{mode}",
        _webhook(receiver, secret),
        methods=["POST"],
    )
    app.add_api_route(
        "/events",
        _events(receiver, api_key),
        methods=["GET"],
        response_model=ReceivedEvents,
    )


def _webhook(
    receiver: Receiver, secret: str
) -> Callable[[Literal["success", "flaky", "fail"], Request], Awaitable[Response]]:
    """Собрать обработчик одного режима стенда."""

    async def webhook(
        mode: Literal["success", "flaky", "fail"], request: Request
    ) -> Response:
        """Принять подписанное событие или воспроизвести временный отказ."""
        return await _handle_webhook(mode, request, receiver=receiver, secret=secret)

    return webhook


def _events(
    receiver: Receiver, api_key: bytes
) -> Callable[[Request], Awaitable[ReceivedEvents | Response]]:
    """Собрать защищённый просмотр попыток."""

    async def events(request: Request) -> ReceivedEvents | Response:
        """Показать попытки только владельцу статического ключа стенда."""
        if not _demo_key_matches(request, api_key):
            return Response(status_code=status.HTTP_401_UNAUTHORIZED)
        return ReceivedEvents(
            items=[receipt.snapshot() for receipt in receiver.receipts.values()]
        )

    return events


async def _handle_webhook(
    mode: Literal["success", "flaky", "fail"],
    request: Request,
    *,
    receiver: Receiver,
    secret: str,
) -> Response:
    """Принять одно уведомление стенда."""
    payload = await _read_body(request)
    if payload is None:
        return Response(status_code=status.HTTP_413_CONTENT_TOO_LARGE)
    return _accept(mode, request, payload, receiver, secret)


def _accept(
    mode: Literal["success", "flaky", "fail"],
    request: Request,
    payload: bytes,
    receiver: Receiver,
    secret: str,
) -> Response:
    """Проверить событие и записать попытку."""
    event = _event(request, payload, secret)
    if isinstance(event, Response):
        return event
    return Response(status_code=receiver.accept(event, payload, mode))


async def _read_body(request: Request) -> bytes | None:
    """Прочитать тело webhook в пределах лимита."""
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > _MAX_BODY_BYTES:
            return None
        body.extend(chunk)
    return bytes(body)


def _event(request: Request, payload: bytes, secret: str) -> WebhookEvent | Response:
    """Проверить подпись и JSON до изменения состояния стенда."""
    if not valid_signature(request, payload, secret=secret):
        return Response(status_code=status.HTTP_401_UNAUTHORIZED)
    return _match_event_id(request, _parse_event(payload))


def _match_event_id(
    request: Request, parsed: WebhookEvent | Response
) -> WebhookEvent | Response:
    """Сверить Webhook-Id с идентификатором внутри тела."""
    if isinstance(parsed, Response):
        return parsed
    if request.headers.get("Webhook-Id") != str(parsed.event_id):
        return Response(status_code=status.HTTP_400_BAD_REQUEST)
    return parsed


def _parse_event(payload: bytes) -> WebhookEvent | Response:
    """Разобрать JSON уведомления."""
    try:
        return WebhookEvent.model_validate_json(payload)
    except ValidationError:
        return Response(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT)


def _demo_key_matches(request: Request, api_key: bytes) -> bool:
    """Сверить один ключ диагностического endpoint."""
    supplied = request.headers.getlist("X-API-Key")
    if len(supplied) != 1:
        return False
    return hmac.compare_digest(supplied[0].encode("utf-8"), api_key)


def _should_fail(mode: Literal["success", "flaky", "fail"], receipt: Receipt) -> bool:
    """Решить, должен ли стенд ещё ответить ошибкой."""
    if receipt.applied:
        return False
    if mode == "fail":
        return True
    return mode == "flaky" and receipt.requests < _FLAKY_FAILURES_BEFORE_SUCCESS

"""API-ключ проверяется до разбора тела, в том числе на служебных маршрутах."""

import asyncio
import hmac
from uuid import UUID, uuid4

from fastapi import status
from starlette.datastructures import Headers, MutableHeaders
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_READ_TIMEOUT_SECONDS = 10


class ApiGuard:
    """Ограничивает размер входа и не отдаёт секреты в ошибках валидации."""

    def __init__(self, app: ASGIApp, *, api_key: str, max_bytes: int) -> None:
        """Передать секрет один раз при сборке приложения."""
        self._app = app
        self._key = api_key.encode("utf-8")
        self._max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Защитить все HTTP-маршруты; lifespan передать без изменений."""
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        await self._handle_http(scope, receive, send)

    async def _handle_http(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Проверить ключ и передать приложению ограниченное тело."""
        headers = Headers(scope=scope)
        request_id = _request_id(headers.get("X-Request-ID"))
        scope.setdefault("state", {})["request_id"] = request_id
        send_with_id = _sender(send, request_id)
        if not _authorized(headers, self._key):
            await _json_error(scope, receive, send_with_id, _UNAUTHORIZED)
            return
        body = await self._limited_body(scope, receive, send_with_id)
        if body is None:
            return
        await self._app(scope, _once(receive, body), send_with_id)

    async def _limited_body(
        self, scope: Scope, receive: Receive, send: Send
    ) -> bytes | None:
        """Прочитать тело либо ответить ошибкой размера или таймаута."""
        try:
            async with asyncio.timeout(_READ_TIMEOUT_SECONDS):
                return await self._read_body(receive)
        except _BodyTooLargeError:
            await _json_error(scope, receive, send, _TOO_LARGE)
        except TimeoutError:
            await _json_error(scope, receive, send, _TIMEOUT)
        return None

    async def _read_body(self, receive: Receive) -> bytes | None:
        """Проверять фактический размер, а не доверять Content-Length."""
        body = bytearray()
        try:
            while not self._consume(body, await receive()):
                pass
        except _DisconnectedError:
            return None
        return bytes(body)

    def _consume(self, body: bytearray, message: Message) -> bool:
        """Добавить один chunk и сообщить, закончилось ли тело."""
        if message["type"] == "http.disconnect":
            raise _DisconnectedError
        chunk = message.get("body", b"")
        if len(body) + len(chunk) > self._max_bytes:
            raise _BodyTooLargeError
        body.extend(chunk)
        return not message.get("more_body", False)


class _BodyTooLargeError(Exception):
    """Внутренний сигнал прекращения чтения тела."""


class _DisconnectedError(Exception):
    """Клиент закрыл соединение до конца тела."""


_UNAUTHORIZED = (
    {"code": "unauthorized", "message": "Нужен верный API-ключ"},
    status.HTTP_401_UNAUTHORIZED,
)
_TOO_LARGE = (
    {"code": "request_too_large", "message": "Тело запроса слишком большое"},
    status.HTTP_413_CONTENT_TOO_LARGE,
)
_TIMEOUT = (
    {"code": "request_timeout", "message": "Тело запроса читалось слишком долго"},
    status.HTTP_408_REQUEST_TIMEOUT,
)


def _authorized(headers: Headers, key: bytes) -> bool:
    """Сверить ровно один API-ключ без утечки сравнения."""
    keys = headers.getlist("X-API-Key")
    if len(keys) != 1:
        return False
    return hmac.compare_digest(keys[0].encode("utf-8"), key)


def _sender(send: Send, request_id: str) -> Send:
    """Добавить корреляцию к старту ответа."""

    async def send_with_id(message: Message) -> None:
        """Дописать X-Request-ID, не копируя остальные заголовки клиента."""
        if message["type"] == "http.response.start":
            MutableHeaders(scope=message)["X-Request-ID"] = request_id
        await send(message)

    return send_with_id


def _once(receive: Receive, body: bytes) -> Receive:
    """Отдать приложению уже прочитанное тело один раз."""
    sent = False

    async def buffered_receive() -> Message:
        """Вернуть сохранённое тело, а дальше читать исходный канал."""
        nonlocal sent
        if sent:
            return await receive()
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return buffered_receive


async def _json_error(
    scope: Scope,
    receive: Receive,
    send: Send,
    payload: tuple[dict[str, str], int],
) -> None:
    """Ответить безопасной JSON-ошибкой до разбора тела."""
    body, status_code = payload
    await JSONResponse(body, status_code=status_code)(scope, receive, send)


def _request_id(value: str | None) -> str:
    """Принять X-Request-ID только как UUID."""
    try:
        return str(UUID(value)) if value is not None else str(uuid4())
    except ValueError:
        return str(uuid4())

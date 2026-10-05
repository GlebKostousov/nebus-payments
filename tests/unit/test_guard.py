"""Аутентификация раньше валидации и ограничение фактического размера запроса."""

from collections.abc import AsyncIterator
from uuid import UUID

import httpx
import pytest
from app.web._deps import InvalidIdempotencyKeyError, get_idempotency_key
from app.web.guard import ApiGuard
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import Receive, Scope, Send

API_KEY = "test-api-key-" * 4


async def endpoint(scope: Scope, receive: Receive, send: Send) -> None:
    """Минимальный ASGI endpoint не подменяет проверки реальных маршрутов API."""
    body = await Request(scope, receive).body()
    await JSONResponse({"bytes": len(body)})(scope, receive, send)


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
async def test_authentication_precedes_body(headers: dict[str, str]) -> None:
    """При неверном ключе тело даже не читается."""
    consumed = False

    async def content() -> AsyncIterator[bytes]:
        """Зафиксировать нежелательное чтение тела неавторизованного запроса."""
        nonlocal consumed
        consumed = True
        yield b"not-json"

    app = ApiGuard(endpoint, api_key=API_KEY, max_bytes=1024)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v1/payments", headers=headers, content=content()
        )
    assert response.status_code == 401
    assert not consumed
    assert UUID(response.headers["X-Request-ID"])


@pytest.mark.parametrize(
    "path", ["/health/ready", "/openapi.json", "/docs", "/unknown"]
)
async def test_service_routes_are_not_exceptions(path: str) -> None:
    """Служебные и несуществующие маршруты также требуют X-API-Key."""
    app = ApiGuard(endpoint, api_key=API_KEY, max_bytes=1024)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get(path)
    assert response.status_code == 401


async def test_bounded_body_passes_through_unchanged() -> None:
    """Middleware не теряет тело, уже проверив его размер."""
    app = ApiGuard(endpoint, api_key=API_KEY, max_bytes=1024)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/", content=b"example", headers={"X-API-Key": API_KEY}
        )
    assert response.status_code == 200
    assert response.json() == {"bytes": 7}


async def test_actual_stream_size_beats_content_length() -> None:
    """Лимит размера считается по прочитанным байтам, не по Content-Length."""

    async def content() -> AsyncIterator[bytes]:
        """Выдать тело двумя порциями, каждая по отдельности меньше лимита."""
        yield b"a" * 800
        yield b"b" * 800

    app = ApiGuard(endpoint, api_key=API_KEY, max_bytes=1024)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/",
            content=content(),
            headers={
                "X-API-Key": API_KEY,
                "Content-Length": "1",
            },
        )
    assert response.status_code == 413


async def test_duplicate_api_key_is_rejected() -> None:
    """Второй заголовок ключа не должен подтверждать первый, даже верный."""
    app = ApiGuard(endpoint, api_key=API_KEY, max_bytes=1024)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.send(
            client.build_request(
                "GET",
                "/",
                headers=[("X-API-Key", API_KEY), ("X-API-Key", API_KEY)],
            )
        )
    assert response.status_code == 401


async def test_query_string_is_not_a_credential() -> None:
    """Ключ в URL не аутентифицирует запрос."""
    app = ApiGuard(endpoint, api_key=API_KEY, max_bytes=1024)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get("/", params={"X-API-Key": API_KEY})
    assert response.status_code == 401


async def test_request_id_cannot_inject_headers() -> None:
    """Произвольный X-Request-ID заменяется на UUID и не попадает в ответ как есть."""
    app = ApiGuard(endpoint, api_key=API_KEY, max_bytes=1024)
    injected = "not-a-uuid\r\nSet-Cookie: session=stolen"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get(
            "/", headers={"X-API-Key": API_KEY, "X-Request-ID": injected}
        )
    assert response.status_code == 200
    assert response.headers["X-Request-ID"] != injected
    assert "\n" not in response.headers["X-Request-ID"]
    assert UUID(response.headers["X-Request-ID"])


@pytest.mark.parametrize(
    "values",
    [
        [],
        [""],
        ["has space"],
        ["x" * 129],
        ["one", "two"],
        ["key\nSet-Cookie"],
        ["ключ"],
    ],
)
def test_idempotency_key_has_one_unambiguous_value(values: list[str]) -> None:
    """Отсутствующий и повторённый заголовок не создаёт неоднозначную операцию."""
    request = Request(
        {
            "type": "http",
            "headers": [(b"idempotency-key", value.encode()) for value in values],
        }
    )
    with pytest.raises(InvalidIdempotencyKeyError):
        get_idempotency_key(request)


def test_valid_idempotency_key() -> None:
    """Ключ сохраняется как задан, без неожиданного обрезания или lower-case."""
    request = Request({"type": "http", "headers": [(b"idempotency-key", b"Order-123")]})
    assert get_idempotency_key(request) == "Order-123"

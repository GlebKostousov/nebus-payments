"""Одна HTTP-попытка, неизменный payload, подпись и безопасный отказ."""

import asyncio
import hashlib
import hmac
from collections.abc import AsyncIterator
from uuid import uuid4

import httpx
import pytest
from app.core.errors import AttemptError, WebhookDestinationError
from app.core.webhook_policy import WebhookPolicy
from app.gateways.webhook import HttpWebhookGateway

from tests.factories import NOW

SECRET = "test-signing-secret-" * 3
URL = "https://receiver.example/hook"


def policy() -> WebhookPolicy:
    """Разрешить ровно один тестовый origin без реальной сети."""
    return WebhookPolicy.build(("https://receiver.example",))


@pytest.mark.parametrize(
    "url",
    [
        "http://receiver.example/hook",
        "https://other.example/hook",
        "https://receiver.example.evil/hook",
        "https://receiver.example:8443/hook",
        "https://user:pass@receiver.example/hook",
        "https://receiver.example/hook#fragment",
        "http://169.254.169.254/latest/meta-data",
        "https://127.0.0.1/hook",
        "https://10.1.2.3/hook",
        "https://0.0.0.0/hook",
        "https://[::1]/hook",
        "https://[fd00::1]/hook",
        "file:///etc/passwd",
    ],
)
def test_forbidden_destination(url: str) -> None:
    """В allowlist ищется целый origin, не кусок имени хоста."""
    with pytest.raises(WebhookDestinationError):
        policy().check(url)


@pytest.mark.parametrize(
    "origin",
    ["https://127.0.0.1", "https://169.254.169.254", "https://[::1]"],
)
def test_allowlist_cannot_permit_private_literal(origin: str) -> None:
    """Loopback и link-local закрыты и когда origin перечислен в allowlist."""
    with pytest.raises(WebhookDestinationError):
        WebhookPolicy.build((origin,))


def test_demo_origin_is_explicit() -> None:
    """Demo-режим открывает только origin из списка, не всю внутреннюю сеть."""
    value = WebhookPolicy.build(("http://webhook:8081",), allow_development=True)
    value.check("http://webhook:8081/hooks/flaky")
    with pytest.raises(WebhookDestinationError):
        value.check("http://127.0.0.1:8081/hooks/flaky")


@pytest.mark.parametrize("status_code", [200, 202, 204, 299])
async def test_success_and_signature(status_code: int) -> None:
    """Пустой 204 успешен; подпись считается от точных отправленных байтов."""
    received: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        """Запомнить полный запрос и вернуть разрешённый статус."""
        received.append(request)
        return httpx.Response(status_code)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        gateway = HttpWebhookGateway(
            client, policy=policy(), signing_secret=SECRET, now=lambda: NOW
        )
        event_id = uuid4()
        await gateway.send(url=URL, payload='{"amount":"10.00"}', event_id=event_id)
    assert len(received) == 1
    outgoing = received[0]
    assert outgoing.method == "POST"
    assert outgoing.content == b'{"amount":"10.00"}'
    assert outgoing.headers["Webhook-Id"] == str(event_id)
    signed = outgoing.headers["Webhook-Timestamp"].encode() + b"." + outgoing.content
    digest = hmac.new(SECRET.encode(), signed, hashlib.sha256).hexdigest()
    assert outgoing.headers["Webhook-Signature"] == f"v1={digest}"
    assert "X-API-Key" not in outgoing.headers


@pytest.mark.parametrize("status_code", [301, 400, 404, 429, 500, 503])
async def test_http_failure_does_not_retry_itself(status_code: int) -> None:
    """Любой не-2xx возвращается workflow как единственный сбой."""
    calls = 0

    def respond(_request: httpx.Request) -> httpx.Response:
        """Редирект на другой адрес тоже не должен быть выполнен."""
        nonlocal calls
        calls += 1
        return httpx.Response(status_code, headers={"Location": "http://127.0.0.1/"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        gateway = HttpWebhookGateway(client, policy=policy(), signing_secret=SECRET)
        with pytest.raises(AttemptError) as caught:
            await gateway.send(url=URL, payload="{}", event_id=uuid4())
    assert calls == 1
    assert caught.value.failure.code == f"webhook_http_{status_code}"


async def test_transport_failure() -> None:
    """Техническая ошибка преобразуется с сохранением причины, не тела ответа."""

    def respond(request: httpx.Request) -> httpx.Response:
        """Эмулировать ошибку подключения."""
        # request нужен конструктору ConnectError.
        message = "private diagnostics"
        raise httpx.ConnectError(message, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        gateway = HttpWebhookGateway(client, policy=policy(), signing_secret=SECRET)
        with pytest.raises(AttemptError) as caught:
            await gateway.send(url=URL, payload="{}", event_id=uuid4())
    assert caught.value.failure.code == "webhook_transport_error"
    assert isinstance(caught.value.__cause__, httpx.ConnectError)
    assert "private" not in str(caught.value)


async def test_whole_request_deadline() -> None:
    """Общий дедлайн действует даже при транспорте без собственных таймаутов."""

    async def respond(_request: httpx.Request) -> httpx.Response:
        """Зависнуть до отмены внешним deadline."""
        await asyncio.Event().wait()
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        gateway = HttpWebhookGateway(
            client, policy=policy(), signing_secret=SECRET, total_timeout_seconds=0.01
        )
        with pytest.raises(AttemptError) as caught:
            await gateway.send(url=URL, payload="{}", event_id=uuid4())
    assert caught.value.failure.code == "webhook_timeout"


async def test_retry_after_is_preserved() -> None:
    """Workflow получает абсолютное время запрета преждевременной отправки."""
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(429, headers={"Retry-After": "120"})
        )
    ) as client:
        gateway = HttpWebhookGateway(
            client, policy=policy(), signing_secret=SECRET, now=lambda: NOW
        )
        with pytest.raises(AttemptError) as caught:
            await gateway.send(url=URL, payload="{}", event_id=uuid4())
    assert caught.value.failure.retry_after is not None
    assert (caught.value.failure.retry_after - NOW).total_seconds() == 120


async def test_excessive_retry_after_requires_manual_review() -> None:
    """Не отправлять раньше Retry-After и не скрывать многодневное ожидание."""
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(503, headers={"Retry-After": "90000"})
        )
    ) as client:
        gateway = HttpWebhookGateway(
            client, policy=policy(), signing_secret=SECRET, now=lambda: NOW
        )
        with pytest.raises(AttemptError) as caught:
            await gateway.send(url=URL, payload="{}", event_id=uuid4())
    assert caught.value.failure.permanent
    assert caught.value.failure.code == "webhook_retry_window_exceeded"


class UnreadableStream(httpx.AsyncByteStream):
    """Проверяет отсутствие ненужного скачивания тела ответа."""

    def __init__(self) -> None:
        """Сохранить факт освобождения ресурса."""
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        """Любая попытка читать тело является ошибкой теста."""
        yield b""
        error = "Тело webhook-ответа не требуется"
        raise AssertionError(error)

    async def aclose(self) -> None:
        """Запомнить освобождение stream."""
        self.closed = True


async def test_response_stream_is_closed_without_reading() -> None:
    """Тело ответа webhook не скачивается. Соединение закрывается."""
    stream = UnreadableStream()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(204, stream=stream)
        )
    ) as client:
        gateway = HttpWebhookGateway(client, policy=policy(), signing_secret=SECRET)
        await gateway.send(url=URL, payload="{}", event_id=uuid4())
    assert stream.closed

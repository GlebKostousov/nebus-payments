"""Проверка настоящего HTTP-шлюза с локальным ASGI-получателем."""

import hashlib
import hmac
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from app.core.config import Settings
from app.core.errors import AttemptError
from app.core.messages import WebhookEvent
from app.core.types import Currency, PaymentStatus
from app.core.webhook_policy import WebhookPolicy
from app.gateways.webhook import HttpWebhookGateway
from fastapi import status
from pydantic import SecretStr
from scripts.webhook_receiver import ReceivedEvents, create_app


@pytest.fixture
def settings() -> Settings:
    """Изолировать стенд от файлов .env и действующих секретов."""
    return Settings(
        _env_file=None,
        database_url=SecretStr("postgresql+asyncpg://unused/unused"),
        rabbitmq_url=SecretStr("amqp://unused/"),
        api_key=SecretStr("demo-receiver-test-api-key-123456789"),
        webhook_signing_secret=SecretStr("demo-receiver-test-signing-key-123456789"),
    )


def event() -> WebhookEvent:
    """Создать окончательный результат с актуальной датой подписи."""
    return WebhookEvent(
        event_id=uuid4(),
        payment_id=uuid4(),
        status=PaymentStatus.SUCCEEDED,
        amount=Decimal("1500.00"),
        currency=Currency.RUB,
        processed_at=datetime.now(UTC),
    )


async def test_real_signature_flaky_endpoint_and_deduplication(
    settings: Settings,
) -> None:
    """Два 503, затем 204; четвёртая доставка не применяется повторно."""
    app = create_app(settings)
    payload = event()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="https://receiver.example"
    ) as client:
        gateway = HttpWebhookGateway(
            client,
            policy=WebhookPolicy.build(("https://receiver.example",)),
            signing_secret=settings.webhook_signing_secret.get_secret_value(),
        )
        for _ in range(2):
            with pytest.raises(AttemptError) as caught:
                await gateway.send(
                    url="https://receiver.example/hooks/flaky",
                    payload=payload.model_dump_json(),
                    event_id=payload.event_id,
                )
            assert caught.value.failure.code == "webhook_http_503"
        for _ in range(2):
            await gateway.send(
                url="https://receiver.example/hooks/flaky",
                payload=payload.model_dump_json(),
                event_id=payload.event_id,
            )
        response = await client.get(
            "/events", headers={"X-API-Key": settings.api_key.get_secret_value()}
        )
        result = ReceivedEvents.model_validate_json(response.content)
        assert len(result.items) == 1
        assert result.items[0].requests == 4
        assert result.items[0].applied
        assert result.items[0].last_status_code == status.HTTP_204_NO_CONTENT


async def test_unsigned_callback_is_not_recorded(settings: Settings) -> None:
    """Наличие event_id не заменяет подпись отправителя."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(settings)),
        base_url="https://receiver.example",
    ) as client:
        response = await client.post(
            "/hooks/success", content=event().model_dump_json()
        )
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
        response = await client.get(
            "/events", headers={"X-API-Key": settings.api_key.get_secret_value()}
        )
        assert not ReceivedEvents.model_validate_json(response.content).items


async def test_diagnostic_endpoint_requires_key(settings: Settings) -> None:
    """Входящие webhook подписаны HMAC, диагностика защищена отдельным API-ключом."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(settings)),
        base_url="https://receiver.example",
    ) as client:
        response = await client.get("/events")
        assert response.status_code == status.HTTP_401_UNAUTHORIZED


async def test_changed_payload_under_same_event_is_rejected(settings: Settings) -> None:
    """Даже корректная подпись не разрешает переопределить существующее событие."""
    payload = event()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(settings)),
        base_url="https://receiver.example",
    ) as client:
        gateway = HttpWebhookGateway(
            client,
            policy=WebhookPolicy.build(("https://receiver.example",)),
            signing_secret=settings.webhook_signing_secret.get_secret_value(),
        )
        await gateway.send(
            url="https://receiver.example/hooks/success",
            payload=payload.model_dump_json(),
            event_id=payload.event_id,
        )
        changed = payload.model_copy(update={"amount": Decimal("1501.00")})
        with pytest.raises(AttemptError) as caught:
            await gateway.send(
                url="https://receiver.example/hooks/success",
                payload=changed.model_dump_json(),
                event_id=changed.event_id,
            )
        assert caught.value.failure.code == "webhook_http_409"


def _signed(*, timestamp: str, event_id: str, signature: str) -> dict[str, str]:
    """Собрать заголовки webhook; подпись можно подменить отдельно."""
    return {
        "Webhook-Timestamp": timestamp,
        "Webhook-Signature": signature,
        "Webhook-Id": event_id,
    }


def _digest(body: bytes, secret: str, timestamp: str) -> str:
    """Посчитать ожидаемую подпись v1."""
    signed = timestamp.encode("ascii") + b"." + body
    return "v1=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()


async def _events_empty(client: httpx.AsyncClient, api_key: str) -> bool:
    """Проверить, что отвергнутый запрос не оставил событие."""
    response = await client.get("/events", headers={"X-API-Key": api_key})
    return not ReceivedEvents.model_validate_json(response.content).items


@pytest.mark.parametrize(
    ("timestamp_shift", "signature_mode", "status_code"),
    [(0, "forged", 401), (-10_000, "valid", 401), (0, "valid", 400)],
)
async def test_forged_stale_and_mismatched_webhook_are_dropped(
    settings: Settings, timestamp_shift: int, signature_mode: str, status_code: int
) -> None:
    """Чужая подпись, старая метка и чужой Webhook-Id не применяются."""
    payload = event()
    body = payload.model_dump_json().encode()
    secret = settings.webhook_signing_secret.get_secret_value()
    timestamp = str(int(datetime.now(UTC).timestamp()) + timestamp_shift)
    signature = (
        "v1=00" if signature_mode == "forged" else _digest(body, secret, timestamp)
    )
    event_id = str(uuid4()) if status_code == 400 else str(payload.event_id)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(settings)),
        base_url="https://receiver.example",
    ) as client:
        response = await client.post(
            "/hooks/success",
            content=body,
            headers=_signed(
                timestamp=timestamp, event_id=event_id, signature=signature
            ),
        )
        assert response.status_code == status_code
        assert await _events_empty(client, settings.api_key.get_secret_value())

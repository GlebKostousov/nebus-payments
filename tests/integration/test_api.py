"""HTTP-маршруты с настоящим workflow и PostgreSQL."""

from uuid import uuid4

import httpx
import pytest
from app.core.config import Settings
from app.core.db import SessionFactory
from app.core.retry import Backoff
from app.core.types import PaymentResult, PaymentStatus
from app.entities.payment.schema import PaymentAccepted
from app.web._deps import ApplicationContext
from app.web.app import build_http_app
from app.workflows.payments.main import PaymentService, PaymentState

from tests.factories import request
from tests.integration.helpers import current_job

pytestmark = pytest.mark.integration


async def test_create_get_and_stable_replay(
    sessions: SessionFactory,
    service: PaymentService,
    settings: Settings,
) -> None:
    """Повтор POST возвращает квитанцию принятия, GET — уже завершённый результат."""
    app = build_http_app(ApplicationContext(service), settings)
    headers = {
        "X-API-Key": settings.api_key.get_secret_value(),
        "Idempotency-Key": str(uuid4()),
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        first = await client.post(
            "/api/v1/payments", headers=headers, json=request().model_dump(mode="json")
        )
        assert first.status_code == 202
        receipt = PaymentAccepted.model_validate_json(first.content)
        state = PaymentState(sessions, lease_seconds=60, backoff=Backoff(lambda: 0))
        work = await state.claim(await current_job(sessions, receipt.payment_id))
        assert work is not None
        saved = await state.save_result(
            work, PaymentResult(PaymentStatus.SUCCEEDED, "sim")
        )
        assert saved is not None
        assert await state.complete(saved)
        repeated = await client.post(
            "/api/v1/payments", headers=headers, json=request().model_dump(mode="json")
        )
        assert repeated.status_code == 202
        assert repeated.json() == first.json()
        detail = await client.get(
            f"/api/v1/payments/{receipt.payment_id}", headers=headers
        )
        assert detail.status_code == 200
        assert detail.json()["status"] == "succeeded"
        assert detail.json()["processing_state"] == "completed"
        assert "idempotency_key" not in detail.json()
        assert "webhook_payload" not in detail.json()
        assert "X-Request-ID" in detail.headers


async def test_http_contract_errors(
    service: PaymentService, settings: Settings
) -> None:
    """Проверить ключи, неизвестный платёж, невалидный запрос и защищённую схему."""
    app = build_http_app(ApplicationContext(service), settings)
    auth = {"X-API-Key": settings.api_key.get_secret_value()}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        assert (await client.get("/health/ready")).status_code == 401
        assert (await client.get("/openapi.json")).status_code == 401
        assert (await client.get("/openapi.json", headers=auth)).status_code == 200
        assert (await client.get("/health/ready", headers=auth)).status_code == 200
        assert (
            await client.get(f"/api/v1/payments/{uuid4()}", headers=auth)
        ).status_code == 404
        invalid = await client.post(
            "/api/v1/payments", headers=auth, json={"secret": "do-not-echo"}
        )
        assert invalid.status_code == 422
        assert "do-not-echo" not in invalid.text

"""Один HTTP-вызов без вложенных retry и без чтения ненужного тела ответа."""

import asyncio
import hashlib
import hmac
from collections.abc import Callable
from datetime import datetime, timedelta
from uuid import UUID

import httpx

from app.core.errors import AttemptError, WebhookDestinationError
from app.core.retry import parse_retry_after
from app.core.types import Failure, FailureStage, utcnow
from app.core.webhook_policy import WebhookPolicy


class HttpWebhookGateway:
    """Доставляет неизменяемый payload; повтор назначает вызывающий workflow."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        policy: WebhookPolicy,
        signing_secret: str,
        total_timeout_seconds: float = 10,
        now: Callable[[], datetime] = utcnow,
    ) -> None:
        """Инжектировать клиент, политику адресов и часы внешнего протокола."""
        self._client = client
        self._policy = policy
        self._secret = signing_secret.encode("utf-8")
        self._timeout = total_timeout_seconds
        self._now = now

    async def send(self, *, url: str, payload: str, event_id: UUID) -> None:
        """Сделать ровно одну попытку; 2xx, включая пустой 204, означает успех."""
        self._ensure_allowed(url)
        try:
            async with asyncio.timeout(self._timeout):
                await self._send(url=url, payload=payload, event_id=event_id)
        except httpx.RequestError as error:
            raise AttemptError(
                Failure("webhook_transport_error", FailureStage.WEBHOOK)
            ) from error
        except TimeoutError as error:
            raise AttemptError(
                Failure("webhook_timeout", FailureStage.WEBHOOK)
            ) from error

    def _ensure_allowed(self, url: str) -> None:
        """Превратить запрет адреса в окончательный отказ без повтора."""
        try:
            self._policy.check(url)
        except WebhookDestinationError as error:
            raise AttemptError(
                Failure(error.code, FailureStage.WEBHOOK, permanent=True)
            ) from error

    async def _send(self, *, url: str, payload: str, event_id: UUID) -> None:
        """Подписать точные байты и проверить только нужные нам заголовки ответа."""
        body = payload.encode("utf-8")
        timestamp = str(int(self._now().timestamp()))
        signature = hmac.new(
            self._secret, timestamp.encode("ascii") + b"." + body, hashlib.sha256
        ).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "Webhook-Id": str(event_id),
            "Webhook-Timestamp": timestamp,
            "Webhook-Signature": f"v1={signature}",
        }
        async with self._client.stream(
            "POST", url, content=body, headers=headers, follow_redirects=False
        ) as response:
            # Контракт не требует тела: не скачиваем потенциально огромный ответ.
            if response.is_success:
                return
            now = self._now()
            retry_after = parse_retry_after(
                response.headers.get("Retry-After"), now=now
            )
            if retry_after is not None and retry_after > now + timedelta(days=1):
                raise AttemptError(
                    Failure(
                        "webhook_retry_window_exceeded",
                        FailureStage.WEBHOOK,
                        permanent=True,
                    )
                )
            raise AttemptError(
                Failure(
                    f"webhook_http_{response.status_code}",
                    FailureStage.WEBHOOK,
                    retry_after=retry_after,
                )
            )


def build_http_client(*, timeout_seconds: float, connections: int) -> httpx.AsyncClient:
    """Создать клиент без прокси из окружения, редиректов и transport-retry."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds, connect=3, pool=3),
        limits=httpx.Limits(
            max_connections=connections, max_keepalive_connections=connections
        ),
        trust_env=False,
        follow_redirects=False,
        headers={"User-Agent": "nebus-payments/0.1"},
    )

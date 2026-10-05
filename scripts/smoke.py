"""Проверить полный путь платежа в запущенном Docker Compose."""

import argparse
import asyncio
import sys
from uuid import uuid4

import httpx
from app.core.config import Settings
from app.core.types import ProcessingState
from app.entities.payment.schema import PaymentAccepted, PaymentRead
from pydantic import BaseModel

from scripts.webhook_receiver import ReceivedEvents

_ACCEPTED = 202
_WEBHOOK_ATTEMPTS = 3


class SmokeError(Exception):
    """Проверяемое ожидание сквозного сценария не выполнено."""


class SmokeResult(BaseModel):
    """Короткий результат команды, пригодный для CI-лога."""

    payment: PaymentRead
    webhook_attempts: int
    distinct_events: int


async def wait_for_receiver(client: httpx.AsyncClient, url: str) -> None:
    """Дождаться старта стенда, не путать его старт с ошибкой платежа."""
    async with asyncio.timeout(20):
        while True:
            try:
                response = await client.get(f"{url}/events")
            except httpx.RequestError:
                await asyncio.sleep(0.25)
                continue
            response.raise_for_status()
            ReceivedEvents.model_validate_json(response.content)
            return


async def run(*, api: str, receiver: str, settings: Settings) -> SmokeResult:
    """Проверить 202, идемпотентность и три доставки одного уведомления."""
    headers = {"X-API-Key": settings.api_key.get_secret_value()}
    async with httpx.AsyncClient(
        headers=headers, timeout=5, trust_env=False, follow_redirects=False
    ) as client:
        await wait_for_receiver(client, receiver)
        key = str(uuid4())
        payload: dict[str, object] = {
            "amount": "1500.00",
            "currency": "RUB",
            "description": "Проверка повторов webhook",
            "metadata": {"test": "smoke"},
            "webhook_url": "http://webhook:8081/hooks/flaky",
        }
        receipt = await _accepted_payment(client, api, payload, key)
        await _repeat_payment(client, api, payload, key, receipt)
        async with asyncio.timeout(45):
            payment = await wait_for_payment(client, api, str(receipt.payment_id))
        return await _smoke_result(client, receiver, receipt, payment)


async def wait_for_payment(
    client: httpx.AsyncClient, api: str, payment_id: str
) -> PaymentRead:
    """Дождаться доставки, а не только окончательного финансового статуса."""
    while True:
        response = await client.get(f"{api}/api/v1/payments/{payment_id}")
        response.raise_for_status()
        payment = PaymentRead.model_validate_json(response.content)
        done = _finished_payment(payment)
        if done is not None:
            return done
        await asyncio.sleep(0.25)


def _finished_payment(payment: PaymentRead) -> PaymentRead | None:
    """Вернуть завершённый платёж или остановить сценарий на DLQ."""
    if payment.processing_state is ProcessingState.DEAD_LETTERED:
        message = f"Платёж отправлен в DLQ: {payment.last_error_code}"
        raise SmokeError(message)
    if payment.processing_state is ProcessingState.COMPLETED:
        return payment
    return None


async def _accepted_payment(
    client: httpx.AsyncClient,
    api: str,
    payload: dict[str, object],
    key: str,
) -> PaymentAccepted:
    """Создать платёж и требовать 202."""
    created = await client.post(
        f"{api}/api/v1/payments",
        json=payload,
        headers={"Idempotency-Key": key},
    )
    if created.status_code != _ACCEPTED:
        message = f"POST вернул {created.status_code} вместо 202"
        raise SmokeError(message)
    return PaymentAccepted.model_validate_json(created.content)


async def _repeat_payment(
    client: httpx.AsyncClient,
    api: str,
    payload: dict[str, object],
    key: str,
    receipt: PaymentAccepted,
) -> None:
    """Повторить POST и сверить неизменную квитанцию."""
    repeated = await client.post(
        f"{api}/api/v1/payments",
        json=payload,
        headers={"Idempotency-Key": key},
    )
    if repeated.status_code != _ACCEPTED:
        message = "Повтор POST не вернул 202"
        raise SmokeError(message)
    if PaymentAccepted.model_validate_json(repeated.content) != receipt:
        message = "Повтор POST изменил исходную квитанцию"
        raise SmokeError(message)


async def _smoke_result(
    client: httpx.AsyncClient,
    receiver: str,
    receipt: PaymentAccepted,
    payment: PaymentRead,
) -> SmokeResult:
    """Проверить одну применённую доставку после трёх попыток."""
    response = await client.get(f"{receiver}/events")
    response.raise_for_status()
    events = ReceivedEvents.model_validate_json(response.content)
    matching = [item for item in events.items if item.payment_id == receipt.payment_id]
    if len(matching) != 1:
        message = "Ожидалось одно событие для платежа"
        raise SmokeError(message)
    event = matching[0]
    _require_flaky_success(event.applied, event.requests, payment.attempt_count)
    return SmokeResult(
        payment=payment, webhook_attempts=event.requests, distinct_events=1
    )


def _require_flaky_success(applied: bool, requests: int, attempts: int) -> None:
    """Требовать два отказа и затем успешное применение."""
    if not applied or requests != _WEBHOOK_ATTEMPTS or attempts != _WEBHOOK_ATTEMPTS:
        message = "Не выполнен сценарий 503 → 503 → 204"
        raise SmokeError(message)


def main() -> None:
    """Вернуть ненулевой exit code при любом нарушении сценария."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:8000")
    parser.add_argument("--receiver", default="http://127.0.0.1:8081")
    args = parser.parse_args()
    try:
        result = asyncio.run(
            run(api=args.api, receiver=args.receiver, settings=Settings())
        )
    except (SmokeError, httpx.HTTPError, TimeoutError) as error:
        # HTTP-исключения могут содержать URL: не печатаем их полный repr.
        sys.stderr.write(f"Smoke-проверка не пройдена: {type(error).__name__}\n")
        raise SystemExit(1) from None
    sys.stdout.write(result.model_dump_json(indent=2) + "\n")


if __name__ == "__main__":
    main()

"""Подмены шлюза и webhook для unit-тестов."""

import asyncio
from dataclasses import replace
from uuid import UUID, uuid4

from app.core.errors import AttemptError
from app.core.messages import PaymentJob
from app.core.types import Failure, FailureStage, PaymentResult, PaymentStatus, Work

from tests.factories import NOW, view


class RecordingState:
    """Записывает вызовы границы; не пытается эмулировать блокировки PostgreSQL."""

    def __init__(self, *, status: PaymentStatus = PaymentStatus.PENDING) -> None:
        """Подготовить уже захваченную попытку для одного сценария."""
        self.work = Work(replace(view(status=status), attempt_count=1), uuid4())
        self.skip = False
        self.lose_result_lease = False
        self.saved = 0
        self.completed = 0
        self.failures: list[Failure] = []

    async def claim(self, _job: PaymentJob) -> Work | None:
        """Вернуть подготовленный снимок; конкурентный захват тестируется с БД."""
        return None if self.skip else self.work

    async def save_result(self, work: Work, result: PaymentResult) -> Work | None:
        """Сохранить результат перед попыткой webhook, как требует протокол."""
        if self.lose_result_lease:
            return None
        self.saved += 1
        self.work = Work(
            replace(
                work.payment,
                status=result.status,
                processed_at=NOW,
                webhook_event_id=uuid4(),
                webhook_payload='{"result":"saved"}',
                provider_reference=result.provider_reference,
            ),
            work.lease_token,
        )
        return self.work

    async def complete(self, _work: Work) -> bool:
        """Запомнить, что доставку отметили доставленной."""
        self.completed += 1
        return True

    async def fail(self, _work: Work, failure: Failure) -> bool:
        """Не копировать старый financial snapshot поверх сохранённого результата."""
        self.failures.append(failure)
        return True


class CountingPaymentGateway:
    """Один вызов возвращает заданный окончательный исход."""

    def __init__(self, status: PaymentStatus = PaymentStatus.SUCCEEDED) -> None:
        """Задать успешную оплату или окончательный отказ."""
        self.calls = 0
        self.status = status

    async def charge(self, payment_id: UUID) -> PaymentResult:
        """Посчитать обращения, а не только проверить конечный status."""
        self.calls += 1
        return PaymentResult(self.status, f"test_{payment_id}")


class RecordingWebhook:
    """Сохраняет точные payload и event_id всех попыток доставки."""

    def __init__(self, *, fail: bool = False) -> None:
        """Настроить ожидаемый сбой внешнего сервера."""
        self.fail = fail
        self.sent: list[tuple[str, UUID]] = []

    async def send(self, *, url: str, payload: str, event_id: UUID) -> None:  # noqa: ARG002
        """Запомнить запрос даже тогда, когда ответ потерян."""
        self.sent.append((payload, event_id))
        if self.fail:
            raise AttemptError(Failure("webhook_http_503", FailureStage.WEBHOOK))


class BlockingWebhook:
    """Останавливается в ожидании внешнего ответа до отмены."""

    def __init__(self) -> None:
        """Создать барьер для воспроизводимой отмены без случайных sleep."""
        self.entered = asyncio.Event()
        self.cleaned_up = False

    async def send(self, *, url: str, payload: str, event_id: UUID) -> None:
        """Подтвердить вход в вызов и обязательно освободить ресурс."""
        del url, payload, event_id
        self.entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cleaned_up = True

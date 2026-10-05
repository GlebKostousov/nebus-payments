"""Один бизнес-обработчик: возобновить оплату и доставить её результат."""

import asyncio
from dataclasses import dataclass
from uuid import UUID

from app.core.errors import AttemptError, InvariantError
from app.core.messages import PaymentJob
from app.core.ports import PaymentGateway, PaymentStatePort, WebhookGateway
from app.core.types import Failure, FailureStage, PaymentStatus, Work


@dataclass(frozen=True, slots=True)
class ProcessingOutcome:
    """Безопасные поля одного итогового лога обработчика."""

    action: str
    attempt: int = 0
    error_code: str | None = None


class PaymentProcessor:
    """Управляет этапами, не зная классов RabbitMQ и SQLAlchemy."""

    def __init__(
        self,
        state: PaymentStatePort,
        *,
        payment_gateway: PaymentGateway,
        webhook_gateway: WebhookGateway,
        timeout_seconds: float = 30,
    ) -> None:
        """Принять шлюз, доставку webhook и хранилище состояния."""
        self._state = state
        self._payment_gateway = payment_gateway
        self._webhook_gateway = webhook_gateway
        self._timeout = timeout_seconds

    async def process(self, job: PaymentJob) -> ProcessingOutcome:
        """ACK разрешён только после возвращения отсюда; отмена выходит наружу."""
        work = await self._state.claim(job)
        if work is None:
            return ProcessingOutcome("ignored")
        try:
            async with asyncio.timeout(self._timeout):
                return await self._resume(work)
        except (AttemptError, TimeoutError) as error:
            return await self._failed(work, _failure_from(error))

    async def _resume(self, work: Work) -> ProcessingOutcome:
        """Пропустить уже сохранённый результат оплаты, но не недоставленный webhook."""
        current = await self._ensure_result(work)
        if current is None:
            return ProcessingOutcome("lease_lost", work.payment.attempt_count)
        payload, event_id = _webhook_parts(current)
        await self._webhook_gateway.send(
            url=current.payment.webhook_url, payload=payload, event_id=event_id
        )
        completed = await self._state.complete(current)
        action = "completed" if completed else "lease_lost"
        return ProcessingOutcome(action, current.payment.attempt_count)

    async def _ensure_result(self, work: Work) -> Work | None:
        """Вызвать шлюз только пока финансовый результат ещё не сохранён."""
        if work.payment.status is not PaymentStatus.PENDING:
            return work
        result = await self._payment_gateway.charge(work.payment.id)
        return await self._state.save_result(work, result)

    async def _failed(self, work: Work, failure: Failure) -> ProcessingOutcome:
        """Сохранить неудачу; старый снимок не перезаписывает уже сохранённый результат."""
        saved = await self._state.fail(work, failure)
        action = "failure_recorded" if saved else "lease_lost"
        return ProcessingOutcome(action, work.payment.attempt_count, failure.code)


def _failure_from(error: AttemptError | TimeoutError) -> Failure:
    """Отличить отказ этапа от истечения дедлайна попытки."""
    if isinstance(error, AttemptError):
        return error.failure
    return Failure("attempt_deadline_exceeded", FailureStage.WORKER)


def _webhook_parts(work: Work) -> tuple[str, UUID]:
    """Вернуть неизменяемое тело webhook сохранённого результата."""
    payload = work.payment.webhook_payload
    event_id = work.payment.webhook_event_id
    if payload is None or event_id is None:
        message = "У окончательного результата отсутствует webhook"
        raise InvariantError(message)
    return payload, event_id

"""Конкурентность, rollback, восстановление и ограничения настоящего PostgreSQL."""

import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from app.core.db import SessionFactory
from app.core.errors import IdempotencyConflictError
from app.core.retry import Backoff
from app.core.types import Failure, FailureStage, PaymentResult, PaymentStatus
from app.entities.outbox.model import Outbox
from app.entities.outbox.repository import OutboxRepository
from app.entities.payment.model import Payment
from app.entities.payment.schema import PaymentAccepted
from app.workflows.payments.main import PaymentService, PaymentState
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from tests.factories import request
from tests.integration.helpers import current_job, make_due

pytestmark = pytest.mark.integration


async def test_fifty_concurrent_creates_have_one_payment_and_event(
    sessions: SessionFactory,
    service: PaymentService,
) -> None:
    """Уникальный индекс защищает гонку реальных соединений, а не только happy path."""
    key = str(uuid4())
    receipts = await asyncio.gather(
        *[service.create(request(), idempotency_key=key) for _ in range(50)]
    )
    assert len({item.payment_id for item in receipts}) == 1
    assert len({item.created_at for item in receipts}) == 1
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_equivalent_payload_replays_original_receipt(
    service: PaymentService,
) -> None:
    """Нормализация суммы не превращает повтор в другой платёж."""
    key = str(uuid4())
    first = await service.create(request(amount="1500.0"), idempotency_key=key)
    second = await service.create(request(amount="1500.00"), idempotency_key=key)
    assert second == first


async def test_metadata_order_replays_original_receipt(service: PaymentService) -> None:
    """Порядок ключей metadata не создаёт второй платёж под тем же ключом."""
    key = str(uuid4())
    left = request().model_copy(update={"metadata": {"a": 1, "b": 2}})
    right = request().model_copy(update={"metadata": {"b": 2, "a": 1}})
    first = await service.create(left, idempotency_key=key)
    second = await service.create(right, idempotency_key=key)
    assert second.payment_id == first.payment_id


async def test_concurrent_conflicting_bodies_keep_one_row(
    sessions: SessionFactory,
    service: PaymentService,
) -> None:
    """Гонка разных тел с одним ключом оставляет ровно одну исходную строку."""
    key = str(uuid4())
    outcomes = await asyncio.gather(
        service.create(request(amount="10.00"), idempotency_key=key),
        service.create(request(amount="20.00"), idempotency_key=key),
        return_exceptions=True,
    )
    successes = [item for item in outcomes if isinstance(item, PaymentAccepted)]
    conflicts = [
        item for item in outcomes if isinstance(item, IdempotencyConflictError)
    ]
    assert len(successes) == len(conflicts) == 1
    payment = await service.get(successes[0].payment_id)
    assert payment.amount in {Decimal("10.00"), Decimal("20.00")}
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1


async def test_hostile_description_is_stored_as_data(service: PaymentService) -> None:
    """Текст описания не исполняется как SQL."""
    hostile = "'; DROP TABLE payments;--"
    receipt = await service.create(
        request().model_copy(update={"description": hostile}),
        idempotency_key=str(uuid4()),
    )
    assert (await service.get(receipt.payment_id)).description == hostile


async def test_conflicting_request_is_not_applied(service: PaymentService) -> None:
    """Повтор ключа с другой суммой не обновляет старый платёж."""
    key = str(uuid4())
    receipt = await service.create(request(), idempotency_key=key)
    with pytest.raises(IdempotencyConflictError):
        await service.create(request(amount="1600.00"), idempotency_key=key)
    payment = await service.get(receipt.payment_id)
    assert payment.amount == request().amount


async def test_outbox_failure_rolls_back_payment(
    sessions: SessionFactory,
    service: PaymentService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Платёж сохраняется только вместе с первой строкой outbox."""

    def fail_to_add(_repository: OutboxRepository, _event: Outbox) -> None:
        """Остановить сценарий между двумя действиями одной транзакции."""
        message = "injected failure"
        raise RuntimeError(message)

    monkeypatch.setattr(OutboxRepository, "add", fail_to_add)
    with pytest.raises(RuntimeError):
        await service.create(request(), idempotency_key=str(uuid4()))
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_concurrent_deliveries_claim_one_attempt(
    sessions: SessionFactory,
    service: PaymentService,
) -> None:
    """Тридцать дублей сообщения расходуют ровно одну сохранённую попытку."""
    receipt = await service.create(request(), idempotency_key=str(uuid4()))
    job = await current_job(sessions, receipt.payment_id)
    state = PaymentState(sessions, lease_seconds=60, backoff=Backoff(lambda: 0))
    claims = await asyncio.gather(*[state.claim(job) for _ in range(30)])
    assert sum(claim is not None for claim in claims) == 1
    assert (await service.get(receipt.payment_id)).attempt_count == 1


async def test_retry_is_saved_once_and_old_message_is_ignored(
    sessions: SessionFactory,
    service: PaymentService,
) -> None:
    """Падение после commit, до ACK не создаёт ещё одно продолжение."""
    receipt = await service.create(request(), idempotency_key=str(uuid4()))
    job = await current_job(sessions, receipt.payment_id)
    state = PaymentState(sessions, lease_seconds=60, backoff=Backoff(lambda: 0))
    work = await state.claim(job)
    assert work is not None
    failure = Failure("webhook_http_503", FailureStage.WEBHOOK)
    assert await state.fail(work, failure)
    assert not await state.fail(work, failure)
    assert await state.claim(job) is None
    next_job = await current_job(sessions, receipt.payment_id)
    assert next_job.message_id != job.message_id
    assert await state.claim(next_job) is None
    await make_due(sessions, next_job.message_id)
    following = await state.claim(next_job)
    assert following is not None
    assert following.payment.attempt_count == 2
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 2


async def test_recovery_keeps_financial_result_and_fences_old_owner(
    sessions: SessionFactory,
    service: PaymentService,
) -> None:
    """Прерванный webhook возобновляется; поздний старый процесс не делает commit."""
    receipt = await service.create(request(), idempotency_key=str(uuid4()))
    state = PaymentState(sessions, lease_seconds=60, backoff=Backoff(lambda: 0))
    work = await state.claim(await current_job(sessions, receipt.payment_id))
    assert work is not None
    saved = await state.save_result(
        work, PaymentResult(PaymentStatus.SUCCEEDED, "provider")
    )
    assert saved is not None
    async with sessions() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == receipt.payment_id)
            .values(lease_until=func.clock_timestamp() - text("interval '1 second'"))
        )
        await session.commit()
    assert await state.recover() == 1
    assert await state.recover() == 0
    assert not await state.complete(saved)
    next_job = await current_job(sessions, receipt.payment_id)
    await make_due(sessions, next_job.message_id)
    next_work = await state.claim(next_job)
    assert next_work is not None
    assert next_work.payment.status is PaymentStatus.SUCCEEDED
    assert next_work.payment.webhook_payload == saved.payment.webhook_payload
    assert next_work.payment.webhook_event_id == saved.payment.webhook_event_id
    assert next_work.payment.attempt_count == 2


async def test_third_aborted_attempt_is_dead_lettered(
    sessions: SessionFactory,
    service: PaymentService,
) -> None:
    """Перезапуск процесса не даёт четвёртой попытки."""
    receipt = await service.create(request(), idempotency_key=str(uuid4()))
    state = PaymentState(sessions, lease_seconds=60, backoff=Backoff(lambda: 0))
    for attempt in range(1, 4):
        job = await current_job(sessions, receipt.payment_id)
        await make_due(sessions, job.message_id)
        work = await state.claim(job)
        assert work is not None
        assert work.payment.attempt_count == attempt
        async with sessions() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == receipt.payment_id)
                .values(
                    lease_until=func.clock_timestamp() - text("interval '1 second'")
                )
            )
            await session.commit()
        assert await state.recover() == 1
    payment = await service.get(receipt.payment_id)
    assert payment.processing_state.value == "dead_lettered"
    assert payment.status is PaymentStatus.PENDING
    assert payment.attempt_count == 3
    assert await state.claim(job) is None
    async with sessions() as session:
        letters = await session.scalar(
            select(func.count())
            .select_from(Outbox)
            .where(Outbox.destination == "dead_letter")
        )
    assert letters == 1


async def test_database_rejects_contradictory_result(
    sessions: SessionFactory,
    service: PaymentService,
) -> None:
    """Статус succeeded без processed_at и сохранённого webhook нарушает CHECK."""
    receipt = await service.create(request(), idempotency_key=str(uuid4()))
    async with sessions() as session:
        with pytest.raises(IntegrityError):
            await _commit_contradiction(session, receipt.payment_id)
        await session.rollback()
    assert (await service.get(receipt.payment_id)).status is PaymentStatus.PENDING


async def _commit_contradiction(session: AsyncSession, payment_id: UUID) -> None:
    """Записать противоречивый статус и зафиксировать транзакцию."""
    await session.execute(
        update(Payment).where(Payment.id == payment_id).values(status="succeeded")
    )
    await session.commit()

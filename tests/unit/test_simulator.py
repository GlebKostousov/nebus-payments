"""Эмулятор не должен заново выбирать исход уже известного платежа."""

from random import Random
from uuid import UUID, uuid4

from app.core.types import PaymentStatus
from app.gateways.simulated_payment import SimulatedPaymentGateway


async def test_stable_outcome_and_nonblocking_delay() -> None:
    """Один payment_id возвращает один исход, задержки находятся в диапазоне ТЗ."""
    delays: list[float] = []

    async def record_delay(seconds: float) -> None:
        """Запомнить запрос на ожидание, не замедляя тест."""
        delays.append(seconds)

    gateway = SimulatedPaymentGateway(
        random=Random(17),  # noqa: S311 — фиксированное зерно эмулятора, не секрет
        sleep=record_delay,
    )
    payment_id = uuid4()
    first = await gateway.charge(payment_id)
    assert all([first == await gateway.charge(payment_id) for _ in range(5)])
    assert len(delays) == 6
    assert all(2 <= delay <= 5 for delay in delays)


async def test_fixed_sample_has_both_results() -> None:
    """Проверить распределение на фиксированных, а не случайных входах."""

    async def no_delay(_seconds: float) -> None:
        """Эмуляция ожидания для детерминированной выборки."""
        return

    gateway = SimulatedPaymentGateway(
        random=Random(0),  # noqa: S311 — фиксированное зерно эмулятора, не секрет
        sleep=no_delay,
    )
    successes = 0
    for value in range(1000):
        result = await gateway.charge(UUID(int=value))
        successes += result.status is PaymentStatus.SUCCEEDED
    assert 850 <= successes <= 950

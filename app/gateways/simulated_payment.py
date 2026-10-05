"""Эмуляция задержки провайдера с устойчивым исходом для одного payment_id."""

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from random import Random
from uuid import UUID

from app.core.types import PaymentResult, PaymentStatus


class SimulatedPaymentGateway:
    """Детерминированные примерно 90% успеха; задержка не блокирует event loop."""

    def __init__(
        self,
        *,
        random: Random,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Передать отдельно генератор задержки и ожидание для быстрых тестов."""
        self._random = random
        self._sleep = sleep

    async def charge(self, payment_id: UUID) -> PaymentResult:
        """Повтор того же платежа возвращает тот же результат, а не новую монетку."""
        await self._sleep(self._random.uniform(2, 5))
        digest = hashlib.sha256(payment_id.bytes).digest()
        sample = int.from_bytes(digest[:8], "big")
        succeeded = sample < 9 * (2**64) // 10
        return PaymentResult(
            status=PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED,
            provider_reference=f"sim_{payment_id.hex}",
        )

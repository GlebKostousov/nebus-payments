"""Небольшие служебные циклы, жизненным циклом которых владеет worker."""

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from time import time

from app.core.logging import get_logger

logger = get_logger(__name__)
HEARTBEAT_MAX_AGE_SECONDS = 15
# Heartbeat живёт на tmpfs контейнера и нужен только healthcheck этого процесса.
HEARTBEAT_FILE = Path("/tmp/payments-worker.heartbeat")  # noqa: S108


async def run_periodically(
    action: Callable[[], Awaitable[int]], *, interval: float, name: str
) -> None:
    """Не скрывать аварийное завершение служебной задачи от TaskGroup."""
    while True:
        count = await action()
        if count:
            logger.info(name, count=count)
        await asyncio.sleep(interval)


async def heartbeat() -> None:
    """Показать, что event loop жив; критические задачи контролирует TaskGroup."""
    try:
        while True:
            await asyncio.to_thread(
                HEARTBEAT_FILE.write_text, str(time()), encoding="ascii"
            )
            await asyncio.sleep(2)
    finally:
        await asyncio.to_thread(HEARTBEAT_FILE.unlink, missing_ok=True)

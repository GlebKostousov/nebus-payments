"""Точка запуска consumer, relay и восстановления в одном процессе."""

import asyncio
import signal
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from random import Random

from app.core.config import Settings
from app.core.db import build_engine, build_sessions
from app.core.logging import configure_logging, get_logger
from app.core.retry import Backoff
from app.core.webhook_policy import WebhookPolicy
from app.gateways.rabbitmq.main import RabbitPublisher, Topology, build_broker
from app.gateways.simulated_payment import SimulatedPaymentGateway
from app.gateways.webhook import HttpWebhookGateway, build_http_client
from app.workers.loops import heartbeat, run_periodically
from app.workers.payment_consumer import register_consumer
from app.workflows.outbox import OutboxRelay
from app.workflows.payments.main import PaymentProcessor, PaymentState

logger = get_logger(__name__)


@asynccontextmanager
async def termination_signal(stop: asyncio.Event) -> AsyncIterator[None]:
    """Установить обработчики сигналов только на время жизни worker."""
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for name in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(name, stop.set)
        installed.append(name)
    try:
        yield
    finally:
        for name in installed:
            loop.remove_signal_handler(name)


async def run_worker(settings: Settings) -> None:
    """Закрыть consumer раньше HTTP-клиента и PostgreSQL, включая аварийный выход."""
    stop = asyncio.Event()
    fatal = asyncio.Event()
    engine = build_engine(settings)
    sessions = build_sessions(engine)
    broker = build_broker(settings)
    topology = Topology.build()
    random = Random()  # noqa: S311 — jitter и эмуляция исхода, не секрет
    state = PaymentState(
        sessions,
        lease_seconds=settings.processing_lease_seconds,
        backoff=Backoff(random.random),
    )
    async with AsyncExitStack() as stack:
        stack.push_async_callback(engine.dispose)
        client = await stack.enter_async_context(
            build_http_client(
                timeout_seconds=settings.webhook_timeout_seconds,
                connections=settings.consumer_prefetch,
            )
        )
        processor = PaymentProcessor(
            state,
            payment_gateway=SimulatedPaymentGateway(random=random),
            webhook_gateway=HttpWebhookGateway(
                client,
                policy=WebhookPolicy.build(
                    settings.webhook_allowed_origins,
                    allow_development=settings.webhook_allow_development,
                ),
                signing_secret=settings.webhook_signing_secret.get_secret_value(),
                total_timeout_seconds=settings.webhook_timeout_seconds,
            ),
            timeout_seconds=settings.attempt_timeout_seconds,
        )
        register_consumer(broker, topology, processor, fatal=fatal)
        stack.push_async_callback(broker.stop)
        await broker.connect()
        await topology.declare(broker)
        await broker.start()
        relay = OutboxRelay(
            sessions,
            RabbitPublisher(
                broker, topology, timeout_seconds=settings.publish_timeout_seconds
            ),
            backoff=Backoff(random.random, base_seconds=1, cap_seconds=30),
            batch_size=settings.outbox_batch_size,
            lease_seconds=settings.outbox_lease_seconds,
        )
        await stack.enter_async_context(termination_signal(stop))
        await _run_tasks(settings, relay, state, stop=stop, fatal=fatal)


async def _run_tasks(
    settings: Settings,
    relay: OutboxRelay,
    state: PaymentState,
    *,
    stop: asyncio.Event,
    fatal: asyncio.Event,
) -> None:
    """Не оставить процесс живым без relay или процедуры восстановления."""
    async with asyncio.TaskGroup() as group:
        tasks = [
            group.create_task(
                run_periodically(
                    relay.tick,
                    interval=settings.outbox_poll_seconds,
                    name="outbox.batch",
                )
            ),
            group.create_task(
                run_periodically(
                    state.recover,
                    interval=settings.recovery_interval_seconds,
                    name="payment.recovered",
                )
            ),
            group.create_task(heartbeat()),
        ]
        stop_waiter = group.create_task(stop.wait())
        fatal_waiter = group.create_task(fatal.wait())
        await asyncio.wait(
            [stop_waiter, fatal_waiter], return_when=asyncio.FIRST_COMPLETED
        )
        for task in tasks:
            task.cancel()
        stop_waiter.cancel()
        fatal_waiter.cancel()
        _stop_if_fatal(fatal)


def _stop_if_fatal(fatal: asyncio.Event) -> None:
    """Остановить процесс, если доставка не была подтверждена."""
    if fatal.is_set():
        message = "Consumer остановлен после ошибки; доставка не ACK-нута"
        raise RuntimeError(message)


def main() -> None:
    """Запустить worker; неожиданный сбой даёт ненулевой код для restart policy."""
    settings = Settings()
    configure_logging(json_logs=settings.json_logs)
    try:
        asyncio.run(run_worker(settings))
    except Exception as error:  # noqa: BLE001 — в тексте исключения бывают URL и SQL
        logger.error(  # noqa: TRY400
            "worker.stopped", error_type=type(error).__name__
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

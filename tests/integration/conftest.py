"""Интеграционные тесты ходят в базу с суффиксом _test."""

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from uuid import uuid4

import aio_pika
import pytest
from alembic import command
from alembic.config import Config
from app.core.config import Settings
from app.core.db import SessionFactory
from app.core.webhook_policy import WebhookPolicy
from app.gateways.rabbitmq.main import Topology, build_broker
from app.workflows.payments.main import PaymentService
from faststream.rabbit import RabbitBroker
from pydantic import SecretStr
from sqlalchemy import Connection, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.fixture
def settings() -> Settings:
    """Отказаться от запуска, если имя БД не оканчивается на _test."""
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Для интеграционных тестов задайте TEST_DATABASE_URL")
    database_name = make_url(database_url).database
    if database_name is None or not database_name.endswith("_test"):
        pytest.fail("Интеграционные тесты разрешены только в базе с суффиксом _test")
    return Settings(
        database_url=SecretStr(database_url),
        rabbitmq_url=SecretStr(os.environ["TEST_RABBITMQ_URL"]),
        api_key=SecretStr("integration-api-key-" * 3),
        webhook_signing_secret=SecretStr("integration-webhook-key-" * 3),
        webhook_allowed_origins=("https://receiver.example",),
        webhook_allow_development=False,
    )


def migrate(connection: Connection) -> None:
    """Накатить миграции Alembic на тестовую базу."""
    config = Config("alembic.ini")
    config.attributes["connection"] = connection
    command.upgrade(config, "head")


@pytest.fixture
async def sessions(settings: Settings) -> AsyncIterator[SessionFactory]:
    """Изолировать тесты очисткой только выделенной тестовой БД."""
    engine = create_async_engine(
        settings.database_url.get_secret_value(),
        pool_size=20,
        max_overflow=0,
        hide_parameters=True,
    )
    try:
        async with engine.begin() as connection:
            await connection.run_sync(migrate)
            await connection.execute(text("TRUNCATE outbox, payments"))
        yield async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
        )
    finally:
        await engine.dispose()


@pytest.fixture
def service(sessions: SessionFactory) -> PaymentService:
    """Собрать настоящий API-сценарий с тестовым allowlist."""
    return PaymentService(
        sessions, webhook_policy=WebhookPolicy.build(("https://receiver.example",))
    )


@dataclass(frozen=True, slots=True)
class RabbitEnvironment:
    """Отдельные очереди каждого теста на настоящем RabbitMQ."""

    broker: RabbitBroker
    topology: Topology
    url: str


@pytest.fixture
async def rabbit(settings: Settings) -> AsyncIterator[RabbitEnvironment]:
    """Создать и удалить уникальную топологию, не трогая рабочие очереди."""
    broker = build_broker(settings)
    topology = Topology.build(prefix=f"payments_test_{uuid4().hex}")
    url = settings.rabbitmq_url.get_secret_value()
    await broker.connect()
    try:
        await topology.declare(broker)
        yield RabbitEnvironment(broker, topology, url)
    finally:
        await broker.stop()
        connection = await aio_pika.connect_robust(url)
        async with connection:
            channel = await connection.channel()
            for name in (topology.queue.name, topology.dead_queue.name):
                queue = await channel.get_queue(name, ensure=False)
                await queue.delete(if_unused=False, if_empty=False)
            for name in (topology.exchange.name, topology.dead_exchange.name):
                exchange = await channel.get_exchange(name, ensure=False)
                await exchange.delete(if_unused=False)

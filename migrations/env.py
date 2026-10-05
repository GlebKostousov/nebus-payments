"""Миграции используют только DATABASE_URL, без ключей API и webhook."""

import asyncio
import os
from logging.config import fileConfig

from alembic import context
from app.core.db import Base
from app.entities.outbox.model import Outbox
from app.entities.payment.model import Payment
from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import create_async_engine

# Импорты регистрируют обе таблицы для autogenerate; схема ревизий от них не зависит.
assert Payment.metadata is Outbox.metadata is Base.metadata
config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)


def migrate_connection(connection: Connection) -> None:
    """Выполнить версии на данном соединении; используется и интеграционными тестами."""
    context.configure(
        connection=connection, target_metadata=Base.metadata, compare_type=True
    )
    with context.begin_transaction():
        context.run_migrations()


async def migrate_online() -> None:
    """Открыть одно соединение и освободить его при любом исходе миграции."""
    engine = create_async_engine(
        os.environ["DATABASE_URL"],
        poolclass=pool.NullPool,
        hide_parameters=True,
        connect_args={"timeout": 10},
    )
    try:
        async with engine.connect() as connection:
            await connection.run_sync(migrate_connection)
    finally:
        await engine.dispose()


def run() -> None:
    """Поддержать онлайн-запуск, offline SQL и соединение из тестовой фикстуры."""
    if context.is_offline_mode():
        context.configure(
            url=os.environ.get("DATABASE_URL", "postgresql+asyncpg://unused/unused"),
            target_metadata=Base.metadata,
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
        )
        with context.begin_transaction():
            context.run_migrations()
        return
    connection = config.attributes.get("connection")
    if isinstance(connection, Connection):
        migrate_connection(connection)
    else:
        asyncio.run(migrate_online())


run()

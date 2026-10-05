"""Фабрика сессий. Commit делает сценарий, который сессию открыл."""

from datetime import datetime

from sqlalchemy import MetaData, func, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from .config import Settings
from .errors import InvariantError


class Base(DeclarativeBase):
    """Общая metadata с устойчивыми именами ограничений для Alembic."""

    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


type SessionFactory = async_sessionmaker[AsyncSession]


def build_engine(settings: Settings) -> AsyncEngine:
    """Создать пул с конечными таймаутами; секреты и SQL-параметры не логируются."""
    timeout_ms = str(int(settings.db_statement_timeout_seconds * 1000))
    return create_async_engine(
        settings.database_url.get_secret_value(),
        pool_size=settings.db_pool_size,
        max_overflow=0,
        pool_timeout=5,
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={
            "timeout": 5,
            "command_timeout": settings.db_statement_timeout_seconds,
            "server_settings": {
                "statement_timeout": timeout_ms,
                "lock_timeout": timeout_ms,
                "idle_in_transaction_session_timeout": "10000",
                "application_name": "nebus-payments",
            },
        },
    )


def build_sessions(engine: AsyncEngine) -> SessionFactory:
    """Создать фабрику отдельных сессий без автоматической фиксации."""
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


async def database_now(session: AsyncSession) -> datetime:
    """Использовать часы PostgreSQL для аренды и доступности заданий."""
    value: object = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(value, datetime) or value.tzinfo is None:
        message = "PostgreSQL вернул не timezone-aware timestamp"
        raise InvariantError(message)
    return value

"""Сборка API и владение его пулом PostgreSQL."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.core.config import Settings
from app.core.db import build_engine, build_sessions
from app.core.logging import configure_logging
from app.core.webhook_policy import WebhookPolicy
from app.web._deps import ApplicationContext
from app.web.app import build_http_app
from app.workflows.payments.main import PaymentService


def create_app() -> FastAPI:
    """Фабрика для uvicorn --factory; сеть и изменяемое состояние не глобальны."""
    settings = Settings()
    configure_logging(json_logs=settings.json_logs)
    engine = build_engine(settings)
    service = PaymentService(
        build_sessions(engine),
        webhook_policy=WebhookPolicy.build(
            settings.webhook_allowed_origins,
            allow_development=settings.webhook_allow_development,
        ),
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        """Закрыть пул и при нормальной остановке, и при ошибке приложения."""
        try:
            yield
        finally:
            await engine.dispose()

    return build_http_app(ApplicationContext(service), settings, lifespan=lifespan)

"""Структурированные логи без тел запросов, секретов и SQL-параметров."""

import logging

import structlog
from structlog.stdlib import BoundLogger


def get_logger(name: str) -> BoundLogger:
    """Вернуть именованный structlog logger с типизированным публичным интерфейсом."""
    return structlog.stdlib.get_logger(name)


def configure_logging(*, json_logs: bool) -> None:
    """Настроить вывод приложения и стандартных логгеров один раз при старте."""
    logging.basicConfig(format="%(message)s", level=logging.INFO, force=True)
    renderer = (
        structlog.processors.JSONRenderer()
        if json_logs
        else (structlog.dev.ConsoleRenderer(colors=False))
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            renderer,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    # Эти библиотеки могут включать URL или SQL в диагностические сообщения.
    for name in ("httpx", "httpcore", "sqlalchemy.engine", "aio_pika", "aiormq"):
        logging.getLogger(name).setLevel(logging.CRITICAL)

"""Схема базы применяется до того, как API начинает слушать порт."""

import os
import sys

from alembic import command
from alembic.config import Config

_SERVER = (
    sys.executable,
    "-m",
    "uvicorn",
    "app.app_factory:create_app",
    "--factory",
    "--host",
    "0.0.0.0",  # noqa: S104 — порт публикует Compose, слушать нужно на всех интерфейсах контейнера
    "--port",
    "8000",
    "--no-access-log",
)


def main() -> None:
    """Применить миграции и заменить этот процесс сервером."""
    command.upgrade(Config("alembic.ini"), "head")
    # Сервер заменяет этот процесс, чтобы сигнал остановки дошёл до uvicorn.
    os.execv(sys.executable, list(_SERVER))  # noqa: S606


if __name__ == "__main__":
    main()

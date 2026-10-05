"""Pytest стартует только при TEST=1."""

import os

import pytest

_GUARD_VARIABLE = "TEST"
_GUARD_VALUE = "1"
_REFUSAL = 'Тесты не запущены: задайте TEST=1. PowerShell: $env:TEST="1"; uv run pytest'


def run_is_confirmed() -> bool:
    """Вернуть True только при точном значении переменной TEST=1."""
    return os.environ.get(_GUARD_VARIABLE) == _GUARD_VALUE


def pytest_sessionstart(session: pytest.Session) -> None:
    """Остановить сессию до сбора и запуска тестов, если TEST не равен 1."""
    del session
    if run_is_confirmed():
        return
    raise pytest.UsageError(_REFUSAL)

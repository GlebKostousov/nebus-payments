"""Pytest стартует только при TEST=1."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.conftest import run_is_confirmed

_ROOT = Path(__file__).resolve().parents[2]


def test_missing_variable_does_not_confirm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Без TEST запуск не подтверждён."""
    monkeypatch.delenv("TEST", raising=False)
    assert not run_is_confirmed()


@pytest.mark.parametrize("value", ["", "0", "true", "yes", "1 "])
def test_other_values_do_not_confirm(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """Подходит только точное значение 1."""
    monkeypatch.setenv("TEST", value)
    assert not run_is_confirmed()


def test_exact_one_confirms(monkeypatch: pytest.MonkeyPatch) -> None:
    """TEST=1 подтверждает запуск."""
    monkeypatch.setenv("TEST", "1")
    assert run_is_confirmed()


def test_pytest_does_not_start_without_confirmation() -> None:
    """Дочерний pytest завершается до выполнения тестов, если TEST нет."""
    completed = _run_pytest(confirm=False)
    output = completed.stdout + completed.stderr
    assert completed.returncode == pytest.ExitCode.USAGE_ERROR
    assert "TEST=1" in output
    assert "passed" not in output


def test_pytest_starts_when_confirmation_is_exact() -> None:
    """При TEST=1 сбор одного модуля проходит."""
    completed = _run_pytest(confirm=True)
    assert completed.returncode == 0, completed.stderr


def _run_pytest(*, confirm: bool) -> subprocess.CompletedProcess[str]:
    """Запустить отдельный pytest, не наследуя подтверждение родителя."""
    env = os.environ.copy()
    env.pop("TEST", None)
    env.pop("PYTEST_ADDOPTS", None)
    if confirm:
        env["TEST"] = "1"
    env["PYTHONUTF8"] = "1"
    return subprocess.run(  # noqa: S603 — команда фиксирована, shell не используется
        [
            sys.executable,
            "-m",
            "pytest",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
            "tests/unit/test_retry.py",
        ],
        cwd=_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )

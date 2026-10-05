"""Проверки для Docker не требуют дополнительного HTTP-сервера в consumer."""

import os
import sys
from time import time

import httpx

from app.workers.loops import HEARTBEAT_FILE, HEARTBEAT_MAX_AGE_SECONDS


def main() -> None:
    """API проверяется с ключом, consumer — по heartbeat живого event loop."""
    if sys.argv[1:] == ["worker"]:
        raise SystemExit(_exit_code(_worker_healthy()))
    raise SystemExit(_exit_code(_api_healthy()))


def _worker_healthy() -> bool:
    """Считать consumer живым, пока heartbeat свежее допустимого возраста."""
    try:
        written_at = float(HEARTBEAT_FILE.read_text(encoding="ascii"))
    except (OSError, ValueError):
        return False
    return time() - written_at < HEARTBEAT_MAX_AGE_SECONDS


def _api_healthy() -> bool:
    """Проверить готовность API тем же ключом, что требует guard."""
    try:
        response = httpx.get(
            "http://127.0.0.1:8000/health/ready",
            headers={"X-API-Key": os.environ["API_KEY"]},
            timeout=5,
            trust_env=False,
        )
    except (httpx.RequestError, KeyError):
        return False
    return response.is_success


def _exit_code(healthy: bool) -> int:
    """Преобразовать признак здоровья в код процесса."""
    if healthy:
        return 0
    return 1


if __name__ == "__main__":
    main()

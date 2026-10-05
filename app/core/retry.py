"""Расчёт задержек без ожидания и без собственного планировщика."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime

_MAX_BACKOFF_SECONDS = 86400
_RETRY_AFTER_MAX_LENGTH = 128


@dataclass(frozen=True, slots=True)
class Backoff:
    """Экспоненциальная задержка с добавочным jitter до 20%."""

    random_fraction: Callable[[], float]
    base_seconds: float = 2.0
    cap_seconds: float = 30.0

    def __post_init__(self) -> None:
        """Проверить параметры, которые определяют срок сохранённого повтора."""
        if not 0 < self.base_seconds <= self.cap_seconds <= _MAX_BACKOFF_SECONDS:
            message = "Нужно 0 < base_seconds <= cap_seconds <= 86400"
            raise ValueError(message)

    def delay(self, failed_attempt: int) -> timedelta:
        """Рассчитать ожидание после указанной неудачной попытки."""
        if failed_attempt < 1:
            message = "Номер попытки должен быть положительным"
            raise ValueError(message)
        fraction = self.random_fraction()
        if not 0 <= fraction <= 1:
            message = "Источник jitter должен вернуть число от 0 до 1"
            raise ValueError(message)
        # Ограничиваем показатель до возведения в степень: у relay нет лимита попыток.
        exponential = self.base_seconds * 2 ** min(failed_attempt - 1, 30)
        seconds = min(self.cap_seconds, exponential * (1 + 0.2 * fraction))
        return timedelta(seconds=seconds)


def parse_retry_after(value: str | None, *, now: datetime) -> datetime | None:
    """Разобрать Retry-After; испорченный заголовок не отменяет обычный backoff."""
    if value is None or len(value) > _RETRY_AFTER_MAX_LENGTH:
        return None
    return _parsed_retry_after(value.strip(), now)


def _parsed_retry_after(value: str, now: datetime) -> datetime | None:
    """Вернуть срок повтора либо None, если заголовок нельзя разобрать."""
    try:
        parsed = _retry_after_datetime(value, now)
    except (ValueError, OverflowError, TypeError):
        return None
    return _aware_deadline(parsed, now)


def _retry_after_datetime(value: str, now: datetime) -> datetime:
    """Разобрать delta-seconds либо HTTP-date."""
    if value.isascii() and value.isdigit():
        return now + timedelta(seconds=int(value))
    return parsedate_to_datetime(value)


def _aware_deadline(parsed: datetime, now: datetime) -> datetime | None:
    """Отбросить дату без пояса и не назначать повтор в прошлое."""
    if parsed.tzinfo is None:
        return None
    return max(now, parsed)

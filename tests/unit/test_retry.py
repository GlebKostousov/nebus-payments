"""Экспонента, границы jitter и стандартные формы Retry-After."""

from datetime import timedelta

import pytest
from app.core.retry import Backoff, parse_retry_after

from tests.factories import NOW


@pytest.mark.parametrize(
    ("attempt", "seconds"), [(1, 2), (2, 4), (3, 8), (1000000, 30)]
)
def test_exponential_delay_is_bounded(attempt: int, seconds: float) -> None:
    """Длинная недоступность брокера не создаёт огромное число при возведении в степень."""
    assert Backoff(lambda: 0).delay(attempt) == timedelta(seconds=seconds)


@pytest.mark.parametrize(("fraction", "seconds"), [(0, 2), (0.5, 2.2), (1, 2.4)])
def test_additive_jitter(fraction: float, seconds: float) -> None:
    """Jitter не сокращает базовую паузу."""
    assert Backoff(lambda: fraction).delay(1) == timedelta(seconds=seconds)


@pytest.mark.parametrize("attempt", [0, -1])
def test_invalid_attempt(attempt: int) -> None:
    """Публикация не должна рассчитывать backoff с нулевой попыткой."""
    with pytest.raises(ValueError, match="положительным"):
        Backoff(lambda: 0).delay(attempt)


@pytest.mark.parametrize("fraction", [-0.1, 1.1])
def test_invalid_random_source(fraction: float) -> None:
    """Ошибка внедрённого источника случайности не скрывается."""
    with pytest.raises(ValueError, match="jitter"):
        Backoff(lambda: fraction).delay(1)


@pytest.mark.parametrize("value", [None, "garbage", "-1", "9" * 200])
def test_bad_retry_after_falls_back(value: str | None) -> None:
    """Некорректный заголовок не мешает обычному повтору."""
    assert parse_retry_after(value, now=NOW) is None


def test_retry_after_seconds() -> None:
    """Секунды Retry-After считаются от переданных часов."""
    assert parse_retry_after(" 120 ", now=NOW) == NOW + timedelta(seconds=120)


def test_retry_after_http_date() -> None:
    """Поддерживается HTTP-date, а не только число секунд."""
    assert parse_retry_after("Mon, 05 Oct 2026 12:02:00 GMT", now=NOW) == (
        NOW + timedelta(minutes=2)
    )


def test_past_retry_after_does_not_move_time_backwards() -> None:
    """Прошлая дата не назначает работу до сохранённого момента ошибки."""
    assert parse_retry_after("Mon, 05 Oct 2026 11:00:00 GMT", now=NOW) == NOW

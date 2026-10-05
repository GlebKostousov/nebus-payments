"""Webhook уходит только на origin из настроек."""

import ipaddress
from dataclasses import dataclass

from pydantic import HttpUrl, TypeAdapter, ValidationError

from app.core.errors import WebhookDestinationError

URL_ADAPTER = TypeAdapter(HttpUrl)


def origin(url: HttpUrl) -> str:
    """Собрать origin с портом. IPv6 остаётся в скобках."""
    host = url.host
    if host is None:
        message = "URL без host"
        raise WebhookDestinationError(message)
    port = url.port or (443 if url.scheme == "https" else 80)
    return f"{url.scheme}://{host}:{port}"


@dataclass(frozen=True, slots=True)
class WebhookPolicy:
    """Список точных origin. Куда резолвится имя, решает сеть."""

    allowed_origins: frozenset[str]
    allow_development: bool = False

    @classmethod
    def build(
        cls, origins: tuple[str, ...], *, allow_development: bool = False
    ) -> "WebhookPolicy":
        """Принять список origin. Путь и query в настройке недопустимы."""
        normalized: set[str] = set()
        for value in origins:
            url = URL_ADAPTER.validate_python(value)
            if url.path not in (None, "/") or url.query is not None:
                message = "В allowlist нужны origin, без пути и query"
                raise ValueError(message)
            cls._validate_url(url, allow_development=allow_development)
            normalized.add(origin(url))
        return cls(frozenset(normalized), allow_development)

    def check(self, value: str) -> None:
        """Проверить адрес при приёме платежа и непосредственно перед отправкой."""
        try:
            url = URL_ADAPTER.validate_python(value)
        except ValidationError as error:
            message = "Некорректный URL"
            raise WebhookDestinationError(message) from error
        self._validate_url(url, allow_development=self.allow_development)
        if origin(url) not in self.allowed_origins:
            message = "Origin не входит в allowlist"
            raise WebhookDestinationError(message)

    @staticmethod
    def _validate_url(url: HttpUrl, *, allow_development: bool) -> None:
        """Отклонить логин в URL, fragment и частные IP вне demo."""
        _reject_credentials(url)
        if allow_development:
            return
        _reject_production_target(url)


def _reject_credentials(url: HttpUrl) -> None:
    """Запретить userinfo и fragment в адресе webhook."""
    if url.username or url.password or url.fragment is not None:
        message = "Credentials и fragment запрещены"
        raise WebhookDestinationError(message)


def _reject_production_target(url: HttpUrl) -> None:
    """Вне demo оставить только HTTPS и публичный literal IP."""
    if url.scheme != "https":
        message = "Вне demo разрешён только HTTPS"
        raise WebhookDestinationError(message)
    _reject_private_ip(url)


def _reject_private_ip(url: HttpUrl) -> None:
    """Отклонить literal-адрес вне глобальной маршрутизации."""
    host = (url.host or "").strip("[]")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return
    if not address.is_global:
        message = "Непубличный IP запрещён"
        raise WebhookDestinationError(message)

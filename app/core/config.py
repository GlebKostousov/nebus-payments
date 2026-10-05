"""Конфигурация проверяется до запуска сети и фоновых задач."""

from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_MIN_SECRET_LENGTH = 32


class Settings(BaseSettings):
    """Настройки одного экземпляра API или consumer."""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        frozen=True,
        hide_input_in_errors=True,
    )

    database_url: SecretStr
    rabbitmq_url: SecretStr
    api_key: SecretStr
    webhook_signing_secret: SecretStr
    webhook_allowed_origins: tuple[str, ...] = ()
    webhook_allow_development: bool = False
    json_logs: bool = True
    db_pool_size: int = Field(default=10, ge=1, le=100)
    db_statement_timeout_seconds: float = Field(default=5, gt=0, le=30)
    consumer_prefetch: int = Field(default=10, ge=1, le=100)
    attempt_timeout_seconds: float = Field(default=30, ge=10, le=120)
    processing_lease_seconds: float = Field(default=60, ge=20, le=300)
    outbox_poll_seconds: float = Field(default=0.25, ge=0.05, le=10)
    outbox_batch_size: int = Field(default=10, ge=1, le=100)
    publish_timeout_seconds: float = Field(default=5, ge=1, le=30)
    outbox_lease_seconds: float = Field(default=30, ge=5, le=120)
    recovery_interval_seconds: float = Field(default=2, ge=0.1, le=30)
    webhook_timeout_seconds: float = Field(default=10, ge=1, le=30)
    request_max_bytes: int = Field(default=65536, ge=1024, le=1048576)

    @model_validator(mode="after")
    def validate_runtime(self) -> Self:
        """Проверить длину секретов и что аренда длиннее внешнего вызова."""
        self._validate_secrets()
        self._validate_urls()
        self._validate_leases()
        return self

    def _validate_secrets(self) -> None:
        """Отклонить короткий API-ключ и секрет подписи webhook."""
        for secret in (self.api_key, self.webhook_signing_secret):
            if len(secret.get_secret_value()) < _MIN_SECRET_LENGTH:
                message = "Секреты должны содержать не менее 32 символов"
                raise ValueError(message)

    def _validate_urls(self) -> None:
        """Проверить схемы PostgreSQL и RabbitMQ до открытия соединений."""
        database_url = self.database_url.get_secret_value()
        if not database_url.startswith("postgresql+asyncpg://"):
            message = "Нужен PostgreSQL через asyncpg"
            raise ValueError(message)
        rabbitmq_url = self.rabbitmq_url.get_secret_value()
        if not rabbitmq_url.startswith(("amqp://", "amqps://")):
            message = "Нужен AMQP URL RabbitMQ"
            raise ValueError(message)

    def _validate_leases(self) -> None:
        """Сверить аренды с дедлайнами внешних вызовов."""
        self._validate_processing_lease()
        self._validate_outbox_lease()

    def _validate_processing_lease(self) -> None:
        """Оставить запас на шлюз и webhook внутри одной попытки."""
        if self.processing_lease_seconds <= self.attempt_timeout_seconds + 10:
            message = "Аренда обработки должна превышать дедлайн минимум на 10 с"
            raise ValueError(message)
        if self.attempt_timeout_seconds <= self.webhook_timeout_seconds + 5:
            message = "Дедлайн обработки должен вмещать шлюз и webhook"
            raise ValueError(message)

    def _validate_outbox_lease(self) -> None:
        """Публикация не должна переживать свою аренду."""
        if self.outbox_lease_seconds <= self.publish_timeout_seconds + 5:
            message = "Аренда outbox должна превышать дедлайн публикации на 5 с"
            raise ValueError(message)

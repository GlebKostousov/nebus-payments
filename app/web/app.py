"""Маршруты API: создать платёж, прочитать статус, проверить готовность."""

from collections.abc import Awaitable, Callable
from typing import cast
from uuid import UUID

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from starlette.types import Lifespan

from app.core.config import Settings
from app.core.errors import ServiceError
from app.core.logging import get_logger
from app.entities.payment.schema import CreatePayment, PaymentAccepted, PaymentRead

from ._deps import ApplicationContext, IdempotencyDependency, PaymentsDependency
from .guard import ApiGuard

logger = get_logger(__name__)


def build_http_app(
    context: ApplicationContext,
    settings: Settings,
    *,
    lifespan: Lifespan[FastAPI] | None = None,
) -> FastAPI:
    """Собрать маршруты из уже созданных зависимостей."""
    app = FastAPI(
        title="Payments",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.context = context
    app.add_middleware(
        ApiGuard,
        api_key=settings.api_key.get_secret_value(),
        max_bytes=settings.request_max_bytes,
    )
    _mount(app)
    return app


def _mount(app: FastAPI) -> None:
    """Подключить маршруты и обработчики ошибок."""
    app.add_api_route(
        "/api/v1/payments",
        create_payment,
        methods=["POST"],
        status_code=status.HTTP_202_ACCEPTED,
    )
    app.add_api_route("/api/v1/payments/{payment_id}", get_payment, methods=["GET"])
    app.add_api_route(
        "/health/ready", readiness, methods=["GET"], include_in_schema=False
    )
    app.add_api_route(
        "/openapi.json",
        _openapi(app),
        methods=["GET"],
        include_in_schema=False,
    )
    app.add_exception_handler(ServiceError, application_error)
    app.add_exception_handler(RequestValidationError, invalid_request)
    app.add_exception_handler(SQLAlchemyError, storage_error)
    app.add_exception_handler(Exception, unexpected_error)


def _openapi(app: FastAPI) -> Callable[[], Awaitable[JSONResponse]]:
    """Закрыть схему на конкретный экземпляр приложения."""

    async def openapi_schema() -> JSONResponse:
        """Выдать схему под тем же API-ключом, что и остальные маршруты."""
        return JSONResponse(app.openapi())

    return openapi_schema


async def create_payment(
    body: CreatePayment,
    payments: PaymentsDependency,
    idempotency_key: IdempotencyDependency,
) -> PaymentAccepted:
    """Принять запрос; текущее состояние всегда доступно через GET."""
    return await payments.create(body, idempotency_key=idempotency_key)


async def get_payment(payment_id: UUID, payments: PaymentsDependency) -> PaymentRead:
    """Получить финансовый результат и состояние доставки уведомления."""
    return PaymentRead.from_view(await payments.get(payment_id))


async def readiness(payments: PaymentsDependency) -> dict[str, str]:
    """Проверить PostgreSQL; API может принимать платежи без доступного RabbitMQ."""
    await payments.ping()
    return {"status": "ready"}


async def application_error(_request: Request, error: Exception) -> JSONResponse:
    """Не раскрывать internal_message в HTTP-ответе."""
    service_error = cast(ServiceError, error)
    return JSONResponse(
        {"code": service_error.code, "message": service_error.public_message},
        status_code=service_error.http_status,
    )


async def invalid_request(_request: Request, error: Exception) -> JSONResponse:
    """Не отражать входные metadata и секреты в ошибке валидации."""
    validation_error = cast(RequestValidationError, error)
    fields = [
        ".".join(str(part) for part in item["loc"])
        for item in validation_error.errors()
    ]
    return JSONResponse(
        {
            "code": "validation_error",
            "message": "Проверьте поля запроса",
            "fields": fields,
        },
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
    )


async def storage_error(request: Request, error: Exception) -> JSONResponse:
    """Недоступность хранилища не маскируется успешным пустым ответом."""
    logger.error(
        "http.storage_failed",
        error_type=type(error).__name__,
        request_id=request.scope.get("state", {}).get("request_id"),
    )
    return JSONResponse(
        {
            "code": "storage_unavailable",
            "message": "Сейчас не получается обратиться к данным. Повторите запрос.",
        },
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
    )


async def unexpected_error(request: Request, error: Exception) -> JSONResponse:
    """Последняя безопасная граница: сообщение исключения может содержать секреты."""
    logger.error(
        "http.failed",
        error_type=type(error).__name__,
        request_id=request.scope.get("state", {}).get("request_id"),
    )
    return JSONResponse(
        {"code": "internal_error", "message": "Не получилось обработать запрос"},
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    )

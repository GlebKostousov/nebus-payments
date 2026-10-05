"""Деньги и идемпотентность проверяются до любой записи в БД."""

import json
from decimal import Decimal

import pytest
from app.core.messages import PaymentJob
from app.entities.payment.schema import CreatePayment
from pydantic import ValidationError

from tests.factories import request


@pytest.mark.parametrize(
    "amount", ["0", "-1", "1.001", "NaN", "Infinity", "10000000000000000.00"]
)
def test_invalid_money(amount: str) -> None:
    """Не округлять и не принимать отрицательные, бесконечные и слишком большие суммы."""
    with pytest.raises(ValidationError):
        request(amount=amount)


@pytest.mark.parametrize("amount", [1, 1.5, True, None])
def test_json_numbers_are_not_money(amount: object) -> None:
    """Сумма приходит строкой."""
    data = request().model_dump(mode="json")
    data["amount"] = amount
    with pytest.raises(ValidationError):
        CreatePayment.model_validate(data)


def test_amount_is_normalized_without_changing_value() -> None:
    """Эквивалентные записи суммы дают одинаковый fingerprint."""
    assert request(amount="1500.0").amount == Decimal("1500.00")
    assert request(amount="1500.0").fingerprint() == request().fingerprint()


def test_metadata_order_is_not_part_of_identity() -> None:
    """Порядок ключей в metadata не меняет отпечаток запроса."""
    left = request().model_copy(update={"metadata": {"a": 1, "b": {"x": 2, "y": 3}}})
    right = request().model_copy(update={"metadata": {"b": {"y": 3, "x": 2}, "a": 1}})
    assert left.fingerprint() == right.fingerprint()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("description", "Другой заказ"),
        ("currency", "EUR"),
        ("metadata", {"order_id": "456"}),
        ("webhook_url", "https://other.example/hook"),
    ],
)
def test_meaningful_fields_change_fingerprint(field: str, value: object) -> None:
    """Webhook и metadata так же значимы, как сумма и валюта."""
    data = request().model_dump(mode="json")
    data[field] = value
    assert CreatePayment.model_validate(data).fingerprint() != request().fingerprint()


@pytest.mark.parametrize("metadata", [{"value": float("nan")}, {"value": "x" * 8193}])
def test_invalid_metadata(metadata: dict[str, object]) -> None:
    """Не пропускать oversized JSON и нечисловые значения."""
    data = request().model_dump(mode="json")
    data["metadata"] = metadata
    with pytest.raises(ValidationError):
        CreatePayment.model_validate(data)


def test_unknown_field_is_rejected() -> None:
    """Опечатка клиента не должна молча исчезать."""
    data = request().model_dump(mode="json")
    data["ammount"] = "20.00"
    with pytest.raises(ValidationError):
        CreatePayment.model_validate(data)


def test_amount_response_is_decimal_string() -> None:
    """Сериализация не превращает Decimal обратно во float."""
    assert json.loads(request().model_dump_json())["amount"] == "1500.00"


@pytest.mark.parametrize("body", [b"not json", b"{}", b'{"schema_version": 999}'])
def test_invalid_broker_payload(body: bytes) -> None:
    """Сообщения очереди проходят собственную валидацию Pydantic."""
    with pytest.raises(ValidationError):
        PaymentJob.model_validate_json(body)

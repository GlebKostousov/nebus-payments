"""Аргументы рабочей очереди должны доезжать до брокера."""

import pamqp.encode
from app.gateways.rabbitmq.main import Topology


def test_working_queue_disables_broker_delivery_limit() -> None:
    """Лимит передоставок снят значением, которое клиент способен закодировать."""
    limit = Topology.build().queue.arguments["x-delivery-limit"]
    assert limit < -128
    assert pamqp.encode.table_integer(limit).startswith(b"s")

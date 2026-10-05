"""Сверить SQL ревизии с моделями, не поднимая сервер."""

import re
import subprocess
import sys
from pathlib import Path

from app.core.db import Base
from app.entities.outbox.model import Outbox
from app.entities.payment.model import Payment


def test_offline_revision_has_the_same_constraint_names_as_models() -> None:
    """Готовые имена ограничений не переименовываются второй раз."""
    assert Payment.metadata is Outbox.metadata is Base.metadata
    result = subprocess.run(  # noqa: S603 — команда фиксирована, shell не используется
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=True,
        timeout=20,
    )
    names = set(re.findall(r"CONSTRAINT ([a-z_]+)", result.stdout))
    expected = {
        str(constraint.name)
        for table in Base.metadata.tables.values()
        for constraint in table.constraints
    }
    assert names - {"alembic_version_pkc"} == expected
    assert "CREATE TABLE payments" in result.stdout
    assert "CREATE TABLE outbox" in result.stdout

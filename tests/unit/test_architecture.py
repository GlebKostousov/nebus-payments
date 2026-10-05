"""Небольшие стражи для правил, которые легко случайно нарушить при правке."""

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SOURCES = tuple(sorted((ROOT / "app").rglob("*.py")))


def test_native_annotations_and_import_direction() -> None:
    """В коде нет from __future__, импорта через несколько точек и typing.Any."""
    violations = [item for path in SOURCES for item in _import_violations(path)]
    assert not violations, violations


def _import_violations(path: Path) -> list[str]:
    """Собрать запрещённые импорты одного модуля."""
    violations: list[str] = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.ImportFrom):
            _collect_import_from(path, node, violations)
    return violations


def _collect_import_from(
    path: Path, node: ast.ImportFrom, violations: list[str]
) -> None:
    """Добавить нарушение, если импорт выходит за разрешённые границы."""
    if node.level > 1 or node.module == "__future__":
        violations.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    if node.module == "typing" and any(alias.name == "Any" for alias in node.names):
        violations.append(f"{path.relative_to(ROOT)}:{node.lineno}")


def test_repositories_do_not_own_commit() -> None:
    """Репозиторий не вызывает commit."""
    for path in (ROOT / "app/entities").rglob("repository.py"):
        _assert_no_commit(path)


def _assert_no_commit(path: Path) -> None:
    """Проверить, что репозиторий не вызывает commit."""
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr != "commit", path


@pytest.mark.parametrize("layer", ["core", "entities", "gateways", "workflows"])
def test_layer_dependencies(layer: str) -> None:
    """Слой импортирует только то, что ему положено."""
    forbidden = {
        "core": ("entities", "gateways", "workflows", "web", "workers"),
        "entities": ("gateways", "workflows", "web", "workers"),
        "gateways": ("entities", "workflows", "web", "workers"),
        "workflows": ("gateways", "web", "workers"),
    }
    prefixes = tuple(f"app.{name}" for name in forbidden[layer])
    for path in (ROOT / "app" / layer).rglob("*.py"):
        _assert_layer_imports(path, prefixes)


def _assert_layer_imports(path: Path, prefixes: tuple[str, ...]) -> None:
    """Проверить импорты одного файла слоя."""
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        _assert_import_node(path, node, prefixes)


def _assert_import_node(path: Path, node: ast.AST, prefixes: tuple[str, ...]) -> None:
    """Отклонить импорт верхнего запрещённого пакета."""
    if isinstance(node, ast.ImportFrom) and node.module:
        assert not node.module.startswith(prefixes), path
    if isinstance(node, ast.Import):
        assert not any(alias.name.startswith(prefixes) for alias in node.names), path

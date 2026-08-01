"""Grep-able CI gates (AGENTS.global.md rule 9).

These are the rules that are cheap to state, expensive to violate, and easy to
re-introduce in a hurry. A test is the only thing that keeps them true.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "pipelines"


def _python_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.py"))


def _code_only(path: Path) -> str:
    """Source with docstrings and full-line comments removed.

    Docstrings in this repo deliberately quote the rules they enforce -- ``session.py``
    explains where ``dbutils`` may appear, ``framework/__init__.py`` says what L1 must
    not mention. Grepping raw source would flag the explanation as the violation.

    Other string literals are kept, because SQL lives in them and the CREATE TABLE gate
    needs to see it.
    """
    source = path.read_text()
    tree = ast.parse(source)
    blank: set[int] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
            and node.end_lineno is not None
        ):
            blank.update(range(node.lineno, node.end_lineno + 1))
    return "\n".join(
        line
        for i, line in enumerate(source.splitlines(), 1)
        if i not in blank and not line.lstrip().startswith("#")
    )


def _sql_literals(path: Path) -> list[str]:
    """Every non-docstring string constant in a module.

    A DDL *statement* begins the string it lives in. Prose that names DDL -- preflight's
    error message says "Do NOT add CREATE TABLE here" -- does not. Anchoring the match
    to the start of the literal is what separates issuing DDL from talking about it.
    """
    tree = ast.parse(path.read_text())
    docstring_nodes = {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstring_nodes
    ]


def _issues_ddl(path: Path, keywords: str) -> bool:
    pattern = re.compile(rf"^\s*CREATE\s+(OR\s+REPLACE\s+)?({keywords})\b", re.IGNORECASE)
    return any(pattern.match(literal) for literal in _sql_literals(path))


def test_the_package_never_issues_create_table() -> None:
    """Rule 1. Liquibase owns DDL; a CREATE TABLE here forks the schema away from
    DATABASECHANGELOG and destroys the migration audit trail.

    ``tools/local_ddl.py`` does render CREATE TABLE, which is exactly why it lives
    outside this package (ADR-004).
    """
    offenders = [
        str(path.relative_to(PACKAGE))
        for path in _python_files(PACKAGE)
        if _issues_ddl(path, "TABLE|VIEW")
    ]
    assert not offenders, f"CREATE TABLE found in the shipped package: {offenders}"


def test_the_package_never_issues_create_schema_or_catalog() -> None:
    """Repo 2's Terraform owns catalogs, schemas and volumes."""
    offenders = [
        str(path.relative_to(PACKAGE))
        for path in _python_files(PACKAGE)
        if _issues_ddl(path, "SCHEMA|DATABASE|CATALOG|VOLUME")
    ]
    assert not offenders


def test_the_guard_would_catch_a_real_create_table() -> None:
    """A guard nobody has seen fail is a guard nobody knows works."""
    offender = PACKAGE.parents[1] / "tools" / "local_ddl.py"
    assert _issues_ddl(offender, "TABLE|VIEW")


def test_dbutils_appears_only_in_entrypoints() -> None:
    """Rule 12. Anything touching dbutils cannot run in the local suite, and the local
    suite is where the two tests that decide this project run."""
    offenders = [
        str(path.relative_to(PACKAGE))
        for path in _python_files(PACKAGE)
        if "dbutils" in _code_only(path) and path.parent.name != "entrypoints"
    ]
    assert not offenders, f"dbutils used outside entrypoints: {offenders}"


def test_no_display_calls() -> None:
    offenders = [
        str(path.relative_to(PACKAGE))
        for path in _python_files(PACKAGE)
        if re.search(r"(?<![\w.])display\s*\(", _code_only(path))
    ]
    assert not offenders


def test_the_framework_layer_knows_nothing_about_edgar() -> None:
    """L1 is generic. If it starts mentioning accession numbers, it belongs in L2-L5."""
    forbidden = ("accession", "cik", "xbrl", "10-K", "edgar.silver", "edgar.gold")
    offenders = []
    for path in _python_files(PACKAGE / "framework"):
        code = _code_only(path)
        for term in forbidden:
            if term.lower() in code.lower():
                offenders.append(f"{path.name}: {term}")
    assert not offenders, f"EDGAR domain terms leaked into L1: {offenders}"


def test_the_wheel_does_not_depend_on_pyspark() -> None:
    """Installing PySpark into a Databricks job shadows the runtime build."""
    pyproject = (PACKAGE.parents[1] / "pyproject.toml").read_text()
    runtime_block = pyproject.split("dependencies = [", 1)[1].split("]", 1)[0]
    assert "pyspark" not in runtime_block
    assert "delta" not in runtime_block


def test_entrypoints_contain_no_logic() -> None:
    """Rule: entrypoints are thin. A long one means transform logic crept in where the
    local suite cannot reach it."""
    for path in _python_files(PACKAGE / "entrypoints"):
        body = [
            line
            for line in path.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        assert len(body) < 60, f"{path.name} has {len(body)} lines; move logic down a layer"


@pytest.mark.parametrize(
    "module",
    ["pipelines.contracts", "pipelines.config", "pipelines.contracts.schemas", "tools.local_ddl"],
)
def test_contract_and_config_modules_do_not_pull_in_pyspark(module: str) -> None:
    """CI's contract-compat job and the DDL renderer both run without Spark.

    Checked in a subprocess: importing these in-process proves nothing once the Spark
    fixtures have already loaded pyspark.
    """
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c", f"import {module}, sys; print('pyspark' in sys.modules)"],
        capture_output=True,
        text=True,
        cwd=PACKAGE.parents[1],
        check=True,
    )
    assert result.stdout.strip() == "False", f"{module} imported pyspark at module scope"

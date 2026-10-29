"""Every package the core install or an extra declares is imported by desflow's code.

A package used only by docs, examples or tests belongs in the dev dependency group,
and the page that uses it says how to install it. A requirement names a feature
extra of such a package only when a tested behaviour needs it, as FEATURE_EXTRAS
lists.
"""

import ast
import importlib.metadata
import re
import tomllib
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]

# The feature extras a declared requirement may name, each with what needs it:
# aiokafka's lz4 is the default `compression_type` of KafkaEgress; pyiceberg's
# pyarrow is how it writes, sql-postgres serves the SQL catalogs on SQLite and
# PostgreSQL (tests/unit/test_egress_iceberg.py,
# tests/integration/test_iceberg_catalogs.py) and glue the Glue catalog (the same
# integration module).
FEATURE_EXTRAS = {
    "aiokafka": {"lz4"},
    "pyiceberg": {"pyarrow", "sql-postgres", "glue"},
}


def _distribution_name(requirement: str) -> str:
    """The normalised distribution name of a requirement string (PEP 503)."""
    name = re.split(r"[\[<>=!~;\s]", requirement, maxsplit=1)[0]
    return re.sub(r"[-_.]+", "-", name).lower()


def _imported_modules() -> set[str]:
    """The top-level module names that `src/desflow` imports, anywhere in a file."""
    modules: set[str] = set()
    for path in (ROOT / "src" / "desflow").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                modules.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                modules.add(node.module.split(".")[0])
    return modules


def _installed(distribution: str) -> bool:
    try:
        importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return False
    return True


def test_every_declared_dependency_is_imported_by_desflow():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    declared = {_distribution_name(r) for r in project["dependencies"]}
    for requirements in project["optional-dependencies"].values():
        declared.update(_distribution_name(r) for r in requirements)
    declared.discard("desflow")
    missing = sorted(name for name in declared if not _installed(name))
    if missing:
        pytest.skip(f"run with --all-extras: {', '.join(missing)} not installed")

    imported = _imported_modules()
    used = {
        _distribution_name(distribution)
        for module, distributions in importlib.metadata.packages_distributions().items()
        if module in imported
        for distribution in distributions
    }

    assert declared - used == set()


def test_every_feature_extra_is_one_a_tested_behaviour_needs():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    requirements = list(project["dependencies"])
    for extra in project["optional-dependencies"].values():
        requirements.extend(extra)
    named = {}
    for requirement in requirements:
        match = re.match(r"\s*([A-Za-z0-9_.-]+)\[([^\]]*)\]", requirement)
        name = _distribution_name(requirement)
        if match and name != "desflow":
            named.setdefault(name, set()).update(
                extra.strip() for extra in match.group(2).split(",")
            )

    unexpected = {
        name: extras - FEATURE_EXTRAS.get(name, set())
        for name, extras in named.items()
        if extras - FEATURE_EXTRAS.get(name, set())
    }
    assert unexpected == {}

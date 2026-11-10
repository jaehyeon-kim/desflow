"""The extra names are frozen: 1.x keeps every extra of `pip install desflow[<extra>]`."""

import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"

# Every key of `[project.optional-dependencies]` at 1.0. A 1.x release may add an
# extra; it may not remove or rename one.
FROZEN_EXTRAS = {
    "kafka",
    "redis",
    "postgres",
    "confluent",
    "glue",
    "parquet",
    "iceberg",
    "fit",
    "watch",
    "fake",
    "all",
}


def test_promised_extras_exist():
    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]
    extras = set(project["optional-dependencies"])
    missing = sorted(FROZEN_EXTRAS - extras)
    assert not missing, (
        "The extra names are frozen from 1.0: a 1.x release keeps every extra and may "
        f"only add one (Installation; Compatibility Policy). Missing: {missing}."
    )

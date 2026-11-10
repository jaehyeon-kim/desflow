"""Where the benchtop projects are: the copy in `tests/copied_projects/benchtop`, which the
tests read and never change.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "copied_projects" / "benchtop"


@pytest.fixture(scope="session")
def project():
    """Gives a function that makes a benchtop project importable and returns its path;
    no bytecode is written into the project."""

    def find(name: str) -> Path:
        path = ROOT / name
        if not path.is_dir():
            raise FileNotFoundError(f"the benchtop project {name} is not at {path}")
        sys.dont_write_bytecode = True
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
        return path

    return find

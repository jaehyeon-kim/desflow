"""Where the benchtop projects are. They are read and never changed.

The folder is `DESFLOW_BENCHTOP` when set, otherwise `~/personal/benchtop`. A test that
needs a project skips when the folder is not there.
"""

import os
import sys
from pathlib import Path

import pytest


def _root() -> Path:
    return Path(os.environ.get("DESFLOW_BENCHTOP", "~/personal/benchtop")).expanduser()


@pytest.fixture(scope="session")
def project():
    """Gives a function that makes a benchtop project importable and returns its path;
    no bytecode is written into the project."""

    def find(name: str) -> Path:
        path = _root() / name
        if not path.is_dir():
            pytest.skip(f"the benchtop project {name} is not at {path}")
        sys.dont_write_bytecode = True
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
        return path

    return find

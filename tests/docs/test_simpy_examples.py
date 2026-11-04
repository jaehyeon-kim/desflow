"""SimPy's example files under docs/snippets/simpy/ print what SimPy ships beside them.

Each `<name>.py` is SimPy 4.1.2's `docs/examples/code/<name>.py` with SimPy's MIT
notice and the snippet markers added, and each `<name>.out` is SimPy's own output
file. Running the file with the locked SimPy and comparing what it prints with the
`.out` file shows that the code and output a page shows are SimPy's own.
"""

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SIMPY = ROOT / "docs" / "snippets" / "simpy"
FILES = sorted(SIMPY.glob("*.py"))


def test_every_file_has_its_output():
    assert len(FILES) == 7
    assert sorted(p.stem for p in SIMPY.glob("*.out")) == [p.stem for p in FILES]


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.stem)
def test_simpy_file_prints_its_output(path):
    run = subprocess.run(
        [sys.executable, "-I", str(path)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=True,
    )
    expected = path.with_suffix(".out").read_text(encoding="utf-8")
    assert run.stdout.replace("\r\n", "\n") == expected.replace("\r\n", "\n")

"""Checks that no file of a reviewer's folder names the version of the run it holds.

Usage:

    python check_hidden.py FOLDER...

Each FOLDER is searched through, every file in it read as text (a `.gz` file unpacked). A
file fails when it names a suite or a version: `before20`, `noskill`, `suite-a` or `suite-b`
as a word, or `before` or `after` as a part of a path (`before/`, `/after`). The script
prints each file and line that does, and exits 1 when there is one. `grade.py` writes the
folders this checks (`--records-out`), and `run_task.md` step 6 runs it on all of them.
"""

import gzip
import re
import sys
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

NAMES = re.compile(
    r"(?<![\w-])(?:before20|noskill|suite-a|suite-b)(?![\w-])"
    r"|(?<![\w-])(?:before|after)(?=/)"
    r"|(?<=/)(?:before|after)(?![\w-])"
)


def _texts(folder: Path) -> Iterator[Tuple[Path, str]]:
    """Yields each file under a folder with its text."""
    for path in sorted(folder.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
                yield path, f.read()
        else:
            yield path, path.read_text("utf-8", "replace")


def leaks(folder: Path) -> List[str]:
    """Returns `FILE:LINE: NAME` for each name of a version in a folder's files."""
    found = []
    for path, text in _texts(folder):
        for number, line in enumerate(text.splitlines(), 1):
            for match in NAMES.finditer(line):
                found.append(f"{path}:{number}: {match.group(0)}")
    return found


def main(argv: Optional[List[str]] = None) -> int:
    """Checks each folder given; returns 1 when a file names a version."""
    folders = [Path(a) for a in (sys.argv[1:] if argv is None else argv)]
    if not folders:
        print(__doc__)
        return 2
    found = []
    for folder in folders:
        found += leaks(folder)
    for text in found:
        print(text)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())

"""Regenerate the docs blocks that show records, from a run of the code beside them.

A block of records on a page is marked by a comment line naming it, directly above
its fence:

    <!-- records: first-trace -->
    ```text
    [EVT] {...}
    ```

The comment can be indented, inside a content tab, and the fence then carries the
same indent. Each name in `BLOCKS` runs a snippet or an example unpaced and keeps the
records the page explains; the functions are in `scripts/blocks/`, one module for
each group of pages. The `timestamp` of a record is the wall-clock time the run
started plus its simulation time, so it differs on every run and is shown as `'...'`.

Usage:
    python scripts/record_blocks.py          # rewrite every block
    python scripts/record_blocks.py --check  # list the blocks that are out of date
"""

import re
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
# The generators are a package beside this file, which a test loads by its path
sys.path.insert(0, str(ROOT / "scripts"))

from blocks import BLOCKS  # noqa: E402

_MARKER = re.compile(
    r"^(?P<indent> *)<!-- records: (?P<name>[\w-]+) -->\n"
    r"(?P=indent)```text\n(?P<body>.*?)\n(?P=indent)```$",
    re.S | re.M,
)


def _pages() -> List[Path]:
    return sorted(DOCS.rglob("*.md"))


def used() -> Dict[str, List[Path]]:
    """Every block name the pages use, with the pages that use it."""
    found: Dict[str, List[Path]] = {}
    for page in _pages():
        for match in _MARKER.finditer(page.read_text(encoding="utf-8")):
            found.setdefault(match["name"], []).append(page)
    return found


def _rewrite(page: Path, cache: Dict[str, List[str]]) -> str:
    text = page.read_text(encoding="utf-8")

    def replace(match: re.Match) -> str:
        indent, name = match["indent"], match["name"]
        if name not in cache:
            cache[name] = BLOCKS[name]()
        body = "\n".join(indent + line for line in cache[name])
        return f"{indent}<!-- records: {name} -->\n{indent}```text\n{body}\n{indent}```"

    return _MARKER.sub(replace, text)


def stale() -> List[str]:
    """The pages whose blocks differ from a new run, as `page: name` lines."""
    cache: Dict[str, List[str]] = {}
    out = []
    for page in _pages():
        text = page.read_text(encoding="utf-8")
        if not _MARKER.search(text):
            continue
        if _rewrite(page, cache) != text:
            names = sorted({m["name"] for m in _MARKER.finditer(text)})
            out.append(f"{page.relative_to(ROOT)}: {', '.join(names)}")
    return out


def main(argv: List[str]) -> int:
    """Rewrites every record block on the pages, or reports the stale ones.

    Args:
        argv: The command-line arguments; `--check` reports instead of writing.

    Returns:
        int: 1 when `--check` finds a stale block, else 0.
    """
    if "--check" in argv:
        out = stale()
        print("\n".join(out) or "Every block is up to date.")
        return 1 if out else 0
    cache: Dict[str, List[str]] = {}
    for page in _pages():
        text = page.read_text(encoding="utf-8")
        if _MARKER.search(text):
            new = _rewrite(page, cache)
            if new != text:
                page.write_text(new, encoding="utf-8")
                print(f"rewrote {page.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

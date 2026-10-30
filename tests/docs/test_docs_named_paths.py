"""The files for contributors and assistants name only files that exist.

`README.md`, `CLAUDE.md` and `CONTRIBUTING.md` name files in backticks, often as a bare
name inside a folder's row of the code map. A file moved or renamed without these docs
leaves the name stale, so every name that looks like a path must be the end of a
tracked path.
"""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_DOCS = ["README.md", "CLAUDE.md", "CONTRIBUTING.md"]
# Names of files a reader or a command creates, not files in the repository.
_NOT_FILES = {"report.md", "first_shop.yaml"}
_SUFFIXES = (
    ".py",
    ".md",
    ".yml",
    ".yaml",
    ".json",
    ".txt",
    ".toml",
    ".sh",
    ".drawio",
    ".csv",
)


def _tracked_paths() -> set[str]:
    listed = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files"], capture_output=True, text=True, check=True
    )
    paths = set()
    for line in listed.stdout.splitlines():
        parts = line.split("/")
        for start in range(len(parts)):
            for end in range(start + 1, len(parts) + 1):
                paths.add("/".join(parts[start:end]))
    return paths


def _named_paths(text: str) -> set[str]:
    names = set()
    for name in re.findall(r"`([^`\s]+)`", text):
        if re.search(r"[<>*{}$~:=()]|^-|^\.|^http", name) or name in _NOT_FILES:
            continue
        if "/" in name or name.endswith(_SUFFIXES):
            names.add(name.rstrip("/"))
    return names


def test_named_paths_exist():
    tracked = _tracked_paths()
    missing = [
        f"{doc}: {name}"
        for doc in _DOCS
        for name in sorted(_named_paths((ROOT / doc).read_text()))
        if name not in tracked
    ]
    assert missing == []

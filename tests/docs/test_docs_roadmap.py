"""The public roadmap follows the version in `pyproject.toml`.

The release commit bumps the version and, in the same commit, moves that release from
the timeline to Released and makes the next release the current entry. These tests fail
when a release is tagged with the roadmap still showing it as planned or current.
"""

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_ROADMAP = (ROOT / "docs" / "about" / "roadmap.md").read_text()
_VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
_PLANNED, _RELEASED = _ROADMAP.split("\n## Released\n", 1)
_ENTRIES = re.findall(r"^- title: '([^']*)'\n((?:  .*\n)+)", _PLANNED, re.MULTILINE)


def _version(text: str) -> tuple[int, ...] | None:
    match = re.match(r"(\d+)\.(\d+)\.(\d+):", text)
    return tuple(int(part) for part in match.groups()) if match else None


def test_the_current_version_is_under_released():
    assert f'??? note "{_VERSION}:' in _RELEASED


def test_no_planned_entry_is_a_released_version():
    current = _version(f"{_VERSION}:")
    stale = [title for title, _ in _ENTRIES if (v := _version(title)) and v <= current]
    assert stale == []


def test_one_entry_is_current_and_it_is_the_next_release():
    now = [title for title, body in _ENTRIES if "  key: now\n" in body]
    assert len(now) == 1
    assert _version(now[0]) > _version(f"{_VERSION}:")
    versions = [v for title, _ in _ENTRIES if (v := _version(title))]
    assert _version(now[0]) == min(versions)


def test_every_planned_release_names_its_issues():
    missing = [
        title
        for title, body in _ENTRIES
        if _version(title) and not re.search(r"/issues/\d+\)", body)
    ]
    assert missing == []

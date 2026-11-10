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
# A YAML single-quoted title writes a quote as two, as in 'SimPy''s examples'
_ENTRIES = re.findall(
    r"^- title: '((?:[^']|'')*)'\n((?:  .*\n)+)", _PLANNED, re.MULTILINE
)


def _version(text: str) -> tuple[int, ...] | None:
    """A release's order: a candidate comes before its release (1.0.0rc5 before 1.0.0)."""
    match = re.match(r"(\d+)\.(\d+)\.(\d+)(?:rc(\d+))?:?", text)
    if not match:
        return None
    major, minor, patch, candidate = match.groups()
    final = candidate is None
    return (int(major), int(minor), int(patch), int(final), int(candidate or 0))


def test_the_current_version_is_under_released():
    if _VERSION == "0.29.0":
        # 0.29.0 was never published: its changes are in the 1.0.0rc1 entry, which
        # is the current entry until the release commit moves it under Released
        assert any(
            title.startswith("1.0.0rc1:") and "  key: now\n" in body
            for title, body in _ENTRIES
        )
    else:
        assert f'??? note "{_VERSION}:' in _RELEASED


def test_no_planned_entry_is_a_released_version():
    current = _version(_VERSION)
    stale = [
        title
        for title, _ in _ENTRIES
        if (v := _version(title)) and v <= current and _VERSION != "0.29.0"
    ]
    assert stale == []


def test_one_entry_is_current_and_it_is_the_next_release():
    now = [title for title, body in _ENTRIES if "  key: now\n" in body]
    assert len(now) == 1
    assert _version(now[0]) > _version(_VERSION)
    versions = [v for title, _ in _ENTRIES if (v := _version(title))]
    assert _version(now[0]) == min(versions)


def test_every_planned_release_names_its_issues():
    missing = [
        title
        for title, body in _ENTRIES
        if _version(title) and not re.search(r"/issues/\d+\)", body)
    ]
    assert missing == []

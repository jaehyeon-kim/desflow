"""A GitHub release's text is its version's section of the docs Changelog."""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "release_notes.py"
_spec = importlib.util.spec_from_file_location("release_notes", _SCRIPT)
assert _spec is not None and _spec.loader is not None
release_notes = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(release_notes)

CHANGELOG = """# Changelog

Intro.

## Version 0.21.0

New distributions. See [Distributions](../reference/distributions.md#gamma) and the [home page](../index.md).

### Upgrading

- One line.

## Version 0.20.0

The rename.
"""


def test_section_is_the_body_without_its_heading():
    body = release_notes.section("0.21.0", CHANGELOG)
    assert body.startswith("New distributions.")
    assert "### Upgrading\n\n- One line." in body
    assert "0.20.0" not in body and "## Version" not in body


def test_the_last_section_runs_to_the_end():
    assert release_notes.section("0.20.0", CHANGELOG) == "The rename.\n"


def test_relative_links_point_at_the_latest_docs():
    body = release_notes.section("0.21.0", CHANGELOG)
    assert (
        "](https://jaehyeon.me/desflow/latest/reference/distributions/#gamma)" in body
    )
    assert "](https://jaehyeon.me/desflow/latest/)" in body


def test_a_missing_version_is_an_error():
    with pytest.raises(ValueError):
        release_notes.section("9.9.9", CHANGELOG)


def test_the_published_changelog_has_the_current_version():
    version = next(
        line.split('"')[1]
        for line in (_SCRIPT.parents[1] / "pyproject.toml").read_text().splitlines()
        if line.startswith("version = ")
    )
    assert release_notes.section(version, release_notes.CHANGELOG.read_text())

"""Sidebar titles stay short, headings carry no step numbers, and figures are images.

A sidebar title names the subject in 24 characters or fewer; the page's own title can
be longer and name the classes, except on a How-To page, whose title is its sidebar
title. Headings are names, and the order of the sections
shows the steps, so a heading never starts with a number. A figure is a draw.io
diagram, never box characters in a text block. Every tutorial and How-To page opens
with its level, so a reader knows which pages can wait.
"""

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MAX_TITLE = 24

# Kept on purpose: the example names both halves of its run.
LONG_TITLES_ALLOWED = {"Parquet History, Kafka Live"}

FENCE = re.compile(r"^```.*?^```", re.S | re.M)
BOX_CHARACTERS = re.compile(r"[\u2500-\u257f]")
NUMBERED = re.compile(r"^#{1,6} \d", re.M)


def _nav_titles(items):
    for item in items:
        if isinstance(item, dict):
            title, value = next(iter(item.items()))
            yield title
            if isinstance(value, list):
                yield from _nav_titles(value)


def _long_titles(nav):
    return {t for t in _nav_titles(nav) if len(t) > MAX_TITLE}


def _pages_with_numbered_headings():
    pages = set()
    for page in (ROOT / "docs").rglob("*.md"):
        text = FENCE.sub("", page.read_text(encoding="utf-8"))
        if NUMBERED.search(text):
            pages.add(str(page.relative_to(ROOT)))
    return pages


def _pages_with_text_diagrams():
    return {
        str(page.relative_to(ROOT))
        for page in (ROOT / "docs").rglob("*.md")
        if any(
            BOX_CHARACTERS.search(b)
            for b in FENCE.findall(page.read_text(encoding="utf-8"))
        )
    }


def test_sidebar_titles_are_short(mkdocs_nav):
    assert _long_titles(mkdocs_nav) - LONG_TITLES_ALLOWED == set(), (
        f"Sidebar titles over {MAX_TITLE} characters. Shorten the nav title and keep "
        "the full name in the page's own title."
    )


def _nav_pages(items):
    for item in items:
        if isinstance(item, dict):
            title, value = next(iter(item.items()))
            if isinstance(value, list):
                yield from _nav_pages(value)
            else:
                yield title, value


def test_how_to_titles_match_the_sidebar(mkdocs_nav):
    """A How-To page names one task, so its own title is its sidebar title."""
    differ = [
        f"{page}: {title!r}"
        for title, page in _nav_pages(mkdocs_nav)
        if page.startswith("how-to/")
        and not page.startswith("how-to/examples/")
        and page != "how-to/projects.md"
        and (ROOT / "docs" / page).read_text(encoding="utf-8").split("\n")[0]
        != f"# {title}"
    ]
    assert differ == [], "Give these pages their sidebar title: " + "; ".join(differ)


def test_headings_do_not_start_with_a_number():
    assert _pages_with_numbered_headings() == set(), (
        "Headings that start with a number. Name the section instead."
    )


def test_figures_are_not_drawn_in_text():
    assert _pages_with_text_diagrams() == set(), (
        "A figure drawn with box characters. Draw it in draw.io under "
        "docs/assets/diagrams/ and export it with scripts/diagrams.py."
    )


def test_no_paragraph_turns_into_a_heading():
    """A line of --- right under a paragraph makes that paragraph a heading."""
    found = []
    for page in (ROOT / "docs").rglob("*.md"):
        lines = FENCE.sub("", page.read_text(encoding="utf-8")).split("\n")
        for before, line in zip(lines, lines[1:]):
            if (
                re.match(r"^(-{3,}|={3,})\s*$", line)
                and before.strip()
                and not before.lstrip().startswith(("|", "#"))
            ):
                found.append(f"{page.relative_to(ROOT)}: {before[:60]}")
    assert found == [], (
        "Put a blank line between the paragraph and the rule: " + "; ".join(found)
    )


def test_no_heading_or_label_starts_with_the():
    """The writing rules: a heading, or a bold label opening a list item, never starts with "The"."""
    found = []
    for page in (ROOT / "docs").rglob("*.md"):
        text = FENCE.sub("", page.read_text(encoding="utf-8"))
        for line in text.split("\n"):
            if re.match(r"^#{1,6} The ", line) or re.match(r"^\s*[*-] \*\*The ", line):
                found.append(f"{page.relative_to(ROOT)}: {line[:60]}")
    assert found == [], "; ".join(found)

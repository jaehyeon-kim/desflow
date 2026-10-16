"""Print one version's Changelog section, as the text of its GitHub release.

The docs Changelog (`docs/about/changelog.md`) is the only place a release is described.
The GitHub release copies its section, so the two never differ:

    python scripts/release_notes.py 0.21.0 > /tmp/notes.md
    gh release create v0.21.0 --title v0.21.0 --notes-file /tmp/notes.md

The section's own heading is left out, because the release has its title. Links to other
docs pages are relative in the Changelog, so they become links to the latest docs. The
notes end with a Full Changelog link comparing the release with the tag before it.

Usage: python scripts/release_notes.py <version>
"""

import re
import subprocess
import sys
from pathlib import Path

CHANGELOG = Path(__file__).resolve().parents[1] / "docs" / "about" / "changelog.md"
DOCS_URL = "https://jaehyeon.me/desflow/latest/"
REPO_URL = "https://github.com/jaehyeon-kim/desflow"

_HEADING = re.compile(r"^## Version (\S+)\s*$", re.MULTILINE)
_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
_RELATIVE_LINK = re.compile(r"\]\((?!https?://|#|mailto:)([^)\s#]+)\.md(#[^)]*)?\)")


def _absolute(match: re.Match) -> str:
    """Turn a link relative to docs/about/ into a link to the latest docs."""
    target = (Path("about") / match.group(1)).as_posix()
    parts: list[str] = []
    for part in target.split("/"):
        if part == "..":
            parts.pop()
        elif part != ".":
            parts.append(part)
    page = "/".join(parts)
    page = "" if page == "index" else page.removesuffix("/index") + "/"
    return f"]({DOCS_URL}{page}{match.group(2) or ''})"


def section(version: str, text: str) -> str:
    """Return the body of `## Version <version>`, without its heading.

    Args:
        version (str): The version, such as `0.21.0`.
        text (str): The Changelog's Markdown.

    Returns:
        str: The section, with relative docs links made absolute.

    Raises:
        ValueError: If the Changelog has no section for the version.
    """
    headings = list(_HEADING.finditer(text))
    for i, heading in enumerate(headings):
        if heading.group(1) == version:
            end = headings[i + 1].start() if i + 1 < len(headings) else len(text)
            body = text[heading.end() : end].strip()
            return _RELATIVE_LINK.sub(_absolute, body) + "\n"
    raise ValueError(f"No '## Version {version}' section in {CHANGELOG}")


def previous_tag(version: str, tags: list[str]) -> str | None:
    """Return the highest `vX.Y.Z` tag below the version, or None for the first release.

    Args:
        version (str): The version being released, such as `0.21.0`.
        tags (list[str]): The repository's tags; others than `vX.Y.Z` are ignored.

    Returns:
        str | None: The previous release tag, such as `v0.20.0`.
    """
    current = tuple(int(part) for part in version.split("."))
    older = []
    for tag in tags:
        match = _TAG.match(tag)
        if match and tuple(map(int, match.groups())) < current:
            older.append((tuple(map(int, match.groups())), tag))
    return max(older)[1] if older else None


def notes(version: str, text: str, tags: list[str]) -> str:
    """Return the release notes: the Changelog section, then the Full Changelog link.

    Args:
        version (str): The version, such as `0.21.0`.
        text (str): The Changelog's Markdown.
        tags (list[str]): The repository's tags, to find the previous release.

    Returns:
        str: The text of the GitHub release.
    """
    body = section(version, text)
    previous = previous_tag(version, tags)
    if previous:
        body += f"\n**Full Changelog**: {REPO_URL}/compare/{previous}...v{version}\n"
    return body


def main() -> None:
    """Print the release notes for the version given on the command line."""
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    tags = subprocess.run(
        ["git", "tag", "--list", "v*"],
        cwd=CHANGELOG.parents[2],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    try:
        print(notes(sys.argv[1], CHANGELOG.read_text(encoding="utf-8"), tags), end="")
    except ValueError as error:
        sys.exit(str(error))


if __name__ == "__main__":
    main()

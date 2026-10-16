"""Print one version's Changelog section, as the text of its GitHub release.

The docs Changelog (`docs/about/changelog.md`) is the only place a release is described.
The GitHub release copies its section, so the two never differ:

    python scripts/release_notes.py 0.21.0 > /tmp/notes.md
    gh release create v0.21.0 --title v0.21.0 --notes-file /tmp/notes.md

The section's own heading is left out, because the release has its title. Links to other
docs pages are relative in the Changelog, so they become links to the latest docs.

Usage: python scripts/release_notes.py <version>
"""

import re
import sys
from pathlib import Path

CHANGELOG = Path(__file__).resolve().parents[1] / "docs" / "about" / "changelog.md"
DOCS_URL = "https://jaehyeon.me/desflow/latest/"

_HEADING = re.compile(r"^## Version (\S+)\s*$", re.MULTILINE)
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


def main() -> None:
    """Print the section for the version given on the command line."""
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    try:
        print(section(sys.argv[1], CHANGELOG.read_text(encoding="utf-8")), end="")
    except ValueError as error:
        sys.exit(str(error))


if __name__ == "__main__":
    main()

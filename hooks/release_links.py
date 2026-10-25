"""Points the site's links to the repository at the release tag instead of `main`.

The Markdown links to files on `main`, so the pages read correctly on GitHub. A reader
of a release's docs needs the code of that release, which `main` can have moved past,
so each rendered page has its `blob/main/`, `tree/main/` and raw `main/` links
rewritten to the tag `v<version>`, with the version read from `pyproject.toml`. The
tag is pushed after the docs of the release are deployed, so these links answer once
it exists.

The rewrite runs on `on_page_content`, before the llmstxt plugin turns the same HTML
into `llms-full.txt`, so the files for AI assistants carry the tag as well. A hook
runs after the plugins of the same priority, hence the higher priority.
"""

import tomllib
from pathlib import Path

from mkdocs.plugins import event_priority

_PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"
_REPOSITORY = "jaehyeon-kim/desflow"
_PREFIXES = (
    f"https://github.com/{_REPOSITORY}/blob/",
    f"https://github.com/{_REPOSITORY}/tree/",
    f"https://raw.githubusercontent.com/{_REPOSITORY}/",
)

_tag = ""


def release_tag() -> str:
    """The tag of the version in `pyproject.toml`, such as `v0.25.0`."""
    with _PYPROJECT.open("rb") as file:
        return "v" + str(tomllib.load(file)["project"]["version"])


def to_release(html: str, tag: str) -> str:
    """`html` with every link to a file on `main` pointed at `tag`.

    Args:
        html: A page's HTML.
        tag: The release tag, such as `v0.25.0`.

    Returns:
        str: The HTML with the links changed.
    """
    for prefix in _PREFIXES:
        html = html.replace(prefix + "main/", f"{prefix}{tag}/")
    return html


def on_config(config):
    """Reads the release tag on every build, so `mkdocs serve` follows a version bump.

    Args:
        config: The MkDocs config.

    Returns:
        The config, unchanged.
    """
    global _tag
    _tag = release_tag()
    return config


@event_priority(50)
def on_page_content(html, page, config, files):
    """Points the page's links to files on `main` at the release tag.

    Args:
        html: The page's HTML.
        page: The page.
        config: The MkDocs config.
        files: The site's files.

    Returns:
        str: The HTML with the links changed.
    """
    return to_release(html, _tag)

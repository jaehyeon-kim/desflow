"""llms.txt lists every page in the sidebar, in the sidebar's order.

The llmstxt plugin takes its page list from its own `sections` in mkdocs.yml, not
from the nav, so a page added to the nav and not to `sections` would be left out of
llms.txt without any warning.
"""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _config():
    return yaml.safe_load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"))


def _nav_pages(items):
    """The page files of the nav, in order, at any depth."""
    for item in items:
        value = next(iter(item.values())) if isinstance(item, dict) else item
        if isinstance(value, list):
            yield from _nav_pages(value)
        else:
            yield value


def _llmstxt_pages(config):
    plugin = next(
        p["llmstxt"]
        for p in config["plugins"]
        if isinstance(p, dict) and "llmstxt" in p
    )
    return [page for pages in plugin["sections"].values() for page in pages]


def test_llmstxt_lists_every_nav_page_in_order():
    config = _config()
    assert _llmstxt_pages(config) == list(_nav_pages(config["nav"])), (
        "The llmstxt sections in mkdocs.yml differ from the nav. Add or move the "
        "page in both."
    )

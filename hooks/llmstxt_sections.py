"""Fills the llmstxt plugin's sections from the nav, so the sidebar is the only page list.

mkdocs-llmstxt takes its pages only from its own `sections` setting and has no option
to follow the nav. It reads that setting when the files are collected, after every
`on_config`, so setting it here is early enough. Each top-level nav entry becomes a
section: a group with its pages, at any depth, or a single page under its own title.
"""


def _pages(items):
    for item in items:
        value = next(iter(item.values())) if isinstance(item, dict) else item
        if isinstance(value, list):
            yield from _pages(value)
        elif not value.startswith(("http://", "https://")):
            yield value


def on_config(config):
    sections = {}
    for item in config.nav:
        title, value = next(iter(item.items()))
        sections[title] = list(_pages(value if isinstance(value, list) else [item]))
    config.plugins["llmstxt"].config["sections"] = sections
    return config

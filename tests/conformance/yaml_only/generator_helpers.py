"""Functions the YAML-only blueprints name with `!python`."""

from typing import Any


def double(context: Any, x: int) -> int:
    """What `{gen: call}` calls: the context first, then the keyword arguments."""
    return x * 2

"""Python for docs/snippets/yaml/generated_values.yaml: a function a `call` generator uses."""

from typing import Any


def with_tax(context: Any, price: float) -> float:
    """The price with 20 percent tax added, to two decimals."""
    return round(price * 1.2, 2)

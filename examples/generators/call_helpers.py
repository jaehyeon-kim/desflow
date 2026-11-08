"""The function that `call.yaml` and its builder and low-level versions call."""

from typing import Any


def with_tax(context: Any, price: int) -> float:
    """Adds a tax of 10 per cent to `price`.

    Args:
        context: The `SimulationContext`, which `call` always passes first. The
            low-level script has none and passes None.
        price: The price before tax.

    Returns:
        float: The price with tax, rounded to cents.
    """
    return round(price * 1.1, 2)

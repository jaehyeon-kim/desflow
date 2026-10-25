"""The blocks of End Conditions, Patience and Live Changes."""

from typing import Callable, Dict, List

from .commands import records_with, resources_and_reasons


def end_explain() -> List[str]:
    """End Conditions: `desflow explain` on the busier shop, which is unstable."""
    return resources_and_reasons("docs/snippets/yaml/end_conditions.yaml")


def patience_reneged() -> List[str]:
    """Patience: the first two `reneged` events of `desflow run --stdout`."""
    return records_with("docs/snippets/yaml/patience.yaml", '"reneged"', 2)


def live_labels() -> List[str]:
    """Live Changes: the two labels of `desflow run --stdout`."""
    return records_with("docs/snippets/yaml/live_changes.yaml", '"label"', 2)


BLOCKS: Dict[str, Callable[[], List[str]]] = {
    "end-explain": end_explain,
    "patience-reneged": patience_reneged,
    "live-labels": live_labels,
}

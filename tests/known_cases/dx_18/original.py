"""DX-18: A sink that keeps only the finished orders.

The original is the snippet's own function: `packed_only` of
docs/snippets/yaml/streaming_out_logic.py. `check` builds the model, takes the filter of its sink
and applies both to the records of the run: they keep the same records.
"""

import sys
from pathlib import Path

from desflow import SimulationContext

HERE = Path(__file__).parent
SNIPPETS = HERE.parents[2] / "docs" / "snippets" / "yaml"


def check(records):
    """The sink's `when` keeps what `packed_only` keeps, and that is the finished orders."""
    sys.path.insert(0, str(SNIPPETS))
    try:
        from streaming_out_logic import packed_only
    finally:
        sys.path.remove(str(SNIPPETS))
    app = SimulationContext.from_yaml(HERE / "model.yaml")
    keep = app._sinks.egress[0].predicate
    kept = [r for r in records if keep(r)]
    assert kept == [r for r in records if packed_only(r)]
    assert kept and all(r["value"]["event_type"] == "order_packed" for r in kept)

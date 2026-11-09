"""What `desflow explain` and `context.analyze()` compute.

`build_graph` reads a model's declared linkage into a `Graph`, `analyze` adds the
queueing estimates that apply to it, `verify` compares them with a run, and
`render_terminal` and `to_json` print the result.
"""

from desflow.explain.analysis import analyze
from desflow.explain.graph import build_graph
from desflow.explain.models import (
    ArrivalResult,
    CapacityRow,
    Comparison,
    Edge,
    Explanation,
    Graph,
    Hint,
    HolderResult,
    Node,
    ResourceResult,
    TaskResult,
    Verification,
)
from desflow.explain.render import render_terminal, to_json
from desflow.explain.verify import verify

__all__ = [
    "ArrivalResult",
    "CapacityRow",
    "Comparison",
    "Edge",
    "Explanation",
    "Graph",
    "Hint",
    "HolderResult",
    "Node",
    "ResourceResult",
    "TaskResult",
    "Verification",
    "analyze",
    "build_graph",
    "render_terminal",
    "to_json",
    "verify",
]

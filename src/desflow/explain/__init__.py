"""What `desflow explain` and `context.analyze()` compute.

`build_graph` reads a model's declared linkage into a `Graph`, `analyze` adds the
queueing estimates that apply to it, `verify` compares them with a run, and
`render_terminal` and `to_json` print the result.
"""

from desflow.explain.analysis import analyze
from desflow.explain.graph import build_graph
from desflow.explain.models import Explanation, Graph
from desflow.explain.verify import verify

__all__ = ["Explanation", "Graph", "analyze", "build_graph", "verify"]

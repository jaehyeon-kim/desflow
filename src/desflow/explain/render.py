"""Renders an `Explanation`: as text for a terminal, with Rich, or as JSON."""

import io
import json
from typing import Dict, List, Optional, Set

from rich.console import Console
from rich.tree import Tree

from desflow.explain.models import VERIFY_HINT, Explanation, Graph, Node
from desflow.explain.tables import capacities, comparisons, resources


def render_terminal(explanation: Explanation, width: Optional[int] = None) -> str:
    """Returns the explanation as plain text: the graph as a tree from each arrival,
    the resources, the units each allows, the bottleneck, what is not estimated and
    why, the hints, and with `--verify` the comparison.

    Args:
        explanation: What `analyze` or `context.analyze()` returned.
        width: The width in characters; the terminal's, or 80, when None.

    Returns:
        str: The text, ending in a newline.
    """
    # An ASCII file makes Rich draw the tree and the tables with ASCII characters,
    # so the text reads the same in any terminal and on a docs page
    out = _AsciiText()
    console = Console(
        file=out,
        width=width,
        color_system=None,
        highlight=False,
        emoji=False,
        soft_wrap=False,
    )
    console.print(_tree(explanation))
    console.print(resources(explanation))
    for resource in explanation.resources:
        if len(resource.capacities) > 1:
            console.print(capacities(resource))
    if explanation.bottleneck is not None:
        console.print(f"Bottleneck: {explanation.bottleneck}", soft_wrap=True)
    elif explanation.bottleneck_reason is not None:
        why = _while_verifying(explanation, explanation.bottleneck_reason)
        console.print(
            f"Bottleneck: none can be named: {why}",
            soft_wrap=True,
        )
    notes = _not_estimated(explanation)
    if notes:
        console.print("Not estimated:", soft_wrap=True)
        for line in notes:
            console.print(f"  {line}", soft_wrap=True)
    if explanation.hints:
        console.print(f"Hints for utilisation {explanation.target:g}:", soft_wrap=True)
        for hint in explanation.hints:
            console.print(f"  {hint.text}", soft_wrap=True)
    if explanation.verification is not None:
        console.print(comparisons(explanation))
        skipped = [c for c in explanation.verification.comparisons if c.reason]
        if skipped:
            console.print("Not compared:", soft_wrap=True)
            for c in skipped:
                console.print(f"  {c.node} {c.measure}: {c.reason}", soft_wrap=True)
        console.print(explanation.verification.note, soft_wrap=True)
    # Rich pads each table line to its width; the padding carries nothing
    return "".join(line.rstrip() + "\n" for line in out.getvalue().splitlines())


class _AsciiText(io.StringIO):
    encoding = "ascii"


def to_json(explanation: Explanation) -> str:
    """Returns the explanation as JSON, with `schema_version` "0".

    The shape is experimental through 1.x; `explain.schema.json`, beside this
    module, is its schema.

    Args:
        explanation: What `analyze` or `context.analyze()` returned.

    Returns:
        str: The JSON text.
    """
    return json.dumps(explanation.model_dump(mode="json"), indent=2)


def _tree(explanation: Explanation) -> Tree:
    """The graph from each arrival, following spawn and next edges."""
    graph = explanation.graph
    nodes = {node.id: node for node in graph.nodes}
    rates: Dict[str, Optional[float]] = {
        f"arrival:{a.name}": a.rate for a in explanation.arrivals
    }
    rates.update({f"task:{t.name}": t.rate for t in explanation.tasks})
    root = Tree(f"{graph.sim_id}")
    for node in graph.nodes:
        if node.kind == "arrival":
            _branch(
                root.add(_label(node, rates)), node.id, graph, nodes, rates, {node.id}
            )
    for node in graph.nodes:
        if node.kind == "opaque" and not any(e.target == node.id for e in graph.edges):
            root.add(f"{node.label} (opaque)")
    return root


def _label(node: Node, rates: Dict[str, Optional[float]]) -> str:
    if node.kind == "exit":
        return "exit"
    text = f"{node.label} ({node.kind}"
    if node.id in rates:
        rate = rates[node.id]
        text += f", rate {'unknown' if rate is None else f'{rate:.4g}/s'}"
    if node.opaque:
        text = f"{node.label} (opaque"
    return text + ")"


def _branch(
    tree: Tree,
    source: str,
    graph: Graph,
    nodes: Dict[str, Node],
    rates: Dict[str, Optional[float]],
    seen: Set[str],
) -> None:
    for edge in graph.edges:
        if edge.source != source:
            continue
        target = nodes[edge.target]
        share = "" if edge.probability is None else f" (share {edge.probability:.3g})"
        if edge.kind in ("uses", "takes", "gives"):
            tree.add(f"{edge.kind} {target.label} ({target.kind})")
        elif target.id in seen:
            tree.add(f"{edge.kind}{share} -> {target.label} (again)")
        else:
            child = tree.add(f"{edge.kind}{share} -> {_label(target, rates)}")
            _branch(child, target.id, graph, nodes, rates, seen | {target.id})


def _not_estimated(explanation: Explanation) -> List[str]:
    """Each unknown value, with its reason."""
    lines = [f"arrival {a.name}: {a.reason}" for a in explanation.arrivals if a.reason]
    lines += [f"task {t.name}: {t.reason}" for t in explanation.tasks if t.reason]
    for r in explanation.resources:
        if r.reason:
            lines.append(f"resource {r.name}: {r.reason}")
        elif r.wait_reason:
            lines.append(f"wait at {r.name}: {r.wait_reason}")
    lines += [f"{h.kind} {h.name}: {h.reason}" for h in explanation.holders]
    return [_while_verifying(explanation, line) for line in lines]


def _while_verifying(explanation: Explanation, reason: str) -> str:
    """The reason, without the advice to run `--verify` when it is running."""
    if explanation.verification is None:
        return reason
    return reason.removesuffix(VERIFY_HINT)

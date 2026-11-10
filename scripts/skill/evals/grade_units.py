"""K7: a run end that differs from the original's, and a time written in the wrong unit.

A time the request gives in minutes, hours or days (`unit_times` in a task's
`records.yaml`) must not be a bare number in the YAML or the Builder, or a Low-level literal
in a call with no comment on its lines. No task of this evaluation has `unit_times`, as no
request gives a time in those units, so here K7 counts only the run end.
"""

import ast
import re
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Set, Tuple

import yaml
from grade_runs import Score, Env, Loader, model_document


YAML_TIME_KEYS = {
    "service_time",
    "gap",
    "timeout",
    "after",
    "until",
    "patience",
    "first_at",
    "stagger",
}


# A `dist` mapping holds a time only where a time is read: under these keys, or as an entry
# of `services` or `arrivals` (an arrival's own gaps). Under `group`, `count` or a `gen:`
# mapping it holds a number of something else.
DIST_TIME_PARENTS = {"service_time", "gap", "timeout", "patience", "after"}


DIST_KEYS = {"value", "mean", "std", "low", "high"}


BUILDER_TIME_CALLS = {"add_service", "add_arrival", "run", "timeout"}


BUILDER_TIME_KEYS = YAML_TIME_KEYS | DIST_KEYS


UNIT_SECONDS = {"minutes": 60, "hours": 3600, "days": 86400}


def bare_number(node: yaml.Node) -> Optional[float]:
    """Returns the value of a YAML scalar written as a number other than 0, else None."""
    if not (
        isinstance(node, yaml.ScalarNode)
        and node.tag in ("tag:yaml.org,2002:int", "tag:yaml.org,2002:float")
    ):
        return None
    try:
        value = float(str(node.value).replace("_", ""))
    except ValueError:
        return None
    return value or None


class Time(NamedTuple):
    """A time written as a bare number: its file, line, key or place, and value."""

    file: str
    line: int
    place: str
    value: float


def scalar_keys(node: yaml.MappingNode) -> Set[str]:
    """Returns the keys of a YAML mapping that are plain scalars."""
    keys = set()
    for k, _ in node.value:
        if isinstance(k, yaml.ScalarNode):
            keys.add(k.value)
    return keys


def yaml_times(root: Optional[yaml.Node]) -> Iterator[Time]:
    """Yields each time in a blueprint written as a bare number."""
    stack: List[Tuple[yaml.Node, str, str]] = (
        [(root, "", "")] if root is not None else []
    )
    while stack:
        node, key, parent = stack.pop()
        if isinstance(node, yaml.SequenceNode):
            stack.extend((item, key, parent) for item in node.value)
            continue
        if not isinstance(node, yaml.MappingNode):
            continue
        keys = scalar_keys(node)
        timed_dist = "dist" in keys and (
            key in DIST_TIME_PARENTS or parent in ("services", "arrivals")
        )
        for k, v in node.value:
            name = k.value if isinstance(k, yaml.ScalarNode) else ""
            value = bare_number(v)
            timed = name in YAML_TIME_KEYS or (timed_dist and name in DIST_KEYS)
            if timed and value is not None and "gen" not in keys:
                yield Time("model.yaml", v.start_mark.line + 1, name, value)
            stack.append((v, name, key))


ARITHMETIC = {
    ast.Mult: lambda a, b: a * b,
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
}


def number_value(
    node: ast.AST, names: Optional[Dict[str, float]] = None
) -> Optional[float]:
    """Returns the value of a literal number, or of arithmetic on literals and `names`."""
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return None if isinstance(node.value, bool) else float(node.value)
    if isinstance(node, ast.Name) and names is not None:
        return names.get(node.id)
    if isinstance(node, ast.BinOp) and type(node.op) in ARITHMETIC:
        left, right = number_value(node.left, names), number_value(node.right, names)
        if left is None or right is None:
            return None
        return ARITHMETIC[type(node.op)](left, right)
    return None


def call_name(node: ast.Call) -> str:
    """Returns the name of the function a call calls."""
    func = node.func
    return func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")


def keyword_args(node: ast.Call, names: Set[str]) -> Dict[str, ast.AST]:
    """Returns the keyword arguments of a call whose names are in `names`."""
    found: Dict[str, ast.AST] = {}
    for keyword in node.keywords:
        if keyword.arg in names:
            found[keyword.arg] = keyword.value
    return found


def python_times(name: str, source: str, tree: ast.Module) -> Iterator[Time]:
    """Yields each time of a Python form written as a literal.

    In the Builder, a time keyword of a call that takes times; in the Low-level form, the
    argument of `timeout` or `run(until=...)` on a line with no comment.
    """
    lines = source.splitlines()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = call_name(node)
        if name == "builder.py" and func in BUILDER_TIME_CALLS:
            places = keyword_args(node, BUILDER_TIME_KEYS)
        elif name == "low_level.py" and func == "timeout" and node.args:
            places = {"timeout": node.args[0]}
        elif name == "low_level.py" and func == "run":
            places = keyword_args(node, {"until"})
        else:
            continue
        block = "\n".join(
            lines[node.lineno - 1 : getattr(node, "end_lineno", node.lineno)]
        )
        if name == "low_level.py" and "#" in block:
            continue
        for place, value_node in places.items():
            value = number_value(value_node)
            if value:
                yield Time(name, node.lineno, place, value)


def score_run_end(env: Env, spec: dict, score: Score) -> None:
    """K7: a run end in model.yaml that differs from the original's."""
    end_wanted = spec.get("run_end")
    document = model_document(env.run_dir / "model.yaml")
    if end_wanted is None or document is None:
        return
    run = document.get("run") if isinstance(document, dict) else None
    got = None
    if isinstance(run, dict):
        got = to_seconds(run.get("until"))
        if got is None and isinstance(run.get("segments"), list):
            for segment in run["segments"]:
                end = (
                    to_seconds(segment.get("until"))
                    if isinstance(segment, dict)
                    else None
                )
                got = end if end is not None else got
    if got is None or abs(got - end_wanted) > 1e-6:
        score.evidence["K7"].append(
            f"model.yaml: run end is {got}, the original's is {end_wanted}"
        )


def to_seconds(value: Any) -> Optional[float]:
    """Returns a blueprint time in seconds: a number, or text such as `5 s` or `2 min`."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if not isinstance(value, str):
        return None
    found = re.fullmatch(
        r"\s*([\d.]+)\s*(s|sec|secs|seconds?|m|min|mins|minutes?|h|hours?|d|days?)?\s*",
        value,
    )
    if not found:
        return None
    unit = (found.group(2) or "s")[0]
    return float(found.group(1)) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]


def score_units(env: Env, spec: dict, files: dict, score: Score) -> None:
    """K7: the run end, and the request's times in other units written as bare numbers."""
    score_run_end(env, spec, score)
    found: List[Time] = []
    path = env.run_dir / "model.yaml"
    if path.is_file():
        try:
            root = yaml.compose(path.read_text("utf-8"), Loader=Loader)
        except yaml.YAMLError:
            root = None
        found += list(yaml_times(root))
    for name in ("builder.py", "low_level.py"):
        if name in files:
            found += list(python_times(name, files[name].text, files[name].tree))
    wanted = spec.get("unit_times") or []
    for time_ in found:
        where = f"{time_.file}:{time_.line}: {time_.place} {time_.value:g}"
        hit = unit_time(time_.value, wanted)
        if hit:
            score.evidence["K7"].append(
                f"{where}, the request gives '{hit['name']}' in {hit['unit']}, "
                "and the file names no unit"
            )
        else:
            score.notes["K7"].append(f"{where} is a bare number of seconds")


def unit_time(value: float, wanted: List[dict]) -> Optional[dict]:
    """Returns the time of `unit_times` a bare number stands for, in its unit or in seconds."""
    for entry in wanted:
        given = (entry["value"], entry["value"] * UNIT_SECONDS[entry["unit"]])
        if any(abs(value - v) < 1e-6 for v in given):
            return entry
    return None

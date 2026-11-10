"""Applies each change of a task's `mutations.yaml` to the agent's YAML, for K9.

Each change tries the keys it lists, in order, on PyYAML's node tree of `model.yaml`, and
keeps the first variant that runs and alters the event records. The node tree is written
back with `yaml.serialize`, so tags (`!python`, `!include`), anchors and a bare `on` key stay
as the agent wrote them. The tests then run on each changed model as on the agent's own
(`grade_tests.run_steady`).
"""

import copy
import itertools
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Tuple

import yaml
from grade_runs import (
    Env,
    canonical,
    json_lines,
    run_form,
    yaml_command,
)
from grade_tests import run_steady

STR = "tag:yaml.org,2002:str"


class Changed(NamedTuple):
    """A change applied to a copy of the run, or why none was kept."""

    folder: Optional[Path]
    applied: str
    reason: str


class Place(NamedTuple):
    """Where a dotted path ends in the node tree: the mapping or sequence node, and the
    index in its `value` list (a new key's text when the key is to be added)."""

    container: yaml.Node
    key: Any


def compose_model(path: Path) -> Optional[yaml.Node]:
    """Reads a blueprint as PyYAML's node tree, which keeps every tag, or None."""
    try:
        return yaml.compose(path.read_text("utf-8"), Loader=yaml.SafeLoader)
    except (OSError, yaml.YAMLError):
        return None


def node_at(place: Place) -> yaml.Node:
    """Returns the node a place holds."""
    item = place.container.value[place.key]
    return item[1] if isinstance(place.container, yaml.MappingNode) else item


def path_parts(root: yaml.Node, dotted: str) -> Optional[List[str]]:
    """Splits a dotted path; a part in braces becomes the text its own path holds, or None."""
    parts = []
    for part in re.findall(r"\{[^}]*\}|[^.]+", dotted):
        if part.startswith("{"):
            place = find_path(root, part[1:-1], add=False)
            value = node_at(place) if place is not None else None
            if not isinstance(value, yaml.ScalarNode) or value.tag != STR:
                return None
            part = value.value
        parts.append(part)
    return parts


def find_path(root: yaml.Node, dotted: str, add: bool) -> Optional[Place]:
    """Returns where a dotted path ends; `*` matches the one key there is.

    A key is matched on its text, so a bare `on` key matches `on`."""
    parts = path_parts(root, dotted) or []
    cur = root
    for i, part in enumerate(parts):
        last = i == len(parts) - 1
        if isinstance(cur, yaml.SequenceNode):
            if not part.isdigit() or int(part) >= len(cur.value):
                return None
            key: Any = int(part)
        elif isinstance(cur, yaml.MappingNode):
            texts = [
                k.value if isinstance(k, yaml.ScalarNode) else None
                for k, _ in cur.value
            ]
            if part == "*":
                if len(texts) != 1:
                    return None
                key = 0
            elif part in texts:
                key = texts.index(part)
            elif add and last:
                return Place(cur, part)
            else:
                return None
        else:
            return None
        if last:
            return Place(cur, key)
        cur = node_at(Place(cur, key))
    return None


def set_value(place: Place, value: Any) -> None:
    """Puts a value at a place; the value becomes the node `yaml.safe_dump` would write."""
    node = yaml.compose(yaml.safe_dump(value), Loader=yaml.SafeLoader)
    if isinstance(place.key, str):
        place.container.value.append((yaml.ScalarNode(STR, place.key), node))
    elif isinstance(place.container, yaml.MappingNode):
        place.container.value[place.key] = (place.container.value[place.key][0], node)
    else:
        place.container.value[place.key] = node


def keys_to_try(root: yaml.Node, change: dict) -> List[Optional[str]]:
    """Returns the keys a change may set, in order; None stands for an optional change left out."""
    existing: List[Optional[str]] = []
    for key in change["keys"]:
        if find_path(root, key, add=False):
            existing.append(key)
    if existing:
        return existing
    if change.get("add"):
        for key in change["keys"]:
            if find_path(root, key, add=True):
                return [key]
    return [None] if change.get("optional") else []


def variants(root: yaml.Node, mutation: dict) -> Iterator[Tuple[str, str]]:
    """Yields each way to apply a mutation: the changed blueprint's text and what was changed.

    The change is made on the node tree and written back with `yaml.serialize`, so tags
    such as `!python` and `!include` stay as they are; only the changed values differ."""
    options = [keys_to_try(root, change) for change in mutation["changes"]]
    for keys in itertools.product(*options):
        changed = copy.deepcopy(root)
        done = []
        for change, key in zip(mutation["changes"], keys):
            if key is None:
                continue
            found = find_path(changed, key, add=bool(change.get("add")))
            if found is None:
                break
            set_value(found, change["new"])
            done.append(f"{key} = {change['new']}")
        else:
            if done:  # every change was optional and none found a key
                yield yaml.serialize(changed, Dumper=yaml.SafeDumper), ", ".join(done)


def apply_mutation(
    env: Env, spec: dict, mutation: dict, unchanged: List[str]
) -> Changed:
    """Applies the first variant of a mutation that runs and alters the event records."""
    root = compose_model(env.run_dir / "model.yaml")
    tried = []
    for text, applied in variants(root, mutation) if root is not None else []:
        folder = env.copy()
        (folder / "model.yaml").write_text(text, "utf-8")
        done = env.run(yaml_command(env, spec, folder), folder)
        if done.rc != 0:
            tried.append((applied, "does not run"))
        elif canonical(json_lines(done.out)) == unchanged:
            tried.append((applied, "same records"))
        else:
            return Changed(folder, applied, "")
    if not tried:
        return Changed(None, "", "not applied (no listed key exists in model.yaml)")
    if all(outcome == "does not run" for _, outcome in tried):
        return Changed(
            None, "", f"not counted: the changed model does not run ({tried[0][0]})"
        )
    detail = "; ".join(f"{applied}: {outcome}" for applied, outcome in tried)
    return Changed(None, "", f"not counted, the records are the same ({detail})")


class ChangeRun(NamedTuple):
    """One change of `mutations.yaml`: what was applied, or why none was, and the outcomes."""

    name: str
    applied: str
    reason: str
    outcomes: Dict[str, str]


def run_changes(env: Env, spec: dict, mutations: dict) -> List[ChangeRun]:
    """Applies each change to a copy of the run and runs every test on the changed model.

    The tests of a changed model run as those of the agent's model do (`run_steady`), so an
    outcome under a change is steady or `flaky`.
    """
    base = run_form(env, spec, "yaml")
    if base is None or base.rc != 0:
        return []
    unchanged = canonical(base.records)
    found = []
    for mutation in mutations.get("mutations", []):
        changed = apply_mutation(env, spec, mutation, unchanged)
        outcomes: Dict[str, str] = {}
        if changed.folder is not None and (changed.folder / "test_model.py").is_file():
            outcomes = run_steady(env, changed.folder).outcomes
        found.append(
            ChangeRun(mutation["name"], changed.applied, changed.reason, outcomes)
        )
    return found

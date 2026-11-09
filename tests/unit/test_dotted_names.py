"""A name that holds a dot reads as any other name.

desflow joins the sim id and element names with dots into registry paths, statistic
names, `path_id`s and the places of generated values, and a name may hold a dot. Every
YAML blueprint that `scripts/regression.py capture` runs (the cases of
`test_second_run.py`) is run twice from fresh contexts with the same seed: as written,
and as a copy in which the sim id and every declared name (tasks, services, arrivals,
variables and telemetry readings) gets dots and a section word, such as `work` to
`p.service.work_.q`, with every reference to them renamed too. No part of a new name at a dot is the old name,
so a reader that takes one part of a joined string reads a wrong name. The renamed
run's records, statistics and error, with each new name mapped back, must equal the
plain run's. So must the value that `published_value` gives for each count of each task
and flow, and for each name made of the parts of a new name before one of its dots.

The copy is made from the blueprint as read (`read_document`), so line numbers stay
as they are. A random sequence is named by its place, which holds names, so the copy's
sequences are named by the place with the names mapped back.

A resource, store, container or signal keeps its name, because an expression names it
and a dot or a space in it is refused (`tests/unit/test_repeated_names.py`).

A name is not renamed where the model reads it in a way that cannot hold a dot: a name
that an expression or another text of the model holds as a word (`users.desk`,
`running(work)`, `record.task == "work"`), and a name equal to a key of the blueprint
schema, which a reference cannot be told apart from. The sim id is not renamed in a
model that holds it in a text other than a registry path.
"""

import contextlib
import importlib.util
import logging
import os
import re
import zlib
from pathlib import Path
from typing import Any, Dict, Iterator, Set, Tuple
from unittest.mock import MagicMock, patch

import pytest

from desflow.blueprint.loader import SourceMap
from desflow.core import sampler as sampler_module
from desflow.core.statistics import LEFT_OUT_AT_ZERO, published_value
from desflow.values import runtime as runtime_module

# It runs every model the regression capture runs, so CI runs it on one Python
# version on each system (scripts/ci_plan.py).
pytestmark = pytest.mark.every_model

# The module, not the function `desflow.blueprint.build` the package gives
build_module = importlib.import_module("desflow.blueprint.build")
ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "second_run_cases", ROOT / "tests" / "unit" / "test_second_run.py"
)
assert _spec is not None and _spec.loader is not None
second_run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(second_run)
regression = second_run.regression

# The sections whose keys are declared names; `telemetry` readings are found apart
DECLARING = (
    "resources",
    "containers",
    "stores",
    "services",
    "arrivals",
    "variables",
    "signals",
    "tasks",
)
# The sections whose names an expression reads (`level.tank`, `users.desk`), so they
# are refused with a dot or a space and stay as they are written
UNDOTTED = ("resources", "containers", "stores", "signals")
# The section words of registry paths and statistic names
PATH_WORDS = (
    "arrival",
    "service",
    "resources",
    "stores",
    "containers",
    "breakdown",
    "variables",
)
STAT_WORDS = ("tasks", "flows", "resources", "stores", "containers", "arrivals")

# The keys whose text, or list of texts, is never a bare declared name, though it may
# equal one: a time unit, a distribution, a payload field's path, an expression, a
# var's name and the attributes a task reads
BARE_TEXT_KEYS = ("unit", "dist", "path", "expr", "name", "attributes")

_NEW = re.compile(r"p\.service\.(.+?)_\.q")

# The runs that are not renamed, each with its reason
_BUILDER = "a builder model's names are in its Python code; its YAML twin is renamed"
NOT_RENAMED: Dict[str, str] = {
    name: _BUILDER for name, *_ in second_run.CASES if name.endswith(":builder")
}
NOT_RENAMED.update(second_run.NOT_COMPARED)
# A `!python` process, function or check that names an element by its written name
_CODE = "its `!python` {} names {} by its written name, which the copy cannot rename"
NOT_RENAMED.update(
    {
        "blueprint:c1_on_fire.yaml": _CODE.format("process", "the task 'watcher'"),
        "blueprint:c1_one_shot.yaml": _CODE.format("process", "the task 'early'"),
        "blueprint:c1_running.yaml": _CODE.format("process", "the task 'start'"),
        "blueprint:c1_running_ended.yaml": _CODE.format("process", "the task 'start'"),
        "blueprint:c1_running_fails.yaml": _CODE.format("process", "the task 'start'"),
        "blueprint:c1_running_hands_on.yaml": _CODE.format(
            "process", "the task 'start'"
        ),
        "blueprint:c1_running_two.yaml": _CODE.format("process", "the task 'charge'"),
        "blueprint:c1_subscribe.yaml": _CODE.format("process", "the task 'driver'"),
        "blueprint:c6_fire_timing.yaml": _CODE.format("process", "the task 'go'"),
        "blueprint:initial_state.yaml": _CODE.format(
            "process", "the resource 'packers'"
        ),
        "file:docs/snippets/yaml/dynamic_topology.yaml": _CODE.format(
            "process", "the resource 'packers'"
        ),
        "file:docs/snippets/yaml/simpy_computed_time.yaml": _CODE.format(
            "function", "the container 'pump'"
        ),
        "file:docs/snippets/yaml/simpy_fixed_arrivals.yaml": _CODE.format(
            "process", "the task 'serve'"
        ),
        "file:docs/snippets/yaml/simpy_held_unit.yaml": _CODE.format(
            "function", "the resource 'machine'"
        ),
        "file:docs/snippets/yaml/simpy_queue_again.yaml": _CODE.format(
            "function", "the resource 'machine'"
        ),
        "file:docs/snippets/yaml/simpy_sub_process.yaml": _CODE.format(
            "function", "the task 'pack'"
        ),
        "file:docs/snippets/yaml/simpy_wait_again.yaml": _CODE.format(
            "function", "the task 'done'"
        ),
        "file:docs/snippets/yaml/simpy_whole_flow.yaml": _CODE.format(
            "process", "the task 'relay'"
        ),
        "file:docs/snippets/yaml/statistics.yaml": _CODE.format(
            "function", "the resource 'packers'"
        ),
        "scenario:live_parameters:yaml": _CODE.format(
            "process", "the registry paths of 'orders', 'packers' and 'packing'"
        ),
    }
)


def _new(name: str) -> str:
    """The renamed form of a name."""
    return f"p.service.{name}_.q"


def mapped_back(value: Any) -> Any:
    """A value with every renamed name in its texts and keys mapped back."""
    if isinstance(value, str):
        return _NEW.sub(lambda found: found.group(1), value)
    if isinstance(value, dict):
        return {mapped_back(k): mapped_back(v) for k, v in value.items()}
    if isinstance(value, list):
        return [mapped_back(v) for v in value]
    if isinstance(value, tuple):
        return tuple(mapped_back(v) for v in value)
    return value


def _texts(value: Any) -> Iterator[str]:
    """Every text of a document, keys and values."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str):
                yield key
            yield from _texts(item)
    elif isinstance(value, list):
        for item in value:
            yield from _texts(item)


def _word(name: str) -> "re.Pattern[str]":
    """A name as a word of a text."""
    return re.compile(rf"(?<!\w){re.escape(name)}(?!\w)")


class Renaming:
    """The renaming of one document: which names it gives a dot, and how.

    A key is renamed where it is a declared name, a name of a telemetry reading or a
    task of a `weights` mapping; every other key, such as a payload field or a wait
    branch, stays. A text is renamed when it is a declared name, and in its registry
    paths and statistic names.
    """

    def __init__(self, data: Dict[str, Any]):
        sim = data.get("simulation") or {}
        self.sim_id = sim.get("sim_id") if isinstance(sim, dict) else None
        names: Set[str] = set()
        undotted: Set[str] = set()
        for section in DECLARING:
            entries = data.get(section)
            if isinstance(entries, dict):
                names |= {k for k in entries if isinstance(k, str)}
                if section in UNDOTTED:
                    undotted |= {k for k in entries if isinstance(k, str)}
        for entry in data.get("telemetry") or []:
            if isinstance(entry, dict) and isinstance(entry.get("readings"), dict):
                names |= set(entry["readings"])
        self.declared = names
        texts = [self._without_paths(t) for t in _texts(data) if t not in names]
        self.held = {
            name for name in names if any(_word(name).search(t) for t in texts)
        }
        self.names = names - self.held - undotted
        self.rename_sim = isinstance(self.sim_id, str) and not any(
            _word(self.sim_id).search(t) for t in texts if t != self.sim_id
        )

    def _path_pattern(self) -> "re.Pattern[str]":
        """A registry path or a statistic name with a declared name in it."""
        alternatives = "|".join(
            re.escape(n) for n in sorted(self.declared, key=len, reverse=True)
        )
        sim = re.escape(self.sim_id) if isinstance(self.sim_id, str) else "(?!)"
        return re.compile(
            rf"(?<![\w.])(?:(?P<sim>{sim})\.(?P<word>{'|'.join(PATH_WORDS)})|"
            rf"(?P<stat>{'|'.join(STAT_WORDS)}))\.(?P<name>{alternatives})"
            rf"(?=[.\[\]'\"]|$)"
        )

    def _without_paths(self, text: str) -> str:
        if not self.declared:
            return text
        return self._path_pattern().sub("", text)

    def text(self, value: str, whole: bool = True) -> str:
        """A text renamed: a whole name, unless `whole` is False, and the names and
        sim id of its paths."""
        if whole and value in self.names:
            return _new(value)
        if whole and value == self.sim_id and self.rename_sim:
            return _new(value)
        if not self.declared:
            return value

        def swap(found: "re.Match[str]") -> str:
            name = found.group("name")
            new_name = _new(name) if name in self.names else name
            if found.group("stat"):
                return f"{found.group('stat')}.{new_name}"
            sim = (
                _new(self.sim_id)
                if self.rename_sim and self.sim_id is not None
                else self.sim_id
            )
            return f"{sim}.{found.group('word')}.{new_name}"

        return self._path_pattern().sub(swap, value)

    def key(self, location: Tuple[Any, ...], key: Any) -> Any:
        """A key at a location of the document, renamed where it is a name."""
        if not isinstance(key, str):
            return key
        if (
            (len(location) == 1 and location[0] in DECLARING)
            or (len(location) == 3 and location[::2] == ("telemetry", "readings"))
            or location[-1:] == ("weights",)
        ):
            return self.text(key)
        return key if key in self.declared else self.text(key)

    def value(self, value: Any, location: Tuple[Any, ...] = ()) -> Any:
        """A part of the document renamed."""
        if isinstance(value, str):
            keys = [part for part in location if isinstance(part, str)]
            bare = bool(keys) and keys[-1] in BARE_TEXT_KEYS
            return self.text(value, whole=not bare)
        if isinstance(value, dict):
            return {
                self.key(location, k): self.value(v, location + (k,))
                for k, v in value.items()
            }
        if isinstance(value, list):
            return [self.value(v, location + (i,)) for i, v in enumerate(value)]
        return value

    def document(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """The whole document renamed."""
        return self.value(data)

    def location(self, location: Tuple[Any, ...]) -> Tuple[Any, ...]:
        """A location of the source map renamed as its keys are."""
        return tuple(
            self.key(location[:index], part) for index, part in enumerate(location)
        )


def _renaming_read(original):
    """`read_document` that gives the renamed document and its source map."""

    def read(path, *args, **kwargs):
        data, source = original(path, *args, **kwargs)
        renaming = Renaming(data)
        lines = {renaming.location(k): v for k, v in source._lines.items()}
        return renaming.document(data), SourceMap(source.path, lines)

    return read


class _MappedCrc:
    """`zlib` for the random sequences: a place's name is mapped back before its
    CRC32 is taken, so the renamed run draws what the plain run draws."""

    def crc32(self, data: bytes, *args: Any) -> int:
        return zlib.crc32(mapped_back(data.decode()).encode(), *args)


def _counts(readings: Dict[str, Any], sim_id: str) -> Dict[str, Any]:
    """What `published_value` gives for each count of each task and flow a run
    published, and for each name made of the parts of an owner before one of its
    dots."""
    prefix = f"{sim_id}.stats."
    owners: Set[Tuple[str, str]] = set()
    for name in readings:
        rest = name[len(prefix) :] if name.startswith(prefix) else None
        if rest is None:
            continue
        kind, _, tail = rest.partition(".")
        owner = tail.rpartition(".")[0]
        if kind in LEFT_OUT_AT_ZERO and owner:
            owners.add((kind, owner))
            parts = owner.split(".")
            for end in range(1, len(parts)):
                owners.add((kind, ".".join(parts[:end])))
    found: Dict[str, Any] = {}
    for kind, owner in sorted(owners):
        for stat in LEFT_OUT_AT_ZERO[kind]:
            name = f"{prefix}{kind}.{owner}.{stat}"
            value = published_value(readings, name, prefix=prefix)
            if value is not None:  # a name nothing published stays out
                found[name] = value
    return found


def _outcome(make: Any, until: Any, run: Dict[str, Any]) -> Dict[str, Any]:
    """One fresh run's records, statistics, counts and error."""
    try:
        app = make()
    except Exception as exc:  # the error is the outcome compared
        return {"error": f"{type(exc).__name__}: {exc}"}
    outcome = second_run._outcome(app, until, run)
    if "statistics" in outcome:
        outcome["counts"] = _counts(outcome["statistics"], app.sim_id)
    return outcome


CASES = [case for case in second_run.CASES if case[0] not in NOT_RENAMED]


@pytest.mark.parametrize("name,make,until,run", CASES, ids=[case[0] for case in CASES])
def test_a_model_with_every_name_dotted_runs_as_written(name, make, until, run):
    logging.disable(logging.CRITICAL)
    try:
        with (
            regression._restored_imports(),
            contextlib.chdir(ROOT),
            # A blueprint may read ${PWD}, which Windows does not set
            patch.dict(os.environ, {"PWD": str(ROOT)}),
            patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
        ):
            plain = _outcome(make, until, run)
        with (
            regression._restored_imports(),
            contextlib.chdir(ROOT),
            # A blueprint may read ${PWD}, which Windows does not set
            patch.dict(os.environ, {"PWD": str(ROOT)}),
            patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
            patch.object(
                build_module,
                "read_document",
                _renaming_read(build_module.read_document),
            ),
            patch.object(sampler_module, "zlib", _MappedCrc()),
            patch.object(runtime_module, "zlib", _MappedCrc()),
        ):
            renamed = mapped_back(_outcome(make, until, run))
    finally:
        logging.disable(logging.NOTSET)
    assert renamed == plain, f"{name}: {_first_difference(renamed, plain)}"


def _first_difference(renamed: Dict[str, Any], plain: Dict[str, Any]) -> str:
    """The first part, statistic, count or record in which two outcomes differ."""
    if renamed.keys() != plain.keys() or "error" in renamed:
        return (
            f"renamed run {renamed.get('error', 'ran')}; "
            f"plain run {plain.get('error', 'ran')}"
        )
    for part in ("statistics", "counts"):
        for key in sorted(set(renamed[part]) | set(plain[part])):
            if renamed[part].get(key) != plain[part].get(key):
                return (
                    f"{part} {key}: renamed run {renamed[part].get(key)!r}, "
                    f"plain run {plain[part].get(key)!r}"
                )
    for index, (left, right) in enumerate(zip(renamed["records"], plain["records"])):
        if left != right:
            return f"record {index}: renamed run {left!r}, plain run {right!r}"
    return (
        f"{len(renamed['records'])} records in the renamed run, "
        f"{len(plain['records'])} in the plain run"
    )


def test_every_run_not_renamed_is_a_run_of_the_capture():
    names = {case[0] for case in second_run.CASES}
    assert sorted(set(NOT_RENAMED) - names) == []


def test_the_copy_gives_every_name_a_dot():
    """The renaming of one model, with a name an expression reads kept."""
    data = {
        "simulation": {"sim_id": "S"},
        "resources": {"desk": {}, "seat": {}},
        "tasks": {
            "work": {
                "resource": "desk",
                "next": [{"task": "side", "when": "users.seat > 0"}],
            },
            "side": {"payload": {"cap": "registry['S.resources.desk.current_cap']"}},
        },
        "scenario": [{"at": 1, "path": "S.resources.desk.current_cap", "value": 2}],
    }
    renaming = Renaming(data)
    assert renaming.held == {"seat"} and renaming.rename_sim
    renamed = renaming.document(data)
    assert renamed["simulation"]["sim_id"] == "p.service.S_.q"
    assert set(renamed["resources"]) == {"desk", "seat"}
    work = renamed["tasks"]["p.service.work_.q"]
    assert work["resource"] == "desk"
    assert work["next"][0]["task"] == "p.service.side_.q"
    assert renamed["scenario"][0]["path"] == (
        "p.service.S_.q.resources.desk.current_cap"
    )
    assert mapped_back(renamed) == data

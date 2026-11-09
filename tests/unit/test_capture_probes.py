"""Places that the load and run checks guard hold up in every YAML model of the capture.

A model's document is changed in memory: a probe is added under names that start with
`zz_` (arrivals, tasks and holders), so each place sits inside a model with its own
sections, stats, scenario, run settings and families. Two loads are made of each model.
The first holds every probe that must work (a list attribute a task changes, NumPy
values in a payload, a wait on a family member, a replay that skips a bad row, equal
family references in a shared batch, a constant key with a count of 1) and is run
twice for 2 s. The second holds one place that must be refused, chosen by the model's
position in the capture, so each place is tried in several models. Every second model
gets the first load and every fourth the second (every sixteenth also an experiment),
which keeps the file near a minute; `DESFLOW_PROBE_ALL=1` runs every model. A model
that does not load as it is, that ends in an error of its own, or that is strict (it stops
at factor 0 as soon as the wall clock moves) is skipped."""

import contextlib
import copy
import json
import logging
import math
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

import test_capture_places as places
from test_capture_places import CASES, build_module, regression, second_run
from desflow.core import arrivals as arrivals_module

ROOT = places.ROOT
SHORT = 2  # a probe starts within its first second
SUM = "+".join(["1"] * 1000)
Change = Callable[[Dict[str, Any]], None]


def _section(data: Dict[str, Any], name: str) -> Dict[str, Any]:
    if data.get(name) is not None and not isinstance(data[name], dict):
        pytest.skip(f"the model writes {name} in a form that is not a mapping")
    if data.get(name) is None:
        data[name] = {}
    return data[name]


def _probe(
    data: Dict[str, Any],
    *,
    attributes: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
    task: Optional[Dict[str, Any]] = None,
    arrival: Optional[Dict[str, Any]] = None,
) -> None:
    """Adds the arrival `zz_a`, which starts one entity at time 0 on the task `zz_t`."""
    _section(data, "arrivals")["zz_a"] = {
        "initial": 1,
        "spawn": "zz_t",
        "attributes": attributes or {},
        **(arrival or {}),
    }
    _section(data, "tasks")["zz_t"] = {"payload": payload or {}, **(task or {})}


@contextlib.contextmanager
def _setting(change: Change):
    """The environment the capture runs a model in, with its document changed."""
    reader = build_module.read_document
    with (
        regression._restored_imports(),
        contextlib.chdir(ROOT),
        patch.dict(os.environ, {"PWD": str(ROOT)}),
        patch("pyiceberg.catalog.rest.RestCatalog", MagicMock()),
        patch.object(build_module, "read_document", places._changing(reader, change)),
    ):
        yield


def _outcome(make: Callable[[], Any], change: Change, runs: int = 1) -> Dict[str, Any]:
    """The outcome of the last of `runs` runs of 2 s with `change` applied, or the
    error that loading gave."""
    out: Dict[str, Any] = {}
    with _setting(change):
        try:
            app = make()
        except Exception as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}
        if app.strict:
            # The capture runs at factor 0, where a strict model stops as soon as the
            # wall clock moves, so its outcome depends on the clock's resolution
            pytest.skip("a strict model at factor 0 stops when the wall clock moves")
        # The clock may start late, and a warm-up keeps back the records it covers
        until = app.initial_time + (app.warmup or 0) + SHORT
        for _ in range(runs):
            out = second_run._outcome(app, until, {})
        out["warmup"] = app.warmup
        out["ended_early"] = app.env.now < until
    return out


def _failure_text(out: Dict[str, Any]) -> Optional[str]:
    """The failed event of a probe task, for a model whose failure policy keeps the
    run going after a task fails."""
    for record in out.get("records", []):
        value = record.get("value")
        if (
            record.get("stream_type") == "event"
            and str(record.get("task")).startswith("zz_")
            and isinstance(value, dict)
            and value.get("status") == "failed"
        ):
            return json.dumps(record)
    return None


def _recording(seen: List[Tuple[str, Dict[str, Any]]], mutate=None):
    """`Entity` that keeps a copy of the attributes each probe entity starts with, and
    then runs `mutate(entity, number)`."""
    real = arrivals_module.Entity
    counts: Dict[str, int] = {}

    def make(*args: Any, **kwargs: Any) -> Any:
        entity = real(*args, **kwargs)
        if str(entity.arrival).startswith("zz_"):
            seen.append((entity.arrival, copy.deepcopy(entity.attributes)))
            number = counts[entity.arrival] = counts.get(entity.arrival, 0) + 1
            if mutate is not None:
                mutate(entity, number - 1)
        return entity

    return patch.object(arrivals_module, "Entity", make)


NUMPY = [np.float32(1.5), np.int64(3), np.int32(3), np.bool_(True)]


def _numpy(entity: Any, number: int) -> None:
    if entity.arrival == "zz_a":
        entity.attributes["zz_np"] = NUMPY[number % len(NUMPY)]
        entity.attributes["zz_list"].append("changed")
        entity.attributes["zz_map"]["k"] = "changed"


def _works(d: Dict[str, Any], rows: str) -> None:
    """Everything that must load and run in a model."""
    _probe(
        d,
        attributes={"zz_np": 0, "zz_list": [], "zz_map": {}},
        payload={"zz": {"gen": "expr", "expr": "attr.zz_np"}},
        arrival={"initial": 4},
    )
    resources = _section(d, "resources")
    resources["zz_desk"] = {"family": True, "members": 2, "current_cap": 1}
    resources["zz_a"] = {"current_cap": 1}
    arrivals = _section(d, "arrivals")
    tasks = _section(d, "tasks")
    key = {"of": "zz_desk", "key": "attr.i"}
    arrivals["zz_hold"] = {"initial": 1, "spawn": "zz_holds", "attributes": {"i": 0}}
    tasks["zz_holds"] = {
        "resource": key,
        "service_time": 10,
        "payload": {"event_type": "zz_held"},
    }
    for kind, where in (("m", "resource.zz_desk[0]"), ("f", "resource.zz_desk")):
        arrivals[f"zz_w{kind}"] = {
            "dist": "constant",
            "value": 0.1,
            "max_events": 1,
            "spawn": f"zz_wait_{kind}",
            "attributes": {"i": 0},
        }
        arrivals[f"zz_bell_{kind}"] = {
            "dist": "constant",
            "value": 0.3,
            "max_events": 1,
            "spawn": f"zz_ring_{kind}",
        }
        tasks[f"zz_wait_{kind}"] = {
            "wait": {"request": key},
            "service_time": 1,
            "on_interrupt": "abandon",
            "payload": {"event_type": f"zz_wait_{kind}"},
        }
        tasks[f"zz_ring_{kind}"] = {
            "payload": {"event_type": "zz_bell"},
            "interrupt": {
                "task": f"zz_wait_{kind}",
                "where": f"process.target == '{where}'",
            },
        }
    for task, x in (("zz_bx", 0), ("zz_by", 1)):
        arrivals[task] = {"initial": 1, "spawn": task + "t", "attributes": {"i": x}}
        tasks[task + "t"] = {
            "resource": key,
            "service_time": 1,
            "batch": {"name": "zz_lot", "size": 2},
            "payload": {},
        }
    arrivals["zz_keyed"] = {
        "gap": 1,
        "spawn": "zz_replayed",
        "key": "x",
        "count": 1,
        "max_events": 1,
    }
    arrivals["zz_replay"] = {
        "spawn": "zz_replayed",
        "replay": {"path": rows, "fields": ["id"], "on_invalid": "skip"},
    }
    tasks["zz_replayed"] = {"payload": {}}


def _interrupted(out: Dict[str, Any], task: str) -> bool:
    return any(
        record.get("stream_type") == "event"
        and record.get("task") == task
        and (
            record["value"].get("status")
            if isinstance(record["value"], dict)
            else record["value"]
        )
        == "interrupted"
        for record in out["records"]
    )


FULL = bool(os.environ.get("DESFLOW_PROBE_ALL"))


def _every(step: int) -> Any:
    """The cases at every `step`-th position of the capture, or all of them when the
    environment sets `DESFLOW_PROBE_ALL` (about 5 minutes on 4 workers)."""
    chosen = [(n, c) for n, c in enumerate(CASES) if FULL or n % step == 0]
    return pytest.mark.parametrize(
        "number,case", chosen, ids=[case[0] for _, case in chosen]
    )


def _skip_if_the_model_alone_fails(make: Callable[[], Any]) -> None:
    """Skips when the model without a probe does not load or does not run."""
    error = _outcome(make, lambda d: None).get("error")
    if error:
        pytest.skip(f"the model alone ends in an error: {error[:100]}")


@_every(2)
def test_what_must_work_works_in_every_model(number, case, tmp_path, caplog):
    name, make, _until, _run = case
    rows = tmp_path / "rows.csv"
    rows.write_text("time,id\n0.5,12_34\nnan,x\n1,0x1\n")
    seen: List[Tuple[str, Dict[str, Any]]] = []
    with _recording(seen, _numpy), caplog.at_level(logging.WARNING):
        out = _outcome(make, lambda d: _works(d, str(rows)), runs=2)
    text = out.get("error") or ""
    if text:
        _skip_if_the_model_alone_fails(make)
    assert not text, text
    assert _failure_text(out) is None, _failure_text(out)
    probe = [a for who, a in seen if who == "zz_a"]
    if not probe:
        pytest.skip("the model's run does not reach the probe")
    assert len(probe) == 8, f"{len(probe)} entities of zz_a in two runs"
    assert all(a["zz_list"] == [] and a["zz_map"] == {} for a in probe), probe
    json.dumps(out["records"], allow_nan=False)
    if out["ended_early"]:
        pytest.skip("the model's own stop ends the run before the probe is read")
    ids = [a.get("id") for who, a in seen if who == "zz_replay"]
    assert ids == ["12_34", "0x1"] * 2, ids
    if out["warmup"]:
        return  # the records of the warm-up are not published
    assert _interrupted(out, "zz_wait_m"), "a wait on a member was not interrupted"
    assert not _interrupted(out, "zz_wait_f"), "the family name matched a member"


def _expression(expr: str) -> Change:
    return lambda d: _probe(d, payload={"zz": {"gen": "expr", "expr": expr}})


def _where(task: Dict[str, Any]) -> Change:
    return lambda d: _probe(d, task=task)


def _signal(entry: Dict[str, Any]) -> Change:
    def change(d: Dict[str, Any]) -> None:
        _probe(d)
        _section(d, "signals").update(zz_s={"on": [entry]})

    return change


def _stop(d: Dict[str, Any]) -> None:
    _probe(d)
    stop = {"condition": 'registry["S.resources.dsk.current_cap"] >= 1'}
    run = _section(d, "run")
    segments = run.get("segments")
    if segments:
        # `run.segments` refuses `run.stop`: a segment takes `until` or `stop`
        last = segments[-1]
        last.pop("until", None)
        last["stop"] = stop
    else:
        run["stop"] = stop


def _scenario(d: Dict[str, Any]) -> None:
    _probe(d)
    step = {"at": 1, "interrupt": {"task": "zz_t", "where": "level.nope > 0"}}
    d["scenario"] = [
        *(d["scenario"] if isinstance(d.get("scenario"), list) else []),
        step,
    ]


def _ranked_store(bad: str) -> Change:
    def apply(d: Dict[str, Any]) -> None:
        _section(d, "stores")["zz_bin"] = {
            "get_queue": {"rank": f"[3, {bad}, 1, 0][attr.n]"}
        }
        arrivals = _section(d, "arrivals")
        arrivals["zz_take"] = {
            "gap": 0.01,
            "max_events": 4,
            "spawn": "zz_get",
            "attributes": {"n": {"gen": "sequence"}},
        }
        arrivals["zz_give"] = {
            "gap": 0.01,
            "first_at": 0.5,
            "max_events": 4,
            "spawn": "zz_put",
        }
        tasks = _section(d, "tasks")
        tasks["zz_get"] = {"get": {"store": "zz_bin"}, "payload": {}}
        tasks["zz_put"] = {"put": {"store": "zz_bin"}, "payload": {}}

    return apply


def _ranked_resource(bad: str) -> Change:
    def apply(d: Dict[str, Any]) -> None:
        _section(d, "resources")["zz_desk"] = {"current_cap": 1, "queue": "priority"}
        _section(d, "services")["zz_work"] = {"dist": "constant", "value": 5}
        _section(d, "arrivals")["zz_take"] = {
            "gap": 0.01,
            "max_events": 5,
            "spawn": "zz_serve",
            "attributes": {"n": {"gen": "sequence"}},
        }
        _section(d, "tasks")["zz_serve"] = {
            "service": "zz_work",
            "resource": "zz_desk",
            "seize": {
                "resource": "zz_desk",
                "priority": f"[9, 3, {bad}, 1, 0][attr.n]",
            },
            "payload": {},
        }

    return apply


def _shared_batch(x: Any, y: Any) -> Change:
    def apply(d: Dict[str, Any]) -> None:
        resources = _section(d, "resources")
        resources["zz_desk"] = {"family": True, "members": 2, "current_cap": 1}
        resources["zz_a"] = {"current_cap": 1}
        resources["zz_b"] = {"current_cap": 1}
        arrivals = _section(d, "arrivals")
        arrivals["zz_x"] = {"initial": 1, "spawn": "zz_tx", "attributes": {"i": 0}}
        arrivals["zz_y"] = {"initial": 1, "spawn": "zz_ty", "attributes": {"i": 1}}
        for task, resource in (("zz_tx", x), ("zz_ty", y)):
            _section(d, "tasks")[task] = {
                "resource": resource,
                "service_time": 1,
                "batch": {"name": "zz_lot", "size": 2},
                "payload": {},
            }

    return apply


def _key_count(d: Dict[str, Any]) -> None:
    _probe(d)
    d["arrivals"]["zz_a"] = {
        "gap": 1,
        "spawn": "zz_t",
        "key": "x",
        "count": 2,
        "max_events": 2,
    }


def _replay(rows: List[str]) -> Callable[[Path], Change]:
    def make(folder: Path) -> Change:
        path = folder / "bad.csv"
        path.write_text("\n".join(["time,id", *rows]) + "\n")

        def apply(d: Dict[str, Any]) -> None:
            _probe(d)
            d["arrivals"]["zz_a"] = {
                "spawn": "zz_t",
                "replay": {"path": str(path), "fields": ["id"]},
            }

        return apply

    return make


def _count(place: Callable[[Any], Dict[str, Any]], bad: Any) -> Change:
    return lambda d: _probe(d, task=place(bad))


def _weights(bad: Any) -> Change:
    def apply(d: Dict[str, Any]) -> None:
        _probe(d, task={"next": {"weights": {"zz_u": bad, "zz_v": 1}}})
        _section(d, "tasks").update(zz_u={"payload": {}}, zz_v={"payload": {}})

    return apply


F = Callable[[Path], Change]
# The place, how it is changed, and what the refusal says
REFUSED: List[Tuple[str, Any, str]] = [
    ("a sum of 1,000 terms", _expression(SUM), "nested too deeply"),
    ("round to -10**8", _expression("round(5, -10**8)"), "too large"),
    ("a negative base to a fractional power", _expression("(-4) ** 0.5"), "complex"),
    ("an unknown name in a payload", _expression("nosuch + 1"), "nosuch"),
    (
        "an unknown name in an attribute",
        lambda d: _probe(d, attributes={"zz": {"gen": "expr", "expr": "nosuch + 1"}}),
        "nosuch",
    ),
    ("a task fail condition", _where({"fail": "level.nope > 0"}), "level.nope"),
    (
        "a task interrupt where",
        _where({"on": [{"interrupt": {"task": "zz_t", "where": "level.nope > 0"}}]}),
        "level.nope",
    ),
    (
        "a signal interrupt where",
        _signal({"interrupt": {"task": "zz_t", "where": "level.nope > 0"}}),
        "level.nope",
    ),
    (
        "a signal resume limit",
        _signal({"resume": {"task": "zz_t", "limit": "level.nope"}}),
        "level.nope",
    ),
    ("a stop condition", _stop, "dsk"),
    ("a scenario interrupt where", _scenario, "level.nope"),
    ("a store rank of NaN", _ranked_store("1e999 - 1e999"), "finite number"),
    ("a store rank of text", _ranked_store("'x'"), "finite number"),
    ("a priority of NaN", _ranked_resource("1e999 - 1e999"), "finite number"),
    ("a priority of text", _ranked_resource("'x'"), "finite number"),
    ("a batch of two plain names", _shared_batch("zz_a", "zz_b"), "differ in resource"),
    (
        "a batch of two family keys",
        _shared_batch(
            {"of": "zz_desk", "key": "attr.i"}, {"of": "zz_desk", "key": "1 - attr.i"}
        ),
        "differ in resource",
    ),
    ("a constant key with a count of 2", _key_count, "same key"),
    ("route weights of infinity", _weights(math.inf), "zz_t"),
    ("route weights of true", _weights(True), "zz_t"),
]
_COUNT_PLACES: List[Tuple[str, Callable[[Any], Dict[str, Any]]]] = [
    ("a spawn count", lambda v: {"next": {"task": "zz_t", "count": v}}),
    ("an on spawn count", lambda v: {"on": [{"spawn": {"task": "zz_t", "count": v}}]}),
    ("a resume limit", lambda v: {"on": [{"resume": {"task": "zz_t", "limit": v}}]}),
    ("a batch size", lambda v: {"batch": {"size": v}}),
]
for _label, _bad in _COUNT_PLACES:
    for _value in (-1, math.nan, math.inf, True):
        if _label == "a batch size" and _value == -1:
            continue  # a size below 0 is read when the batch closes, not at load
        REFUSED.append((f"{_label} {_value!r}", _count(_bad, _value), ""))
REPLAYS = {
    f"a replay time of {bad!r}": (["1,a", f"{bad},b", "3,c"], "row 2 has no valid time")
    for bad in ("nan", "inf", "-inf", "1_0", "0x1", "Infinity", "")
}
REPLAYS["a replay row before the row above"] = (["5,a", "1,b"], "time order")


@_every(4)
def test_one_refusal_place_is_refused_with_its_line_in_every_model(
    number, case, tmp_path
):
    name, make, _until, _run = case
    total = len(REFUSED) + len(REPLAYS)
    index = (number if FULL else number // 4) % total
    if index < len(REFUSED):
        label, change, needle = REFUSED[index]
        # A rank is read as the run goes, and a replay row is named by its number
        located = needle != "finite number"
    else:
        label = sorted(REPLAYS)[index - len(REFUSED)]
        rows, needle = REPLAYS[label]
        change, located = _replay(rows)(tmp_path), False
    out = _outcome(make, change)
    text = out.get("error") or _failure_text(out) or ""
    if not ((".yaml:" in text or not located) and needle in text):
        _skip_if_the_model_alone_fails(make)
    assert (".yaml:" in text or not located) and needle in text, (
        f"{label}: {text[:300]}"
    )


@_every(16)
def test_a_sweep_and_a_variant_give_summary_rows_of_every_replication(
    number, case, tmp_path
):
    from desflow.analysis.runner import run_experiment

    name, make, _until, run = case
    path = getattr(make, "args", (None,))[0]
    if path is None or not str(path).endswith(".yaml") or run:
        pytest.skip("not a YAML file that runs to its own until")
    if places._load(make) is not None:
        pytest.skip("the model does not load as it is")
    data = places._document(make)
    resource = next(
        (
            (n, v["current_cap"])
            for n, v in (data.get("resources") or {}).items()
            if isinstance(v, dict)
            and not v.get("family")
            and isinstance(v.get("current_cap"), int)
            and not isinstance(v.get("current_cap"), bool)
            and v["current_cap"] >= 1
        ),
        None,
    )
    sim = (data.get("simulation") or {}).get("sim_id")
    if resource is None or not sim:
        pytest.skip("no plain resource or no sim_id to sweep")
    key = f"{sim}.resources.{resource[0]}.current_cap"
    spec = {
        "experiment": "zz",
        "model": str(path),
        "until": SHORT,
        "replications": 2,
        "seed": 1,
        "measures": [f"resources.{resource[0]}.utilization_avg"],
        "sweep": {key: [resource[1], 0]},
        "variants": {"one": {}, "two": {}},
    }
    with _setting(lambda d: None):
        result = run_experiment(spec, output=tmp_path, raise_on_failure=False)
    short = [
        f"{row['design']} {row['statistic']} n={row['n']}"
        for row in result.summary
        if row["n"] != 2
    ]
    assert result.summary and len(result.designs) == 4, "too few designs"
    assert not short, "\n".join(short[:5])

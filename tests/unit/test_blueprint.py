"""YAML blueprints: validation, error lines and the builder calls they make."""

from dataclasses import replace
import json
import re
import textwrap
import time
from datetime import datetime

import pytest
from typer.testing import CliRunner

from desflow import SimulationContext
from desflow.blueprint import BlueprintError
from desflow.cli import app as cli
from desflow.connectors.egress.base import BaseEgress

MINIMAL = """\
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 7
resources:
  packers: {current_cap: 2, max_cap: 5}
services:
  packing: {dist: normal, mean: 3.0, std: 0.5}
arrivals:
  standard: {dist: exponential, rate: 1.0, spawn: part}
tasks:
  part:
    service: packing
    resource: packers
    payload: {status: finished}
    id_field: order_id
run:
  until: 30
"""


def write(tmp_path, text, name="blueprint.yaml"):
    path = tmp_path / name
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


class _Capture(BaseEgress):
    """Keeps every record it receives, for assertions after the run."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        import asyncio
        import queue

        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def test_blueprint_builds_through_the_builder(tmp_path):
    app = SimulationContext.from_yaml(write(tmp_path, MINIMAL))

    assert (app.sim_id, app.factor, app.random_seed) == ("Shop", 0, 7)
    assert app._holders.resources_config["packers"].current_cap == 2
    packing = app._services_config["packing"]
    # Only the parameters the family takes are set
    assert (packing.dist, packing.mean, packing.std, packing.rate) == (
        "normal",
        3.0,
        0.5,
        None,
    )
    assert app._arrivals.configs["standard"].rate == 1.0
    assert app._defaults.until == 30.0


def test_tasks_spawned_from_an_arrival_publish_the_payload(tmp_path):
    app = SimulationContext.from_yaml(write(tmp_path, MINIMAL))
    capture = _Capture()
    app.add_egress(capture)
    app.run()

    events = [r["value"] for r in capture.records if r["stream_type"] == "event"]
    finished = [value for value in events if "order_id" in value]
    assert finished
    assert finished[0] == {"status": "finished", "order_id": finished[0]["order_id"]}
    assert max(r["sim_ts"] for r in capture.records) <= 30.0


def test_yaml_spawn_goes_through_add_arrival(tmp_path, monkeypatch):
    """A YAML arrival's spawn is the builder's add_arrival(spawn=), so both APIs
    start their tasks through one loop, in the order the arrivals are written."""
    calls = []
    add_arrival = SimulationContext.add_arrival

    def recording(self, name, dist, **kwargs):
        calls.append((name, kwargs.get("spawn")))
        return add_arrival(self, name, dist, **kwargs)

    monkeypatch.setattr(SimulationContext, "add_arrival", recording)
    text = MINIMAL.replace(
        "arrivals:\n", "arrivals:\n  walk_in: {dist: constant, value: 2}\n"
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    assert calls == [("walk_in", None), ("standard", "part")]
    assert [meta for _, meta in app._startup_loops] == ["standard"]


@pytest.mark.parametrize(
    "old,new,line,fragment",
    [
        ("  factor: 0\n", "  factor: 0\n  speed: 2\n", 4, "simulation.speed"),
        ("mean: 3.0, std: 0.5", "mean: 3.0, std: 0.5, shape: 2", 8, "shape"),
        ("dist: exponential", "dist: beta", 10, "Unsupported distribution"),
        (
            "  packers: {current_cap: 2, max_cap: 5}",
            "  packers: {max_cap: 0.5}",
            6,
            "max_cap 0.5 is below 1, the default current_cap",
        ),
        (
            "  packers: {current_cap: 2, max_cap: 5}",
            "  packers: {current_cap: 2, max_cap: 5, type: !python datetime.datetime}",
            6,
            "Input should be a subclass of ResourceBase",
        ),
        (
            "  packers: {current_cap: 2, max_cap: 5}",
            "  packers: {current_cap: 2, max_cap: 5, queue_limit: -1}",
            6,
            "queue_limit must be 0 or more",
        ),
        ("until: 30", "until: soon", 18, "run.until"),
        (
            "services:",
            "stores:\n  parcels: {current_cap: 1, max_cap: 1, items: [a, b]}\nservices:",
            8,
            "cannot start with 2; raise current_cap and max_cap, or give fewer items",
        ),
        # An unknown key names the closest key that place takes, or every key
        (
            "  random_seed: 7",
            "  ranom_seed: 7",
            4,
            "unknown key 'ranom_seed'; did you mean 'random_seed'?",
        ),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    patiense: 5",
            17,
            "tasks.part.patiense: unknown key 'patiense'; did you mean 'patience'?",
        ),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    spawn: {task: part, after: 0}",
            17,
            "a spawn after is more than 0, got 0",
        ),
        ("run:", "storez: {}\nrun:", 17, "did you mean 'stores'?"),
        ("until: 30", "until: 30\n  zzz: 1", 19, "this place takes before, drain"),
    ],
)
def test_schema_errors_name_the_line(tmp_path, old, new, line, fragment):
    path = write(tmp_path, MINIMAL.replace(old, new))
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert fragment in message


_VALUE_HEAD = (
    "simulation: {sim_id: Shop, factor: 0}\n"
    "variables: {total: 1}\n"
    "signals: {s: {}}\n"
    "services: {work: {dist: constant, value: 1}}\n"
)
_ARRIVAL = "arrivals: {a: {dist: constant, value: 1, spawn: t%s}}\n"
# Each of the ten places a value is written, with @@ where the value goes
_VALUE_PLACES = {
    "attributes": _ARRIVAL % ", attributes: {n: @@}"
    + "tasks: {t: {payload: {e: 1}}}\n",
    "items": "stores: {shelf: {items: [@@]}}\n",
    "cause": _ARRIVAL % ""
    + "tasks:\n  t: {payload: {e: 1}, interrupt: {task: t, cause: @@}}\n",
    "fire": _ARRIVAL % ""
    + "tasks:\n  t: {payload: {e: 1}, on: [{fire: {signal: s, value: @@}}]}\n",
    "on": _ARRIVAL % ""
    + "tasks:\n  t: {payload: {e: 1}, on: [{path: Shop.variables.total, value: @@}]}\n",
    "timeout": _ARRIVAL % ""
    + "tasks:\n  t: {wait: {timeout: {after: 1, value: @@}}, payload: {e: 1}}\n",
    "nested wait": _ARRIVAL
    % ""
    + "tasks:\n  t:\n    payload: {e: 1}\n    wait:\n      any:\n"
    "        late: {timeout: {after: 1, value: @@}}\n"
    "        never: {timeout: 50}\n",
    "payload": _ARRIVAL % "" + "tasks: {t: {payload: {e: @@}}}\n",
    "set": _ARRIVAL % "" + "tasks: {t: {payload: {e: 1}, set: {n: @@}}}\n",
    "scenario value": _ARRIVAL % "" + "tasks: {t: {payload: {e: 1}}}\n"
    "scenario:\n  - {at: 1, path: Shop.variables.total, value: @@}\n",
    "scenario record": _ARRIVAL % "" + "tasks: {t: {payload: {e: 1}}}\n"
    "scenario:\n  - {at: 1, record: {k: @@}}\n",
}


def _with_value(place, value):
    text = _VALUE_HEAD + _VALUE_PLACES[place]
    line = next(i for i, row in enumerate(text.splitlines(), 1) if "@@" in row)
    return text.replace("@@", value), line


@pytest.mark.parametrize("place", sorted(_VALUE_PLACES))
def test_a_mapping_with_gen_is_a_generator_in_every_place_a_value_is_written(
    tmp_path, place
):
    """MF1: in each of the ten places a mapping with a `gen` key is a generator, so a
    kind that does not exist is a load error with the line of the value, and
    `{gen: literal, value: {gen: 5}}` keeps the mapping as data."""
    text, line = _with_value(place, "{gen: nope}")
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    # A wait lists what each of its branch kinds found, so one line is the value's
    assert any(
        row.startswith(f"{path}:{line}:") and "nope" in row
        for row in str(error.value).splitlines()
    ), str(error.value)

    text, _ = _with_value(place, "{gen: literal, value: {gen: 5}}")
    SimulationContext.from_yaml(write(tmp_path, text, "kept.yaml"))


# A scenario value and an `on:` path value set a number, so a mapping there is refused
# by the type of the path
@pytest.mark.parametrize("place", sorted(set(_VALUE_PLACES) - {"scenario value", "on"}))
def test_a_nested_mapping_with_a_key_that_is_not_text_still_loads_in_every_place(
    tmp_path, place
):
    """YAML reads a bare `on` as true and `1` as a number, so a nested mapping in a
    value place may have a key that is not text, as it could before generators."""
    for value in ("{1: x}", "{a: {on: 1, 2026-01-01: 2}}"):
        text, _ = _with_value(place, value)
        SimulationContext.from_yaml(write(tmp_path, text))


@pytest.mark.parametrize("place", ["set", "attributes", "scenario record"])
def test_a_gen_key_directly_under_set_attributes_or_record_is_a_field_name(
    tmp_path, place
):
    """These places are mappings of field names to values, so a `gen` key at their top
    names a field and stays data; a value one level down is the mapping that is a
    generator."""
    top = {
        "set": "tasks: {t: {payload: {e: 1}, set: {gen: 5}}}\n",
        "attributes": "tasks: {t: {payload: {e: 1}}}\n",
        "scenario record": "tasks: {t: {payload: {e: 1}}}\n"
        "scenario:\n  - {at: 1, record: {gen: 5}}\n",
    }[place]
    text = _VALUE_HEAD + (
        _ARRIVAL % (", attributes: {gen: 5}" if place == "attributes" else "")
    )
    SimulationContext.from_yaml(write(tmp_path, text + top))


def test_a_mapping_with_gen_runs_as_a_generator_and_literal_keeps_it_as_data(tmp_path):
    """The old meaning of a `gen` mapping, as data, is kept by `literal` in the payload,
    a `set` value and a nested wait's timeout value, and a generator elsewhere gives
    its drawn value."""
    text, _ = _with_value("payload", "{gen: literal, value: {gen: 5}}")
    assert _events_of(tmp_path, text, until=1.5) == [(1.0, {"e": {"gen": 5}})]
    text, _ = _with_value("payload", "{gen: integer, bounds: [4, 4]}")
    assert _events_of(tmp_path, text, until=1.5) == [(1.0, {"e": 4})]
    text, _ = _with_value("nested wait", "{gen: literal, value: {gen: 5}}")
    events = _events_of(tmp_path, text, until=3)
    assert (2.0, {"e": 1}) in events


def test_generator_errors_name_the_line(tmp_path):
    """A generator that is wrong names the file and the line of the value, and the
    location is the path of keys without the tags of the schema union."""
    cases = [
        ("{gen: uniform, low: 1}", "high"),
        ("{gen: integer, bounds: [1, 2], zzz: 3}", "zzz"),
        ("{gen: expr, expr: 'attr.n +'}", "expr"),
        ("{gen: var, name: missing}", "missing"),
        ("{gen: sample, service: nowhere}", "nowhere"),
        ("{gen: param, path: Shop.nothing}", "registry path"),
        ("{gen: row, file: absent.csv}", "absent.csv"),
    ]
    for value, fragment in cases:
        text, line = _with_value("payload", value)
        path = write(tmp_path, text)
        with pytest.raises(BlueprintError) as error:
            SimulationContext.from_yaml(path)
        message = str(error.value)
        assert message.startswith(f"{path}:{line}:"), message
        assert fragment in message, message
        assert "<gen>" not in message and "<map>" not in message, message


def _events_of(tmp_path, text, until):
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=until)
    return [
        (r["sim_ts"], r["value"])
        for r in capture.records
        if r["stream_type"] == "event"
    ]


def test_text_in_a_value_place_stays_text_and_a_time_place_reads_it(tmp_path):
    """Text in a value place stays text: a payload value `attr.n + 1` is that text.
    A time place reads a number as seconds and text `time_to_seconds` accepts as a
    duration, so `timeout: 2 h` waits 7200 seconds."""
    header = (
        "simulation: {sim_id: Shop, factor: 0}\n"
        "arrivals: {a: {dist: constant, value: 1, spawn: t}}\n"
    )
    events = _events_of(
        tmp_path,
        header + "tasks: {t: {payload: {e: 'attr.n + 1'}}}\n",
        until=1.5,
    )
    assert events == [(1.0, {"e": "attr.n + 1"})]

    for timeout, seconds in ((5, 5.0), ("'2 h'", 7200.0)):
        events = _events_of(
            tmp_path,
            header
            + f"tasks: {{t: {{wait: {{timeout: {timeout}}}, payload: {{e: 1}}}}}}\n",
            until=seconds + 1.5,
        )
        assert [e for e in events if e[1] == {"e": 1}][0] == (1.0 + seconds, {"e": 1})

    # In a place that reads an expression, text is one: a patience of `attr.n` is the
    # attribute's value: the second order, asking at 2, gives up at 6
    events = _events_of(
        tmp_path,
        "simulation: {sim_id: Shop, factor: 0}\n"
        "resources: {m: {current_cap: 1, max_cap: 1}}\n"
        "services: {long: {dist: constant, value: 9}}\n"
        "arrivals: {a: {dist: constant, value: 1, spawn: t, attributes: {n: 4}}}\n"
        "tasks: {t: {resource: m, service: long, patience: 'attr.n', "
        "payload: {e: 1}}}\n",
        until=8,
    )
    assert any(v.get("status") == "reneged" and t == 6.0 for t, v in events)


@pytest.mark.parametrize(
    "old,new,line,fragment",
    [
        (
            "    service: packing",
            "    service: drilling",
            13,
            "service 'drilling', which is not defined under services; add it under "
            "services",
        ),
        (
            "    resource: packers",
            "    resource: packer",
            14,
            "resource 'packer', which is not defined under resources; did you mean "
            "'packers'?",
        ),
        ("spawn: part", "spawn: widget", 10, "task 'widget'"),
        ("current_cap: 2,", "current_cap: 2.5,", 6, "whole number"),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    next: {when: [{condition: item.size > 1, task: part}]}",
            17,
            "item.<field> reads the item a store get took",
        ),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    signal: ready",
            17,
            "signal 'ready'",
        ),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    wait: {task: inspect}",
            17,
            "task 'inspect'",
        ),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    wait: {timeout: 5}\n"
            "    next: {when: [{condition: wait.limit.done, task: part}]}",
            18,
            "wait.limit is not a branch of this task's wait, read after it; read it "
            "in the task's payload, put, next or fail, after the wait",
        ),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    next: {when: [{condition: signal.ready.ok, task: part}]}",
            17,
            "signal.ready names no signal under signals; add it under signals",
        ),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    next: {when: [{condition: level.shelf > 0, task: part}]}",
            17,
            "level.shelf names no store or container; add it under stores or "
            "containers",
        ),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    balk: queue.packer >= 2",
            17,
            "queue.packer names no resource; did you mean 'packers'?",
        ),
        (
            "    id_field: order_id",
            "    id_field: order_id\n    wait: {condition: users.bench < 1}",
            17,
            "users.bench names no resource; add it under resources",
        ),
    ],
)
def test_cross_reference_errors_name_the_line(tmp_path, old, new, line, fragment):
    path = write(tmp_path, MINIMAL.replace(old, new))
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert fragment in message


def test_unknown_connector_type_names_the_line(tmp_path):
    text = MINIMAL + "egress:\n  - type: Carrier\n"
    path = write(tmp_path, text)
    with pytest.raises(
        BlueprintError, match=rf"{re.escape(str(path))}:20: unknown connector type"
    ):
        SimulationContext.from_yaml(path)


def test_bad_connector_config_names_the_line(tmp_path):
    text = MINIMAL + "egress:\n  - type: Console\n    config: {colour: red}\n"
    path = write(tmp_path, text)
    with pytest.raises(
        BlueprintError, match=rf"{re.escape(str(path))}:21: ConsoleEgress rejected"
    ):
        SimulationContext.from_yaml(path)


def test_yaml_syntax_error_names_the_line(tmp_path):
    path = write(tmp_path, "simulation:\n  sim_id: [Shop\n")
    with pytest.raises(BlueprintError, match=rf"{re.escape(str(path))}:\d+:"):
        SimulationContext.from_yaml(path)


def test_top_level_must_be_a_mapping(tmp_path):
    path = write(tmp_path, "- simulation\n")
    with pytest.raises(BlueprintError, match="top level must be a mapping"):
        SimulationContext.from_yaml(path)


def test_cli_runs_a_blueprint(tmp_path):
    path = write(tmp_path, MINIMAL + "egress:\n  - type: Console\n")
    result = CliRunner().invoke(cli, ["run", str(path), "--until", "5"])

    assert result.exit_code == 0, result.output


def test_cli_reports_an_invalid_blueprint(tmp_path):
    path = write(tmp_path, MINIMAL.replace("spawn: part", "spawn: widget"))
    result = CliRunner().invoke(cli, ["run", str(path)])

    assert result.exit_code == 1
    assert f"{path}:10:" in result.output


def test_cli_rejects_a_bad_until(tmp_path):
    path = write(tmp_path, MINIMAL)
    result = CliRunner().invoke(cli, ["run", str(path), "--until", "soon"])

    assert result.exit_code == 2
    assert "Invalid time format" in result.output


def test_cli_rejects_a_missing_file(tmp_path):
    result = CliRunner().invoke(cli, ["run", str(tmp_path / "absent.yaml")])

    assert result.exit_code == 2


def test_cli_exits_with_an_error_when_the_stop_signal_fails():
    """A run that `run.stop` ends on a failing signal raises `SignalFailed` from
    `finish()`, so `desflow run` exits with an error and not with the run's value."""
    from pathlib import Path

    from desflow import SignalFailed

    path = Path(__file__).parents[1] / "conformance/blueprints/c6_stop_failed.yaml"
    result = CliRunner().invoke(cli, ["run", str(path)])

    assert result.exit_code == 1
    assert isinstance(result.exception, SignalFailed)


def test_cli_prints_the_version():
    from desflow import __version__

    result = CliRunner().invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() == __version__


def test_schema_command_prints_the_file():
    from desflow.blueprint.schema import schema_text

    result = CliRunner().invoke(cli, ["schema"])
    assert result.exit_code == 0
    assert result.output == schema_text()
    assert json.loads(result.output)["$schema"].startswith("https://json-schema.org/")


# ---------------------------------------------------------------------------
# !python references (#16)
# ---------------------------------------------------------------------------
LOGIC = """\
import itertools

calls = []


def payload(task_id, context):
    return {"task": task_id, "at": context.env.now}


def ticker(context, step, label="tick"):
    for count in itertools.count():
        yield context.env.timeout(step)
        context.publish(label, count)


def not_a_generator(context):
    return None


def sample(context):
    context.publish("sampled", 1)


def keep_events(record):
    return record["stream_type"] == "event"


def setup():
    calls.append("setup")


class Sink:
    def __init__(self, label):
        self.label = label

    async def run(self, egress_queue):
        return None

"""


@pytest.fixture
def logic(tmp_path, request):
    """A module beside the blueprint, with a name unique to the test."""
    name = f"logic_{request.node.name.replace('[', '_').replace(']', '_')}"
    name = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)
    (tmp_path / f"{name}.py").write_text(LOGIC, encoding="utf-8")
    return name


def test_python_payload_is_called_with_the_task_id_and_context(tmp_path, logic):
    text = MINIMAL.replace(
        "    payload: {status: finished}\n    id_field: order_id\n",
        f"    payload: !python {logic}.payload\n",
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=10)

    events = [r["value"] for r in capture.records if r["stream_type"] == "event"]
    finished = [value for value in events if "task" in value]
    assert finished and finished[0]["at"] > 0


@pytest.mark.parametrize("first,second", [("a", "b"), ("b", "a")])
def test_arrivals_start_in_file_order(tmp_path, first, second):
    """File order is creation order: arrivals due at one time start their entities in
    the order they are written."""
    arrivals = (
        f"arrivals: {{{first}: {{dist: constant, value: 3, spawn: t}}, "
        f"{second}: {{dist: constant, value: 3, spawn: t}}}}\n"
    )
    text = (
        "simulation: {sim_id: R, factor: 0}\n"
        + arrivals
        + "tasks: {t: {payload: {e: 1}}}\n"
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=4)
    keys = [r["key"] for r in capture.records if r["stream_type"] == "event"]
    assert keys[0].startswith(first) and keys[1].startswith(second), keys


def test_file_order_decides_only_ties_between_first_due_times(tmp_path):
    """Events due at one time run in the order they were scheduled. Arrival `a` (gap 1)
    comes first in the file, but at 3 arrival `b` (gap 3) runs first, because its
    timeout was scheduled at 0 and the one of `a` at 2."""
    text = (
        "simulation: {sim_id: R, factor: 0}\n"
        "arrivals: {a: {dist: constant, value: 1, spawn: t}, "
        "b: {dist: constant, value: 3, spawn: t}}\n"
        "tasks: {t: {payload: {e: 1}}}\n"
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=3.5)
    keys = [
        (r["sim_ts"], r["key"]) for r in capture.records if r["stream_type"] == "event"
    ]
    assert keys == [(1.0, "a-1"), (2.0, "a-2"), (3.0, "b-1"), (3.0, "a-3")]


def test_arrivals_start_before_a_process_due_at_the_same_time(tmp_path):
    """Arrivals are created before `processes`, so an entity an arrival starts runs
    before a `processes` entry due at the same time."""
    (tmp_path / "order_logic_mf6.py").write_text(
        "ORDER = []\n"
        "def task_ran(task_id, context):\n"
        "    ORDER.append('task')\n"
        "    return {'e': 1}\n"
        "def process(context):\n"
        "    yield context.env.timeout(3)\n"
        "    ORDER.append('process')\n",
        encoding="utf-8",
    )
    text = (
        "simulation: {sim_id: R, factor: 0}\n"
        "arrivals: {a: {dist: constant, value: 3, spawn: t}}\n"
        "tasks: {t: {payload: !python order_logic_mf6.task_ran}}\n"
        "processes:\n  - !python order_logic_mf6.process\n"
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    app.run(until=4)
    import order_logic_mf6

    assert order_logic_mf6.ORDER == ["task", "process"]


def test_processes_take_kwargs(tmp_path, logic):
    text = MINIMAL + textwrap.dedent(f"""\
        processes:
          - function: !python {logic}.ticker
            kwargs: {{step: 2}}
          - function: !python {logic}.ticker
            kwargs: {{step: 4, label: slow}}
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=9)

    ticks = [
        (r["path_id"], r["sim_ts"])
        for r in capture.records
        if r["stream_type"] == "telemetry" and not r["path_id"].startswith("system")
    ]
    assert ("Shop.tick", 2.0) in ticks and ("Shop.tick", 8.0) in ticks
    assert ("Shop.slow", 4.0) in ticks and ("Shop.slow", 8.0) in ticks


def test_telemetry_takes_a_python_function(tmp_path, logic):
    text = MINIMAL + textwrap.dedent(f"""\
        telemetry:
          - interval: 5
            function: !python {logic}.sample
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=10)
    assert any(r.get("path_id") == "Shop.sampled" for r in capture.records)


def test_egress_type_and_when_take_python_objects(tmp_path, logic):
    text = MINIMAL + textwrap.dedent(f"""\
        egress:
          - type: !python {logic}.Sink
            config: {{label: one}}
            when: !python {logic}.keep_events
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    provider = app._sinks.egress[0].provider
    assert type(provider).__name__ == "Sink" and provider.label == "one"
    assert app._sinks.egress[0].predicate({"stream_type": "event"}) is True


def test_run_before_is_called_before_the_run(tmp_path, logic):
    import importlib

    text = MINIMAL.replace(
        "run:\n  until: 30\n",
        f"run:\n  until: 1\n  before: [!python {logic}.setup]\n",
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    module = importlib.import_module(logic)
    assert module.calls == []
    app.run()
    assert module.calls == ["setup"]


def test_the_blueprint_folder_is_on_sys_path(tmp_path, logic, monkeypatch):
    """A blueprint resolves the module beside it from any working directory."""
    import sys

    monkeypatch.chdir("/")
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p != str(tmp_path)])
    text = (
        MINIMAL + f"telemetry:\n  - interval: 1\n    function: !python {logic}.sample\n"
    )
    SimulationContext.from_yaml(write(tmp_path, text))
    assert sys.path[0] == str(tmp_path.resolve())


@pytest.mark.parametrize(
    "wait",
    [
        "{timeout: {after: 1, value: {gen: var, name: nosuch}}}",
        "{any: {x: {timeout: {after: 1, value: {gen: var, name: nosuch}}}}}",
        "{each: 2, branch: {timeout: {after: 1, value: {gen: var, name: nosuch}}}}",
    ],
    ids=["plain", "inside any", "inside each"],
)
def test_a_generator_in_a_timeout_value_is_refused_at_load_wherever_it_sits(
    tmp_path, wait
):
    text = MINIMAL.replace("    resource: packers\n", f"    wait: {wait}\n")
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(write(tmp_path, text))
    message = str(error.value)
    assert message.startswith(f"{tmp_path / 'blueprint.yaml'}:14: "), message
    assert "'nosuch' is not a var that has a value here" in message


@pytest.mark.parametrize(
    "wait",
    [
        {"timeout": {"after": 1, "value": {"gen": "var", "name": "nosuch"}}},
        {
            "any": {
                "x": {
                    "timeout": {"after": 1, "value": {"gen": "var", "name": "nosuch"}}
                }
            }
        },
        {
            "each": 2,
            "branch": {
                "timeout": {"after": 1, "value": {"gen": "var", "name": "nosuch"}}
            },
        },
    ],
    ids=["plain", "inside any", "inside each"],
)
def test_a_generator_in_a_timeout_value_is_refused_before_the_run_in_the_builder(wait):
    from desflow.values import generator

    def with_generators(node):
        if isinstance(node, dict):
            if node.get("gen") == "var":
                return generator(node)
            return {key: with_generators(value) for key, value in node.items()}
        return node

    app = SimulationContext("S", factor=0, random_seed=1)
    app.add_arrival("a", spawn="t", dist="constant", value=1, max_events=1)
    app.task(name="t", wait=with_generators(wait))(lambda task_id: {"x": 1})
    with pytest.raises(ValueError) as error:
        app.run(until=5)
    assert type(error.value) is ValueError
    assert "'nosuch' is not a var that has a value here" in str(error.value)


def test_a_filter_on_a_priority_store_is_refused_at_load_through_a_list(tmp_path):
    text = MINIMAL.replace(
        "tasks:\n",
        "stores: {plain: {items: [1, 2]}, ranked: {priority: true, items: [[1, 5]]}}\n"
        "tasks:\n",
    ).replace(
        "    resource: packers\n",
        "    item_field: got\n"
        "    get: {store: {of: [plain, ranked], key: attr.w}, filter: 'item > 0'}\n",
    )
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(write(tmp_path, text))
    message = str(error.value)
    assert message.startswith(f"{tmp_path / 'blueprint.yaml'}:"), message
    assert "store 'ranked' serves by priority" in message


def test_a_store_list_with_a_filter_and_an_undeclared_name_is_refused_by_name(tmp_path):
    text = MINIMAL.replace(
        "tasks:\n",
        "stores: {plain: {items: [1, 2]}}\ntasks:\n",
    ).replace(
        "    resource: packers\n",
        "    item_field: got\n"
        "    get: {store: {of: [plain, nosuch], key: attr.w}, filter: 'item > 0'}\n",
    )
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(write(tmp_path, text))
    message = str(error.value)
    assert message.startswith(f"{tmp_path / 'blueprint.yaml'}:"), message
    assert "'nosuch', which is not a declared store" in message


def test_two_blueprints_in_different_folders_each_use_their_own_module(
    tmp_path, monkeypatch
):
    """`desflow check a/shop.yaml b/shop.yaml` loads both in one process."""
    import sys

    monkeypatch.delitem(sys.modules, "shared_settings_two_folders", raising=False)
    names = {}
    for folder, sim_id in (("a", "Shop_a"), ("b", "Shop_b")):
        (tmp_path / folder).mkdir()
        (tmp_path / folder / "shared_settings_two_folders.py").write_text(
            f"SIM_ID = {sim_id!r}\n", encoding="utf-8"
        )
        text = MINIMAL.replace(
            "sim_id: Shop", "sim_id: !python shared_settings_two_folders.SIM_ID"
        )
        write(tmp_path / folder, text, "shop.yaml")
    for folder in ("a", "b", "a"):
        loaded = SimulationContext.from_yaml(tmp_path / folder / "shop.yaml")
        names.setdefault(folder, []).append(loaded.sim_id)

    assert names == {"a": ["Shop_a", "Shop_a"], "b": ["Shop_b"]}
    sys.modules.pop("shared_settings_two_folders", None)


def test_a_folder_the_caller_puts_on_sys_path_after_a_load_from_it_stays_found(
    tmp_path, monkeypatch
):
    """A load from `lib` puts `lib` on `sys.path`, and the next load hides it, so each
    file finds only its own folder's modules. A caller that then puts `lib` on
    `sys.path` itself, as a script that keeps its modules there does, still finds
    them from a blueprint in another folder."""
    import sys

    monkeypatch.delitem(sys.modules, "lib_settings_on_path", raising=False)
    lib, other = tmp_path / "lib", tmp_path / "other"
    lib.mkdir()
    other.mkdir()
    (lib / "lib_settings_on_path.py").write_text("SIM_ID = 'Shop_lib'\n", "utf-8")
    write(lib, MINIMAL, "shop.yaml")
    text = MINIMAL.replace(
        "sim_id: Shop", "sim_id: !python lib_settings_on_path.SIM_ID"
    )
    write(other, text, "shop.yaml")
    SimulationContext.from_yaml(lib / "shop.yaml")
    monkeypatch.syspath_prepend(str(lib))
    assert SimulationContext.from_yaml(other / "shop.yaml").sim_id == "Shop_lib"
    sys.modules.pop("lib_settings_on_path", None)


def test_a_folder_put_back_on_sys_path_after_its_loader_entry_was_taken_off_stays_found(
    tmp_path, monkeypatch
):
    """A load from `lib` puts `lib` on `sys.path`. Something else then takes that
    entry off, as pytest's `monkeypatch` does when it restores `sys.path` after a
    test, and a caller puts `lib` back itself. The caller's only copy is not the
    loader's, so a blueprint in another folder still finds `lib`'s modules."""
    import sys

    monkeypatch.delitem(sys.modules, "lib_settings_put_back", raising=False)
    lib, other = tmp_path / "lib", tmp_path / "other"
    lib.mkdir()
    other.mkdir()
    (lib / "lib_settings_put_back.py").write_text("SIM_ID = 'Shop_lib'\n", "utf-8")
    write(lib, MINIMAL, "shop.yaml")
    text = MINIMAL.replace(
        "sim_id: Shop", "sim_id: !python lib_settings_put_back.SIM_ID"
    )
    write(other, text, "shop.yaml")
    with monkeypatch.context() as inner:
        inner.syspath_prepend(str(tmp_path))
        SimulationContext.from_yaml(lib / "shop.yaml")
    assert str(lib) not in sys.path
    monkeypatch.syspath_prepend(str(lib))
    assert SimulationContext.from_yaml(other / "shop.yaml").sim_id == "Shop_lib"
    sys.modules.pop("lib_settings_put_back", None)


def test_a_folder_added_twice_by_nested_loads_stays_hidden_from_the_next_folder(
    tmp_path, monkeypatch
):
    """A load from `b` puts `b` on `sys.path`. A load from `a` that includes a file of
    `b` hides that entry, so the include puts `b` on `sys.path` a second time, and the
    first entry comes back when the load of `a` ends. Both entries are the loader's, so
    a blueprint in a third folder still does not find `b`'s modules."""
    import sys

    from desflow.blueprint.loader import read_document

    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.delitem(sys.modules, "b_only_settings", raising=False)
    for folder in ("a", "b", "c"):
        (tmp_path / folder).mkdir()
    (tmp_path / "b" / "b_only_settings.py").write_text("WHO = 'b'\n", "utf-8")
    (tmp_path / "b" / "part.yaml").write_text(
        "who: !python b_only_settings.WHO\n", encoding="utf-8"
    )
    (tmp_path / "a" / "main.yaml").write_text(
        "part: !include ../b/part.yaml\n", encoding="utf-8"
    )
    (tmp_path / "c" / "main.yaml").write_text(
        "who: !python b_only_settings.WHO\n", encoding="utf-8"
    )
    read_document(tmp_path / "b" / "part.yaml")
    assert read_document(tmp_path / "a" / "main.yaml")[0] == {"part": {"who": "b"}}
    with pytest.raises(BlueprintError, match="no module named 'b_only_settings'"):
        read_document(tmp_path / "c" / "main.yaml")
    sys.modules.pop("b_only_settings", None)


def test_a_module_a_run_imports_is_not_shared_with_the_next_folder(
    tmp_path, monkeypatch
):
    """The payload function of each file imports `table_at_run_time` when it runs,
    after its load is over, as `desflow test a/m.yaml b/m.yaml` does one file after
    the other."""
    import sys

    monkeypatch.delitem(sys.modules, "table_at_run_time", raising=False)
    for folder, value in (("a", 1), ("b", 5)):
        (tmp_path / folder).mkdir()
        (tmp_path / folder / "table_at_run_time.py").write_text(
            f"V = {value}\n", encoding="utf-8"
        )
        (tmp_path / folder / "logic_at_run_time.py").write_text(
            "def pay(task_id, context):\n"
            "    import table_at_run_time\n"
            "    return {'v': table_at_run_time.V}\n",
            encoding="utf-8",
        )
        text = MINIMAL.replace(
            "    payload: {status: finished}\n    id_field: order_id\n",
            "    payload: !python logic_at_run_time.pay\n",
        )
        write(tmp_path / folder, text, "shop.yaml")
    seen = []
    for folder in ("a", "b", "a"):
        app = SimulationContext.from_yaml(tmp_path / folder / "shop.yaml")
        capture = _Capture()
        app.add_egress(capture)
        app.run()
        values = {
            r["value"]["v"]
            for r in capture.records
            if r["stream_type"] == "event" and "v" in r["value"]
        }
        seen.append((folder, values))

    assert seen == [("a", {1}), ("b", {5}), ("a", {1})]
    for name in ("table_at_run_time", "logic_at_run_time"):
        sys.modules.pop(name, None)


def test_a_package_on_another_path_entry_under_an_entry_folder_is_not_swapped_out(
    tmp_path, monkeypatch
):
    """An editable install puts `proj/src` on `sys.path`. A package imported from it
    after a load of `proj/shop.yaml` is not the folder's, because `src` is not a file
    or a package directly in `proj`, so a load from `proj/sub` keeps the same module."""
    import sys

    monkeypatch.delitem(sys.modules, "editable_pkg_under_folder", raising=False)
    package = tmp_path / "src" / "editable_pkg_under_folder"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("SEEN = []\n", encoding="utf-8")
    (tmp_path / "sub").mkdir()
    monkeypatch.syspath_prepend(str(tmp_path / "src"))
    write(tmp_path, MINIMAL, "shop.yaml")
    write(tmp_path / "sub", MINIMAL, "shop.yaml")

    SimulationContext.from_yaml(tmp_path / "shop.yaml")
    import editable_pkg_under_folder as first

    first.SEEN.append("first run")
    SimulationContext.from_yaml(tmp_path / "sub" / "shop.yaml")
    import editable_pkg_under_folder as second

    assert second is first
    assert second.SEEN == ["first run"]
    sys.modules.pop("editable_pkg_under_folder", None)


def test_a_package_of_one_entry_folder_stays_for_a_folder_without_that_name(
    tmp_path, monkeypatch
):
    """A flat-layout project: `proj/flat_pkg_beside_models/` is the package of
    `proj`, and `proj/models` has no module of that name, so a load from `proj/models`
    keeps the module a script imported after the load of `proj/shop.yaml`."""
    import sys

    monkeypatch.delitem(sys.modules, "flat_pkg_beside_models", raising=False)
    package = tmp_path / "flat_pkg_beside_models"
    package.mkdir()
    (package / "__init__.py").write_text("SEEN = []\n", encoding="utf-8")
    (tmp_path / "models").mkdir()
    monkeypatch.syspath_prepend(str(tmp_path))
    write(tmp_path, MINIMAL, "shop.yaml")
    write(tmp_path / "models", MINIMAL, "shop.yaml")

    SimulationContext.from_yaml(tmp_path / "shop.yaml")
    import flat_pkg_beside_models as first

    first.SEEN.append("first run")
    SimulationContext.from_yaml(tmp_path / "models" / "shop.yaml")
    import flat_pkg_beside_models as second

    assert second is first
    assert second.SEEN == ["first run"]
    sys.modules.pop("flat_pkg_beside_models", None)


def test_finding_the_folder_of_a_module_does_not_go_through_every_entry_folder(
    tmp_path, monkeypatch
):
    """A long process, such as a test run, loads files from thousands of folders.
    The folder of each module is looked up from the module's own path, so a load
    does not cost more with every folder loaded before it."""
    from desflow.blueprint import loader

    class Folders(dict):
        def __iter__(self):
            raise AssertionError("the entry folders were walked one by one")

    folders = Folders({tmp_path / f"f{n}": {} for n in range(2000)})
    monkeypatch.setattr(loader, "_folder_modules", folders)
    write(tmp_path, MINIMAL, "shop.yaml")
    SimulationContext.from_yaml(tmp_path / "shop.yaml")


def test_a_file_loaded_from_inside_an_import_uses_the_modules_of_its_own_folder(
    tmp_path, monkeypatch
):
    """`a/outer_loader.py` imports `helpers_nested` of `a`, then loads `a/sub/x.yaml`,
    whose `!python helpers_nested.SIM_ID` is the one of `a/sub`."""
    import sys

    for name in ("helpers_nested", "outer_loader"):
        monkeypatch.delitem(sys.modules, name, raising=False)
    (tmp_path / "sub").mkdir()
    (tmp_path / "helpers_nested.py").write_text("SIM_ID = 'outer'\n", encoding="utf-8")
    (tmp_path / "sub" / "helpers_nested.py").write_text(
        "SIM_ID = 'inner'\n", encoding="utf-8"
    )
    write(
        tmp_path / "sub",
        MINIMAL.replace("sim_id: Shop", "sim_id: !python helpers_nested.SIM_ID"),
        "x.yaml",
    )
    (tmp_path / "outer_loader.py").write_text(
        "import helpers_nested\n"
        "from desflow import SimulationContext\n"
        f"INNER = SimulationContext.from_yaml({str(tmp_path / 'sub' / 'x.yaml')!r}).sim_id\n"
        "OUTER = helpers_nested.SIM_ID\n",
        encoding="utf-8",
    )
    write(
        tmp_path,
        MINIMAL.replace("sim_id: Shop", "sim_id: !python outer_loader.OUTER"),
        "outer.yaml",
    )
    outer = SimulationContext.from_yaml(tmp_path / "outer.yaml")
    inner = sys.modules["outer_loader"].INNER

    assert (outer.sim_id, inner) == ("outer", "inner")
    for name in ("helpers_nested", "outer_loader"):
        sys.modules.pop(name, None)


def test_a_file_does_not_import_the_module_of_a_file_loaded_earlier(
    tmp_path, monkeypatch
):
    """`fa` has `helpers.py` and `fb` has none, so `fb/bp.yaml` is refused whether or
    not `fa/bp.yaml` was loaded before it in the process."""
    import sys

    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.delitem(sys.modules, "helpers_not_in_fb", raising=False)
    text = MINIMAL.replace("sim_id: Shop", "sim_id: !python helpers_not_in_fb.SIM_ID")
    (tmp_path / "fa").mkdir()
    (tmp_path / "fa" / "helpers_not_in_fb.py").write_text(
        "SIM_ID = 'fa'\n", encoding="utf-8"
    )
    (tmp_path / "fb").mkdir()
    write(tmp_path / "fa", text)
    write(tmp_path / "fb", text)

    assert (
        SimulationContext.from_yaml(tmp_path / "fa" / "blueprint.yaml").sim_id == "fa"
    )
    with pytest.raises(BlueprintError, match="no module named 'helpers_not_in_fb'"):
        SimulationContext.from_yaml(tmp_path / "fb" / "blueprint.yaml")
    assert (
        SimulationContext.from_yaml(tmp_path / "fa" / "blueprint.yaml").sim_id == "fa"
    )
    sys.modules.pop("helpers_not_in_fb", None)


def test_an_included_file_from_another_folder_uses_the_modules_of_its_own_folder(
    tmp_path, monkeypatch
):
    """`x/main.yaml` and `y/part.yaml`, which it includes, each import `helpers_inc`
    from their own folder, and `main.yaml` imports its own again after the include."""
    import sys

    from desflow.blueprint.loader import read_document

    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.delitem(sys.modules, "helpers_inc", raising=False)
    for folder in ("x", "y"):
        (tmp_path / folder).mkdir()
        (tmp_path / folder / "helpers_inc.py").write_text(
            f"WHO = '{folder}'\n", encoding="utf-8"
        )
    (tmp_path / "y" / "part.yaml").write_text(
        "who: !python helpers_inc.WHO\n", encoding="utf-8"
    )
    (tmp_path / "x" / "main.yaml").write_text(
        "first: !python helpers_inc.WHO\npart: !include ../y/part.yaml\n"
        "last: !python helpers_inc.WHO\n",
        encoding="utf-8",
    )

    data, _source = read_document(tmp_path / "x" / "main.yaml")

    assert data == {"first": "x", "part": {"who": "y"}, "last": "x"}
    sys.modules.pop("helpers_inc", None)


@pytest.mark.parametrize(
    "reference,fragment",
    [
        ("no_such_module_xyz.func", "no module named 'no_such_module_xyz'"),
        ("{logic}.missing", "has no attribute 'missing'"),
        ("{logic}", "expected a dotted path"),
        ("{logic}.not valid", "expected a dotted path"),
    ],
)
def test_python_reference_failures_name_the_line(tmp_path, logic, reference, fragment):
    text = MINIMAL + f"processes:\n  - !python {reference.format(logic=logic)}\n"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:20: !python"), message
    assert fragment in message


def test_python_reference_reports_an_import_failure_inside_the_module(tmp_path):
    (tmp_path / "logic_fails_on_import.py").write_text(
        "import no_such_dependency_xyz\n", encoding="utf-8"
    )
    path = write(
        tmp_path, MINIMAL + "processes:\n  - !python logic_fails_on_import.run\n"
    )
    with pytest.raises(
        BlueprintError, match=r"importing 'logic_fails_on_import' failed: No module"
    ):
        SimulationContext.from_yaml(path)


def test_python_tag_on_a_mapping_is_rejected(tmp_path):
    path = write(tmp_path, MINIMAL + "processes:\n  - !python {a: b}\n")
    with pytest.raises(
        BlueprintError, match=rf"{re.escape(str(path))}:20: !python takes a dotted"
    ):
        SimulationContext.from_yaml(path)


def test_a_process_must_be_a_generator_function(tmp_path, logic):
    text = MINIMAL + f"processes:\n  - !python {logic}.not_a_generator\n"
    path = write(tmp_path, text)
    with pytest.raises(
        BlueprintError, match=rf"{re.escape(str(path))}:20: .*generator function"
    ):
        SimulationContext.from_yaml(path)


def test_id_field_needs_a_mapping_payload(tmp_path, logic):
    text = MINIMAL.replace(
        "    payload: {status: finished}\n",
        f"    payload: !python {logic}.payload\n",
    )
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError, match="id_field applies only to a mapping"):
        SimulationContext.from_yaml(path)


def test_a_fractional_store_capacity_is_refused(tmp_path):
    """A store capacity must be a whole number, as a resource capacity must."""
    text = MINIMAL.replace(
        "resources:\n  packers: {current_cap: 2, max_cap: 5}\n",
        "resources:\n  packers: {current_cap: 2, max_cap: 5}\n"
        "stores:\n  shelf: {current_cap: 1.5, max_cap: 5}\n",
    )
    with pytest.raises(BlueprintError, match="whole number"):
        SimulationContext.from_yaml(write(tmp_path, text))


def test_item_field_needs_a_store_get(tmp_path):
    """item_field adds the item a store get took, so a task without one is refused."""
    text = MINIMAL.replace("    id_field: order_id\n", "    item_field: picked\n")
    with pytest.raises(
        BlueprintError, match="item_field adds the item a store get took"
    ):
        SimulationContext.from_yaml(write(tmp_path, text))


def test_item_field_needs_a_mapping_payload(tmp_path, logic):
    """item_field adds an item to a mapping payload, so a non-mapping payload is refused."""
    text = MINIMAL.replace(
        "resources:\n  packers: {current_cap: 2, max_cap: 5}\n",
        "resources:\n  packers: {current_cap: 2, max_cap: 5}\n"
        "stores:\n  shelf: {current_cap: 5, max_cap: 5}\n",
    ).replace(
        "    payload: {status: finished}\n    id_field: order_id\n",
        f"    get: {{store: shelf}}\n    item_field: picked\n"
        f"    payload: !python {logic}.payload\n",
    )
    with pytest.raises(BlueprintError, match="item_field applies only to a mapping"):
        SimulationContext.from_yaml(write(tmp_path, text))


def test_plain_safe_load_is_unaffected():
    """The tag is registered on the blueprint loader only."""
    import yaml

    with pytest.raises(yaml.constructor.ConstructorError):
        yaml.safe_load("x: !python os.system\n")


# ---------------------------------------------------------------------------
# Scenarios (#15)
# ---------------------------------------------------------------------------
SCENARIO = """\
scenario:
  - {at: 20, path: Shop.resources.packers.current_cap, value: 1}
  - {at: 10, path: Shop.resources.packers.current_cap, value: 4}
  - {at: 15 s, path: Shop.arrival.standard.rate, value: 3}
  - {at: 0, path: Shop.variables.mode, value: warm}
"""


def test_scenario_applies_on_simulation_time_at_factor_0(tmp_path):
    text = (
        MINIMAL
        + "variables:\n  mode: cold\n"
        + SCENARIO
        + "stats: {level: minimal, interval: 1}\n"
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=25)

    capacity = {
        r["sim_ts"]: r["value"]
        for r in capture.records
        if r.get("path_id") == "Shop.stats.resources.packers.capacity"
    }
    assert capacity[9.0] == 2
    assert capacity[11.0] == 4
    assert capacity[21.0] == 1

    registry = app.env.registry
    assert registry.get("Shop.arrival.standard.rate").value == 3.0
    assert registry.get("Shop.variables.mode").value == "warm"


@pytest.mark.parametrize(
    "step,fragment",
    [
        (
            "{at: 5, path: Shop.resources.press.current_cap, value: 1}",
            "not a registry",
        ),
        ("{at: 5, path: Shop.arrival.standard.mean, value: 1}", "not a registry"),
        (
            "{at: 5, path: Warehouse.resources.packers.current_cap, value: 1}",
            "not a registry",
        ),
        (
            "{at: 5, path: Shop.resources.packers.current_cap, value: many}",
            "needs a whole number of 0 or more for current_cap",
        ),
        (
            "{at: 5, path: Shop.arrival.standard.rate, value: 0}",
            "exponential rate must be greater than 0",
        ),
        (
            "{at: 5, path: Shop.service.packing.std, value: -1}",
            "normal std must be 0 or more",
        ),
    ],
)
def test_scenario_paths_are_checked_before_the_run(tmp_path, step, fragment):
    path = write(tmp_path, MINIMAL + f"scenario:\n  - {step}\n")
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:20:"), message
    assert fragment in message


def test_a_scenario_value_valid_only_after_an_earlier_step_loads(tmp_path):
    """Steps are checked in the order the run applies them, as written at equal times."""
    text = (
        MINIMAL.replace(
            "packing: {dist: normal, mean: 3.0, std: 0.5}",
            "packing: {dist: uniform, low: 1, high: 2}",
        )
        + "scenario:\n"
        "  - {at: 5, path: Shop.service.packing.low, value: 3}\n"
        "  - {at: 5, path: Shop.service.packing.high, value: 6}\n"
    )
    # low 3 is above the current high 2 until the second step raises it
    with pytest.raises(BlueprintError):
        SimulationContext.from_yaml(write(tmp_path, text))
    ordered = text.replace(
        "at: 5, path: Shop.service.packing.low", "at: 6, path: Shop.service.packing.low"
    )
    app = SimulationContext.from_yaml(write(tmp_path, ordered))
    app.run(until=30)

    registry = app.env.registry
    assert (
        registry.get_config("Shop.service.packing").low,
        registry.get_config("Shop.service.packing").high,
    ) == (3, 6)


def test_checking_the_scenario_leaves_the_starting_values(tmp_path):
    """The load check applies the steps to a copy, not to the model's own parameters."""
    text = (
        MINIMAL + "scenario:\n  - {at: 5, path: Shop.arrival.standard.rate, value: 9}\n"
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))

    assert app.compile_parameters().arrival["standard"].rate == 1.0


def test_scenario_at_must_be_a_time(tmp_path):
    path = write(
        tmp_path,
        MINIMAL + "scenario:\n  - {at: later, path: Shop.variables.x, value: 1}\n",
    )
    with pytest.raises(
        BlueprintError, match=rf"{re.escape(str(path))}:20: scenario.0.at"
    ):
        SimulationContext.from_yaml(path)


def test_compile_parameters_matches_what_run_registers(tmp_path):
    """A run registers a copy of the parameters, so a live change leaves the
    declarations as written."""
    app = SimulationContext.from_yaml(write(tmp_path, MINIMAL))
    params = app.compile_parameters()
    app.run(until=1)

    assert params.sim_id == "Shop"
    registered = app.env.registry.get_config("Shop.resources.packers")
    assert registered == params.resources["packers"]
    assert registered is not params.resources["packers"]
    app.env.registry.update("Shop.resources.packers.current_cap", 3)
    assert app.compile_parameters().resources["packers"].current_cap == 2


# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
ENV_BLUEPRINT = """\
simulation:
  sim_id: ${SIM_ID}
  factor: ${FACTOR:-0}
  random_seed: 7
variables:
  host: ${DB_HOST:-localhost}
  url: "postgres://${DB_HOST:-localhost}:${DB_PORT:-5432}/sim"
  port: ${DB_PORT:-5432}
  quoted_port: "${DB_PORT:-5432}"
  literal: $${NOT_A_VARIABLE}
run:
  until: ${UNTIL:-1 min}
"""


def test_environment_variables_are_substituted(tmp_path, monkeypatch):
    monkeypatch.setenv("SIM_ID", "Warehouse")
    monkeypatch.setenv("DB_HOST", "db.internal")
    monkeypatch.delenv("FACTOR", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    monkeypatch.delenv("UNTIL", raising=False)
    monkeypatch.delenv("NOT_A_VARIABLE", raising=False)
    app = SimulationContext.from_yaml(write(tmp_path, ENV_BLUEPRINT))

    assert (app.sim_id, app.factor, app._defaults.until) == ("Warehouse", 0, 60.0)
    variables = app._variables_config
    assert variables["host"] == "db.internal"
    assert variables["url"] == "postgres://db.internal:5432/sim"
    # An unquoted value is typed after substitution; a quoted one stays a string.
    assert variables["port"] == 5432
    assert variables["quoted_port"] == "5432"
    assert variables["literal"] == "${NOT_A_VARIABLE}"


def test_an_empty_variable_takes_the_default(tmp_path, monkeypatch):
    monkeypatch.setenv("SIM_ID", "Warehouse")
    monkeypatch.setenv("DB_PORT", "")
    app = SimulationContext.from_yaml(write(tmp_path, ENV_BLUEPRINT))
    assert app._variables_config["port"] == 5432


def test_an_unset_variable_without_a_default_names_the_line(tmp_path, monkeypatch):
    monkeypatch.delenv("SIM_ID", raising=False)
    path = write(tmp_path, ENV_BLUEPRINT)
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:2: environment variable SIM_ID is not set",
    ):
        SimulationContext.from_yaml(path)


def test_python_references_take_environment_variables(tmp_path, logic, monkeypatch):
    monkeypatch.setenv("LOGIC", logic)
    text = MINIMAL + "processes:\n  - function: !python ${LOGIC}.ticker\n"
    text += "    kwargs: {step: 2}\n"
    app = SimulationContext.from_yaml(write(tmp_path, text))
    assert app._startup_loops[-1][0].func.__name__ == "ticker"


# ---------------------------------------------------------------------------
# Relative times
# ---------------------------------------------------------------------------
def _with_times(start, live):
    return MINIMAL.replace(
        "  random_seed: 7\n",
        f"  random_seed: 7\n  logical_start_time: {start}\n  go_live_at: {live}\n",
    )


@pytest.mark.parametrize(
    "start,live,start_offset,live_offset",
    [
        ("-1d", "now", -86400, 0),
        ("-7d", "-10m", -7 * 86400, -600),
        ("-10 min", "+30s", -600, 30),
        ("now", "now", 0, 0),
    ],
)
def test_relative_times_read_one_now(tmp_path, start, live, start_offset, live_offset):
    from datetime import datetime, timedelta, timezone

    # `now` is an instant, read in the run's zone, UTC by default
    before = datetime.now(timezone.utc)
    app = SimulationContext.from_yaml(write(tmp_path, _with_times(start, live)))
    after = datetime.now(timezone.utc)
    assert app.logical_start_time.tzinfo is timezone.utc

    assert app.go_live_at - app.logical_start_time == timedelta(
        seconds=live_offset - start_offset
    )
    now = app.logical_start_time - timedelta(seconds=start_offset)
    assert before <= now <= after


def test_absolute_times_are_read_as_datetimes(tmp_path):
    from datetime import datetime

    text = _with_times("2026-01-01T00:00:00", "'2026-01-02T06:30:00'")
    app = SimulationContext.from_yaml(write(tmp_path, text))
    assert app.logical_start_time == datetime(2026, 1, 1)
    assert app.go_live_at == datetime(2026, 1, 2, 6, 30)


def test_times_take_python_objects(tmp_path, logic):
    from datetime import datetime

    (tmp_path / f"{logic}_times.py").write_text(
        "from datetime import datetime\nSTART = datetime(2026, 3, 1)\n",
        encoding="utf-8",
    )
    text = _with_times(f"!python {logic}_times.START", "now")
    app = SimulationContext.from_yaml(write(tmp_path, text))
    assert app.logical_start_time == datetime(2026, 3, 1)


@pytest.mark.parametrize(
    "value,fragment",
    [
        ("-1 fortnight", "not a duration"),
        ("yesterday", "not a time"),
        ("20260101", "a number is not a time"),
        ("'0'", "a number is not a time"),
    ],
)
def test_a_bad_time_names_the_line(tmp_path, value, fragment):
    path = write(tmp_path, _with_times(value, "now"))
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:5: simulation.logical_start_time: .*{fragment}",
    ):
        SimulationContext.from_yaml(path)


def test_a_date_that_does_not_exist_is_refused_at_its_line(tmp_path):
    path = write(
        tmp_path,
        MINIMAL.replace(
            "  random_seed: 7\n", "  random_seed: 7\n  logical_start_time: 2026-13-01\n"
        ),
    )
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    assert str(error.value).startswith(f"{path}:5: '2026-13-01' is not a date or time")


def test_a_file_that_is_not_utf8_is_refused_with_the_line_of_the_bad_byte(tmp_path):
    path = tmp_path / "blueprint.yaml"
    path.write_bytes((MINIMAL + "# caf\xe9\n").encode("latin-1"))
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    line = MINIMAL.count("\n") + 1
    assert str(error.value).startswith(f"{path}:{line}: the file is not UTF-8 text")


# ---------------------------------------------------------------------------
# when: history and when: live
# ---------------------------------------------------------------------------
SPLIT = """\
egress:
  - type: Console
    when: history
  - type: Console
    when: live
stats: {level: minimal, interval: 1}
"""


def test_history_and_live_split_records_at_go_live(tmp_path):
    text = _with_times("2026-01-01T00:00:00", "2026-01-01T00:00:10") + SPLIT
    app = SimulationContext.from_yaml(write(tmp_path, text))
    history, live = _Capture(), _Capture()
    # The YAML providers are swapped for captures; the predicates stay as built.
    app._sinks.egress[:] = [
        replace(sink, provider=provider)
        for sink, provider in zip(app._sinks.egress, [history, live])
    ]
    # Ten simulation seconds of history, then a short live tail in real time.
    app.run(until=10.3)

    assert history.records and live.records
    assert all(r["timestamp"] < "2026-01-01T00:00:10.000" for r in history.records)
    assert all(r["timestamp"] >= "2026-01-01T00:00:10.000" for r in live.records)
    assert max(r["sim_ts"] for r in history.records) < 10
    assert min(r["sim_ts"] for r in live.records) == 10


def test_live_is_compared_in_the_zone_of_the_start_time(tmp_path):
    text = _with_times("2026-01-01T00:00:00+00:00", "2026-01-01T10:00:10+10:00") + SPLIT
    app = SimulationContext.from_yaml(write(tmp_path, text))
    history, live = [sink.predicate for sink in app._sinks.egress]
    record = {"timestamp": "2026-01-01T00:00:10.000+00:00"}
    assert live(record) and not history(record)
    record = {"timestamp": "2026-01-01T00:00:09.999+00:00"}
    assert history(record) and not live(record)


def test_history_needs_go_live_at(tmp_path):
    path = write(tmp_path, MINIMAL + "egress:\n  - type: Console\n    when: history\n")
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:21: when: history needs simulation.go_live_at",
    ):
        SimulationContext.from_yaml(path)


def test_an_unknown_when_names_the_line(tmp_path):
    path = write(tmp_path, MINIMAL + "egress:\n  - type: Console\n    when: later\n")
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:21: egress.0.when: .*'later' is not a when. Use history, live",
    ):
        SimulationContext.from_yaml(path)


# ---------------------------------------------------------------------------
# Tasks with no service and no resource
# ---------------------------------------------------------------------------
def test_a_task_without_service_or_resource_emits_at_once(tmp_path):
    text = MINIMAL.replace("    service: packing\n    resource: packers\n", "")
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=10)

    events = [r for r in capture.records if r["stream_type"] == "event"]
    assert events
    # Only the payload: no queued or started event, and no time in service.
    assert all(set(r["value"]) == {"status", "order_id"} for r in events)
    assert [r["value"]["order_id"] for r in events] == list(range(len(events)))


def test_a_resource_needs_a_service(tmp_path):
    path = write(tmp_path, MINIMAL.replace("    service: packing\n", ""))
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:12: tasks.part: .*a resource needs a service",
    ):
        SimulationContext.from_yaml(path)


def test_stats_minimal_publishes_resource_readings(tmp_path):
    text = MINIMAL + "stats: {level: minimal, interval: 1}\n"
    app = SimulationContext.from_yaml(write(tmp_path, text))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=20)

    prefix = "Shop.stats.resources.packers."
    samples = {}
    for r in capture.records:
        if r["stream_type"] == "telemetry" and r["path_id"].startswith(prefix):
            samples.setdefault(r["sim_ts"], {})[r["path_id"].removeprefix(prefix)] = r[
                "value"
            ]
    # Every second, and once more at the end of the run
    assert len(samples) == 20
    assert all(s["capacity"] == 2 for s in samples.values())
    assert all(
        s["in_use"] / s["capacity"] * 100 == s["utilization"] for s in samples.values()
    )
    assert any(s["in_use"] > 0 for s in samples.values())


# ---------------------------------------------------------------------------
# Containers, positive settings, connector errors and time zones
# ---------------------------------------------------------------------------
def test_container_capacity_keeps_fractions(tmp_path):
    text = MINIMAL + textwrap.dedent("""\
        containers:
          tank: {current_cap: 50.5, max_cap: 100}
        scenario:
          - {at: 5, path: Shop.containers.tank.current_cap, value: 62.5}
        """)
    app = SimulationContext.from_yaml(write(tmp_path, text))
    app.run(until=10)

    assert app.env.registry.get("Shop.containers.tank.current_cap").value == 62.5
    assert app.env.registry.get("Shop.containers.tank.max_cap").value == 100


@pytest.mark.parametrize(
    "extra,line,fragment",
    [
        ("stats: {level: minimal, interval: 0}\n", 19, "interval"),
        ("batching: {batch_size: 0, flush_interval: 1}\n", 19, "batch_size"),
        ("batching: {batch_size: 10, flush_interval: 0}\n", 19, "flush_interval"),
        ("egress:\n  - type: Console\n    flush_interval: 0\n", 21, "flush_interval"),
    ],
)
def test_zero_intervals_and_sizes_are_rejected(tmp_path, extra, line, fragment):
    path = write(tmp_path, MINIMAL + extra)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert fragment in message


def test_zero_until_is_rejected(tmp_path):
    path = write(tmp_path, MINIMAL.replace("until: 30", "until: 0"))
    with pytest.raises(
        BlueprintError, match=rf"{re.escape(str(path))}:18: .*run.until"
    ):
        SimulationContext.from_yaml(path)


SEGMENTED = """\
simulation: {sim_id: S, factor: 0, random_seed: 1}
services: {one: {dist: constant, value: 1}}
arrivals: {a: {replay: {path: a.csv}, spawn: ticking}}
tasks:
  ticking: {service: one, payload: {event_type: tick}}
  looking: {payload: {event_type: look}}
run:
  until: 6
"""


SEEN = """\
def seen(task_id, context):
    return {"at": context.env.now, "next_event": context.env.peek()}
"""


def _segmented(tmp_path, run, tasks=None, until=True):
    text = SEGMENTED.split("run:")[0] + "run:\n" + textwrap.indent(run, "  ")
    if until:
        text = text.replace("run:\n", "run:\n  until: 6\n", 1)
    if tasks:
        text = text.replace("  looking:", tasks + "\n  looking:")
    (tmp_path / "a.csv").write_text("time\n1\n1\n3\n")
    return write(tmp_path, text)


def _readings(records, path_id):
    return [r["value"] for r in records if r.get("path_id") == path_id]


def test_segments_run_through_desflow_run(tmp_path):
    path = _segmented(
        tmp_path, "segments:\n  - {until: 2, look: looking}\n  - {until: 6}\n"
    )
    result = CliRunner().invoke(cli, ["run", str(path), "--stdout"])
    assert result.exit_code == 0, result.output
    assert result.output.count("system.simulation.segment") == 2


def test_segments_run_in_every_replication(tmp_path, monkeypatch):
    path = _segmented(
        tmp_path, "segments:\n  - {until: 2, look: looking}\n  - {until: 6}\n"
    )
    calls = []
    original = SimulationContext._run_segments

    def spy(self, segments):
        calls.append(len(segments))
        return original(self, segments)

    monkeypatch.setattr(SimulationContext, "_run_segments", spy)
    result = CliRunner().invoke(cli, ["run", str(path), "--replications", "2"])
    assert result.exit_code == 0, result.output
    assert len(calls) >= 2


@pytest.mark.parametrize(
    ("run", "last_event"),
    [
        ("segments: [{until: 3}, {until: 6}]\n", 5.0),
        ("segments: [{until: 3}, {}]\n", 51.0),
        ("until: 20\nsegments: [{until: 3}, {until: 6}]\n", 19.0),
    ],
    ids=[
        "ends at the last until",
        "an empty entry runs to the end",
        "run.until ends it",
    ],
)
def test_segments_with_no_run_until_end_at_the_last_stop(tmp_path, run, last_event):
    """A list of segments is a SimPy program that stops calling `env.run`: with an
    arrival of 50 events and no `run.until`, the run ends at the last `until` (the last
    event is at 5 for an `until` of 6). A last entry `{}` runs to the end, and a
    `run.until` ends the run there."""
    text = SEGMENTED.split("run:")[0].replace(
        "arrivals: {a: {replay: {path: a.csv}, spawn: ticking}}",
        "arrivals: {a: {gap: 1, max_events: 50, spawn: ticking}}",
    )
    path = write(tmp_path, text + "run:\n" + textwrap.indent(run, "  "))
    capture = _Capture()
    app = SimulationContext.from_yaml(path)
    app.add_egress(capture)
    app.run()
    events = [r["sim_ts"] for r in capture.records if r["stream_type"] == "event"]
    assert max(events) == last_event


@pytest.mark.parametrize(
    "segments,stop",
    [
        ("  - {until: 2}\n  - {until: 4, look: looking}\n  - {until: 6}\n", 4),
        ("  - {until: 2}\n  - {until: 6, look: looking}\n", 6),
    ],
    ids=["middle", "last"],
)
def test_look_reads_before_the_events_at_the_stop(tmp_path, segments, stop):
    """The look's `!python` payload reads the context at the stop time, before the
    events due at that time run: the clock is the stop and the next event is due
    at it. A task that ends at 4 is recorded after the look."""
    (tmp_path / "seen_logic.py").write_text(SEEN, encoding="utf-8")
    path = _segmented(tmp_path, "segments:\n" + segments)
    path.write_text(
        path.read_text().replace(
            "looking: {payload: {event_type: look}}",
            "looking: {payload: !python seen_logic.seen}",
        )
    )
    capture = _Capture()
    app = SimulationContext.from_yaml(path)
    app.add_egress(capture)
    app.run()
    events = [r for r in capture.records if r["stream_type"] == "event"]
    look = next(r for r in events if r["task"] == "looking")
    assert look["value"]["at"] == stop
    if stop == 4:
        assert look["value"]["next_event"] == 4
        after = [r for r in events if r["task"] == "ticking" and r["sim_ts"] == 4]
        assert after and events.index(look) < events.index(after[0])
    assert [r["now"] for r in _readings(capture.records, "system.simulation.segment")][
        -1
    ] == 6


def test_look_task_refuses_a_timed_step(tmp_path):
    path = _segmented(
        tmp_path,
        "segments:\n  - {until: 2, look: looking}\n",
        tasks="  looking: {service: one, payload: {event_type: look}}",
    )
    text = path.read_text().replace("  looking: {payload: {event_type: look}}\n", "")
    path.write_text(text)
    with pytest.raises(BlueprintError, match="a look"):
        SimulationContext.from_yaml(path)


@pytest.mark.parametrize(
    "other,message",
    [
        ("segments:\n  - {until: 2}\n", "run.steps cannot be used with run.segments"),
        ("drain: true\n", "run.steps cannot be used with run.stop or run.drain"),
        (
            "stop: {signal: gate}\n",
            "run.steps cannot be used with run.stop or run.drain",
        ),
    ],
    ids=["segments", "drain", "stop"],
)
def test_run_steps_refuses_segments_stop_and_drain(tmp_path, other, message):
    path = _segmented(tmp_path, "steps: true\n" + other)
    path.write_text(
        path.read_text().replace("services:", "signals: {gate: {}}\nservices:")
    )
    with pytest.raises(BlueprintError, match=message):
        SimulationContext.from_yaml(path)


@pytest.mark.parametrize(
    "other", ["drain: true\n", "stop: {signal: gate}\n"], ids=["drain", "stop"]
)
def test_segments_refuse_stop_and_drain(tmp_path, other):
    path = _segmented(tmp_path, other + "segments:\n  - {until: 2}\n")
    path.write_text(
        path.read_text().replace("services:", "signals: {gate: {}}\nservices:")
    )
    with pytest.raises(BlueprintError, match="run.segments cannot be used"):
        SimulationContext.from_yaml(path)


def test_the_cli_drain_flag_cannot_be_used_with_segments_or_steps(tmp_path):
    for run in ("segments:\n  - {until: 2}\n", "steps: true\n"):
        path = _segmented(tmp_path, run)
        result = CliRunner().invoke(cli, ["run", str(path), "--drain"])
        assert result.exit_code != 0
        assert "cannot be used with run.stop or run.drain" in result.output


def test_run_with_a_stop_or_drain_refuses_steps_and_segments(tmp_path):
    steps = SimulationContext.from_yaml(_segmented(tmp_path, "steps: true\n"))
    with pytest.raises(ValueError, match="run.steps cannot be used"):
        steps.run(drain=True)
    segments = SimulationContext.from_yaml(
        _segmented(tmp_path, "segments:\n  - {until: 2}\n")
    )
    with pytest.raises(ValueError, match="run.segments cannot be used"):
        segments.run(drain=True)


def test_segment_untils_must_increase(tmp_path):
    path = _segmented(tmp_path, "segments:\n  - {until: 4}\n  - {until: 2}\n")
    with pytest.raises(BlueprintError, match="segment untils increase"):
        SimulationContext.from_yaml(path)


def test_an_entry_after_an_open_segment_is_refused(tmp_path):
    path = _segmented(tmp_path, "segments:\n  - {}\n  - {until: 3}\n")
    with pytest.raises(BlueprintError, match="this segment never runs"):
        SimulationContext.from_yaml(path)


def test_every_needs_until(tmp_path):
    path = _segmented(tmp_path, "segments: {every: 2}\n", until=False)
    with pytest.raises(BlueprintError, match="every needs run.until"):
        SimulationContext.from_yaml(path)


def test_every_counts_from_the_initial_time(tmp_path):
    path = _segmented(tmp_path, "segments: {every: 10}\n")
    text = (
        path.read_text()
        .replace("until: 6", "until: 130")
        .replace("factor: 0,", "factor: 0, initial_time: 100,")
    )
    path.write_text(text)
    (tmp_path / "a.csv").write_text("time\n101\n")
    capture = _Capture()
    app = SimulationContext.from_yaml(path)
    app.add_egress(capture)
    app.run()
    nows = [r["now"] for r in _readings(capture.records, "system.simulation.segment")]
    assert nows == [110, 120, 130]


def test_run_steps_publishes_one_record_per_model_event(tmp_path):
    path = _segmented(tmp_path, "steps: true\n", until=False)
    capture = _Capture()
    app = SimulationContext.from_yaml(path)
    app.add_egress(capture)
    app.run()
    steps = _readings(capture.records, "system.simulation.step")
    assert [s["step"] for s in steps] == list(range(1, len(steps) + 1))
    assert len(steps) >= 4


def test_a_connector_value_error_names_the_line(tmp_path):
    pytest.importorskip("pyiceberg")
    text = MINIMAL + textwrap.dedent("""\
        egress:
          - type: Iceberg
            config:
              catalog: {type: rest, uri: "http://localhost:8181"}
        """)
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)

    message = str(error.value)
    assert message.startswith(f"{path}:21:"), message
    assert "IcebergStorageEgress rejected its config" in message


@pytest.mark.parametrize(
    "times",
    [
        "  logical_start_time: 2026-01-01T00:00:00\n"
        "  go_live_at: 2026-01-01T00:00:10+00:00\n",
        "  logical_start_time: 2026-01-01T00:00:00+00:00\n"
        "  go_live_at: 2026-01-01T00:00:10\n",
    ],
)
def test_a_time_without_an_offset_is_read_in_the_zone(tmp_path, times):
    """A start and a go-live, one with an offset and one without, are ten seconds
    apart: the one without is read in the run's zone, UTC by default."""
    path = write(
        tmp_path, MINIMAL.replace("  random_seed: 7\n", "  random_seed: 7\n" + times)
    )
    app = SimulationContext.from_yaml(path)
    app.run(until=1)
    assert app.env._go_live_sim_time == 10.0


def test_an_unknown_timezone_names_the_line(tmp_path):
    text = MINIMAL.replace(
        "  random_seed: 7\n", "  random_seed: 7\n  timezone: Mars/Base\n"
    )
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    message = str(error.value)
    assert message.startswith(f"{path}:"), message
    assert "'Mars/Base' is not an IANA time zone name" in message


def test_timezone_in_a_blueprint_and_the_builder(tmp_path):
    """Timestamps carry the zone's offset; a start without one is read in it."""
    text = MINIMAL.replace(
        "  random_seed: 7\n",
        "  random_seed: 7\n  timezone: Australia/Melbourne\n"
        "  logical_start_time: 2026-10-03T12:00:00\n",
    )

    def first_timestamp(app):
        sink = _Capture()
        app.add_egress(sink)
        app.run(until=5)
        return sink.records[0]["timestamp"]

    app = SimulationContext.from_yaml(write(tmp_path, text))
    assert first_timestamp(app).endswith("+10:00")
    assert first_timestamp(app).startswith("2026-10-03T12:00:")
    builder = SimulationContext(
        "Shop",
        factor=0,
        timezone="Australia/Melbourne",
        logical_start_time=datetime(2026, 10, 3, 12, 0),
    )
    builder.add_resource("packers", current_cap=1, max_cap=1)
    builder.with_stats("minimal", interval=1)
    stamp = first_timestamp(builder)
    assert stamp.startswith("2026-10-03T12:00:") and stamp.endswith("+10:00")
    with pytest.raises(ValueError, match="not an IANA time zone name"):
        SimulationContext("Shop", timezone="Mars/Base")


def test_resources_in_opposite_orders_warn_and_run(tmp_path, caplog):
    text = (
        MINIMAL.replace(
            "  packers: {current_cap: 2, max_cap: 5}",
            "  packers: {current_cap: 2, max_cap: 5}\n  bench: {current_cap: 1, max_cap: 1}",
        )
        .replace(
            "    resource: packers\n",
            "    resource: [packers, bench]\n",
        )
        .replace(
            "run:",
            "  other:\n    service: packing\n    resource: [bench, packers]\n"
            "    payload: {status: done}\nrun:",
        )
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    with caplog.at_level("WARNING"):
        app.run(until=1)
    assert "in one order everywhere" in caplog.text


def test_cli_drain_runs_until_the_model_is_empty(tmp_path):
    """--drain stops the arrivals at the end time and finishes the orders in the
    shop: every order that started also finished."""
    text = MINIMAL.replace("run:\n  until: 30", "stats: basic\nrun:\n  until: 30")
    path = write(tmp_path, text)
    app = SimulationContext.from_yaml(path)
    app.run(drain=True)
    readings = app.env.stats.readings()
    assert readings["Shop.stats.flows.standard.in_system"] == 0
    assert app.env.now >= 30
    result = CliRunner().invoke(cli, ["run", str(path), "--drain"])
    assert result.exit_code == 0, result.output


def test_cli_drain_without_until_is_an_error_line(tmp_path):
    """--drain on a blueprint with no until names the problem in one line and
    exits 1, as a blueprint error does, instead of a traceback."""
    path = write(tmp_path, MINIMAL.replace("run:\n  until: 30\n", ""))
    result = CliRunner().invoke(cli, ["run", str(path), "--drain"])
    assert result.exit_code == 1
    assert "Error: a drain starts at until, so it needs until" in result.output
    assert result.exception is None or isinstance(result.exception, SystemExit)


def test_lag_interval_in_a_blueprint_and_the_builder(tmp_path):
    """No lag records at factor 0 by default; `simulation.lag_interval` and
    `SimulationContext(lag_interval=)` publish them at that interval."""

    def lags(app):
        sink = _Capture()
        app.add_egress(sink)
        app.run(until=5)
        return [
            r["sim_ts"]
            for r in sink.records
            if r.get("path_id") == "system.simulation.lag_seconds"
        ]

    assert lags(SimulationContext.from_yaml(write(tmp_path, MINIMAL))) == []
    text = MINIMAL.replace(
        "  random_seed: 7\n", "  random_seed: 7\n  lag_interval: 2 s\n"
    )
    assert lags(SimulationContext.from_yaml(write(tmp_path, text))) == [0.0, 2.0, 4.0]
    assert lags(SimulationContext("Shop", factor=0, lag_interval=2)) == [0.0, 2.0, 4.0]
    with pytest.raises(ValueError, match="lag_interval gave -1, which is below 0"):
        SimulationContext("Shop", lag_interval=-1)


@pytest.mark.parametrize(
    "run,line,fragment",
    [
        ("run: {until: 30, stop: {signal: closing}}\n", 18, "signal 'closing'"),
        ("run: {until: 30, stop: {condition: level.bins == 0}}\n", 18, "level.bins"),
        ("run: {until: 30, stop: {condition: attr.size > 1}}\n", 18, "attr.<...>"),
        ("simulation:\n  initial_time: 30\nrun: {until: 30}\n", 18, "until (30)"),
    ],
)
def test_run_section_errors_name_the_line(tmp_path, run, line, fragment):
    """A stop that names nothing or reads an entity, and an until not after the
    initial time, are refused when the file loads, with their line."""
    text = MINIMAL.replace("run:\n  until: 30\n", run)
    if run.startswith("simulation:"):
        text = MINIMAL.replace(
            "  random_seed: 7\n", "  random_seed: 7\n  initial_time: 30\n"
        )
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    assert str(error.value).startswith(f"{path}:"), str(error.value)
    assert fragment in str(error.value)


@pytest.mark.parametrize("key", ["balk", "patience"])
def test_balk_and_patience_cannot_read_the_wait(tmp_path, key):
    """balk and patience are read before the wait starts, so wait.<branch> in them
    is refused when the file loads."""
    value = "wait.stock.done" if key == "balk" else "1 if wait.stock.done else 5"
    text = MINIMAL.replace(
        "    id_field: order_id\n",
        f"    id_field: order_id\n    wait: {{any: {{stock: {{timeout: 5}}}}}}\n"
        f"    {key}: {value}\n",
    )
    with pytest.raises(BlueprintError, match="wait.stock is not a branch"):
        SimulationContext.from_yaml(write(tmp_path, text))


def test_on_stays_a_string_key_only_in_task_signal_and_check_mappings(tmp_path):
    """`on` is kept a string key where desflow uses it (task and signal `on:` lists,
    an experiment check's `on`); a payload key `on` keeps YAML's reading, True."""
    from desflow.blueprint.loader import read_document

    path = tmp_path / "doc.yaml"
    path.write_text(
        "tasks:\n  pack: {on: []}\n"
        "signals:\n  closing: {on: []}\n"
        "checks:\n  - {on: mean}\n"
        "arrivals:\n  orders: {payload: {on: 1}}\n"
    )
    data, _ = read_document(path)
    assert "on" in data["tasks"]["pack"]
    assert "on" in data["signals"]["closing"]
    assert data["checks"][0] == {"on": "mean"}
    assert data["arrivals"]["orders"]["payload"] == {True: 1}


@pytest.mark.parametrize(
    "line,expected",
    [
        ("{}", (1, 1)),
        ("{max_cap: 3}", (1, 3)),
        ("{current_cap: 2}", (2, 2)),
        ("{current_cap: .inf}", (float("inf"), float("inf"))),
    ],
)
def test_a_resource_may_leave_its_capacity_out(tmp_path, line, expected):
    path = write(tmp_path, MINIMAL.replace("{current_cap: 2, max_cap: 5}", line))
    app = SimulationContext.from_yaml(path)
    app.run()
    resource = app.get_resource("packers")
    assert resource.capacity == expected[0]
    assert app.env.registry.get("Shop.resources.packers.max_cap").value == expected[1]


def test_a_resource_takes_a_queue_limit_and_a_type(tmp_path):
    path = write(
        tmp_path,
        MINIMAL.replace(
            "{current_cap: 2, max_cap: 5}",
            "{queue_limit: 3, type: !python desflow.ResourceBase}",
        ),
    )
    app = SimulationContext.from_yaml(path)
    app.run()
    resource = app.get_resource("packers")
    assert type(resource).__name__ == "ResourceBase" and resource.queue_limit == 3


def test_a_stop_condition_reads_the_queue_of_a_resource(tmp_path):
    text = MINIMAL.replace(
        "until: 30", "until: 30\n  stop: {condition: queue.packers >= 3}"
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    app.run()
    assert len(app.get_resource("packers").queue) == 3 and app.env.now < 30

    path = write(tmp_path, text.replace("queue.packers", "queue.packer"), "other.yaml")
    with pytest.raises(BlueprintError, match="queue.packer names no resource"):
        SimulationContext.from_yaml(path)


WAIT_REQUEST = """\
simulation: {sim_id: Shop, factor: 0}
resources:
  picker: {current_cap: 1, max_cap: 1, queue: QUEUE}
  bench: {current_cap: 1, max_cap: 1}
services:
  packing: {dist: constant, value: 1}
arrivals:
  standard: {dist: constant, value: 5, spawn: pack}
  other: {dist: constant, value: 5, spawn: tidy}
tasks:
  pack:
    wait: {request: picker}
    PACK
    service: packing
    payload: {status: finished}
  tidy:
    TIDY
    service: packing
    payload: {status: tidied}
run: {until: 10}
"""


def _wait_request(pack="", tidy="resource: bench", queue="fifo"):
    return (
        WAIT_REQUEST.replace("PACK", pack).replace("TIDY", tidy).replace("QUEUE", queue)
    )


def test_a_resource_in_the_wait_and_in_resource_warns_and_runs_in_yaml(
    tmp_path, caplog
):
    text = _wait_request(pack="resource: picker")
    with caplog.at_level("WARNING"):
        SimulationContext.from_yaml(write(tmp_path, text)).run(until=1)
    assert "takes a unit of resource 'picker' twice" in caplog.text


def test_a_wait_request_counts_in_the_acquisition_order_in_yaml(tmp_path, caplog):
    text = _wait_request(pack="resource: bench", tidy="resource: [bench, picker]")
    with caplog.at_level("WARNING"):
        SimulationContext.from_yaml(write(tmp_path, text)).run(until=1)
    assert "in one order everywhere" in caplog.text
    caplog.clear()
    same = _wait_request(pack="resource: bench", tidy="resource: [picker, bench]")
    with caplog.at_level("WARNING"):
        SimulationContext.from_yaml(write(tmp_path, same, "same.yaml")).run(until=1)
    assert "in one order everywhere" not in caplog.text


WAIT_ALL = """\
simulation: {sim_id: Shop, factor: 0}
resources:
  picker: {current_cap: 1, max_cap: 1}
  bench: {current_cap: 1, max_cap: 1}
services:
  packing: {dist: constant, value: 1}
arrivals:
  standard: {dist: constant, value: 5, spawn: pack}
  other: {dist: constant, value: 5, spawn: tidy}
tasks:
  pack:
    wait: {MODE: {p: {request: picker}, b: {request: bench}}}
    service: packing
    payload: {status: finished}
  tidy:
    TIDY
    service: packing
    payload: {status: tidied}
run: {until: 10}
"""


def test_requests_under_one_all_are_acquired_together_in_yaml(tmp_path, caplog):
    for listed in ("[picker, bench]", "[bench, picker]"):
        text = WAIT_ALL.replace("MODE", "all").replace("TIDY", f"resource: {listed}")
        caplog.clear()
        with caplog.at_level("WARNING"):
            SimulationContext.from_yaml(write(tmp_path, text)).run(until=1)
        assert "together in its wait" in caplog.text
    either = WAIT_ALL.replace("MODE", "any").replace(
        "TIDY", "wait: {any: {b: {request: bench}, p: {request: picker}}}"
    )
    caplog.clear()
    with caplog.at_level("WARNING"):
        SimulationContext.from_yaml(write(tmp_path, either, "either.yaml")).run(until=1)
    assert "in one order everywhere" not in caplog.text


def test_a_kept_resource_requested_in_the_next_wait_is_refused_in_yaml(tmp_path):
    text = WAIT_ALL.replace(
        "wait: {MODE: {p: {request: picker}, b: {request: bench}}}",
        "resource: picker\n    keep: [picker]\n    next: tidy",
    ).replace("TIDY", "wait: {request: picker}")
    with pytest.raises(BlueprintError, match="requests 'picker' in its wait"):
        SimulationContext.from_yaml(write(tmp_path, text))


def test_a_priority_with_a_wait_request_needs_a_priority_queue_in_yaml(tmp_path):
    ranked = _wait_request(pack="priority: 2", queue="priority")
    SimulationContext.from_yaml(write(tmp_path, ranked)).run(until=6)
    fifo = _wait_request(pack="priority: 2")
    with pytest.raises(BlueprintError, match="does not set queue: priority"):
        SimulationContext.from_yaml(write(tmp_path, fifo, "fifo.yaml"))
    bare = _wait_request(pack="priority: 2", tidy="priority: 1").replace(
        "wait: {request: picker}", "wait: {timeout: 1}"
    )
    with pytest.raises(
        BlueprintError,
        match="needs a resource, a seize or a request in its wait or open",
    ):
        SimulationContext.from_yaml(write(tmp_path, bare, "bare.yaml"))


def test_on_in_any_case_is_read_as_the_key_on(tmp_path):
    """`On:` and `ON:` are read as `on`, as YAML 1.1 reads them as true."""
    from desflow.blueprint.loader import read_document

    path = tmp_path / "doc.yaml"
    path.write_text(
        "tasks:\n  pack: {On: []}\n  tidy: {ON: []}\n"
        "signals:\n  closing: {On: []}\n"
        "checks:\n  - {ON: mean}\n"
    )
    data, _ = read_document(path)
    assert list(data["tasks"]["pack"]) == ["on"]
    assert list(data["tasks"]["tidy"]) == ["on"]
    assert list(data["signals"]["closing"]) == ["on"]
    assert data["checks"][0] == {"on": "mean"}


def test_on_stays_a_string_key_in_an_included_task_or_section_file(tmp_path):
    """An `!include`d task file, section file, or task with extra keys keeps its `on`
    keys as strings, as the same text in the blueprint does."""
    from desflow.blueprint.loader import read_document

    (tmp_path / "pack.yaml").write_text("payload: {e: 1}\non:\n  - fire: {signal: s}\n")
    (tmp_path / "plain.yaml").write_text("payload: {e: 1}\n")
    (tmp_path / "tasks.yaml").write_text(
        "pack:\n  payload: {e: 1}\n  on:\n    - fire: {signal: s}\n"
    )
    (tmp_path / "signal.yaml").write_text("once: true\n")
    path = tmp_path / "doc.yaml"
    path.write_text(
        "tasks:\n  pack: !include pack.yaml\n  more: !include {file: plain.yaml, On: []}\n"
    )
    data, _ = read_document(path)
    assert "on" in data["tasks"]["pack"]
    assert data["tasks"]["more"]["on"] == []
    path.write_text("tasks: !include tasks.yaml\n")
    data, _ = read_document(path)
    assert "on" in data["tasks"]["pack"]
    path.write_text("signals:\n  closing: !include {file: signal.yaml, on: []}\n")
    data, _ = read_document(path)
    assert data["signals"]["closing"] == {"once": True, "on": []}


def test_a_collection_that_holds_itself_through_an_alias_is_refused_at_its_line(
    tmp_path,
):
    from desflow.blueprint.loader import read_document

    path = tmp_path / "doc.yaml"
    path.write_text("x: 1\na: &x [1, *x]\n", encoding="utf-8")

    with pytest.raises(
        BlueprintError, match=r"doc.yaml:2: this collection holds itself"
    ):
        read_document(path)


def test_an_alias_is_walked_once_so_a_short_file_of_aliases_reads_at_once(tmp_path):
    """Eight lines of ten aliases each have 10^8 leaves when every alias is walked
    again; the line map walked them all."""
    from desflow.blueprint.loader import read_document

    lines = ["l0: &l0 [x, x, x, x, x, x, x, x, x, x]"]
    for n in range(1, 8):
        aliases = ", ".join([f"*l{n - 1}"] * 10)
        lines.append(f"l{n}: &l{n} [{aliases}]")
    path = tmp_path / "doc.yaml"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    started = time.perf_counter()
    data, source = read_document(path)

    assert time.perf_counter() - started < 5
    assert len(data["l7"]) == 10
    assert source.line(("l7",)) == 8


def test_a_default_that_holds_another_variable_is_refused_at_its_line(
    tmp_path, monkeypatch
):
    """A nested default left the text of the inner reference in the value."""
    monkeypatch.delenv("DESFLOW_NOPE", raising=False)
    monkeypatch.setenv("DESFLOW_B", "bee")
    nested = "sim_id: ${DESFLOW_NOPE:-${DESFLOW_B}}"
    path = write(tmp_path, MINIMAL.replace("sim_id: Shop", nested))

    with pytest.raises(BlueprintError, match=r"blueprint.yaml:2: .*holds another"):
        SimulationContext.from_yaml(path)
    plain = "sim_id: ${DESFLOW_NOPE:-plain}"
    path = write(tmp_path, MINIMAL.replace("sim_id: Shop", plain))
    assert SimulationContext.from_yaml(path).sim_id == "plain"


def test_an_include_refuses_a_key_the_included_file_already_has(tmp_path):
    """`!include {file: fit.yaml, rate: 9}` replaced the fitted `rate` with no sign."""
    from desflow.blueprint.loader import read_document

    (tmp_path / "fit.yaml").write_text("dist: exponential\nrate: 0.5\n")
    path = tmp_path / "doc.yaml"
    path.write_text("a: !include {file: fit.yaml, spawn: pack, rate: 9}\n")

    with pytest.raises(
        BlueprintError,
        match=r"doc.yaml:1: !include fit.yaml: the key 'rate' is in .*doc.yaml and in "
        r".*fit.yaml",
    ):
        read_document(path)
    path.write_text("a: !include {file: fit.yaml, spawn: pack}\n")
    assert read_document(path)[0] == {
        "a": {"dist": "exponential", "rate": 0.5, "spawn": "pack"}
    }


def test_from_yaml_context_reports_the_source_of_each_change(tmp_path):
    """A YAML model takes callbacks on the context `from_yaml` returns."""
    path = write(
        tmp_path,
        MINIMAL
        + """\
scenario:
  - {at: 2, path: Shop.resources.packers.current_cap, value: 3}
ingress:
  - type: Local
    config:
      schedule:
        - [0.0, Shop.resources.packers.max_cap, 4]
""",
    )
    app = SimulationContext.from_yaml(path)
    changes: list = []
    app.on_change.append(changes.append)
    env = app.start()
    deadline = time.monotonic() + 2
    while env.ingress_queue.qsize() < 1:
        assert time.monotonic() < deadline, "the ingress item never arrived"
        time.sleep(0.005)
    app.finish()

    assert sorted((c.source, c.actor, c.path) for c in changes) == [
        ("ingress", "LocalIngress", "Shop.resources.packers.max_cap"),
        ("scenario", None, "Shop.resources.packers.current_cap"),
    ]


def test_from_yaml_file_ingress_reports_its_source(tmp_path):
    """`type: File` loads and its change has source "ingress" and actor FileIngress."""
    params = tmp_path / "params.yaml"
    params.write_text("Shop.resources.packers.max_cap: 4\n")
    path = write(
        tmp_path,
        MINIMAL
        + f"""\
ingress:
  - type: File
    config:
      path: {params}
      poll: 0.05
""",
    )
    app = SimulationContext.from_yaml(path)
    changes: list = []
    app.on_change.append(changes.append)
    env = app.start()
    deadline = time.monotonic() + 2
    while env.ingress_queue.qsize() < 1:
        assert time.monotonic() < deadline, "the ingress item never arrived"
        time.sleep(0.005)
    app.finish()

    assert [(c.source, c.actor, c.path) for c in changes] == [
        ("ingress", "FileIngress", "Shop.resources.packers.max_cap")
    ]


def test_a_yaml_ingress_poll_of_zero_is_refused_at_its_line(tmp_path):
    """`poll: 0` would check the file in a loop with no wait, so the load refuses it
    and names the setting and the line."""
    path = write(
        tmp_path,
        MINIMAL
        + """\
ingress:
  - type: File
    config:
      path: params.yaml
      poll: 0
""",
    )

    with pytest.raises(BlueprintError, match=r"blueprint.yaml:21: .*poll is a number"):
        SimulationContext.from_yaml(path)


def test_registry_paths_of_a_yaml_model_match_its_run(tmp_path):
    """`registry_paths()` of a YAML model equals the registry of its run."""
    app = SimulationContext.from_yaml(write(tmp_path, MINIMAL))

    paths = app.registry_paths()

    env = app.start()
    try:
        assert paths == {p: env.registry.get(p).value for p in env.registry.paths()}
    finally:
        app.finish()


def test_load_context_of_a_builder_file_gives_the_paths_of_its_yaml_twin(tmp_path):
    """`load_context(folder, "main.py").registry_paths()` matches the YAML twin's."""
    import shutil
    from pathlib import Path

    from desflow.loading import load_context

    examples = Path(__file__).resolve().parents[2] / "examples"
    shutil.copy(examples / "builder" / "local_example.py", tmp_path / "main.py")

    from_builder = load_context(tmp_path, "main.py").registry_paths()
    from_yaml = SimulationContext.from_yaml(
        examples / "yaml" / "local.yaml"
    ).registry_paths()

    assert from_builder == from_yaml
    assert from_builder


# ---------------------------------------------------------------------------
# Sink options of Batch 3: when names and expressions, envelope, key_field
# ---------------------------------------------------------------------------
TWO_TASKS = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 7}
services:
  packing: {dist: constant, value: 1.0}
resources:
  packers: {current_cap: 2, max_cap: 5}
arrivals:
  standard: {dist: constant, value: 2.0, spawn: pack}
tasks:
  pack:
    service: packing
    resource: packers
    payload: {event_type: order_packed, sku: A1}
    id_field: order_id
    next: ship
  ship:
    payload: {event_type: order_shipped, sku: A1}
run:
  until: 10
stats: {level: minimal, interval: 1}
"""


def _run_with_sinks(tmp_path, sinks, text=TWO_TASKS):
    """Builds the blueprint with Console sinks carrying `sinks`, swapped for captures."""
    egress = "".join(f"  - type: Console\n    {entry}\n" for entry in sinks)
    app = SimulationContext.from_yaml(write(tmp_path, text + "egress:\n" + egress))
    captures = [_Capture() for _ in sinks]
    app._sinks.egress[:] = [
        replace(sink, provider=provider)
        for sink, provider in zip(app._sinks.egress, captures)
    ]
    app.run()
    return captures


def test_when_expr_filters_records(tmp_path):
    """`when: {expr: ...}` over `record.value.<field>` keeps the records it is true
    for, as the `!python` function of the Streaming Out page did, and a telemetry
    record or an event without the field is false, not an error."""
    packed, events, telemetry, key = _run_with_sinks(
        tmp_path,
        [
            "when: {expr: \"record.value.event_type == 'order_packed'\"}",
            "when: events",
            "when: telemetry",
            "when: {expr: \"record.stream_type == 'event' and record.task == 'ship'\"}",
        ],
    )

    assert packed.records
    assert {r["value"]["event_type"] for r in packed.records} == {"order_packed"}
    assert {r["stream_type"] for r in events.records} == {"event"}
    assert {r["stream_type"] for r in telemetry.records} == {"telemetry"}
    assert {r["task"] for r in key.records} == {"ship"}
    assert len(events.records) > len(packed.records)


def test_when_expr_refuses_an_unknown_name(tmp_path):
    """A name outside `record.stream_type`, `.key`, `.task` and `.value.<field>`
    is refused at load, at the line of the `when`."""
    for expr, fragment in (
        ("record.sim_ts > 1", "'record.sim_ts'"),
        ("t > 3", "the name 't'"),
        ("attr.x > 1", "a name that is not record.stream_type"),
        ("record > 1", "the name 'record' alone"),
        ("record.value.a.b == 1", "'record.value.a.b'"),
    ):
        path = write(
            tmp_path,
            MINIMAL + f'egress:\n  - type: Console\n    when: {{expr: "{expr}"}}\n',
        )
        with pytest.raises(BlueprintError, match=re.escape(fragment)):
            SimulationContext.from_yaml(path)
    # `record` is a name only an egress `when` reads
    from desflow.core.expressions import Expression

    with pytest.raises(ValueError, match="only an egress when reads"):
        Expression("record.key == 'a'")


def test_when_expr_failure_is_a_run_failure(tmp_path):
    """An expression that fails on a record stops the run, with the expression in
    the message: comparing a missing field with a number is a TypeError."""
    text = TWO_TASKS.replace(
        "run:\n  until: 10", "run:\n  until: 10\n  on_unhandled_failure: stop"
    )
    app = SimulationContext.from_yaml(
        write(
            tmp_path,
            text
            + 'egress:\n  - type: Console\n    when: {expr: "record.value.total > 1"}\n',
        )
    )
    app._sinks.egress[:] = [
        replace(sink, provider=provider)
        for sink, provider in zip(app._sinks.egress, [_Capture()])
    ]

    with pytest.raises(
        ValueError, match="egress when: cannot evaluate 'record.value.total > 1'"
    ):
        app.run()


def test_envelope_false_reaches_the_provider_and_filters_records(tmp_path):
    """`envelope: false` on an entry is `add_egress(envelope=False)`: the provider
    gets only the events a task finishes, and no queued or started record."""
    plain, full = _run_with_sinks(tmp_path, ["envelope: false", "envelope: true"])

    assert plain.envelope is False and full.envelope is True
    assert plain.records
    assert {r["stream_type"] for r in plain.records} == {"event"}
    assert all("status" not in r["value"] for r in plain.records)
    assert any(
        isinstance(r["value"], dict) and r["value"].get("status") == "queued"
        for r in full.records
    )
    assert {r["stream_type"] for r in full.records} == {"event", "telemetry"}


def test_key_field_is_the_key_of_the_finished_event(tmp_path):
    """`key_field` names the payload field that becomes the record key of the
    finished event. The queued and started records keep the entity's key."""
    text = TWO_TASKS.replace(
        "    id_field: order_id\n    next: ship",
        "    id_field: order_id\n    key_field: sku\n    next: ship",
    )
    (capture,) = _run_with_sinks(tmp_path, ["when: events"], text)

    packed = [r for r in capture.records if r["task"] == "pack"]
    finished = [r for r in packed if "event_type" in r["value"]]
    lifecycle = [r for r in packed if "status" in r["value"]]
    assert finished and {r["key"] for r in finished} == {"A1"}
    assert lifecycle and all(r["key"].startswith("standard-") for r in lifecycle)
    ship = [r for r in capture.records if r["task"] == "ship"]
    assert all(r["key"].startswith("standard-") for r in ship)


@pytest.mark.parametrize("number,text", [(7, "7"), (0, "0"), (1.5, "1.5")])
def test_key_field_takes_a_number_as_text(tmp_path, number, text):
    """A numeric id, such as `order_id: 7`, becomes the key `7`, as the Kafka sink
    already writes a key."""
    replaced = TWO_TASKS.replace(
        "    id_field: order_id\n    next: ship",
        "    id_field: order_id\n    key_field: qty\n    next: ship",
    ).replace("sku: A1}", f"sku: A1, qty: {number}}}", 1)
    (capture,) = _run_with_sinks(tmp_path, ["when: events"], replaced)

    finished = [
        r for r in capture.records if r["task"] == "pack" and "event_type" in r["value"]
    ]
    assert finished and {r["key"] for r in finished} == {text}


def test_key_field_missing_is_a_run_failure(tmp_path):
    text = TWO_TASKS.replace(
        "    id_field: order_id\n    next: ship",
        "    id_field: order_id\n    key_field: nope\n    next: ship",
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    app._sinks.egress[:] = []

    with pytest.raises(
        ValueError,
        match="task 'pack' has key_field 'nope', and its payload has no value for it",
    ):
        app.run()


def test_sink_options_pass_through_config(tmp_path):
    """`topic_policy`, `topics`, `serializer` and `ssl` are keys of a Kafka entry's
    `config`, read by `KafkaEgress` itself."""
    pytest.importorskip("aiokafka")
    path = write(
        tmp_path,
        MINIMAL
        + """\
egress:
  - type: Kafka
    envelope: false
    config:
      bootstrap_servers: broker:9092
      topic_policy: {mode: drop_and_create, partitions: 3}
      topics:
        orders: {partitions: 6, serializer: {type: glue_json_schema, registry_name: r, schema: {type: object}, boto3: {region_name: us-east-1}}}
""",
    )
    app = SimulationContext.from_yaml(path)
    kafka = app._sinks.egress[0].provider

    assert kafka.topic_policy["mode"] == "drop_and_create"
    assert kafka.topics["orders"]["partitions"] == 6
    assert "orders" in kafka.topic_serializers
    assert [sink.envelope for sink in app._sinks.egress] == [False]


def test_a_bad_sink_option_names_the_line(tmp_path):
    pytest.importorskip("aiokafka")
    path = write(
        tmp_path,
        MINIMAL
        + "egress:\n  - type: Kafka\n    config:\n      bootstrap_servers: b:9092\n      topic_policy: sometimes\n",
    )
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:21: KafkaEgress rejected its config: .*mode 'sometimes' is not one of",
    ):
        SimulationContext.from_yaml(path)


def test_an_on_list_is_checked_against_the_state_its_earlier_actions_make(tmp_path):
    """Setting `high` to 3 and then `low` to 2 is valid after a `low` of 0 and a `high`
    of 1; the reverse order is refused, as the run would refuse it."""

    def text(first, second):
        return MINIMAL.replace(
            "{dist: normal, mean: 3.0, std: 0.5}", "{dist: uniform, low: 0, high: 1}"
        ).replace(
            "    resource: packers\n",
            "    resource: packers\n    on:\n"
            f"      - {{path: Shop.service.packing.{first[0]}, value: {first[1]}}}\n"
            f"      - {{path: Shop.service.packing.{second[0]}, value: {second[1]}}}\n",
            1,
        )

    SimulationContext.from_yaml(
        write(tmp_path, text(("high", 3), ("low", 2)), "ordered.yaml")
    )
    with pytest.raises(Exception, match="low"):
        SimulationContext.from_yaml(
            write(tmp_path, text(("low", 2), ("high", 3)), "reversed.yaml")
        )


def test_a_literal_after_a_generated_value_of_the_same_parent_is_not_refused(
    tmp_path,
):
    """A generated `high` is not known before the run, so a literal `low` after it is
    not checked against the `high` the model starts with."""
    text = MINIMAL.replace(
        "{dist: normal, mean: 3.0, std: 0.5}", "{dist: uniform, low: 0, high: 1}"
    ).replace(
        "    resource: packers\n",
        "    resource: packers\n    on:\n"
        "      - {path: Shop.service.packing.high, "
        "value: {gen: integer, bounds: [3, 3]}}\n"
        "      - {path: Shop.service.packing.low, value: 2}\n",
        1,
    )
    SimulationContext.from_yaml(write(tmp_path, text, "generated.yaml"))


def test_a_literal_after_a_conditional_value_of_the_same_parent_is_not_refused(
    tmp_path,
):
    """An entry with `when` may not run, so a later literal is not checked against
    its value; the conditional value itself is still checked."""

    def text(conditional):
        return MINIMAL.replace(
            "{dist: normal, mean: 3.0, std: 0.5}", "{dist: uniform, low: 0, high: 1}"
        ).replace(
            "    resource: packers\n",
            "    resource: packers\n    on:\n"
            f"      - {{path: Shop.service.packing.high, value: {conditional}, "
            "when: 'False'}\n"
            "      - {path: Shop.service.packing.low, value: 0.75}\n",
            1,
        )

    SimulationContext.from_yaml(write(tmp_path, text(0.5), "conditional.yaml"))
    with pytest.raises(BlueprintError, match="cannot be converted"):
        SimulationContext.from_yaml(write(tmp_path, text("'x'"), "refused.yaml"))


def test_a_conditional_value_valid_after_a_scenario_step_is_not_refused(tmp_path):
    """The scenario raises `high` to 20 before the entry runs, so `low: 10` is valid
    when it runs, though the declared `high` is 1."""
    text = MINIMAL.replace(
        "{dist: normal, mean: 3.0, std: 0.5}", "{dist: uniform, low: 0, high: 1}"
    ).replace(
        "    resource: packers\n",
        "    resource: packers\n    on:\n"
        "      - {path: Shop.service.packing.low, value: 10, when: 'True'}\n",
        1,
    )
    text += "\nscenario:\n  - {at: 1, path: Shop.service.packing.high, value: 20}\n"
    SimulationContext.from_yaml(write(tmp_path, text, "stepped.yaml"))


def test_an_on_entry_with_a_wrong_path_or_value_is_refused_at_its_line(tmp_path):
    """`on:` entries load when their path is in the registry; one whose path is not,
    or whose value the path cannot hold, is refused at the entry's line."""
    text = MINIMAL.replace(
        "    resource: packers\n",
        "    resource: packers\n"
        "    on:\n"
        "      - {path: Shop.resources.lath.current_cap, value: 3}\n",
        1,
    )
    path = write(tmp_path, text)
    line = text.splitlines().index(
        "      - {path: Shop.resources.lath.current_cap, value: 3}"
    )
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:{line + 1}: .*'Shop.resources.lath.current_cap' "
        "is not a registry path",
    ):
        SimulationContext.from_yaml(path)
    refused = MINIMAL.replace(
        "    resource: packers\n",
        "    resource: packers\n"
        "    on:\n"
        "      - {path: Shop.resources.packers.current_cap, value: many}\n",
        1,
    )
    with pytest.raises(Exception, match="whole number of 0 or more.*'many'"):
        SimulationContext.from_yaml(write(tmp_path, refused, "refused.yaml"))
    ok = MINIMAL.replace(
        "    resource: packers\n",
        "    resource: packers\n"
        "    on:\n      - {path: Shop.resources.packers.current_cap, value: 3}\n",
        1,
    )
    SimulationContext.from_yaml(write(tmp_path, ok, "ok.yaml"))


def test_a_scenario_step_on_a_family_member_with_no_such_attribute_names_the_line(
    tmp_path,
):
    """A member's path is checked at load, at the step's line, for the attributes a
    member has."""
    text = MINIMAL + (
        "scenario:\n"
        "  - {at: 3, path: 'Shop.resources.desk[0].current_cap', value: 1}\n"
        "  - {at: 4, path: 'Shop.resources.desk[0].curent_cap', value: 1}\n"
    )
    text = text.replace(
        "resources:\n",
        "resources:\n  desk: {family: true, members: 2, current_cap: 1}\n",
        1,
    )
    path = write(tmp_path, text)
    line = text.splitlines().index(
        "  - {at: 4, path: 'Shop.resources.desk[0].curent_cap', value: 1}"
    )
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:{line + 1}: .*desk\[0\].curent_cap' is not a "
        "registry path",
    ):
        SimulationContext.from_yaml(path)


def test_a_step_on_a_member_the_family_can_never_have_names_the_line(tmp_path):
    """`desk[7]` of a family with `members: 2` is refused at load, at the step's line,
    and is taken when a task picks the family by `key`, which makes it at first use."""
    step = "  - {at: 4, path: 'Shop.resources.desk[7].current_cap', value: 1}"
    text = MINIMAL + "scenario:\n" + step + "\n"
    text = text.replace(
        "resources:\n",
        "resources:\n  desk: {family: true, members: 2, current_cap: 1}\n",
        1,
    )
    path = write(tmp_path, text)
    line = text.splitlines().index(step)
    with pytest.raises(
        BlueprintError,
        match=rf"{re.escape(str(path))}:{line + 1}: .*desk\[7\].current_cap' is "
        r"not a registry path.*'desk\[7\]' is a member the family 'desk' never "
        r"makes: `members: 2` makes desk\[0\] to desk\[1\], and no task or `on:` action "
        r"picks a member by `key`",
    ):
        SimulationContext.from_yaml(path)
    ok = text.replace(
        "tasks:\n",
        "tasks:\n  pick:\n    service: packing\n    resource: {of: desk, key: attr.i}\n"
        "    payload: {status: finished}\n    id_field: order_id\n",
        1,
    )
    assert ok != text
    SimulationContext.from_yaml(write(tmp_path, ok, "ok.yaml"))


_COMPUTED = """\
simulation: {sim_id: C, factor: 0, random_seed: 1}
"""


def _computed_records(tmp_path, body):
    """Runs a blueprint into a capture sink and returns its event records."""
    app = SimulationContext.from_yaml(write(tmp_path, _COMPUTED + body))
    sink = _Capture()
    app.add_egress(sink)
    app.run()
    return [r for r in sink.records if r["stream_type"] == "event"]


def test_payload_null_publishes_no_record(tmp_path):
    """`payload: null` publishes no finished record, and the task runs its other
    steps: here its `set` reaches the next task."""
    records = _computed_records(
        tmp_path,
        "arrivals: {a: {gap: 1, max_events: 1, spawn: t}}\n"
        "tasks:\n"
        "  t: {payload: null, set: {seen: 1}, next: u}\n"
        "  u: {payload: {seen: {gen: attr, name: seen}}}\n"
        "run: {until: 5}\n",
    )

    assert {r["task"] for r in records} == {"u"}
    assert records[0]["value"] == {"seen": 1}


def test_payload_null_refuses_a_default_put(tmp_path):
    """A store `put` carries the task's payload, so it is refused on a task that has
    none, at the line of the task."""
    path = write(
        tmp_path,
        _COMPUTED + "stores: {q: {}}\n"
        "arrivals: {a: {gap: 1, spawn: t}}\n"
        "tasks:\n  t: {payload: null, put: {store: q}}\n",
    )

    with pytest.raises(BlueprintError, match="payload: null publishes no record"):
        SimulationContext.from_yaml(path)


def test_first_at_with_count(tmp_path):
    """With `count`, `first_at` is the time of each entity's first start, and the
    gaps follow."""
    records = _computed_records(
        tmp_path,
        "arrivals: {a: {gap: 4, count: 2, first_at: 3, max_events: 2, spawn: t}}\n"
        "tasks:\n  t: {payload: {ran: true}}\n"
        "run: {until: 20}\n",
    )

    assert [(r["sim_ts"], r["key"]) for r in records] == [
        (3, "a-1"),
        (3, "a-2"),
        (7, "a-1"),
        (7, "a-2"),
    ]


def test_keys_and_key_are_exclusive(tmp_path):
    path = write(
        tmp_path,
        _COMPUTED + "arrivals: {a: {gap: 1, keys: [x, y], key: z, spawn: t}}\n"
        "tasks:\n  t: {payload: {}}\n",
    )

    with pytest.raises(BlueprintError, match="takes key or keys, not both"):
        SimulationContext.from_yaml(path)


def test_gap_refuses_attr(tmp_path):
    """A gap is read for the arrival, before any entity has attributes."""
    for gap in ("attr.x", "has(attr.x)"):
        path = write(
            tmp_path,
            _COMPUTED + f'arrivals: {{a: {{gap: "{gap}", spawn: t}}}}\n'
            "tasks:\n  t: {payload: {}}\n",
        )

        with pytest.raises(BlueprintError, match="gap .* reads attr"):
            SimulationContext.from_yaml(path)


def test_count_place_refuses_a_float_literal(tmp_path):
    """A count place takes a whole number: 2.0, 2.5 and true are refused where they
    are written. A computed 2.0 fails the run, which the A1 conformance test covers."""
    for literal in ("2.0", "2.5", "true"):
        path = write(
            tmp_path,
            _COMPUTED
            + f"arrivals: {{a: {{gap: 1, max_events: {literal}, spawn: t}}}}\n"
            "tasks:\n  t: {payload: {}}\n",
        )

        with pytest.raises(BlueprintError, match="max_events"):
            SimulationContext.from_yaml(path)


def test_cause_refused_where_no_edge_reaches(tmp_path):
    """`cause.*` is read only by a task that an `on_interrupt_next` edge reaches; a task
    that a plain `next` reaches is refused where it is written, with the task named."""
    body = (
        "resources: {m: {current_cap: 1}}\n"
        "arrivals: {a: {gap: 1, spawn: use}}\n"
        "tasks:\n"
        "  use: {resource: m, service_time: 1, payload: {}, on_interrupt: abandon, "
        "on_interrupt_next: lost, next: plain}\n"
        "  lost: {payload: {by: {gen: expr, expr: cause.by}}}\n"
        "  plain: {payload: {by: {gen: expr, expr: cause.by}}}\n"
    )

    with pytest.raises(BlueprintError, match="task .plain."):
        SimulationContext.from_yaml(write(tmp_path, _COMPUTED + body))

    fixed = body.replace(", next: plain", "").replace(
        "  plain: {payload: {by: {gen: expr, expr: cause.by}}}\n", ""
    )
    SimulationContext.from_yaml(write(tmp_path, _COMPUTED + fixed, "ok.yaml"))


def test_initial_alone_needs_no_dist(tmp_path):
    """An arrival with `initial` and nothing else that makes entities needs no `dist`,
    `replay` or `gap`; it starts that many entities before the run."""
    records = _computed_records(
        tmp_path,
        "arrivals: {a: {initial: 2, spawn: t}}\n"
        "tasks:\n  t: {payload: {e: ran}}\n"
        "run: {until: 1}\n",
    )

    assert [r["key"] for r in records] == ["a-1", "a-2"]

    with pytest.raises(BlueprintError, match="arrivals.a"):
        SimulationContext.from_yaml(
            write(
                tmp_path,
                _COMPUTED + "arrivals: {a: {spawn: t}}\ntasks:\n  t: {payload: {}}\n",
                "bare.yaml",
            )
        )


@pytest.mark.parametrize("initial", ["1.0", "'1'", "true", "-1"])
def test_initial_takes_a_whole_number_of_0_or_more(tmp_path, initial):
    """`initial` is a count, so a float, text, a boolean or a negative number is
    refused at load, as `group` and `count` refuse them."""
    text = (
        _COMPUTED
        + f"arrivals: {{a: {{initial: {initial}, spawn: t}}}}\n"
        + "tasks:\n  t: {payload: {}}\n"
    )
    with pytest.raises(BlueprintError, match="arrivals.a.initial"):
        SimulationContext.from_yaml(write(tmp_path, text))


def test_source_that_starts_off(tmp_path):
    """`active: false` starts a source off; `activate` from a task turns it on, and a
    name in `activate` that is no arrival is refused."""
    records = _computed_records(
        tmp_path,
        "arrivals:\n"
        "  src: {gap: 1, spawn: t, active: false}\n"
        "  ctl: {gap: 2.5, spawn: go, max_events: 1}\n"
        "tasks:\n"
        "  t: {payload: {e: t}}\n"
        "  go: {payload: {e: go}, activate: src}\n"
        "run: {until: 5}\n",
    )

    assert [r["sim_ts"] for r in records if r["task"] == "t"][:2] == [3.0, 4.0]
    assert [r["sim_ts"] for r in records if r["task"] == "go"] == [2.5]

    with pytest.raises(BlueprintError, match="nothing"):
        SimulationContext.from_yaml(
            write(
                tmp_path,
                _COMPUTED + "arrivals: {src: {gap: 1, spawn: t}}\n"
                "tasks:\n  t: {payload: {}, activate: nothing}\n",
                "bad.yaml",
            )
        )


_PLACES = {
    # place -> a task body that reads EXPR there
    "payload": "payload: {e: {gen: expr, expr: '%s'}}",
    "set": "payload: {}, set: {k: {gen: expr, expr: '%s'}}",
    "set_args": "payload: {}, set_args: {k: {gen: expr, expr: '%s'}}",
    "on_start": "payload: {}, on_start: {k: {gen: expr, expr: '%s'}}",
    "put.item": "payload: {}, put: {store: s, item: {gen: expr, expr: '%s'}}",
    "on value": "payload: {}, on: [{path: S.variables.v, value: {gen: expr, expr: '%s'}}]",
    "spawn args": "payload: {}, spawn: {task: u, args: {k: {gen: expr, expr: '%s'}}}",
    "wait args": "payload: {}, wait: {task: u, args: {k: {gen: expr, expr: '%s'}}}",
    "vars_once": "payload: {}, vars_once: {k: {gen: expr, expr: '%s'}}",
    "vars": "payload: {}, vars: {k: {gen: expr, expr: '%s'}}",
    "finish_vars": "payload: {}, finish_vars: {k: {gen: expr, expr: '%s'}}",
    "interrupt": "payload: {}, interrupt: {task: u, cause: {gen: expr, expr: '%s'}}",
    "key": "payload: {}, key: {gen: expr, expr: '%s'}",
}


def _places_blueprint(place, expression):
    return (
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "resources: {m: {current_cap: 1}}\n"
        "stores: {s: {}}\n"
        "variables: {v: 0}\n"
        "signals: {z: {}}\n"
        "arrivals: {a: {gap: 1, spawn: t}}\n"
        "tasks:\n"
        f"  t: {{{_PLACES[place] % expression}}}\n"
        "  u: {payload: {}}\n"
    )


_REFUSED_NAMES = [
    ("cause.by", "on_interrupt_next edge"),
    ("item", "the item a store get took"),
    ("item_priority", "the item a store get took"),
    ("item.n", "item.<field> reads the item"),
    ("done_count", "done_count is read in the when"),
    ("running(nosuch)", "running\\(nosuch\\) names no task"),
    ("len(held(nosuch))", "held\\(nosuch\\) names no resource"),
    ("len(waiting(nosuch))", "waiting\\(nosuch\\) names no resource"),
    ("len(putting(nosuch))", "putting\\(nosuch\\) names no store or container"),
    ("len(getting(nosuch))", "getting\\(nosuch\\) names no store or container"),
    ("users.nosuch", "users.nosuch names no resource"),
    ("queue.nosuch", "queue.nosuch names no resource"),
    ("level.nosuch", "level.nosuch names no store or container"),
    ("signal.nosuch.count", "signal.nosuch names no signal"),
    ("signal.nosuch.processed", "signal.nosuch names no signal"),
    ("count(nosuch)", "count\\(nosuch\\) names no store"),
    ('registry["S.variables.nosuch"]', "is not a registry path"),
    ("wait.nosuch.done", "wait.nosuch is not a branch"),
]
# The names of the run are handled by one check, so the whole list runs in one place.
# `users` tests that walk and `registry` tests the walk of `_check_registry_reads`, in
# every place
_REFUSED_CASES = (
    [("payload", e, m) for e, m in _REFUSED_NAMES]
    + [
        (place, "users.nosuch", "users.nosuch names no resource")
        for place in sorted(_PLACES)
        if place != "payload"
    ]
    + [
        (place, 'registry["S.variables.nosuch"]', "is not a registry path")
        for place in sorted(_PLACES)
        if place != "payload"
    ]
)


@pytest.mark.parametrize(
    "place, expression, message",
    _REFUSED_CASES,
    ids=[f"{place}-{expression}" for place, expression, _ in _REFUSED_CASES],
)
def test_names_are_refused_in_every_generator_place(
    tmp_path, place, expression, message
):
    """A generator reads `cause`, `item`, `item_priority`, `done_count`, the names in
    `held`, `waiting`, `putting`, `getting` and `running`, and a resource, store,
    container, signal or registry name under the rules of a plain expression place, in
    `payload`, `set`, `set_args`, `on_start`, `put.item`, an `on:` value, a `spawn`
    `args`, a wait `args`, the vars, an `interrupt` cause and a `key`."""
    path = write(tmp_path, _places_blueprint(place, expression))

    with pytest.raises(BlueprintError, match=message):
        SimulationContext.from_yaml(path)


_EXISTING_NAMES = (
    "len(held(m))",
    "running(u)",
    "len(putting(s))",
    "users.m + queue.m + level.s",
    "signal.z.count + signal.z.processed + count(s)",
    'registry["S.variables.v"]',
)


@pytest.mark.parametrize(
    "place, expressions",
    [("payload", _EXISTING_NAMES)]
    + [(p, _EXISTING_NAMES[:1]) for p in sorted(_PLACES) if p != "payload"],
)
def test_names_that_exist_load_in_every_generator_place(tmp_path, place, expressions):
    for expression in expressions:
        SimulationContext.from_yaml(
            write(tmp_path, _places_blueprint(place, expression))
        )


_OUTSIDE_CASES = [
    ("attributes", "users.nosuch", "users.nosuch names no resource"),
    ("attributes", "signal.nosuch.processed", "signal.nosuch names no signal"),
    ("attributes", 'registry["S.variables.nosuch"]', "is not a registry path"),
    ("scenario record", "users.nosuch", "users.nosuch names no resource"),
    ("store items", "users.nosuch", "users.nosuch names no resource"),
]


@pytest.mark.parametrize(
    "place, expression, message",
    _OUTSIDE_CASES,
    ids=[f"{place}-{expression}" for place, expression, _ in _OUTSIDE_CASES],
)
def test_names_are_refused_in_a_generator_outside_a_task(
    tmp_path, place, expression, message
):
    """An arrival's `attributes`, a scenario `record` and a store's `items` refuse an
    unknown name at load, as `next.when` does, not when the run reaches them."""
    generator = "{gen: expr, expr: '%s'}" % expression
    top = {
        "attributes": f"arrivals: {{a: {{gap: 1, spawn: t, attributes: {{k: {generator}}}}}}}\n",
        "scenario record": "arrivals: {a: {gap: 1, spawn: t}}\n"
        f"scenario: [{{at: 1, record: {{k: {generator}}}}}]\n",
        "store items": "arrivals: {a: {gap: 1, spawn: t}}\n"
        f"stores: {{s: {{items: [{generator}]}}}}\n",
    }[place]
    text = (
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "resources: {m: {current_cap: 1}}\nvariables: {v: 0}\n"
        + top
        + "tasks:\n  t: {payload: {}}\n"
    )
    with pytest.raises(BlueprintError, match=message):
        SimulationContext.from_yaml(write(tmp_path, text))


@pytest.mark.parametrize(
    "section, entry, message",
    [
        ("tasks", "{payload: {x: 1}}", "the name of a task is at least one"),
        ("arrivals", "{gap: 1, max_events: 2, spawn: t}", "the name of an arrival is"),
    ],
)
def test_an_empty_task_or_arrival_name_is_refused_at_its_line(
    tmp_path, section, entry, message
):
    """The empty name ran as no name: a task was stored under another key, and an
    arrival passed `desflow check` and failed when the run started."""
    other = "arrivals: {a: {gap: 1, max_events: 2, spawn: ''}}\n" * (section == "tasks")
    text = (
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        + other
        + f"{section}:\n  '': {entry}\n"
        + "tasks:\n  t: {payload: {}}\n" * (section == "arrivals")
    )
    with pytest.raises(BlueprintError, match=rf"blueprint.yaml:\d+: {message}"):
        SimulationContext.from_yaml(write(tmp_path, text, "blueprint.yaml"))


@pytest.mark.parametrize("signals", ["[{closing: {once: false}}]", "[[x]]"])
def test_signals_written_as_a_list_hold_names_only(tmp_path, signals):
    """A mapping or a list in the list of signals was a raw TypeError."""
    text = MINIMAL + f"signals: {signals}\n"
    with pytest.raises(BlueprintError, match="a list of signals holds names only"):
        SimulationContext.from_yaml(write(tmp_path, text))


@pytest.mark.parametrize(
    "name, read", [("on", "True"), ("no", "False"), ("1", "1"), ("null", "None")]
)
def test_a_name_that_yaml_reads_as_another_type_is_reported_as_written(
    tmp_path, name, read
):
    """An unquoted task called `on` was reported as `tasks.1`, at the `tasks` line."""
    text = (
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "tasks:\n"
        "  part: {payload: {x: 1}}\n"
        f"  {name}: {{payload: {{}}}}\n"
    )
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(write(tmp_path, text))

    assert f"tasks.{name}: YAML reads the name {name} as {read}" in str(error.value)
    assert f'write it in quotes, "{name}"' in str(error.value)
    assert ":4:" in str(error.value)


_TEXT_PLACES = {
    "family members": "resources: {desk: {family: true, members: '%s'}}\n"
    "tasks:\n  t: {payload: {}}\n",
    "family current_cap": "resources: {desk: {family: true, current_cap: '%s'}}\n"
    "tasks:\n  t: {payload: {}}\n",
    "container family level": "containers: {k: {family: true, members: 2, max_cap: 10, "
    "level: '%s'}}\ntasks:\n  t: {payload: {}}\n",
    "spawn count": "tasks:\n  t: {spawn: {task: u, count: '%s'}, payload: {}}\n",
    "on spawn count": "tasks:\n  t: {on: [{spawn: {task: u, count: '%s'}}], "
    "payload: {}}\n",
    "signal on spawn count": "signals: {go: {on: [{spawn: {task: u, count: '%s'}}]}}\n"
    "tasks:\n  t: {payload: {}}\n",
    "wait get amount": "containers: {c: {max_cap: 10, level: 5}}\n"
    "tasks:\n  t: {wait: {get: {container: c, amount: '%s'}}, payload: {}}\n",
    "wait put priority": "stores: {p: {priority: true}}\n"
    "tasks:\n  t: {wait: {put: {store: p, item: 1, priority: '%s'}}, payload: {}}\n",
    "open get amount": "containers: {c: {max_cap: 10, level: 5}}\n"
    "tasks:\n  t: {open: {h: {get: {container: c, amount: '%s'}}}, payload: {}}\n",
}


@pytest.mark.parametrize("place", sorted(_TEXT_PLACES))
@pytest.mark.parametrize(
    "expression, message",
    [
        ("level.nothere", "level.nothere names no store or container"),
        ('registry["S.no.path"]', 'registry\\["S.no.path"\\] is not a registry path'),
    ],
)
def test_names_are_refused_in_every_expression_text_place(
    tmp_path, place, expression, message
):
    """The `count` of a spawn, the `amount` and `priority` of a `get` or `put` branch of
    a `wait` or an `open`, and the `members`, capacity and `level` of a family are
    checked for names at load, as the same text is in a task's own `get`."""
    text = (
        "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
        "arrivals: {a: {gap: 1, max_events: 1, spawn: t}}\n"
        + _TEXT_PLACES[place]
        % expression
    )
    if "u:" not in text and "spawn: {task: u" in text:
        text += "  u: {payload: {}}\n"
    with pytest.raises(BlueprintError, match=message):
        SimulationContext.from_yaml(write(tmp_path, text))


@pytest.mark.parametrize(
    "where",
    [
        "tasks:\n  t: {payload: {}, on: [{activate: nosuch}]}\n",
        "tasks:\n  t: {payload: {}, on: [{deactivate: nosuch}]}\n",
        "tasks:\n  t: {payload: {}}\nsignals: {z: {on: [{activate: nosuch}]}}\n",
        "tasks:\n  t: {payload: {}}\nsignals: {z: {on: [{deactivate: [a, nosuch]}]}}\n",
        "tasks:\n  t: {payload: {}, activate: nosuch}\n",
    ],
)
def test_activate_and_deactivate_name_an_arrival_wherever_they_are_written(
    tmp_path, where
):
    path = write(
        tmp_path,
        _COMPUTED + "arrivals: {a: {gap: 1, spawn: t}}\n" + where,
    )

    with pytest.raises(BlueprintError, match="nosuch"):
        SimulationContext.from_yaml(path)


def test_activate_refuses_an_arrival_with_no_spawn(tmp_path):
    path = write(
        tmp_path,
        _COMPUTED + "arrivals: {a: {gap: 1}}\n"
        "tasks:\n  t: {payload: {}, activate: a}\n",
    )

    with pytest.raises(BlueprintError, match="has no spawn"):
        SimulationContext.from_yaml(path)


@pytest.mark.parametrize(
    "key",
    ["group: 3", "max_events: 3", "off_mode: restart", "active: false", "first_at: 3"],
)
def test_initial_alone_refuses_keys_it_would_ignore(tmp_path, key):
    path = write(
        tmp_path,
        _COMPUTED + f"arrivals: {{a: {{initial: 1, spawn: t, {key}}}}}\n"
        "tasks:\n  t: {payload: {}}\n",
    )

    with pytest.raises(BlueprintError, match="no source"):
        SimulationContext.from_yaml(path)


def test_a_loop_name_may_not_be_a_var(tmp_path):
    """Y5: the loop name of a comprehension is not a var of the task, in a plain place
    and in a generator."""
    for place in (
        "vars: {x: 1}, payload: {}, next: {when: [{condition: '[x for x in [1, 2]] == 1', task: u}]}",
        "vars: {x: 1}, payload: {y: {gen: expr, expr: '[x for x in [1, 2]]'}}",
    ):
        path = write(
            tmp_path,
            _COMPUTED + "arrivals: {a: {gap: 1, spawn: t}}\n"
            f"tasks:\n  t: {{{place}}}\n  u: {{payload: {{}}}}\n",
        )
        with pytest.raises(BlueprintError, match="loop name 'x'"):
            SimulationContext.from_yaml(path)


READINGS = """\
telemetry:
  - interval: 10
    readings:
      waiting: queue.packers
      busy_share: "users.packers / registry['Shop.resources.packers.current_cap']"
"""


def _telemetry(records):
    return [
        (r["sim_ts"], r["path_id"], r["value"])
        for r in records
        if r["stream_type"] == "telemetry" and r["path_id"] in READ_PATHS
    ]


READ_PATHS = ("Shop.waiting", "Shop.busy_share")


def test_readings_publish_per_interval(tmp_path):
    """Each reading of `telemetry.readings` is one telemetry record every interval (and
    at the start, as a `function` is called), under `<sim_id>.<reading>`, in the order
    written, with the expression's value."""
    app = SimulationContext.from_yaml(write(tmp_path, MINIMAL + READINGS))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=30)
    found = _telemetry(capture.records)
    assert [(t, p) for t, p, _ in found] == [
        (t, p) for t in (0.0, 10.0, 20.0) for p in READ_PATHS
    ]
    assert all(isinstance(v, (int, float)) for _, _, v in found)


def test_readings_refuse_function_too(tmp_path, logic):
    """A telemetry entry gives `function` or `readings`, not both and not neither."""
    both = MINIMAL + READINGS.replace(
        "    readings:", f"    function: !python {logic}.sample\n    readings:"
    )
    with pytest.raises(
        BlueprintError, match=re.escape("exactly one of function and readings")
    ):
        SimulationContext.from_yaml(write(tmp_path, both))
    neither = MINIMAL + "telemetry:\n  - interval: 10\n"
    with pytest.raises(
        BlueprintError, match=re.escape("exactly one of function and readings")
    ):
        SimulationContext.from_yaml(write(tmp_path, neither, "neither.yaml"))


@pytest.mark.parametrize(
    "reading,fragment,line",
    [
        ("queue.nobody", "nobody", 22),
        ("1 +", "not a valid expression", 20),
        ("attr.x", "reads attr", 20),
        ("registry['Shop.resources.nobody.current_cap']", "nobody", 22),
    ],
)
def test_readings_parse_at_load(tmp_path, reading, fragment, line):
    """A reading is parsed and checked when the file loads, in the scope of the run:
    a bad expression, an entity read and a name or registry path that does not exist
    are refused with the line."""
    text = (
        MINIMAL + f'telemetry:\n  - interval: 10\n    readings:\n      r: "{reading}"\n'
    )
    with pytest.raises(BlueprintError, match=re.escape(fragment)) as error:
        SimulationContext.from_yaml(write(tmp_path, text))
    assert f"blueprint.yaml:{line}:" in str(error.value), error.value


def test_a_reading_that_fails_to_evaluate_is_a_run_failure(tmp_path):
    text = (
        MINIMAL
        + 'telemetry:\n  - interval: 10\n    readings:\n      r: "1 / (t - 10)"\n'
    )
    app = SimulationContext.from_yaml(write(tmp_path, text))
    with pytest.raises(ValueError, match=re.escape("telemetry reading 'r'")):
        app.run(until=30)


def test_readings_match_the_docs_packers_report(tmp_path):
    """DX-7: the readings that replace the docs snippet's `packers_report` function
    give the same records, as a whole stream, as the function."""
    import shutil
    from pathlib import Path

    docs = Path(__file__).parents[2] / "docs" / "snippets" / "yaml"
    streams = []
    for form in ("function", "readings"):
        folder = tmp_path / form
        folder.mkdir()
        shutil.copy(docs / "shop_logic.py", folder / "shop_logic.py")
        text = (docs / "statistics.yaml").read_text(encoding="utf-8")
        old = "  - {interval: 30, function: !python shop_logic.packers_report}\n"
        assert old in text
        if form == "readings":
            text = text.replace(
                old,
                "  - interval: 30\n"
                "    readings:\n"
                '      waiting_per_packer: "queue.packers / max(registry['
                "'Shop.resources.packers.current_cap'], 1)\"\n"
                "      arrival_rate: \"registry['Shop.arrival.orders.rate']\"\n",
            )
        text = text.replace("egress:\n  - {type: Console}\n\n", "")
        path = folder / "statistics.yaml"
        path.write_text(text, encoding="utf-8")
        app = SimulationContext.from_yaml(path)
        capture = _Capture()
        app.add_egress(capture)
        app.run()
        streams.append(
            [{k: v for k, v in r.items() if k != "timestamp"} for r in capture.records]
        )
    assert streams[0] == streams[1]
    assert any(r.get("path_id") == "Shop.waiting_per_packer" for r in streams[0])


FAMILIES = """\
simulation: {sim_id: S, factor: 0, random_seed: 1}
resources:
  desk: {family: true, members: 2}
  plain: {current_cap: 1}
stores: {q: {family: true}}
signals: {reply: {family: true}}
arrivals:
  a: {dist: constant, value: 1, spawn: t, max_events: 1}
tasks:
  t:
    resource: {of: desk, key: "1"}
    service_time: 1
    payload: {e: x}
run: {until: 10}
"""


@pytest.mark.parametrize(
    "old,new,fragment",
    [
        (
            'resource: {of: desk, key: "1"}',
            "resource: desk",
            "names the family 'desk'",
        ),
        (
            'resource: {of: desk, key: "1"}',
            'resource: {of: plain, key: "1"}',
            "'plain', which is not a family",
        ),
        (
            'resource: {of: desk, key: "1"}',
            'resource: {of: nope, key: "1"}',
            "uses resource 'nope'",
        ),
        (
            'resource: {of: desk, key: "1"}',
            "resource: {of: desk}",
            "key or lowest",
        ),
        (
            'resource: {of: desk, key: "1"}',
            'resource: {of: desk, key: "1", lowest: "member.users"}',
            "key or lowest",
        ),
        (
            'resource: {of: desk, key: "1"}',
            "resource: {of: [plain, desk], key: \"['plain', 'x'][0]\"}",
            "lists 'desk', which is a family",
        ),
        (
            'resource: {of: desk, key: "1"}',
            "resource: {of: [plain, nope], key: \"['plain', 'x'][0]\"}",
            "lists 'nope', which is not a declared resource",
        ),
        (
            'resource: {of: desk, key: "1"}',
            'resource: {of: desk, key: "1"}\n    keep: [desk]',
            "keep",
        ),
        (
            "plain: {current_cap: 1}",
            "plain: {current_cap: 1, members: 2}",
            "members needs family: true",
        ),
        (
            "plain: {current_cap: 1}",
            'plain: {current_cap: "attr.i"}',
            "text in a family",
        ),
        (
            "desk: {family: true, members: 2}",
            'desk: {family: true, members: 2, current_cap: "attr.i"}',
            "family with members reads the run",
        ),
        (
            "desk: {family: true, members: 2}",
            'desk: {family: true, members: 2}\n  "desk[3]": {}',
            "starts with the name of the family 'desk' and a bracket",
        ),
        (
            "payload: {e: x}",
            'payload: {e: x, x: {gen: expr, expr: "users.desk"}}',
            "reads users.desk of a family",
        ),
        (
            "payload: {e: x}",
            'payload: {e: x, x: {gen: expr, expr: "users.plain[0]"}}',
            "'plain' is not a family",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    put: {store: q, item: 1}",
            "names the family 'q'",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    signal: reply",
            "names the family 'reply'",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    wait: {each: {gen: lookup, task: nosuch, path: n}, branch: {timeout: 1}}",
            "tasks.t.wait.each.task: lookup names task 'nosuch'",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    open: {p: {each: {gen: lookup, task: nosuch, path: n}, branch: {timeout: 1}}}",
            "tasks.t.open.p.each.task: lookup names task 'nosuch'",
        ),
        (
            "desk: {family: true, members: 2}",
            "desk: {family: true, members: {gen: lookup, task: nosuch, path: n}}",
            "resources.desk.members.task: lookup names task 'nosuch'",
        ),
        (
            "payload: {e: x}",
            'payload: {e: x}\n    release: {of: nope, key: "1"}',
            "nope",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    wait: {each: 2, group: {at_least: 0}, branch: {timeout: 1}}",
            "at_least",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    wait: {each: 2, branch: {any: {a: {timeout: 1}}}}",
            "an each branch makes one event for each element, so it is one of signal, task, timeout, request, get, put, handle, got an any branch",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    open: {p: {request: plain}}\n    cancel: {handle: p, index: 0}",
            "an index needs an each group",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    open: {p: {each: 2, branch: {timeout: 1}}}\n    cancel: p",
            "'p' is opened as an each timeout in task 't', and cancel needs",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    wait: {each: 2.0, branch: {timeout: 1}}",
            "each is a whole number of 0 or more, got 2.0",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    subscribe: {each: 2, branch: {timeout: 1}}",
            "subscribe does not take an each branch",
        ),
        (
            "payload: {e: x}",
            'payload: {e: x}\n    wait: {each: 2, group: {when: "wait.each.flags[1]"}, branch: {timeout: 1}}',
            "the when of an each group reads done_count",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    seize: {resource: {of: desk, key: each.index}}",
            "are read in the branch of a wait or an open with each",
        ),
        (
            "payload: {e: x}",
            'payload: {e: x}\n    wait: {each: 2, branch: {timeout: 1}}\n    on: [{when: "each.index == 0", path: S.variables.v, value: 1}]',
            "each.index is read only in the branch of an each",
        ),
        (
            "payload: {e: x}",
            'payload: {e: x}\n    wait: {each: 2, branch: {timeout: 1}}\n    fail: "1 > 5"\n    fail_reason: {gen: expr, expr: "each.index"}',
            "each.index is read only in the branch of an each",
        ),
        (
            "payload: {e: x}",
            'payload: {e: x}\n    wait: {each: 2, group: {when: "done_count >= 1 and each.index == 0"}, branch: {timeout: 1}}',
            "each.index is read only in the branch of an each",
        ),
        (
            "payload: {e: x}",
            'payload: {e: x}\n    wait: {each: 2, group: {at_least: "each.index + 1"}, branch: {timeout: 1}}',
            "each.index is read only in the branch of an each",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    wait: {each: 2, branch: {timeout: 1}}\n    listen: {signal: {of: reply, key: each.index}, on: []}",
            "each.index is read only in the branch of an each",
        ),
    ],
)
def test_family_refusals_name_the_place(tmp_path, old, new, fragment):
    """Each wrong use of a family, a reference or `each` is refused when the file loads,
    and the message names the task and the key."""
    text = FAMILIES.replace(old, new)
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    assert fragment in str(error.value), str(error.value)


@pytest.mark.parametrize(
    "old,new,row,message",
    [
        (
            "signals: {reply: {family: true}}",
            "signals: {reply: {family: true}, go: {on: [{fire: {signal: reply}}]}}",
            "signals:",
            "signal 'go': on names the family 'reply', which is named by a reference: "
            "{of: reply, key: <expression>}",
        ),
        (
            "payload: {e: x}",
            'payload: {e: x}\n    set: {x: {gen: expr, expr: "len(users.desk)"}}',
            "    set:",
            "task 't': set reads users.desk of a family; read one member with "
            "users.desk[<key>], or list the keys with members(desk)",
        ),
        # Two errors: the task's generators are checked before the rules, so the
        # `set` is named and not the queue rank of `plain`
        (
            FAMILIES[FAMILIES.index("  plain:") : FAMILIES.index("run:")],
            '  plain: {current_cap: 1, queue: {rank: "len(users.desk)"}}\n'
            + FAMILIES[FAMILIES.index("stores:") : FAMILIES.index("run:")]
            + "    set:\n      x:\n        gen: expr\n"
            + '        expr: "len(users.desk)"\n',
            "    set:",
            "task 't': set reads users.desk of a family; read one member with "
            "users.desk[<key>], or list the keys with members(desk)",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    release: {resource: nope, task: t}",
            "    release:",
            "task 't' uses resource 'nope', which is not defined under resources; add "
            "it under resources",
        ),
        (
            "stores: {q: {family: true}}",
            "stores: {q: {family: true}, pq: {priority: true}}",
            None,
            "store 'pq' serves by priority, and SimPy has no store that both ranks and "
            "filters its items; remove the filter, or the store's priority",
        ),
        (
            "run: {until: 10}",
            "run: {until: 10}\nscenario:\n  - {at: 1, interrupt: {task: nosuch}}",
            "  - {at: 1",
            "the step interrupts task 'nosuch', which is not defined under tasks; add it "
            "under tasks",
        ),
        (
            "payload: {e: x}",
            "payload: {e: x}\n    on: [{interrupt: {task: nosuch}}]",
            "    on:",
            "the on: action of task 't' interrupts task 'nosuch', which is not defined "
            "under tasks; add it under tasks",
        ),
        (
            "signals: {reply: {family: true}}",
            "signals: {reply: {family: true}, go: {on: [{interrupt: {task: nosuch}}]}}",
            "signals:",
            "the on: action of signal 'go' interrupts task 'nosuch', which is not "
            "defined under tasks; add it under tasks",
        ),
        (
            "signals: {reply: {family: true}}",
            "signals: {reply: {family: true}, go: {on: [{resume: {task: nosuch}}]}}",
            "signals:",
            "the on: action of signal 'go' resumes task 'nosuch', which is not defined "
            "under tasks; add it under tasks",
        ),
    ],
)
def test_a_load_refusal_gives_its_whole_text_at_its_line(
    tmp_path, old, new, row, message
):
    """Each of these refusals of a file is the whole message, at the line of the key
    it names: a signal's `on:` that names a family, a generator of `set` that reads
    one, a `release` of an unknown resource, a `filter` on a store that serves by
    priority, and an unknown task that a scenario step or an `on:` entry interrupts
    or resumes."""
    text = FAMILIES.replace(old, new)
    if row is None:
        text = text.replace(
            "payload: {e: x}",
            'payload: {e: x}\n    get: {store: pq, filter: "item > 1"}',
        )
        row = "    get:"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    line = next(
        n for n, text_row in enumerate(text.splitlines(), 1) if text_row.startswith(row)
    )
    assert str(error.value) == f"{path}:{line}: {message}"


EVENTS = FAMILIES.replace(
    "signals: {reply: {family: true}}", "signals: {reply: {once: true}}"
)


@pytest.mark.parametrize(
    "extra,fragment",
    [
        ("listen: {on: []}", "tasks.t.listen: Value error, listen takes exactly one"),
        (
            "listen: {signal: reply, handle: h, on: []}",
            "tasks.t.listen: Value error, listen takes exactly one",
        ),
        (
            "listen: {signal: reply, index: 0, on: []}",
            "tasks.t.listen: Value error, listen index picks an element of a handle",
        ),
        (
            "listen: {signal: nosuch, on: [{fire: {signal: reply}}]}",
            "task 't' listens to signal 'nosuch', which is not defined under signals",
        ),
        (
            "listen: {handle: nosuch, on: [{fire: {signal: reply}}]}",
            "task 't': listen names 'nosuch', which no task opens",
        ),
        (
            "on: [{when: 'attr.i +', fire: {signal: reply}}]",
            "tasks.t.on.0.when: Value error, 'attr.i +' is not a valid expression",
        ),
        (
            "on: [{fire: {signal: reply, fail: true, from: x}}]",
            "tasks.t.on.0.fire: Value error, fail: true fails the signal with the value",
        ),
        (
            "on: [{fire: {signal: reply, defused: true}}]",
            "tasks.t.on.0.fire: Value error, defused is for a failure, so it needs fail: true",
        ),
        (
            "on: [{fire: {signal: reply, urgent: true, priority: 0}}]",
            "tasks.t.on.0.fire: Value error, urgent is priority 0",
        ),
        (
            "on: [{fire: {signal: reply, priority: 0.5}}]",
            "tasks.t.on.0.fire.priority: Input should be a valid integer",
        ),
        (
            "on: [{fire: {signal: reply, priority: true}}]",
            "tasks.t.on.0.fire.priority: Input should be a valid integer",
        ),
        (
            "fail_reason: {gen: expr, expr: 'attr.i'}",
            "tasks.t: Value error, fail_reason needs a fail condition",
        ),
        (
            "fail: \"True\"\n    fail_reason: {gen: expr, expr: 'attr.i +'}",
            "tasks.t.fail_reason.expr: Value error, 'attr.i +' is not a valid expression",
        ),
        (
            'fail: "True"\n    fail_reason: {gen: lookup, task: nosuch, path: n}',
            "tasks.t.fail_reason.task: lookup names task 'nosuch'",
        ),
        (
            "listen: {signal: reply, on: [{path: S.variables.x, value: {gen: lookup, task: nosuch, path: n}}]}",
            "tasks.t.listen.on.0.value.task: lookup names task 'nosuch'",
        ),
        (
            "listen: {signal: reply, on: [{fire: {signal: reply, value: {gen: lookup, task: nosuch, path: n}}}]}",
            "tasks.t.listen.on.0.fire.value.task: lookup names task 'nosuch'",
        ),
        (
            "release: {of: plain, key: '1'}",
            "task 't': release names 'plain', which is not a family of resources",
        ),
        (
            "release: {of: nope, key: '1'}",
            "task 't': release names 'nope', which is not a family of resources",
        ),
    ],
)
def test_events_refusals_name_the_place(tmp_path, extra, fragment):
    """Y9 to Y11: a `fire` that fails, is defused or has a priority, a `listen`, a `when`
    on an `on:` entry and a computed `fail_reason`, written wrongly, are refused when the
    file loads, with the task and the key named. A `release` that names a reference to
    something that is not a family is refused by the reference check alone."""
    path = write(
        tmp_path, EVENTS.replace("payload: {e: x}", "payload: {e: x}\n    " + extra)
    )
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    assert fragment in str(error.value), str(error.value)


@pytest.mark.parametrize(
    "when,fragment",
    [
        ("signal.zz.value == 1", "signal.zz names no signal under signals"),
        (
            "registry['S.variables.nope'] == 1",
            'registry["S.variables.nope"] is not a registry path',
        ),
        ("wait.zz.ok", "a signal's on entries run with no wait"),
    ],
)
def test_a_when_on_the_on_of_a_signal_is_checked_at_load(tmp_path, when, fragment):
    """Y11: the names a `when` reads are checked when the file loads in a signal's own
    `on`, as in a task's `on` and a `listen`."""
    text = EVENTS.replace(
        "signals: {reply: {once: true}}",
        f'signals: {{reply: {{once: true, on: [{{when: "{when}", fire: {{signal: reply}}}}]}}}}',
    )
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(write(tmp_path, text))
    assert fragment in str(error.value), str(error.value)


def test_a_family_read_in_the_when_of_a_signal_names_the_place_and_its_line(tmp_path):
    """A `users.plain[0]` in the `when` of a signal's `on` is refused with the place
    named once ("reads" is not repeated) and the line of the signal in the file."""
    text = EVENTS.replace(
        "signals: {reply: {once: true}}",
        'signals: {reply: {once: true, on: [{when: "users.plain[0] > 1", fire: {signal: reply}}]}}',
    )
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    line = next(
        n for n, row in enumerate(text.splitlines(), 1) if row.startswith("signals:")
    )
    assert (
        f"{path}:{line}: signal 'reply' on entry 0 when reads users.plain[...], "
        "but 'plain' is not a family" in str(error.value)
    ), str(error.value)


def test_a_value_on_the_on_of_a_signal_cannot_read_a_wait(tmp_path):
    """A signal's `on` entries run with no wait in scope, so a generator `value` that
    reads `wait.<branch>` is refused when the file loads, as its `when` is."""
    text = EVENTS.replace(
        "signals: {reply: {once: true}}",
        'signals: {reply: {once: true, on: [{fire: {signal: reply, value: {gen: expr, expr: "wait.zz.ok"}}}]}}',
    )
    with pytest.raises(BlueprintError, match="a signal's on entries run with no wait"):
        SimulationContext.from_yaml(write(tmp_path, text))


def test_a_when_on_the_on_of_a_task_reads_the_item_of_its_get_as_its_value_does(
    tmp_path,
):
    """A task's `on` runs after its `get`, so its `when` may read `item.` as the
    `value` beside it does."""
    text = EVENTS.replace("stores: {q: {family: true}}", "stores: {q: {}}").replace(
        "payload: {e: x}",
        'payload: {e: x}\n    get: {store: q}\n    on: [{when: "item.x == 1", fire: {signal: reply}}]',
    )
    SimulationContext.from_yaml(write(tmp_path, text))


def test_a_listen_entry_cannot_read_the_item_of_a_get(tmp_path):
    """Y11 and V12: a `listen` attaches at step 6, before the task's `get` at step 11, so
    an entry of it that reads `item.` is refused when the file loads, as for an `open`."""
    text = EVENTS.replace("stores: {q: {family: true}}", "stores: {q: {}}").replace(
        "payload: {e: x}",
        'payload: {e: x}\n    get: {store: q}\n    listen: {signal: reply, on: [{fire: {signal: reply, value: {gen: expr, expr: "item.x"}}}]}',
    )
    with pytest.raises(BlueprintError, match="item"):
        SimulationContext.from_yaml(write(tmp_path, text))


_PUT_ITEM = """
simulation: {sim_id: A, factor: 0}
stores: {s: {}}
arrivals: {a: {initial: 1, spawn: t}}
tasks:
  t:
    put: {store: s, item: {x: {gen: expr, expr: "1 + 1"}, y: 3, z: [{gen: expr, expr: "2 * 3"}], w: {v: {gen: expr, expr: "7"}}}}
    payload: {event_type: put}
    next: g
  g: {get: {store: s}, payload: {event_type: got, item: {gen: expr, expr: item}}}
"""


def test_a_put_item_mapping_evaluates_the_generators_nested_in_it(tmp_path):
    """MF1: `put.item` is a value place, so a generator at any depth of a mapping or
    list is read as the put is made, as a payload's is."""
    app = SimulationContext.from_yaml(write(tmp_path, _PUT_ITEM))
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=5)
    got = [
        row["value"]["item"]
        for row in capture.records
        if row.get("value", {}).get("event_type") == "got"
    ]
    assert got == [{"x": 2, "y": 3, "z": [6], "w": {"v": 7}}]


@pytest.mark.parametrize(
    "key, message",
    [
        ("${DESFLOW_UNSET_KEY_VAR}: 1", "DESFLOW_UNSET_KEY_VAR is not set"),
        ("2026-13-01: 1", "is not a date or time"),
    ],
)
def test_a_key_that_fails_to_construct_gives_its_own_error(
    tmp_path, monkeypatch, key, message
):
    """The error names the key's problem, not a recursive node."""
    monkeypatch.delenv("DESFLOW_UNSET_KEY_VAR", raising=False)
    path = write(tmp_path, MINIMAL.replace("tasks:\n", f"extra:\n  {key}\ntasks:\n", 1))
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    assert message in str(error.value)
    assert "recursive" not in str(error.value)


def test_a_name_that_is_a_store_and_a_container_is_the_first_store(tmp_path):
    """The name reported follows the file's order, not a set's order."""
    names = ["zeta", "alpha", "mid", "beta"]
    both = "".join(f"  {name}: {{}}\n" for name in names)
    text = MINIMAL.replace("tasks:\n", f"stores:\n{both}containers:\n{both}tasks:\n", 1)
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    assert "'zeta' is both a store and a container" in str(error.value)


def test_reserved_gen_key_names_the_line(tmp_path):
    """A mapping with a `gen` key is a generator, so one that names no generator is a
    load error with its line; a `gen` key directly under `set` is a field name; and
    `{gen: literal, value: ...}` keeps such a mapping as data (Upgrading to 1.0)."""
    from tests.conformance.harness import run_yaml_text

    head = "simulation: {sim_id: S, factor: 0, random_seed: 1}\n"
    head += "arrivals: {a: {dist: constant, value: 1, spawn: t, max_events: 1}}\n"
    path = tmp_path / "bad.yaml"
    path.write_text(head + "tasks:\n  t:\n    payload:\n      gen: hello\n")
    with pytest.raises(
        BlueprintError, match=rf"{re.escape(str(path))}:5: tasks.t.payload: "
    ):
        SimulationContext.from_yaml(path)

    kept = run_yaml_text(
        tmp_path,
        head + "tasks:\n  t:\n    set: {gen: 3}\n"
        "    payload: {kept: {gen: literal, value: {gen: 5}}, field: '${attr.gen}'}\n"
        "run: {until: 5}\n",
        {},
    )
    assert kept.error is None
    values = [
        r["value"]
        for r in kept.records
        if r["stream_type"] == "event" and "kept" in r["value"]
    ]
    assert values and values[0]["kept"] == {"gen": 5}

"""YAML blueprints: validation, error lines and the builder calls they make."""

import textwrap
from datetime import datetime

import pytest
from typer.testing import CliRunner

from desflow import SimulationContext
from desflow.blueprint import BlueprintError
from desflow.blueprint.cli import app as cli
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
    assert app._resources_config["packers"].current_cap == 2
    packing = app._services_config["packing"]
    # Only the parameters the family takes are set
    assert (packing.dist, packing.mean, packing.std, packing.rate) == (
        "normal",
        3.0,
        0.5,
        None,
    )
    assert app._arrivals_config["standard"].rate == 1.0
    assert app._default_until == 30.0


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


@pytest.mark.parametrize(
    "old,new,line,fragment",
    [
        ("  factor: 0\n", "  factor: 0\n  speed: 2\n", 4, "simulation.speed"),
        ("mean: 3.0, std: 0.5", "mean: 3.0, std: 0.5, shape: 2", 8, "shape"),
        ("dist: exponential", "dist: poisson", 10, "Unsupported distribution"),
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
    with pytest.raises(BlueprintError, match=rf"{path}:20: unknown connector type"):
        SimulationContext.from_yaml(path)


def test_bad_connector_config_names_the_line(tmp_path):
    text = MINIMAL + "egress:\n  - type: Console\n    config: {colour: red}\n"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError, match=rf"{path}:21: ConsoleEgress rejected"):
        SimulationContext.from_yaml(path)


def test_yaml_syntax_error_names_the_line(tmp_path):
    path = write(tmp_path, "simulation:\n  sim_id: [Shop\n")
    with pytest.raises(BlueprintError, match=rf"{path}:\d+:"):
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
    provider = app._egress_providers[0]
    assert type(provider).__name__ == "Sink" and provider.label == "one"
    assert app._egress_predicates[0]({"stream_type": "event"}) is True


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
    with pytest.raises(BlueprintError, match=rf"{path}:20: !python takes a dotted"):
        SimulationContext.from_yaml(path)


def test_a_process_must_be_a_generator_function(tmp_path, logic):
    text = MINIMAL + f"processes:\n  - !python {logic}.not_a_generator\n"
    path = write(tmp_path, text)
    with pytest.raises(BlueprintError, match=rf"{path}:20: .*generator function"):
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
            "holds a int",
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


def test_scenario_at_must_be_a_time(tmp_path):
    path = write(
        tmp_path,
        MINIMAL + "scenario:\n  - {at: later, path: Shop.variables.x, value: 1}\n",
    )
    with pytest.raises(BlueprintError, match=rf"{path}:20: scenario.0.at"):
        SimulationContext.from_yaml(path)


def test_compile_parameters_matches_what_run_registers(tmp_path):
    app = SimulationContext.from_yaml(write(tmp_path, MINIMAL))
    params = app.compile_parameters()
    app.run(until=1)

    assert params.sim_id == "Shop"
    assert (
        app.env.registry.get_config("Shop.resources.packers")
        is (params.resources["packers"])
    )


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

    assert (app.sim_id, app.factor, app._default_until) == ("Warehouse", 0, 60.0)
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
        BlueprintError, match=rf"{path}:2: environment variable SIM_ID is not set"
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
    "value,fragment", [("-1 fortnight", "not a duration"), ("yesterday", "not a time")]
)
def test_a_bad_time_names_the_line(tmp_path, value, fragment):
    path = write(tmp_path, _with_times(value, "now"))
    with pytest.raises(
        BlueprintError, match=rf"{path}:5: simulation.logical_start_time: .*{fragment}"
    ):
        SimulationContext.from_yaml(path)


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
    app._egress_providers[:] = [history, live]
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
    history, live = app._egress_predicates
    record = {"timestamp": "2026-01-01T00:00:10.000+00:00"}
    assert live(record) and not history(record)
    record = {"timestamp": "2026-01-01T00:00:09.999+00:00"}
    assert history(record) and not live(record)


def test_history_needs_go_live_at(tmp_path):
    path = write(tmp_path, MINIMAL + "egress:\n  - type: Console\n    when: history\n")
    with pytest.raises(
        BlueprintError, match=rf"{path}:21: when: history needs simulation.go_live_at"
    ):
        SimulationContext.from_yaml(path)


def test_an_unknown_when_names_the_line(tmp_path):
    path = write(tmp_path, MINIMAL + "egress:\n  - type: Console\n    when: later\n")
    with pytest.raises(
        BlueprintError,
        match=rf"{path}:21: egress.0.when: .*'later' is not a when. Use history, live",
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
        BlueprintError, match=rf"{path}:12: tasks.part: .*a resource needs a service"
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
    with pytest.raises(BlueprintError, match=rf"{path}:18: .*run.until"):
        SimulationContext.from_yaml(path)


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


def test_resources_in_opposite_orders_name_the_tasks_line(tmp_path):
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
    with pytest.raises(BlueprintError, match="in one order everywhere") as error:
        SimulationContext.from_yaml(write(tmp_path, text))
    assert ":12:" in str(error.value)  # the tasks section


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
    with pytest.raises(ValueError, match="lag_interval must be 0 or more"):
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


def test_a_resource_in_the_wait_and_in_resource_is_refused_in_yaml(tmp_path):
    text = _wait_request(pack="resource: picker")
    with pytest.raises(BlueprintError, match="holds each resource once"):
        SimulationContext.from_yaml(write(tmp_path, text))


def test_a_wait_request_counts_in_the_acquisition_order_in_yaml(tmp_path):
    text = _wait_request(pack="resource: bench", tidy="resource: [bench, picker]")
    with pytest.raises(BlueprintError, match="in one order everywhere") as error:
        SimulationContext.from_yaml(write(tmp_path, text))
    assert ":10:" in str(error.value)  # the tasks section
    same = _wait_request(pack="resource: bench", tidy="resource: [picker, bench]")
    SimulationContext.from_yaml(write(tmp_path, same, "same.yaml"))


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


def test_requests_under_one_all_are_acquired_together_in_yaml(tmp_path):
    for listed in ("[picker, bench]", "[bench, picker]"):
        text = WAIT_ALL.replace("MODE", "all").replace("TIDY", f"resource: {listed}")
        with pytest.raises(BlueprintError, match="together in its wait"):
            SimulationContext.from_yaml(write(tmp_path, text))
    either = WAIT_ALL.replace("MODE", "any").replace(
        "TIDY", "wait: {any: {b: {request: bench}, p: {request: picker}}}"
    )
    SimulationContext.from_yaml(write(tmp_path, either, "either.yaml"))


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
    with pytest.raises(BlueprintError, match="needs a resource or a request"):
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
    (tmp_path / "tasks.yaml").write_text(
        "pack:\n  payload: {e: 1}\n  on:\n    - fire: {signal: s}\n"
    )
    (tmp_path / "signal.yaml").write_text("once: true\n")
    path = tmp_path / "doc.yaml"
    path.write_text(
        "tasks:\n  pack: !include pack.yaml\n  more: !include {file: pack.yaml, On: []}\n"
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

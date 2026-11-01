"""`desflow check`, `desflow test`, `--var` and `--watch`: what `--help` cannot show.

Each test runs the command with `typer.testing.CliRunner`; the watch tests start real
child processes, as `desflow run --watch` does.
"""

import importlib.util
import json
import multiprocessing
import os
import signal
import sys
import textwrap
import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from desflow.cli import app as cli
from desflow.cli import run as run_module

CONFORMANCE = Path(__file__).parent.parent / "conformance" / "yaml_only"
WITH_TESTS = CONFORMANCE / "b3_tests_and_sinks.yaml"

SHOP = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 7}
resources:
  packers: {current_cap: "${PACKERS:-1}", max_cap: 9}
services: {pack: {dist: constant, value: 2}}
arrivals:
  orders: {dist: constant, value: 5, spawn: pack_order}
tasks:
  pack_order: {service: pack, resource: packers, payload: {event_type: packed}}
run: {until: 21}
"""


def _write(folder, name, text):
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


def test_check_reports_each_file(tmp_path):
    """Two valid files and one invalid: exit 1, an `ok` line for each valid file and
    the line number of the invalid one's error."""
    good = _write(tmp_path, "good.yaml", SHOP)
    other = _write(tmp_path, "other.yaml", SHOP.replace("Shop", "Other"))
    bad = _write(tmp_path, "bad.yaml", SHOP.replace("arrivals:", "arivals:"))
    result = CliRunner().invoke(cli, ["check", str(good), str(bad), str(other)])
    assert result.exit_code == 1
    oks = [line for line in result.stdout.splitlines() if line.startswith("ok ")]
    assert oks == [f"ok {good} (sim_id Shop)", f"ok {other} (sim_id Other)"]
    assert f"{bad}:" in result.stderr
    assert "arivals" in result.stderr


def test_check_refuses_a_python_file(tmp_path):
    path = _write(tmp_path, "model.py", "x = 1\n")
    result = CliRunner().invoke(cli, ["check", str(path)])
    assert result.exit_code == 1
    assert "takes a YAML blueprint" in result.stderr


def test_check_names_the_extra_a_parquet_entry_needs(tmp_path, monkeypatch):
    path = _write(
        tmp_path,
        "shop.yaml",
        SHOP
        + "egress:\n  - type: Parquet\n    config: {default_path: out/e.parquet}\n",
    )
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a, **k: None if name == "pyarrow" else real(name, *a, **k),
    )
    result = CliRunner().invoke(cli, ["check", str(path)])
    assert result.exit_code == 1
    assert 'pip install "desflow[parquet]"' in result.stderr


def test_var_beats_the_environment(tmp_path, monkeypatch):
    """`--var` is read before the environment, and a name with neither takes its
    default, in the file and in a file it includes."""
    from desflow import SimulationContext

    path = _write(tmp_path, "shop.yaml", SHOP)
    monkeypatch.setenv("PACKERS", "2")
    both = SimulationContext.from_yaml(path, variables={"PACKERS": "3"})
    assert both._resources_config["packers"].current_cap == 3
    monkeypatch.delenv("PACKERS")
    assert (
        SimulationContext.from_yaml(path)._resources_config["packers"].current_cap == 1
    )
    _write(tmp_path, "resources.yaml", "packers: {current_cap: '${PACKERS:-1}'}\n")
    included = _write(
        tmp_path,
        "shop_include.yaml",
        SHOP.replace(
            'packers: {current_cap: "${PACKERS:-1}", max_cap: 9}',
            "!include resources.yaml",
        ),
    )
    context = SimulationContext.from_yaml(included, variables={"PACKERS": "4"})
    assert context._resources_config["packers"].current_cap == 4
    assert (
        SimulationContext.from_yaml(included)._resources_config["packers"].current_cap
        == 1
    )


def test_var_without_a_value_is_a_usage_error(tmp_path):
    path = _write(tmp_path, "shop.yaml", SHOP)
    for command in ("run", "check", "test"):
        result = CliRunner().invoke(cli, [command, str(path), "--var", "PACKERS"])
        assert result.exit_code == 2, command
        assert "NAME=VALUE" in result.output, command


def test_check_takes_var(tmp_path, monkeypatch):
    path = _write(tmp_path, "shop.yaml", SHOP.replace("${PACKERS:-1}", "${PACKERS}"))
    monkeypatch.delenv("PACKERS", raising=False)
    refused = CliRunner().invoke(cli, ["check", str(path)])
    assert refused.exit_code == 1 and "PACKERS" in refused.stderr
    accepted = CliRunner().invoke(cli, ["check", str(path), "--var", "PACKERS=2"])
    assert accepted.exit_code == 0


def test_test_passes_and_fails_by_entry(tmp_path):
    """One passing and one failing entry: exit 1, one `ok` line, and the failing
    statistic with its expected and its measured value."""
    text = WITH_TESTS.read_text(encoding="utf-8").replace(
        "{value: 3, within: 1}", "{value: 9, within: 1}"
    )
    path = _write(tmp_path, "model.yaml", text)
    result = CliRunner().invoke(cli, ["test", str(path)])
    assert result.exit_code == 1
    assert result.stdout.count(f"ok {path}: orders_are_packed") == 1
    assert f"FAILED {path}: queue_is_short" in result.stdout
    assert "tasks.pack_order.finished: expected 9 within 1, measured 3" in (
        result.stdout
    )


def test_test_without_tests_says_so(tmp_path):
    path = _write(tmp_path, "shop.yaml", SHOP)
    result = CliRunner().invoke(cli, ["test", str(path)])
    assert result.exit_code == 0
    assert f"{path}: no tests" in result.stdout


def test_test_fix_changes_only_the_failing_value(tmp_path):
    """`--fix` writes the measured value, and a comment beside it and every other
    line stay. A value inside its tolerance is left as it is."""
    text = WITH_TESTS.read_text(encoding="utf-8")
    text = text.replace("finished: 3}\n", "finished: 5}  # kept\n", 1)
    text = text.replace("{value: 3, within: 1}", "{value: 4, within: 1}")
    path = _write(tmp_path, "model.yaml", text)
    result = CliRunner().invoke(cli, ["test", str(path), "--fix"])
    assert result.exit_code == 0, result.output
    fixed = path.read_text(encoding="utf-8")
    changed = [
        (old, new)
        for old, new in zip(text.splitlines(), fixed.splitlines())
        if old != new
    ]
    assert changed == [
        (
            "    expect: {tasks.pack_order.finished: 5}  # kept",
            "    expect: {tasks.pack_order.finished: 3}  # kept",
        )
    ]
    assert len(text.splitlines()) == len(fixed.splitlines())
    assert CliRunner().invoke(cli, ["test", str(path)]).exit_code == 0


def test_test_fix_fills_an_empty_expect(tmp_path):
    text = textwrap.dedent(SHOP) + (
        "stats: basic\ntests:\n  - name: first\n    expect:\n"
        "      tasks.pack_order.finished: 0\n  - name: second\n"
    )
    path = _write(tmp_path, "shop.yaml", text)
    assert CliRunner().invoke(cli, ["test", str(path), "--fix"]).exit_code == 0
    assert CliRunner().invoke(cli, ["test", str(path)]).exit_code == 0
    assert "tasks.pack_order.finished: 3" in path.read_text(encoding="utf-8")


def test_test_of_a_builder_file_reads_tests_yaml(tmp_path):
    """A builder file with a `tests.yaml` beside it gives the result its YAML twin
    gives."""
    _write(
        tmp_path,
        "model.py",
        """\
        from desflow import SimulationContext

        app = (
            SimulationContext(sim_id="B3", factor=0, random_seed=7)
            .add_service("pack", dist="constant", value=2)
            .add_arrival("orders", dist="constant", value=5)
            .with_stats("basic")
        )


        @app.task(service_id="pack")
        def pack_order(task_id, context):
            return {"event_type": "order_packed"}


        @app.arrival_loop("orders")
        def orders(context):
            while True:
                entity = yield context.wait_for_arrival("orders")
                context.spawn(
                    pack_order(context.next_instance("pack_order"), context, entity=entity)
                )
        """,
    )
    tests = _write(
        tmp_path,
        "tests.yaml",
        """\
        tests:
          - name: orders_are_packed
            until: 21
            expect: {tasks.pack_order.finished: 3}
        """,
    )
    ok = CliRunner().invoke(cli, ["test", str(tmp_path / "model.py")])
    assert ok.exit_code == 0, ok.output
    assert f"ok {tests}: orders_are_packed" in ok.stdout
    tests.write_text(tests.read_text().replace(": 3}", ": 4}"), encoding="utf-8")
    failed = CliRunner().invoke(cli, ["test", str(tmp_path / "model.py")])
    assert failed.exit_code == 1
    assert "expected 4, measured 3" in failed.stdout
    lonely = _write(tmp_path / "lonely", "model.py", "x = 1\n")
    refused = CliRunner().invoke(cli, ["test", str(lonely)])
    assert refused.exit_code == 1 and "tests.yaml beside it" in refused.stderr


def test_test_refuses_repeated_names(tmp_path):
    text = WITH_TESTS.read_text(encoding="utf-8").replace(
        "queue_is_short", "orders_are_packed"
    )
    path = _write(tmp_path, "model.yaml", text)
    result = CliRunner().invoke(cli, ["test", str(path)])
    assert result.exit_code == 1
    assert "each test needs its own name" in result.stderr


def test_blueprint_filter_counts_the_file_and_python_files_beside_it(tmp_path):
    pytest.importorskip("watchfiles")
    from watchfiles import Change

    file = _write(tmp_path, "shop.yaml", SHOP)
    accept = run_module._BlueprintFilter(file)
    assert accept(Change.modified, str(file))
    assert accept(Change.modified, str(tmp_path / "logic.py"))
    assert accept(Change.added, str(tmp_path / "sub" / "more.py"))
    assert not accept(Change.modified, str(tmp_path / "data" / "events.parquet"))
    assert not accept(Change.modified, str(tmp_path / "other.yaml"))
    pycache = tmp_path / "__pycache__" / "logic.cpython-312.pyc"
    assert not accept(Change.modified, str(pycache))


def test_watch_needs_watchfiles(tmp_path, monkeypatch):
    path = _write(tmp_path, "shop.yaml", SHOP)
    monkeypatch.setitem(sys.modules, "watchfiles", None)
    result = CliRunner().invoke(cli, ["run", str(path), "--watch"])
    assert result.exit_code == 1
    assert 'pip install "desflow[watch]"' in result.stderr


WATCHED = """\
simulation: {sim_id: Watched, factor: 1, random_seed: 1}
services: {tick: {dist: constant, value: 1}}
arrivals:
  ticks: {dist: constant, value: 1, spawn: ticking}
tasks:
  ticking: {service: tick, payload: {event_type: tick}}
egress:
  - type: !python watched_sink.Sink
    config: {path: OUT}
run: {until: 100000}
"""

SINK = '''\
import json
import os
import asyncio
import queue
import time

from desflow.connectors.egress.base import BaseEgress


class Sink(BaseEgress):
    """Writes one line for each batch it receives, with the time it was received."""

    def __init__(self, path):
        self.path = path

    async def run(self, egress_queue):
        self._write({"pid": os.getpid(), "started": time.time()})
        while True:
            try:
                batch = egress_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.02)
                continue
            self._write({"pid": os.getpid(), "n": len(batch), "at": time.time()})

    def _write(self, line):
        with open(self.path, "a", encoding="utf-8") as out:
            out.write(json.dumps(line) + "\\n")
'''


@pytest.mark.skipif(sys.platform == "win32", reason="SIGINT stops the child")
def test_watch_stops_the_first_run_with_its_teardown_and_starts_a_second(
    tmp_path, monkeypatch
):
    """With a fake `watchfiles.watch` that reports one change: two children run, the
    first is stopped by SIGINT and writes its final batch, and the parent returns."""
    watchfiles = pytest.importorskip("watchfiles")
    out = tmp_path / "out.jsonl"
    _write(tmp_path, "watched_sink.py", SINK)
    path = _write(tmp_path, "watched.yaml", WATCHED.replace("OUT", out.as_posix()))

    def lines():
        if not out.exists():
            return []
        return [json.loads(line) for line in out.read_text().splitlines()]

    changed_at = []

    def fake_watch(*args, **kwargs):
        deadline = time.monotonic() + 60
        while not lines() and time.monotonic() < deadline:
            time.sleep(0.1)
        time.sleep(2.5)
        changed_at.append(time.time())
        yield {(watchfiles.Change.modified, str(path))}
        deadline = time.monotonic() + 60
        while len({line["pid"] for line in lines()}) < 2 and (
            time.monotonic() < deadline
        ):
            time.sleep(0.1)
        # What Ctrl+C does: it reaches the child as well as the parent
        for child in multiprocessing.active_children():
            os.kill(child.pid, signal.SIGINT)

    monkeypatch.setattr(watchfiles, "watch", fake_watch)
    monkeypatch.setattr(run_module, "_STOP_WAIT", 20)
    run_module._watch(path, run_module._RunOptions())
    records = lines()
    pids = list(dict.fromkeys(line["pid"] for line in records))
    assert len(pids) == 2
    # The first run's last batch is the flush of its teardown, after the stop
    first = [line for line in records if line["pid"] == pids[0] and "at" in line]
    assert first and max(line["at"] for line in first) >= changed_at[0]


def test_a_zero_count_statistic_reads_zero(tmp_path):
    """A run that never balks has no `balked` count, and a test that expects 0 passes."""
    text = WITH_TESTS.read_text(encoding="utf-8").replace(
        "  - name: queue_is_short",
        "  - name: nobody_balks\n    expect: {tasks.pack_order.balked: 0}\n"
        "  - name: queue_is_short",
    )
    path = _write(tmp_path, "model.yaml", text)
    result = CliRunner().invoke(cli, ["test", str(path)])
    assert result.exit_code == 0, result.output
    assert f"ok {path}: nobody_balks" in result.stdout


def test_the_conformance_blueprint_passes_its_own_tests():
    result = CliRunner().invoke(cli, ["test", str(WITH_TESTS)])
    assert result.exit_code == 0, result.output


def test_check_reports_a_missing_schema_file_and_goes_on(tmp_path):
    """A `schema_file` or `ssl` file that does not exist is reported with its line,
    and the next file is still checked."""
    pytest.importorskip("aiokafka")
    pytest.importorskip("fastavro")
    base = _write(tmp_path, "base.yaml", SHOP)
    ser = _write(
        tmp_path,
        "ser.yaml",
        SHOP
        + "egress:\n  - type: Kafka\n    config:\n      bootstrap_servers: b:9092\n"
        "      serializer: {type: confluent_avro, registry_url: 'http://x', "
        "schema_file: missing.avsc}\n",
    )
    ssl = _write(
        tmp_path,
        "ssl.yaml",
        SHOP
        + "egress:\n  - type: Kafka\n    config:\n      bootstrap_servers: b:9092\n"
        "      security_protocol: SSL\n      ssl: {cafile: missing.pem}\n",
    )
    result = CliRunner().invoke(cli, ["check", str(ser), str(ssl), str(base)])
    assert result.exit_code == 1
    assert f"{ser}:" in result.stderr and "missing.avsc" in result.stderr
    assert f"{ssl}:" in result.stderr and "KafkaEgress rejected its config" in (
        result.stderr
    )
    assert "Traceback" not in result.output
    assert f"ok {base} (sim_id Shop)" in result.stdout


def test_fix_writes_numbers_the_file_reads_back(tmp_path):
    """An unlimited store has the capacity `inf`, and a small float prints as
    `1e-05`; `--fix` writes both so that the loader reads numbers, and the next
    `desflow test` passes."""
    from desflow.blueprint.expectations import number_text

    assert number_text(float("inf")) == ".inf"
    assert number_text(float("-inf")) == "-.inf"
    assert number_text(1e-05) == "1.0e-05"
    assert number_text(1e16) == "1.0e+16"
    assert number_text(1.5e-06) == "1.5e-06"
    assert number_text(3) == "3"

    text = """\
simulation: {sim_id: Shop, random_seed: 7, factor: 0}
stores:
  bin: {}
arrivals:
  orders: {dist: exponential, rate: 1.0, spawn: pack}
tasks:
  pack:
    payload: {a: 1}
    put: {store: bin}
run: {until: 5}
stats: {level: all}
tests:
  - name: one
"""
    path = _write(tmp_path, "inf.yaml", text)
    fixed = CliRunner().invoke(cli, ["test", str(path), "--fix"])
    assert fixed.exit_code == 0, fixed.output
    assert "stores.bin.capacity: .inf" in path.read_text(encoding="utf-8")
    again = CliRunner().invoke(cli, ["test", str(path)])
    assert again.exit_code == 0, again.output


def test_the_on_warning_is_shown_once_for_replications_and_tests(tmp_path, caplog):
    """A wrong `on:` path warns at the first build; `--replications` and `desflow
    test` build the file again for each run and do not repeat it."""
    text = (
        SHOP.replace(
            "payload: {event_type: packed}}",
            "payload: {event_type: packed}, "
            "on: [{path: Shop.resources.lath.current_cap, value: 3}]}",
        )
        + "stats: basic\ntests:\n  - name: a\n    expect: {tasks.pack_order.finished: 3}\n"
        "  - name: b\n    expect: {tasks.pack_order.finished: 3}\n"
    )
    path = _write(tmp_path, "on.yaml", text)
    for args in (["run", str(path), "--replications", "3"], ["test", str(path)]):
        caplog.clear()
        with caplog.at_level("WARNING", logger="desflow.blueprint.build"):
            CliRunner().invoke(cli, args)
        shown = [r for r in caplog.records if "not a registry path" in r.getMessage()]
        assert len(shown) == 1, args
    # The level is restored afterwards
    import logging

    assert logging.getLogger("desflow.blueprint.build").level != logging.ERROR


@pytest.mark.skipif(sys.platform == "win32", reason="a Unix signal")
def test_ctrl_c_while_a_pass_restarts_ends_the_watch_quietly(tmp_path, monkeypatch):
    """A Ctrl+C that arrives while the old pass is being stopped ends the watch
    without an error, as one that arrives while it waits does."""
    watchfiles = pytest.importorskip("watchfiles")
    path = _write(tmp_path, "shop.yaml", SHOP)
    stops = []

    class _Child:
        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

    monkeypatch.setattr(run_module, "_start", lambda *args: _Child())

    def interrupted(child, interrupt):
        stops.append(interrupt)
        if interrupt:
            raise KeyboardInterrupt

    monkeypatch.setattr(run_module, "_stop", interrupted)
    monkeypatch.setattr(
        watchfiles,
        "watch",
        lambda *a, **k: iter([{(watchfiles.Change.modified, str(path))}]),
    )
    run_module._watch(path, run_module._RunOptions())
    assert stops == [True, False]

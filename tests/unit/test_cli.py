"""The `desflow` command: the behaviours no other test pins.

Every command's options and exit codes are pinned by `test_public_api.py`, and each
command's own behaviour by the test module of its package. These tests pin what
neither covers: the help with no arguments, Ctrl+C and SIGTERM, the lower bounds of
`--replications` and `--workers`, how a Console record line prints, the file
`run --summary` writes after a single run, or not at all, and `desflow mcp` without
its extra.
"""

import json
import logging
import os
import signal
import sys
import time

import click
import pytest

from typer.testing import CliRunner

from desflow import SimulationContext
from desflow.analysis import results
from desflow.analysis.models import run_once
from desflow.cli import app

SHOP = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 7}
arrivals:
  orders: {dist: exponential, rate: 1}
run: {until: 5}
"""


def test_help_lists_every_command():
    """With no arguments the command shows its help, as `--help` does."""
    names = ("run", "experiment", "report", "fit", "schema", "examples")
    shown = CliRunner().invoke(app, ["--help"])
    assert shown.exit_code == 0
    bare = CliRunner().invoke(app, [])
    # Click exits 2 for a group called with no arguments and no_args_is_help
    assert bare.exit_code == 2
    # Rich colours the help under GitHub Actions or FORCE_COLOR
    for result in (shown, bare):
        for name in names:
            assert f" {name} " in click.unstyle(result.output), name


# A shop with a task and no `stats`, so a single run collects no statistics of its own
PACKING = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 7}
resources: {packers: {current_cap: 1, max_cap: 1}}
arrivals: {orders: {dist: exponential, rate: 0.5, spawn: pack}}
services: {packing: {dist: exponential, rate: 1.0}}
tasks: {pack: {service: packing, resource: packers, payload: {event_type: packed}}}
run: {until: 200}
"""


def _stop_with_ctrl_c(context):
    raise KeyboardInterrupt


def _stop_with_sigterm(context):
    # The handler raises KeyboardInterrupt at the next instruction of the main thread
    os.kill(os.getpid(), signal.SIGTERM)
    time.sleep(5)


@pytest.mark.parametrize(
    "stop",
    [
        _stop_with_ctrl_c,
        pytest.param(
            _stop_with_sigterm,
            marks=pytest.mark.skipif(
                sys.platform == "win32", reason="os.kill ends the process on Windows"
            ),
        ),
    ],
)
@pytest.mark.parametrize("method", ["start", "finish"])
def test_ctrl_c_and_sigterm_log_and_exit_0_with_no_summary(
    tmp_path, monkeypatch, caplog, stop, method
):
    """SIGTERM stops a run as Ctrl+C does, while the model starts or runs, and a
    stopped run writes no summary."""
    path = tmp_path / "shop.yaml"
    path.write_text(SHOP, encoding="utf-8")
    summary = tmp_path / "summary.json"

    def interrupted(self, *args, **kwargs):
        if method == "finish":
            self.close()
        stop(self)

    monkeypatch.setattr(SimulationContext, method, interrupted)
    handler = signal.getsignal(signal.SIGTERM)
    with caplog.at_level(logging.INFO, logger="desflow.cli"):
        result = CliRunner().invoke(app, ["run", str(path), "--summary", str(summary)])
    assert result.exit_code == 0, result.output
    assert "Simulation stopped by user." in caplog.text
    assert "no summary was written" in result.stderr
    assert not summary.exists()
    assert signal.getsignal(signal.SIGTERM) == handler


@pytest.mark.parametrize(
    "stop",
    [
        _stop_with_ctrl_c,
        pytest.param(
            _stop_with_sigterm,
            marks=pytest.mark.skipif(
                sys.platform == "win32", reason="os.kill ends the process on Windows"
            ),
        ),
    ],
)
def test_stopped_replications_log_and_exit_0_with_no_summary(
    tmp_path, monkeypatch, caplog, stop
):
    """Replications stopped with Ctrl+C or SIGTERM end as a stopped single run does."""
    from desflow.analysis import runner

    path = tmp_path / "shop.yaml"
    path.write_text(SHOP, encoding="utf-8")
    summary = tmp_path / "summary.json"
    monkeypatch.setattr(runner, "replicate", lambda *args, **kwargs: stop(None))
    with caplog.at_level(logging.INFO, logger="desflow.cli"):
        result = CliRunner().invoke(
            app,
            [
                "run",
                str(path),
                "--replications",
                "2",
                "--until",
                "10",
                "--summary",
                str(summary),
            ],
        )
    assert result.exit_code == 0, result.output
    assert "Simulation stopped by user." in caplog.text
    assert "no summary was written" in result.stderr
    assert not summary.exists()


def test_summary_of_a_single_run_holds_its_statistics_at_typical(tmp_path):
    """The rows are the statistics `run_once` reads at the typical level, although the
    blueprint sets none, each with the unit and population of the result files."""
    path = tmp_path / "shop.yaml"
    path.write_text(PACKING, encoding="utf-8")
    summary = tmp_path / "new" / "summary.json"
    result = CliRunner().invoke(app, ["run", str(path), "--summary", str(summary)])
    assert result.exit_code == 0, result.output
    document = json.loads(summary.read_text(encoding="utf-8"))
    assert list(document) == [
        *("command", "model", "versions", "settings", "end_time", "statistics")
    ]
    assert document["model"]["sim_id"] == "Shop"
    assert document["settings"]["seed"] == 7
    assert document["settings"]["until"] == document["end_time"] == 200
    expected = run_once(str(path), seed=7, until=200, stats="typical").readings
    rows = {row["statistic"]: row for row in document["statistics"]}
    assert rows["tasks.pack.wait_mean"] == {
        "statistic": "tasks.pack.wait_mean",
        "unit": results.unit_of("tasks.pack.wait_mean"),
        "population": results.population_of("tasks.pack.wait_mean"),
        "value": expected["Shop.stats.tasks.pack.wait_mean"],
    }
    assert {f"Shop.stats.{name}": row["value"] for name, row in rows.items()} == {
        name: value for name, value in expected.items() if not isinstance(value, bool)
    }


@pytest.mark.parametrize("option", [["--sample", "2"], ["--watch"]])
def test_summary_is_refused_with_sample_and_watch(tmp_path, option):
    path = tmp_path / "shop.yaml"
    path.write_text(SHOP, encoding="utf-8")
    summary = tmp_path / "summary.json"
    result = CliRunner().invoke(
        app, ["run", str(path), *option, "--summary", str(summary)]
    )
    assert result.exit_code == 2
    assert "--summary" in result.stderr
    assert not summary.exists()


def test_a_failed_run_removes_an_earlier_summary_and_writes_none(tmp_path):
    path = tmp_path / "shop.yaml"
    path.write_text("simulation: {sim_id: Shop}\nnot_a_section: 1\n", encoding="utf-8")
    summary = tmp_path / "summary.json"
    summary.write_text("{}", encoding="utf-8")
    result = CliRunner().invoke(app, ["run", str(path), "--summary", str(summary)])
    assert result.exit_code == 1
    assert not summary.exists()


def test_a_summary_that_cannot_be_written_exits_1_and_leaves_no_file(
    tmp_path, monkeypatch
):
    path = tmp_path / "shop.yaml"
    path.write_text(SHOP, encoding="utf-8")
    folder = CliRunner().invoke(app, ["run", str(path), "--summary", str(tmp_path)])
    assert folder.exit_code == 1 and "is a folder" in folder.stderr

    def full_disk(target, data):
        target.write_text("{", encoding="utf-8")
        raise OSError("No space left on device")

    monkeypatch.setattr(results, "write_json", full_disk)
    summary = tmp_path / "out" / "summary.json"
    result = CliRunner().invoke(app, ["run", str(path), "--summary", str(summary)])
    assert result.exit_code == 1
    assert "No space left on device" in result.stderr
    assert list(summary.parent.iterdir()) == []


def test_mcp_without_the_extra_exits_1_and_names_it(monkeypatch):
    # An entry of None makes the import fail, as it does without the SDK
    monkeypatch.setitem(sys.modules, "desflow.mcp_server", None)
    result = CliRunner().invoke(app, ["mcp"])
    assert result.exit_code == 1
    assert "pip install 'desflow[mcp]'" in result.stderr


def test_replications_below_2_is_a_usage_error(tmp_path):
    path = tmp_path / "shop.yaml"
    path.write_text(SHOP, encoding="utf-8")
    result = CliRunner().invoke(app, ["run", str(path), "--replications", "1"])
    assert result.exit_code == 2


def test_workers_below_1_is_a_usage_error(tmp_path):
    path = tmp_path / "shop.yaml"
    path.write_text(SHOP, encoding="utf-8")
    result = CliRunner().invoke(
        app, ["run", str(path), "--replications", "2", "--workers", "0"]
    )
    assert result.exit_code == 2


def test_a_console_record_prints_without_the_log_prefix():
    """A Console sink logs a batch in one call, so its lines print as they are, while
    every other log line keeps the time, level and name."""
    from desflow.cli.run import _RecordFormatter

    formatter = _RecordFormatter("%(levelname)s %(name)s: %(message)s")

    def line(name, message):
        record = logging.LogRecord(name, logging.INFO, __file__, 1, message, None, None)
        return formatter.format(record)

    assert line("desflow.connectors.egress.local", "[EVT] {}\n[EVT] {}") == (
        "[EVT] {}\n[EVT] {}"
    )
    assert (
        line("desflow.builder.context", "started")
        == "INFO desflow.builder.context: started"
    )


def _all_commands(command, path):
    """Each command path under `command`, with the Click command, top first."""
    yield path, command
    for name, sub in getattr(command, "commands", {}).items():
        yield from _all_commands(sub, f"{path} {name}")


def test_every_help_example_names_a_command_and_its_options():
    """Each `$ desflow ...` line of an Examples list runs a command that exists,
    with options that command takes."""
    import re

    import typer

    found = dict(_all_commands(typer.main.get_command(app), "desflow"))
    shown = 0
    for path, command in found.items():
        for line in re.findall(
            r"\$ \[bold cyan\](.*?)\[/bold cyan\]", command.epilog or ""
        ):
            words = line.split()
            # The longest command path the line starts with is the command it runs
            target = max(
                (p for p in found if words[: len(p.split())] == p.split()), key=len
            )
            options = {o for param in found[target].params for o in param.opts}
            for word in words:
                if word.startswith("--"):
                    assert word in options, f"{line}: {target} has no {word}"
            shown += 1
    # Every command and the examples get sub-command show at least one example
    for path, command in found.items():
        assert command.epilog, f"{path} has no Examples list"
    assert shown >= len(found)


def test_examples_get_reports_a_read_that_times_out(tmp_path, monkeypatch):
    """A time-out while a file is read is an error line and exit 1, not a traceback."""
    from desflow import examples

    def timed_out(url):
        raise TimeoutError("timed out")

    monkeypatch.setattr(examples, "_fetch", timed_out)
    name = next(iter(examples.EXAMPLES))
    result = CliRunner().invoke(
        app, ["examples", "get", name, "--dest", str(tmp_path), "--ref", "main"]
    )
    assert result.exit_code == 1
    assert "could not download" in result.output
    assert "timed out" in result.output

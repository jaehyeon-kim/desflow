"""The `desflow` command: the behaviours no other test pins.

Every command's options and exit codes are pinned by `test_public_api.py`, and each
command's own behaviour by the test module of its package. These tests pin what
neither covers: the help with no arguments, Ctrl+C, the lower bounds of
`--replications` and `--workers`, and how a Console record line prints.
"""

import logging

import click

from typer.testing import CliRunner

from desflow import SimulationContext
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


def test_ctrl_c_logs_and_exits_0(tmp_path, monkeypatch, caplog):
    path = tmp_path / "shop.yaml"
    path.write_text(SHOP, encoding="utf-8")

    def interrupted(self):
        self.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(SimulationContext, "finish", interrupted)
    with caplog.at_level(logging.INFO, logger="desflow.cli"):
        result = CliRunner().invoke(app, ["run", str(path)])
    assert result.exit_code == 0, result.output
    assert "Simulation stopped by user." in caplog.text


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
        line("desflow.core.context", "started") == "INFO desflow.core.context: started"
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

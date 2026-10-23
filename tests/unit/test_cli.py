"""The `desflow` command: the behaviours no other test pins.

Every command's options and exit codes are pinned by `test_public_api.py`, and each
command's own behaviour by the test module of its package. These tests pin what
neither covers: the help with no arguments, Ctrl+C, and the lower bounds of
`--replications` and `--workers`.
"""

import logging

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
    for result in (shown, bare):
        for name in names:
            assert f" {name} " in result.output, name


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

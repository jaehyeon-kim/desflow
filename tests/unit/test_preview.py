"""`desflow run --sample` and `--stdout`: the preview runs, which replace every sink
of a blueprint with JSON lines on stdout."""

import logging
import subprocess
import sys
import textwrap
import time

import orjson
import pytest
from typer.testing import CliRunner

from desflow.cli import app

SHOP = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 7}
resources:
  packers: {current_cap: 1, max_cap: 1}
services:
  packing: {dist: constant, value: 0.5}
arrivals:
  orders: {dist: constant, value: 1, spawn: pack}
tasks:
  pack: {service: packing, resource: packers, payload: {packed: true}}
"""

KAFKA = """\
egress:
  - type: Kafka
    config: {bootstrap_servers: "localhost:1"}
"""


def _write(tmp_path, text, name="shop.yaml"):
    path = tmp_path / name
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


def _run(*arguments):
    return CliRunner().invoke(app, ["run", *map(str, arguments)])


def _records(result):
    return [orjson.loads(line) for line in result.stdout.splitlines()]


def _events(result):
    return [r for r in _records(result) if r["stream_type"] == "event"]


def test_sample_prints_n_events_and_never_imports_the_files_sinks(tmp_path):
    """With the kafka extra hidden, --sample 3 on a Kafka blueprint prints exactly
    3 events, every stdout line is JSON, and the Kafka connector is not imported."""
    path = _write(tmp_path, SHOP + KAFKA)
    code = textwrap.dedent(
        f"""
        import sys
        sys.modules["aiokafka"] = None  # as if the kafka extra were not installed
        from typer.testing import CliRunner
        from desflow.cli import app
        result = CliRunner().invoke(app, ["run", {str(path)!r}, "--sample", "3"])
        assert result.exit_code == 0, result.output
        assert "desflow.connectors.egress.kafka" not in sys.modules
        sys.stdout.write(result.stdout)
        """
    )
    run = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    records = [orjson.loads(line) for line in run.stdout.splitlines()]
    assert len([r for r in records if r["stream_type"] == "event"]) == 3


def test_sample_ending_at_until_says_how_many_events_it_printed(tmp_path):
    result = _run(_write(tmp_path, SHOP), "--sample", 1000, "--until", 5)
    assert result.exit_code == 0, result.output
    count = len(_events(result))
    assert 0 < count < 1000
    assert f"sample ended at until with {count} of 1000 events" in result.stderr


def test_sample_runs_a_backfill_then_live_file_unpaced(tmp_path):
    """--sample sets factor 0 and clears go_live_at, so the events a paced run
    would print about 4 and 9 seconds after it starts print at once."""
    text = SHOP.replace(
        "factor: 0, random_seed: 7",
        "factor: 1, random_seed: 7, logical_start_time: -1s, go_live_at: now",
    ).replace("value: 1, spawn", "value: 5, spawn")
    began = time.monotonic()
    result = _run(_write(tmp_path, text), "--sample", 5)
    assert result.exit_code == 0, result.output
    assert len(_events(result)) == 5
    assert time.monotonic() - began < 4


def test_stdout_prints_each_record_once_whatever_the_files_entries(tmp_path):
    """Two egress entries that split the records by when print what one would."""
    _write(
        tmp_path,
        """\
        def early(record):
            return record["sim_ts"] < 2

        def late(record):
            return record["sim_ts"] >= 2
        """,
        "split.py",
    )
    split = """\
    egress:
      - {type: Console, when: !python split.early}
      - {type: Console, when: !python split.late}
    """
    one = "egress:\n  - type: Console\n"
    shop = SHOP.replace("random_seed: 7", "random_seed: 7, logical_start_time: 2026-01-01")
    outputs = [
        _run(_write(tmp_path, shop + egress, name), "--stdout", "--until", 4)
        for name, egress in (("split.yaml", textwrap.dedent(split)), ("one.yaml", one))
    ]
    assert [r.exit_code for r in outputs] == [0, 0]
    assert len(_events(outputs[0])) > 0
    assert _records(outputs[0]) == _records(outputs[1])


def test_sample_zero_is_a_usage_error(tmp_path):
    assert _run(_write(tmp_path, SHOP), "--sample", 0).exit_code == 2


def test_run_before_is_skipped_and_said_so(tmp_path, caplog):
    marker = tmp_path / "prepared"
    _write(
        tmp_path,
        f"""\
        def prepare():
            open({str(marker)!r}, "w").close()
        """,
        "setup.py",
    )
    path = _write(tmp_path, SHOP + "run:\n  before: [!python setup.prepare]\n")
    with caplog.at_level(logging.WARNING, logger="desflow.core.context"):
        result = _run(path, "--sample", 1)
    assert result.exit_code == 0, result.output
    assert not marker.exists()
    assert "run.before skipped: preview runs use no real sinks" in caplog.text


def test_stdout_prints_a_label_once(tmp_path):
    text = (
        SHOP
        + "egress:\n  - type: Console\n  - {type: Console, when: labels}\n"
        + "scenario:\n  - {at: 1, record: {fault: jam}}\n"
    )
    result = _run(_write(tmp_path, text), "--stdout", "--until", 3)
    assert result.exit_code == 0, result.output
    labels = [r for r in _records(result) if r["stream_type"] == "label"]
    assert len(labels) == 1


@pytest.mark.parametrize(
    "name, arguments, message",
    [
        (
            "shop.yaml",
            ["--sample", "2", "--replications", "2"],
            "Error: --sample and --stdout print one run; leave out --replications",
        ),
        (
            "shop.py",
            [],
            "Error: desflow run takes a YAML blueprint; run a Python file with "
            "python FILE",
        ),
    ],
)
def test_one_run_options_and_a_python_file_are_refused(
    tmp_path, name, arguments, message
):
    result = _run(_write(tmp_path, SHOP, name), *arguments)
    assert result.exit_code == 2
    assert message in result.stderr


@pytest.mark.parametrize(
    "section, option",
    [("ingress", ["--sample", "1"]), ("egress", ["--stdout", "--until", "1"])],
)
def test_a_replaced_connector_with_an_unknown_type_is_refused(
    tmp_path, section, option
):
    """A preview does not build the connectors it replaces or skips, but still
    checks their types."""
    result = _run(_write(tmp_path, SHOP + f"{section}:\n  - type: Wrong\n"), *option)
    assert result.exit_code == 1
    assert "unknown connector type 'Wrong'" in result.stderr

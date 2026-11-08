"""Every command desflow prints for the reader to paste names a file that exists.

The commands that `desflow examples get` prints, and the repeat lines of `desflow run`
(for a replication, a run with no seed, and the log line of a run), are split as a shell
splits them, from a file in a folder whose name has a space and a `--dest` other than
`.`. Each must name the file or the folder it is about.
"""

import logging
import shlex
from pathlib import Path
from typing import List

import pytest
from typer.testing import CliRunner

from desflow.cli import app as cli
from desflow.examples import EXAMPLES

runner = CliRunner()

MODEL = """\
simulation: {sim_id: S, factor: 0}
resources: {desk: {current_cap: 1}}
services: {work: {dist: constant, value: 1}}
arrivals: {a: {dist: constant, value: 1, spawn: serve}}
tasks: {serve: {service: work, resource: desk, payload: {}}}
run: {until: 5}
"""


def _commands(output: str) -> List[str]:
    """The shell commands in a printed text: a line's part after its first colon."""
    return [line.partition(": ")[2] for line in output.splitlines() if ": " in line]


def _check_cd_commands(output: str, folder_for: Path) -> int:
    """Each `cd <folder> && <command> <file>` line names a folder that exists and a
    file in it."""
    checked = 0
    for line in output.splitlines():
        if " && " not in line or "cd " not in line:
            continue
        text = line[line.index("cd ") :]
        move, _, rest = text.partition(" && ")
        tokens = shlex.split(move)
        assert tokens[0] == "cd" and len(tokens) == 2, line
        folder = Path(tokens[1])
        assert folder.is_dir(), f"{line!r}: {folder} is not a folder"
        command = shlex.split(rest.partition(" (")[0])
        files = [t for t in command[1:] if t.endswith((".yaml", ".py"))]
        assert files, line
        for name in files:
            assert (folder / name).is_file(), f"{line!r}: {folder / name} is missing"
        checked += 1
    return checked


@pytest.mark.parametrize("name", sorted(EXAMPLES))
def test_the_commands_of_examples_get_name_files_that_exist(
    name, tmp_path, monkeypatch
):
    monkeypatch.setattr("desflow.examples._fetch", lambda url: b"x")
    dest = tmp_path / "my downloads"
    result = runner.invoke(cli, ["examples", "get", name, "--dest", str(dest)])
    assert result.exit_code == 0, result.output
    assert _check_cd_commands(result.output, dest) >= 1, result.output


def test_a_dashboard_note_names_the_folder_the_script_was_written_to(
    tmp_path, monkeypatch
):
    monkeypatch.setattr("desflow.examples._fetch", lambda url: b"x")
    dest = tmp_path / "my downloads"
    result = runner.invoke(
        cli, ["examples", "get", "kafka", "--api", "yaml", "--dest", str(dest)]
    )
    assert result.exit_code == 0, result.output
    notes = [
        line
        for line in result.output.splitlines()
        if line.startswith("Watch and steer")
    ]
    assert len(notes) == 1, result.output
    folder = shlex.split(notes[0].partition("cd ")[2].partition(" && ")[0])[0]
    assert (Path(folder) / "kafka_dashboard.py").is_file(), notes[0]


def _model(tmp_path: Path) -> Path:
    folder = tmp_path / "my shop"
    folder.mkdir()
    path = folder / "shop.yaml"
    path.write_text(MODEL)
    return path


def _split_repeat(text: str) -> List[str]:
    line = next(row for row in text.splitlines() if "desflow run" in row)
    return shlex.split(line[line.index("desflow run") :])


def test_the_repeat_line_of_replications_quotes_the_file(tmp_path):
    path = _model(tmp_path)
    result = runner.invoke(
        cli, ["run", str(path), "--replications", "2", "--until", "5"]
    )
    assert result.exit_code == 0, result.output
    command = _split_repeat(result.output)
    assert Path(command[2]) == path and path.is_file(), command


def test_the_repeat_line_of_a_run_with_no_seed_quotes_the_file(tmp_path, caplog):
    path = _model(tmp_path)
    with caplog.at_level(logging.INFO):
        result = runner.invoke(cli, ["run", str(path), "--until", "5"])
    assert result.exit_code == 0, result.output
    text = result.output + "\n" + caplog.text
    command = _split_repeat(text)
    assert Path(command[2]) == path, command

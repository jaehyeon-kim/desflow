"""Running a `desflow` command and keeping some of the lines it prints."""

import os
import re
from pathlib import Path
from typing import List
from unittest import mock

from .capture import ROOT, cwd


def command(*arguments: str) -> List[str]:
    """What `desflow` prints on stdout for these arguments, run from ROOT."""
    from typer.testing import CliRunner

    from desflow.cli import app

    with cwd(ROOT):
        result = CliRunner().invoke(app, list(arguments))
    assert result.exit_code == 0, result.output
    return result.stdout.rstrip("\n").split("\n")


def sample(path: str, events: int, *options: str) -> List[str]:
    """The JSON lines `desflow run` prints for the first `events` events of a blueprint,
    unpaced and with no service started.

    Args:
        path: The blueprint, relative to the repository.
        events: How many events to sample.
        *options: More options for `desflow run`.

    Returns:
        List[str]: The lines, with each timestamp replaced by `...`.
    """
    lines = command("run", path, "--sample", str(events), *options)
    return [re.sub(r'"timestamp":"[^"]*"', '"timestamp":"..."', line) for line in lines]


def explain(*arguments: str, **variables: str) -> List[str]:
    """What `desflow explain` prints, with these environment variables and no others
    of the names a blueprint reads."""
    names = {"PACKERS": None, "RATE": None, **variables}
    kept = {k: v for k, v in names.items() if v is not None}
    with mock.patch.dict(os.environ, kept):
        for name in names:
            if name not in kept:
                os.environ.pop(name, None)
        return command("explain", *arguments)


def lines_from(lines: List[str], start: str) -> List[str]:
    """The lines from the first that starts with `start`.

    Args:
        lines: The lines to cut.
        start: The text the first kept line starts with.

    Returns:
        List[str]: The lines kept.
    """
    first = next(i for i, line in enumerate(lines) if line.startswith(start))
    return lines[first:]


def lines_between(lines: List[str], start: str, end: str) -> List[str]:
    """The lines from the first that starts with `start` to the one before `end`.

    Args:
        lines: The lines to cut.
        start: The text the first kept line starts with.
        end: The text the first line after the kept ones starts with.

    Returns:
        List[str]: The lines kept.
    """
    first = next(i for i, line in enumerate(lines) if line.startswith(start))
    last = next(i for i, line in enumerate(lines) if line.startswith(end))
    return lines[first:last]


def in_folder(folder: Path, *arguments: str) -> List[str]:
    """What `desflow` prints on stdout for these arguments, run in `folder`, with
    `desflow examples get` served from this repository's examples/ folder.

    Args:
        folder: The folder to run in.
        *arguments: The arguments after `desflow`.

    Returns:
        List[str]: The printed lines.
    """
    from typer.testing import CliRunner

    from desflow import examples
    from desflow.cli import app

    def fetch(url: str) -> bytes:
        return (ROOT / "examples" / url.split("/examples/", 1)[1]).read_bytes()

    with cwd(folder), mock.patch.object(examples, "_fetch", side_effect=fetch):
        result = CliRunner().invoke(app, list(arguments))
    assert result.exit_code == 0, result.output
    # A command that prints a resolved path prints it from the folder
    return result.stdout.rstrip("\n").replace(f"{folder}/", "").split("\n")


def resources_and_reasons(*arguments: str) -> List[str]:
    """`desflow explain`'s resources table and what it leaves out, without the table
    by units and the hints.

    Args:
        *arguments: The blueprint and any options, as for `explain`.

    Returns:
        List[str]: The lines kept.
    """
    lines = explain(*arguments)
    kept = lines_between(lines, "Resources", next(x for x in lines if "by units" in x))
    if any(line.startswith("Not estimated:") for line in lines):
        kept += lines_between(lines, "Not estimated:", "Hints for")
    return kept


def records_with(path: str, text: str, count: int) -> List[str]:
    """The first `count` JSON lines of `desflow run --stdout` that contain `text`.

    Args:
        path: The blueprint, relative to the repository.
        text: The text a kept line contains, as `grep` would match it.
        count: How many lines to keep, as `head -n` would.

    Returns:
        List[str]: The lines, with each timestamp replaced by `...`.
    """
    lines = [line for line in command("run", path, "--stdout") if text in line]
    return [
        re.sub(r'"timestamp":"[^"]*"', '"timestamp":"..."', line)
        for line in lines[:count]
    ]

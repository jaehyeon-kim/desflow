"""`desflow test`: checks the statistics a model gives at a fixed seed."""

import functools
import logging
from pathlib import Path
from typing import Annotated, Any, Dict, List, Mapping, Optional, Tuple

import typer

from desflow.cli._common import parse_variables, quiet_builds, setup_logging
from desflow.utils import suggestion

# The examples --help prints after the options, as odctl's help does
_EPILOG = """
[bold underline]Examples:[/bold underline]\n
  [dim]# Check the tests in a blueprint[/dim]\n
  $ [bold cyan]desflow test shop.yaml[/bold cyan]\n\n
  [dim]# Write the statistics the runs gave into the expect values, then read them[/dim]\n
  $ [bold cyan]desflow test shop.yaml --fix[/bold cyan]
"""


def register(app: typer.Typer) -> None:
    """Adds the `test` command to the app.

    Args:
        app: The `desflow` Typer app.
    """
    app.command("test", rich_help_panel="Run and Check", epilog=_EPILOG)(_test)


# Each command is private: typer shows its docstring as the help text, so it
# carries no Args section, and register() gives the command its name
def _test(
    files: Annotated[
        List[Path],
        typer.Argument(
            help="The blueprint files, or builder files with a tests.yaml beside them.",
            metavar="FILE...",
            exists=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    fix: Annotated[
        bool,
        typer.Option(
            "--fix",
            help="Write the statistics the runs gave into the expect values that "
            "differ, and into a test with none, instead of reporting differences. "
            "Read the changes before you keep them.",
        ),
    ] = False,
    var: Annotated[
        Optional[List[str]],
        typer.Option(
            "--var",
            metavar="NAME=VALUE",
            help="Fill ${NAME} in a blueprint and in the files it includes, before "
            "the environment does. Repeat it for more.",
        ),
    ] = None,
) -> None:
    """Run the tests of each file in FILE... and report the statistics that differ.

    A test names a seed, an end time and the statistics its run must give, such as
    tasks.pack_order.finished. It runs the model once, unpaced, with no ingress and no
    egress, so a test contacts no service. A test states its measures at one seed in
    one version of desflow: a new release may draw its random numbers differently, so
    exact seeded values are not promised across versions. Run it with --fix after an
    upgrade or a deliberate change and read the changes. A builder file reads its
    tests from a tests.yaml in its folder.

    Prints ok for each passing test, and each difference of a failing one. Exits 1 if
    any test fails.
    """
    from desflow.blueprint.loader import BlueprintError

    variables = parse_variables(var)
    setup_logging(logging.WARNING)
    failed = False
    for file in files:
        try:
            failed = _test_file(file, fix, variables) or failed
        except (BlueprintError, ValueError) as exc:
            typer.echo(f"Error: {exc}", err=True)
            failed = True
    if failed:
        raise typer.Exit(code=1)


def _test_file(file: Path, fix: bool, variables: Mapping[str, str]) -> bool:
    """Runs the tests of one file; returns whether any failed.

    Raises:
        BlueprintError: If the file or its tests are invalid.
        ValueError: If a test cannot run, such as one with no seed or no end time.
    """
    from desflow.blueprint.build import load_blueprint, read_tests_file
    from desflow.blueprint.expectations import fix_expectations
    from desflow.builder.context import SimulationContext, parts_of
    from desflow.core.statistics import published_value
    from desflow.loading import is_builder_file, load_context

    model: Any
    if is_builder_file(file):
        if variables:
            raise ValueError(
                f"{file}: --var fills ${{NAME}} in a YAML blueprint, and a builder "
                f"file reads its own settings"
            )
        holder = file.with_name("tests.yaml")
        if not holder.is_file():
            raise ValueError(
                f"{file}: its tests are in tests.yaml beside it, and there is no "
                f"{holder}"
            )
        tests, source = read_tests_file(holder)
        context = load_context(file.parent, file.name)
        end = parts_of(context).defaults.until
        model = functools.partial(load_context, file.parent, file.name)
    else:
        built = load_blueprint(file, None, variables)
        tests, source, context, end = (
            built.blueprint.tests,
            built.source,
            built.context,
            built.run.until,
        )
        holder = file
        model = (
            functools.partial(SimulationContext.from_yaml, file, variables=variables)
            if variables
            else str(file)
        )
    if not tests:
        typer.echo(f"{holder}: no tests")
        return False

    failed = False
    measured: List[Mapping[str, Any]] = []
    for test in tests:
        with quiet_builds():
            found = _measure(test, model, context.random_seed, end, holder)
        for name in test.expect:
            value = published_value(found, name)
            if value is not None:
                found[name] = value
        measured.append(found)
        differing, missing = _differences(test.expect, found)
        if missing or (differing and not fix):
            failed = True
            typer.echo(f"FAILED {holder}: {test.name}")
            for line in missing + (differing if not fix else []):
                typer.echo(f"  {line}")
        elif fix and (differing or not test.expect):
            typer.echo(f"fixed {holder}: {test.name}")
        else:
            typer.echo(f"ok {holder}: {test.name}")
    if fix:
        text = holder.read_bytes().decode("utf-8")
        fixed = fix_expectations(text, source, tests, measured)
        if fixed != text:
            holder.write_bytes(fixed.encode("utf-8"))
    return failed


def _measure(
    test: Any,
    model: Any,
    file_seed: Optional[int],
    file_until: Optional[float],
    holder: Path,
) -> Dict[str, Any]:
    """Runs one test once and returns the numeric statistics it published, by name
    without the sim id."""
    from desflow.analysis.runner import level_for, run_replications
    from desflow.blueprint.build import missing_seed

    seed = test.seed if test.seed is not None else file_seed
    if seed is None:
        raise ValueError(f"{holder}: {missing_seed(test.name)}")
    until = test.until if test.until is not None else file_until
    if until is None:
        raise ValueError(
            f"{holder}: test '{test.name}' needs an end time: give `until` in the "
            f"test or run.until in the file"
        )
    result = run_replications(
        model,
        [seed],
        until=until,
        stats=level_for(list(test.expect) or None),
    )[0]
    prefix = f"{result.sim_id}.stats."
    return {
        name.removeprefix(prefix): value
        for name, value in result.readings.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }


def _differences(
    expect: Mapping[str, Any], found: Mapping[str, Any]
) -> Tuple[List[str], List[str]]:
    """The lines for the expected statistics that differ from the run, and for those
    the run did not publish at all."""
    from desflow.blueprint.expectations import holds, number_text
    from desflow.blueprint.models import ExpectedWithin

    differing: List[str] = []
    missing: List[str] = []
    for name, expected in expect.items():
        if name not in found:
            missing.append(
                f"{name}: the run publishes no such statistic{suggestion(name, found)}"
            )
            continue
        measured = found[name]
        if holds(expected, measured):
            continue
        if isinstance(expected, ExpectedWithin):
            want = (
                f"{number_text(expected.value)} within {number_text(expected.within)}"
            )
        else:
            want = number_text(expected)
        differing.append(f"{name}: expected {want}, measured {number_text(measured)}")
    return differing, missing

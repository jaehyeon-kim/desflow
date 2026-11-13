"""Checks a model file the way `desflow check` does, and reads what a run of it needs."""

from pathlib import Path
from typing import Mapping, NamedTuple, Optional

from desflow.blueprint.models import Run
from desflow.builder.context import SimulationContext
from desflow.utils import is_builder_file


class ModelCheck(NamedTuple):
    """What `check_model` found in one file.

    Attributes:
        context: The built context, or None when the file was refused.
        problem: Why the file was refused, as one message, or None.
        no_context: True when the file is a Python file refused only because it
            defines no module-level `SimulationContext`, as a Low-level file does.
        run: The `run` section of a YAML blueprint that was accepted, else None.
        start: The `simulation.logical_start_time` of an accepted YAML blueprint as
            the file gives it after `${NAME}` and `!include` are read, as text even
            when YAML read a time, else None.
    """

    context: Optional[SimulationContext]
    problem: Optional[str]
    no_context: bool = False
    run: Optional[Run] = None
    start: Optional[str] = None


def check_model(
    file: Path, variables: Optional[Mapping[str, str]] = None
) -> ModelCheck:
    """Checks one blueprint or builder file as `desflow check` does, without running it.

    For a YAML blueprint that is accepted, it also reads the `run` section and the
    start time, with the loader that `desflow run` uses.

    Args:
        file: A YAML blueprint, or a Python file with a module-level context.
        variables: Values for `${NAME}` in a blueprint, as `--var` gives them.

    Returns:
        ModelCheck: The built context, or why the file was refused, and for an
            accepted YAML blueprint its `run` section and start time.

    Raises:
        Exception: An error of a YAML file other than a `BlueprintError`, an
            `ImportError` or a `ValueError`.
    """
    from desflow.blueprint.build import load_blueprint
    from desflow.blueprint.loader import read_document
    from desflow.cli.check import check_file

    checked = check_file(file, variables)
    if checked.context is None or is_builder_file(file):
        return ModelCheck(checked.context, checked.problem, checked.no_context)
    run = load_blueprint(file, variables=variables).run
    data = read_document(file, variables=variables)[0]
    start = (data.get("simulation") or {}).get("logical_start_time")
    if start is not None and not isinstance(start, str):
        start = str(start)
    return ModelCheck(checked.context, None, False, run, start)

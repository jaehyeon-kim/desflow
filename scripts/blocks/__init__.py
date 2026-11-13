"""The generators of the record blocks, one module for each group of pages.

Each module's `BLOCKS` maps the name a page marks a block with to the function that
runs its code and returns the lines the block shows. `capture` and `commands` hold
what the generators share.
"""

import functools
from typing import Callable, Dict, List

from . import (
    cli,
    examples,
    flows,
    generate_data,
    guides,
    inputs,
    tutorial_runs,
    tutorial_yaml,
    tutorials,
    user_guide,
    user_guide_runs,
)
from .capture import without_blueprint_variables


def _own_environment(run: Callable[[], List[str]]) -> Callable[[], List[str]]:
    """Wraps a generator so that it runs without the caller's variables for blueprints."""

    @functools.wraps(run)
    def wrapper() -> List[str]:
        with without_blueprint_variables():
            return run()

    return wrapper


_GENERATORS: Dict[str, Callable[[], List[str]]] = {
    **cli.BLOCKS,
    **examples.BLOCKS,
    **guides.BLOCKS,
    **user_guide.BLOCKS,
    **user_guide_runs.BLOCKS,
    **tutorials.BLOCKS,
    **tutorial_runs.BLOCKS,
    **tutorial_yaml.BLOCKS,
    **inputs.BLOCKS,
    **flows.BLOCKS,
    **generate_data.BLOCKS,
}
BLOCKS: Dict[str, Callable[[], List[str]]] = {
    name: _own_environment(run) for name, run in _GENERATORS.items()
}

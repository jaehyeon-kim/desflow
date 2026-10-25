"""The generators of the record blocks, one module for each group of pages.

Each module's `BLOCKS` maps the name a page marks a block with to the function that
runs its code and returns the lines the block shows. `capture` and `commands` hold
what the generators share.
"""

from typing import Callable, Dict, List

from . import (
    cli,
    examples,
    flows,
    guides,
    inputs,
    tutorial_runs,
    tutorials,
    user_guide,
    user_guide_runs,
)

BLOCKS: Dict[str, Callable[[], List[str]]] = {
    **cli.BLOCKS,
    **examples.BLOCKS,
    **guides.BLOCKS,
    **user_guide.BLOCKS,
    **user_guide_runs.BLOCKS,
    **tutorials.BLOCKS,
    **tutorial_runs.BLOCKS,
    **inputs.BLOCKS,
    **flows.BLOCKS,
}

"""How `--verify` decides whether a measured value differs from the analysis."""

from typing import Any, List, Optional

from desflow.explain.models import Comparison
from desflow.explain.pairs import Pair

# One run differs from the analysis by more than this share of the analytical value
TOLERANCE = 0.10
# Replications differ only by more than their half-width and this share, so an
# interval of width 0, from constant times, does not flag the last unfinished task
FLOOR = 0.01


def compare(pair: Pair, values: Optional[List[float]], interval: Any) -> Comparison:
    """One comparison: within 10 percent for one run; for replications, within the
    interval's half-width or 1 percent, whichever is larger.

    Args:
        pair: The measure.
        values: What each run measured, in the analysis's units; None when a run
            has no reading for it.
        interval: `desflow.analysis.intervals.interval`.

    Returns:
        Comparison: The row; `differs` is None for a measure not compared or
            without a reading.
    """
    if values is None:
        no_reading = "no reading: nothing it measures happened in the run"
        return Comparison(
            node=pair.node,
            measure=pair.measure,
            analytical=pair.analytical,
            reason=pair.reason if pair.analytical is None else no_reading,
        )
    if len(values) == 1:
        measured, half_width = values[0], None
    else:
        result = interval(values)
        measured, half_width = result.mean, result.half_width
    analytical, differs = pair.analytical, None
    if analytical is not None:
        if half_width is not None:
            differs = abs(analytical - measured) > max(
                half_width, FLOOR * abs(analytical)
            )
        elif analytical == 0:
            differs = abs(measured) > 1e-9
        else:
            differs = abs(measured - analytical) / abs(analytical) > TOLERANCE
    return Comparison(
        node=pair.node,
        measure=pair.measure,
        analytical=analytical,
        measured=measured,
        half_width=half_width,
        differs=differs,
        reason=pair.reason,
    )

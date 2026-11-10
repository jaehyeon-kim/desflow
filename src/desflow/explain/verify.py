"""`--verify`: runs a model unpaced and compares its statistics with the analysis.

A comparison checks the implementation against the analysis where the analysis
applies (`compare.py`). It says nothing about whether the model is close enough to
the real system.
"""

import secrets
from typing import Any, Optional, Union

from desflow.explain.compare import compare
from desflow.explain.pairs import pairs
from desflow.explain.models import Explanation, Verification
from desflow.utils import checked_whole

NOTE = (
    "--verify checks the implementation against the queueing analysis where it "
    "applies; it does not show that the model is close enough to the real system"
)


def verify(
    model: Any,
    explanation: Explanation,
    until: Union[float, str, None] = None,
    seed: Optional[int] = None,
    replications: Optional[int] = None,
) -> Explanation:
    """Runs the model unpaced and adds the comparison to the explanation.

    Each run is a replication's run (`run_once`): no ingress, no egress, statistics
    at `typical`. It compares, per resource, `utilization_avg` / 100 with ρ and
    `queue_length_avg` with Lq; per task with one resource, `wait_mean` with Wq; per
    arrival, `flows.<arrival>.finished` over the time after the warm-up with λ.

    Args:
        model: A blueprint path, or a function that builds the context again.
        explanation: The analysis to compare with.
        until: The end of each run; the blueprint's `run.until` when None.
        seed: The run's seed, or the seed the replications' seeds come from; the
            model's `random_seed`, or one drawn, when None.
        replications: The number of runs, 2 or more; None for one run.

    Returns:
        Explanation: A copy of `explanation` with its `verification`.

    Raises:
        ValueError: If the model is a `LowLevelModel`, there is no end time, the end
            is not after the warm-up, or `replications` is not a whole number of 2 or
            more.
    """
    from desflow.analysis.intervals import interval
    from desflow.analysis.models import context_of, run_once, seconds, spawn_seeds
    from desflow.analysis.runner import run_replications
    from desflow.core.context import parts_of
    from desflow.core.statistics import published_value

    if replications is not None:
        checked_whole(replications, "replications", 2)
    if seed is not None:
        checked_whole(seed, "seed", 0)
    context = context_of(model)
    if context is None:
        raise ValueError(
            "verify needs a model it can build again; a LowLevelModel cannot be "
            "verified, so pass a blueprint path or a factory"
        )
    end = seconds(until)
    if end is None:
        end = seconds(parts_of(context).defaults.until)
    if end is None:
        raise ValueError(
            "verify needs an end time: pass until, or set run.until in the blueprint"
        )
    start = context.initial_time + (context.warmup or 0.0)
    if end <= start:
        raise ValueError(window_error(end, start, "verify"))
    if seed is None:
        seed = context.random_seed
    if seed is None:
        seed = secrets.randbelow(2**32)
    if replications is None:
        results = [run_once(model, seed=seed, until=end, stats="typical")]
    else:
        results = run_replications(
            model, spawn_seeds(seed, replications), until=end, stats="typical"
        )
    comparisons = []
    seen = {name for r in results for name in r.readings}
    for pair in pairs(explanation, end - start):
        found = [
            published_value(
                r.readings,
                f"{r.sim_id}.stats.{pair.statistic}",
                seen,
                prefix=f"{r.sim_id}.stats.",
            )
            for r in results
        ]
        numbers = [float(v) * pair.scale for v in found if v is not None]
        # A run in which nothing it measures happened has no reading
        complete = numbers if len(numbers) == len(found) else None
        comparisons.append(compare(pair, complete, interval))
    return explanation.model_copy(
        update={
            "verification": Verification(
                until=end,
                warmup=context.warmup,
                seed=seed,
                replications=replications,
                comparisons=comparisons,
                note=NOTE,
            )
        }
    )


def window_error(end: float, start: float, option: str) -> str:
    """The message for an end at or before the start of the measured time.

    Args:
        end: The run's end, in seconds.
        start: `initial_time` plus the warm-up, in seconds.
        option: What asked for the check: `verify` for the Python argument,
            `--verify` for the command's option.

    Returns:
        str: The message, naming both.
    """
    return (
        f"{option} measures after the warm-up, so until must be after "
        f"initial_time + warmup ({start:g}); got until {end:g}"
    )

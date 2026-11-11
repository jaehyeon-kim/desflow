"""A run that keeps its pace reads a lag near 0, whatever its factor or logical start."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "conformance"))
from harness import RecordingEgress  # noqa: E402

from desflow.builder.context import SimulationContext  # noqa: E402


@pytest.mark.paced
@pytest.mark.parametrize(
    "factor, back",
    [(2, 0), (1, 3600), (1, 0)],
    ids=["factor 2", "backdated one hour", "factor 1"],
)
def test_a_run_on_pace_reads_a_lag_near_zero(factor, back):
    start = datetime.now(timezone.utc) - timedelta(seconds=back)
    app = SimulationContext(
        "S", factor=factor, random_seed=1, lag_interval=0.5, logical_start_time=start
    )
    app.add_arrival("a", "constant", value=1, spawn="job", max_events=2)
    app.task(service_time=0, name="job")(lambda task_id: {})
    sink = RecordingEgress()
    app.add_egress(sink)
    app.run(until=2)
    lags = [
        r["value"]
        for r in sink.records
        if r.get("path_id") == "system.simulation.lag_seconds"
    ]
    assert lags, "no lag reading"
    assert max(lags) < 0.5, lags

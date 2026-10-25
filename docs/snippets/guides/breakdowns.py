"""The Model Breakdowns guide: a station's down periods, and a check of its counts.

python breakdowns.py prints the down periods and the counts of the breakdowns
example, run from the desflow repository root.
"""

import asyncio
import queue

from desflow import SimulationContext


class Capture:
    """An egress that keeps every record it receives."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        try:
            while True:
                try:
                    self.records.extend(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.005)
        except asyncio.CancelledError:
            pass


# --8<-- [start:check]
def availability(path: str) -> dict:
    """The down periods of the station, checked against its statistics."""
    app = SimulationContext.from_yaml(path)
    capture = Capture()
    app.add_egress(capture)
    app.run()
    periods, outcomes = [], {"interrupted": 0, "resumed": 0, "order_packed": 0}
    for record in capture.records:
        if record["stream_type"] != "event":
            continue
        value = record["value"]
        status = value.get("status", value.get("event_type"))
        if record["key"] == "station" and status == "down":
            periods.append([record["sim_ts"], None])
        elif record["key"] == "station" and status == "repaired":
            periods[-1][1] = record["sim_ts"]
        elif status in outcomes:
            outcomes[status] += 1
    end = app.env.now
    # A repair still going on at the end counts up to the end
    downtime = sum((repaired or end) - down for down, repaired in periods)
    stats = app.env.stats.readings()
    name = "Shop.stats.resources.station.{}".format
    assert stats[name("failures")] == len(periods)
    # Records round times to the millisecond
    assert abs(stats[name("downtime")] - downtime) < 0.01
    return {
        "periods": [(round(d, 1), r if r is None else round(r, 1)) for d, r in periods],
        "downtime": round(downtime, 1),
        "share_down": round(downtime / end, 3),
        **outcomes,
    }


# --8<-- [end:check]

if __name__ == "__main__":
    print(availability("examples/yaml/breakdowns.yaml"))

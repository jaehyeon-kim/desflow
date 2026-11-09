"""The Model Breakdowns guide, low-level API: a station that fails after 5 seconds of
work, not after 5 seconds of time.

`down_periods(clock)` returns the (time, status) of each record the station
publishes. One order arrives at 10 seconds and takes 8.
"""

import asyncio
import queue

from desflow import (
    BreakdownConfig,
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    run_breakdowns,
)


class Capture:
    """An egress that keeps every event it receives."""

    def __init__(self):
        self.events = []

    async def run(self, egress_queue):
        try:
            while True:
                try:
                    batch = egress_queue.get_nowait()
                    self.events += [r for r in batch if r["stream_type"] == "event"]
                except queue.Empty:
                    await asyncio.sleep(0.005)
        except asyncio.CancelledError:
            pass


def down_periods(clock: str = "busy") -> list:
    """(time, status) of the station's failures and repairs."""
    params = SimParameter(
        sim_id="Shop",
        resources={"station": CapacityConfig(current_cap=1)},
        # --8<-- [start:busy]
        breakdowns={
            "station": BreakdownConfig(
                time_to_failure=DistributionConfig(dist="constant", value=5),
                repair=DistributionConfig(dist="constant", value=2),
                work="finish",
                # Count the time to failure only while a unit is in use. The default,
                # "calendar", counts it from the start or the last repair.
                clock=clock,
            )
        },
        # --8<-- [end:busy]
    )
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(params)
    capture = Capture()
    env.setup_egress([capture])
    station = DynamicResource(env, "Shop", "station")

    def order():
        yield env.timeout(10)
        with station.request() as request:
            yield request
            yield env.timeout(8)

    # --8<-- [start:process]
    # A SimPy process that puts the station down at each failure and repairs it;
    # clock is passed to it as well as set in the config
    env.process(
        run_breakdowns(env, station, Sampler(seed=42), work="finish", clock=clock)
    )
    # --8<-- [end:process]
    env.process(order())
    try:
        env.run(until=30)
    finally:
        env.teardown()
    return [
        (e["sim_ts"], e["value"]["status"])
        for e in capture.events
        if e["key"] == "station"
    ]


if __name__ == "__main__":
    print("check: busy clock:", down_periods("busy"))
    print("check: calendar clock:", down_periods("calendar"))

"""The Model Breakdowns guide: a station's down periods, and a check of its counts.

python breakdowns.py prints the down periods and the counts of the breakdowns
example in each of its three forms, run from the desflow repository root.
"""

import asyncio
import importlib.util
import queue

from desflow import DynamicRealtimeEnvironment, Sampler, SimulationContext


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
def availability(records: list, end: float, stats: dict) -> dict:
    """The down periods of the station, checked against its statistics."""
    periods, outcomes = [], {"interrupted": 0, "resumed": 0, "order_packed": 0}
    for record in records:
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
    # A repair still going on at the end counts up to the end
    downtime = sum((repaired or end) - down for down, repaired in periods)
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


def _example(path: str):
    """Loads an example script as a module, without running it."""
    spec = importlib.util.spec_from_file_location("breakdowns_example", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --8<-- [start:yaml]
def yaml_form(path: str = "examples/yaml/breakdowns.yaml") -> dict:
    app = SimulationContext.from_yaml(path)
    capture = Capture()
    app.add_egress(capture)
    app.run()  # until 300, from the blueprint
    return availability(capture.records, app.env.now, app.env.stats.readings())


# --8<-- [end:yaml]


# --8<-- [start:builder]
def builder_form(path: str = "examples/builder/breakdowns_example.py") -> dict:
    app = _example(path).app
    capture = Capture()
    app.add_egress(capture)
    app.run(until=300)
    return availability(capture.records, app.env.now, app.env.stats.readings())


# --8<-- [end:builder]


# --8<-- [start:low-level]
def low_level_form(path: str = "examples/low-level/breakdowns_example.py") -> dict:
    example = _example(path)
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(example.PARAMS)
    capture = Capture()
    env.setup_egress([capture])
    env.setup_stats("Shop", "basic", interval=60.0)
    station = example.build(env)
    for process in example.processes(env, Sampler(seed=42), station):
        env.process(process)
    env.run(until=300)
    env.teardown()  # publishes the last statistics and flushes the capture
    return availability(capture.records, env.now, env.stats.readings())


# --8<-- [end:low-level]

if __name__ == "__main__":
    for form in (yaml_form, builder_form, low_level_form):
        print(form())

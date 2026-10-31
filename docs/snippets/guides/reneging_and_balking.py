"""Reneging and balking: ties between a grant and a patience, and an entity balance.

`tie(waiter_at, patience)` runs one packer held from 0 to 6 seconds and one waiting
order, and returns what the order did at 6; `tie_yaml` and `tie_low_level` do the
same with reneging_tie.yaml (docs/snippets/yaml/) and the low-level API. `balance`
checks that every order of the patience example that arrived is packed, reneged,
balked or still waiting, and `yaml_form`, `builder_form` and `low_level_form` run
the example in each form.

python reneging_and_balking.py prints each result on a line that starts with
"check: ". Run it from a folder that holds reneging_tie.yaml and the folder that
`desflow examples get patience` writes.
"""

# ruff: noqa: E402

# --8<-- [start:capture]
import asyncio
import importlib.util
import queue
import shutil
import tempfile
from collections import Counter
from pathlib import Path


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


# --8<-- [end:capture]

from desflow import (
    CapacityConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    SimParameter,
    SimulationContext,
)


# --8<-- [start:outcome]
def _outcome(records: list) -> str:
    """What the order did: reneged if any record of its pack task says so."""
    statuses = [
        r["value"].get("status")
        for r in records
        if r["stream_type"] == "event" and r["task"] == "pack"
    ]
    return "reneged" if "reneged" in statuses else "served"


# --8<-- [end:outcome]


# --8<-- [start:tie]
def tie(waiter_at: float, patience: float) -> str:
    """The outcome of an order whose patience ends when the packer comes free."""
    app = SimulationContext("Shop", factor=0)
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("hold", dist="constant", value=6)
    app.add_service("packing", dist="constant", value=1)
    capture = Capture()
    app.add_egress(capture)

    @app.task(service_id="hold", resource_id="packers")
    def holder(task_id):
        return {"event_type": "held"}

    @app.task(service_id="packing", resource_id="packers", patience=patience)
    def pack(task_id):
        return {"event_type": "order_packed"}

    def script(context):
        context.spawn(holder(0))  # takes the packer at 0 and keeps it until 6
        if waiter_at:
            yield context.env.timeout(waiter_at)
        context.spawn(pack(0))
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=8)
    return _outcome(capture.records)


# --8<-- [end:tie]


# --8<-- [start:tie-yaml]
def tie_yaml(
    waiter_at: float,
    patience: float,
    blueprint: str = "reneging_tie.yaml",
) -> str:
    """The same, with the blueprint and the two arrivals replayed from CSV files."""
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        shutil.copy(blueprint, folder / "reneging_tie.yaml")
        (folder / "holders.csv").write_text("time\n0\n")
        (folder / "orders.csv").write_text(f"time,patience\n{waiter_at},{patience}\n")
        app = SimulationContext.from_yaml(folder / "reneging_tie.yaml")
        capture = Capture()
        app.add_egress(capture)
        app.run()  # until 8, from the blueprint
    return _outcome(capture.records)


# --8<-- [end:tie-yaml]


# --8<-- [start:tie-low-level]
def tie_low_level(waiter_at: float, patience: float) -> str:
    """The same, with SimPy's request | timeout."""
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop",
            resources={"packers": CapacityConfig(current_cap=1, max_cap=1)},
        )
    )
    capture = Capture()
    env.setup_egress([capture])
    packers = DynamicResource(env, "Shop", "packers")

    def holder():
        with packers.request() as unit:
            yield unit
            yield env.timeout(6)  # keeps the packer until 6

    def pack():
        if waiter_at:  # at 0, its patience starts before the holder's service
            yield env.timeout(waiter_at)
        with packers.request() as unit:
            yield unit | env.timeout(patience)
            if not unit.triggered:
                env.publish_event("orders-1", {"status": "reneged"}, "pack", 0)
                return
            yield env.timeout(1)
            env.publish_event("orders-1", {"event_type": "order_packed"}, "pack", 0)

    env.process(holder())
    env.process(pack())
    env.run(until=8)
    env.teardown()
    return _outcome(capture.records)


# --8<-- [end:tie-low-level]


# --8<-- [start:balance]
def balance(records: list, stats: dict) -> Counter:
    """Every order that arrived, by what became of it."""
    last = {}
    for record in records:
        if record["stream_type"] == "event":
            value = record["value"]
            last[record["key"]] = value.get("status", value.get("event_type"))
    outcome = Counter(last.values())
    # queued or started: still in the shop when the run ended
    name = "Shop.stats.tasks.pack.{}".format
    assert outcome["order_packed"] == stats[name("finished")]
    assert outcome["reneged"] == stats.get(name("reneged"), 0)
    assert outcome["balked"] == stats.get(name("balked"), 0)
    return outcome


# --8<-- [end:balance]


# --8<-- [start:loader]
def _example(path: str):
    """Loads an example script as a module, without running it."""
    spec = importlib.util.spec_from_file_location("patience_example", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --8<-- [end:loader]


# --8<-- [start:yaml]
def yaml_form(path: str = "patience/yaml/patience.yaml") -> Counter:
    app = SimulationContext.from_yaml(path)
    capture = Capture()
    app.add_egress(capture)
    app.run()  # until 120, from the blueprint
    return balance(capture.records, app.env.stats.readings())


# --8<-- [end:yaml]


# --8<-- [start:builder]
def builder_form(path: str = "patience/builder/patience_example.py") -> Counter:
    app = _example(path).app
    capture = Capture()
    app.add_egress(capture)
    app.run(until=120)
    return balance(capture.records, app.env.stats.readings())


# --8<-- [end:builder]


# --8<-- [start:low-level]
def low_level_form(path: str = "patience/low-level/patience_example.py") -> Counter:
    example = _example(path)
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(example.PARAMS)
    capture = Capture()
    env.setup_egress([capture])
    env.setup_stats("Shop", "basic", interval=30.0)
    packers = example.build(env)
    for process in example.processes(env, Sampler(seed=42), packers):
        env.process(process)
    env.run(until=120)
    env.teardown()  # publishes the last statistics and flushes the capture
    return balance(capture.records, env.stats.readings())


# --8<-- [end:low-level]

# --8<-- [start:main]
if __name__ == "__main__":
    print("check: arrives at 3, patience 3:", tie(3, 3))
    print("check: arrives at 0, patience 6:", tie(0, 6))
    print("check: the same in YAML:", tie_yaml(3, 3), tie_yaml(0, 6))
    print("check: the same, low-level:", tie_low_level(3, 3), tie_low_level(0, 6))
    for form in (yaml_form, builder_form, low_level_form):
        print("check:", dict(form()))
# --8<-- [end:main]

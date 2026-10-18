"""The Performance Tuning guide: records per second of a model, without its sinks.

python performance_tuning.py prints the records a one-hour run of the shop below
published, and how many a second.
"""

import asyncio
import gc
import queue
import time

from desflow import SimulationContext


class Count:
    """An egress that counts the records it receives and keeps none."""

    def __init__(self):
        self.records = 0

    async def run(self, egress_queue):
        try:
            while True:
                try:
                    self.records += len(egress_queue.get_nowait())
                except queue.Empty:
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass


# --8<-- [start:shop]
def shop() -> SimulationContext:
    """Ten orders a second and 25 packers, with no sink of its own."""
    app = (
        # factor 0: no wall clock to wait for
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=25, max_cap=25)
        .add_service("packing", dist="normal", mean=2.0, std=0.4)
        .add_arrival("orders", dist="exponential", rate=10.0)
        # minimal: readings every 10 s, not the costlier task statistics
        .with_stats("minimal", interval=10)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_loop(context: SimulationContext):
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(context.next_instance("pack_order"), entity=order))

    return app


# --8<-- [end:shop]


# --8<-- [start:measure]
def throughput(app: SimulationContext, until: float) -> dict:
    """Runs a model with a counting sink: the records and records per second."""
    count = Count()
    app.add_egress(count)
    gc.collect()
    gc.freeze()  # leave the objects built so far out of garbage collection
    started = time.perf_counter()
    app.run(until=until)
    seconds = time.perf_counter() - started
    gc.unfreeze()
    return {"records": count.records, "per_second": round(count.records / seconds)}


# --8<-- [end:measure]

if __name__ == "__main__":
    print(throughput(shop(), until=3600))

"""The hand-calculation checks of the Checking and Validating page.

tests/docs runs this file. Each run is unpaced and covers one simulated hour, so the file
finishes in under a second.
"""

import sys
from pathlib import Path

from desflow import SimulationContext

sys.path.insert(0, str(Path(__file__).parent))
from queueing_basics import little  # noqa: E402


# --8<-- [start:fixed]
def run_fixed(gap: float, service: float, until: float, packers: int = 1) -> dict:
    """The shop with constant gaps and constant packing times, so every number can be worked out by hand."""
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    app.add_arrival("orders", dist="constant", value=gap)
    app.add_service("packing", dist="constant", value=service)
    app.add_resource("packers", current_cap=packers, max_cap=packers)
    app.with_stats("typical", interval=3600)

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    app.run(until=until)
    return app.env.stats.readings()


# --8<-- [end:fixed]

# --8<-- [start:checks]
# One order every 3 s and 1.5 s to pack: orders arrive at 3, 6, ... 3597 s, so 1,199 of
# them, and none waits. The packer works 1,199 x 1.5 s of the 3,600 s: 49.96%.
easy = run_fixed(gap=3, service=1.5, until=3600)
assert easy["Shop.stats.flows.orders.started"] == 1199
assert easy["Shop.stats.tasks.pack_order.wait_max"] == 0
assert (
    abs(easy["Shop.stats.resources.packers.utilization_avg"] - 1199 * 1.5 / 3600 * 100)
    < 0.01
)

# One order every second and 1.5 s to pack: the packer is never idle. The 3,599 orders
# that arrive leave 2,399 finished, so 1,200 are still in the shop.
overloaded = run_fixed(gap=1, service=1.5, until=3600)
assert overloaded["Shop.stats.flows.orders.started"] == 3599
assert overloaded["Shop.stats.tasks.pack_order.finished"] == 2399
assert overloaded["Shop.stats.flows.orders.in_system"] == 3599 - 2399
assert overloaded["Shop.stats.resources.packers.utilization_avg"] > 99.9
# --8<-- [end:checks]

# --8<-- [start:extremes]
# No order arrives within the hour: none starts and none finishes.
no_arrivals = run_fixed(gap=10_000, service=1.5, until=3600)
assert no_arrivals.get("Shop.stats.flows.orders.started", 0) == 0
assert no_arrivals.get("Shop.stats.tasks.pack_order.finished", 0) == 0

# A hundred packers for an order a second: a packer is always free, so no order waits.
many_packers = run_fixed(gap=1, service=1.5, until=3600, packers=100)
assert many_packers["Shop.stats.flows.orders.started"] == 3599
assert many_packers["Shop.stats.tasks.pack_order.wait_max"] == 0
# --8<-- [end:extremes]
print(
    "easy: wait_max 0, utilisation",
    round(easy["Shop.stats.resources.packers.utilization_avg"], 2),
)
print("overloaded: in system", overloaded["Shop.stats.flows.orders.in_system"])

# --8<-- [start:little]
# Little's law on the busy hour of Queueing Basics: queue length = arrival rate x mean wait
rate, (queue, wait), _ = little()
print(
    f"queue {queue:.3f}, arrival rate {rate:.3f} x wait {wait:.4f} = {rate * wait:.3f}"
)
# --8<-- [end:little]
assert abs(queue - 3.417) < 0.0005 and abs(rate - 1.782) < 0.0005
assert abs(wait - 1.9175) < 0.00005 and abs(queue - rate * wait) < 0.01 * queue

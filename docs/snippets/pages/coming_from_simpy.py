"""Coming from SimPy: a plain SimPy shop in the usual four-class structure, and the same shop in desflow.

One packer, orders every 2 seconds on average (exponential gaps) and 1.5 seconds of packing.
Both versions run 10 times for one simulated hour from an empty shop, and each reports the mean wait
of each run. The check asserts that the two mean waits differ by less than the sum of their 95% half-widths,
and the values the page shows: 2.30 ± 0.20 s for plain SimPy and 2.45 ± 0.27 s for desflow.
"""

import random
import statistics

import simpy

from desflow import SimulationContext, replicate


# --8<-- [start:globals]
class G:
    """Global parameters: read as G.packers, never instantiated."""

    arrival_mean = 2.0  # seconds between orders
    pack_time = 1.5  # seconds to pack an order
    packers = 1
    run_length = 3600  # seconds
    runs = 10


# --8<-- [end:globals]


# --8<-- [start:entity]
class Order:
    """The entity: what travels through the model, and what it remembers."""

    def __init__(self, order_id):
        self.id = order_id
        self.wait = 0.0


# --8<-- [end:entity]


# --8<-- [start:model]
class Model:
    """One run of the shop."""

    def __init__(self, run_number):
        self.env = simpy.Environment()
        self.random = random.Random(run_number)
        self.packers = simpy.Resource(self.env, capacity=G.packers)
        self.waits = []

    def generate_orders(self):
        order_id = 0
        while True:
            order_id += 1
            self.env.process(self.pack_order(Order(order_id)))
            yield self.env.timeout(self.random.expovariate(1 / G.arrival_mean))

    def pack_order(self, order):
        arrived = self.env.now
        with self.packers.request() as request:
            yield request
            order.wait = self.env.now - arrived
            self.waits.append(order.wait)
            yield self.env.timeout(G.pack_time)

    def run(self):
        self.env.process(self.generate_orders())
        self.env.run(until=G.run_length)
        return statistics.mean(self.waits)


# --8<-- [end:model]


# --8<-- [start:trial]
class Trial:
    """The runs of one design: one result for each."""

    def run_trial(self):
        return [Model(run).run() for run in range(G.runs)]


# --8<-- [end:trial]


# --8<-- [start:desflow]
def shop() -> SimulationContext:
    """The same shop: parameters in the registry, the entity and the statistics built in."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=1 / 2.0)
    app.add_service("packing", dist="constant", value=1.5)
    app.add_resource("packers", current_cap=1, max_cap=10)
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

    return app


def trial():
    """The trial loop is `replicate`: one seed for each run, and an interval over the runs."""
    result = replicate(
        shop, 10, seed=42, until=3600, measures=["tasks.pack_order.wait_mean"]
    )
    return result.summary[0]


# --8<-- [end:desflow]


if __name__ == "__main__":
    waits = Trial().run_trial()
    mean = statistics.mean(waits)
    spread = (
        statistics.stdev(waits) * 2.262 / len(waits) ** 0.5
    )  # 95% half-width, 9 degrees of freedom
    print(f"plain SimPy: mean wait {mean:.2f} ± {spread:.2f} s over {len(waits)} runs")
    row = trial()
    print(
        f"desflow:     mean wait {row['mean']:.2f} ± {row['half_width']:.2f} s over {row['n']} runs"
    )
    assert abs(mean - row["mean"]) <= spread + row["half_width"]
    assert abs(mean - 2.30) < 0.005 and abs(spread - 0.20) < 0.005
    assert abs(row["mean"] - 2.45) < 0.005 and abs(row["half_width"] - 0.27) < 0.005

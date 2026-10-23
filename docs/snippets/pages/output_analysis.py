"""The runnable check of the Output Analysis page.

The busy hour with three packers, packing in 1.5 s and in 1.45 s, 20 replications each,
run with the same seeds. The same 40 runs give two intervals for the difference of the mean waits:
one that pairs the runs by seed, and one that treats the two designs as independent. The
`__main__` block prints both and asserts that the paired one is less than half as wide, and
the difference 0.69 s and the half-widths 0.12 s and 0.29 s that the page quotes. Every run is unpaced, and the file takes a few seconds.
"""

import math
import statistics

from desflow import SimulationContext, replicate
from desflow.analysis.intervals import interval, t_quantile

WAIT = "tasks.pack_order.wait_mean"


def shop(service: float) -> SimulationContext:
    """The busy hour: an order every 1/1.8 s on average, `service` s to pack, 3 packers."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", dist="exponential", rate=1.8)
    app.add_service("packing", dist="constant", value=service)
    app.add_resource("packers", current_cap=3, max_cap=10)
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


# --8<-- [start:paired]
def waits(service: float, n: int = 20) -> list[float]:
    """The mean wait of each replication; the same seed gives the same seeds to every design."""
    runs = replicate(lambda: shop(service), n, seed=42, until=3600, measures=[WAIT])
    return runs.values[WAIT]


def paired_and_unpaired(slow: list[float], fast: list[float]):
    """Half-widths of the difference of the means: paired by seed, and as if independent."""
    n = len(slow)
    paired = interval([a - b for a, b in zip(slow, fast)])
    # Welch's interval: the two designs' variances add, and the degrees of freedom shrink
    va, vb = statistics.variance(slow) / n, statistics.variance(fast) / n
    df = (va + vb) ** 2 / (va**2 / (n - 1) + vb**2 / (n - 1))
    unpaired = t_quantile(0.975, df) * math.sqrt(va + vb)
    return paired, unpaired


# --8<-- [end:paired]


if __name__ == "__main__":
    slow, fast = waits(1.5), waits(1.45)
    paired, unpaired = paired_and_unpaired(slow, fast)
    print(f"difference {paired.mean:.2f} s")
    print(f"paired by seed: ± {paired.half_width:.2f} s")
    print(f"as if independent: ± {unpaired:.2f} s")
    assert paired.half_width < unpaired / 2
    assert abs(paired.mean - 0.69) < 0.005
    assert abs(paired.half_width - 0.12) < 0.005
    assert abs(unpaired - 0.29) < 0.005

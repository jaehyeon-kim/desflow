"""The runnable checks of the Common Mistakes page.

Each check runs the shop and prints the number the page quotes. Every number the page
states is asserted here, so a wrong number fails this test, except the 2.25 s quoted from
Trusting Results, which `tests/docs/test_docs_tutorials.py` asserts. The shop packs orders with
`packers` packers; every run is unpaced and seeded, and the whole file takes a few seconds.
"""

import random

from desflow import SimulationContext, replicate

WAIT = "tasks.pack_order.wait_mean"


# --8<-- [start:shop]
def shop(arrival: dict, service: dict, packers: int) -> SimulationContext:
    """The shop with the given arrival gaps and packing times, and `packers` packers."""
    app = SimulationContext(sim_id="Shop", factor=0)
    app.add_arrival("orders", **arrival)
    app.add_service("packing", **service)
    app.add_resource("packers", current_cap=packers, max_cap=10)
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


def exponential(mean: float) -> dict:
    return {"dist": "exponential", "rate": 1 / mean}


def constant(value: float) -> dict:
    return {"dist": "constant", "value": value}


def waits(arrival, service, packers, n, seed, until, warmup=None):
    """The mean wait of each of `n` replications: one value per run."""
    result = replicate(
        lambda: shop(arrival, service, packers),
        n,
        seed=seed,
        until=until,
        warmup=warmup,
        measures=[WAIT],
    )
    return result.values[WAIT]


def mean(values):
    return sum(values) / len(values)


# --8<-- [end:shop]


# --8<-- [start:means]
def means():
    """One packer, an order every 2.0 s and 1.5 s of packing: all constant, then exponential gaps."""
    fixed = waits(constant(2.0), constant(1.5), 1, n=10, seed=42, until=3600)
    random_gaps = waits(exponential(2.0), constant(1.5), 1, n=10, seed=42, until=3600)
    return mean(fixed), mean(random_gaps)


# --8<-- [end:means]


# --8<-- [start:normal]
def negative_share(mean_time=1.5, sd=1.0, draws=100_000):
    """The share of packing times a normal distribution draws below zero."""
    rng = random.Random(42)
    return sum(rng.gauss(mean_time, sd) < 0 for _ in range(draws)) / draws


# --8<-- [end:normal]


# --8<-- [start:one_run]
def spread_of_runs():
    """The busy hour, 3 packers: the mean wait of each of 10 runs, shortest and longest."""
    runs = waits(exponential(1 / 1.8), constant(1.5), 3, n=10, seed=42, until=3600)
    return min(runs), max(runs), mean(runs)


# --8<-- [end:one_run]


# --8<-- [start:warmup]
def empty_start(until=1800, warmup=None):
    """One packer at 95% utilisation: 40 runs, and the formula's long-run wait (M/D/1)."""
    runs = replicate(
        lambda: shop(exponential(1.5 / 0.95), constant(1.5), 1),
        40,
        seed=42,
        until=until,
        warmup=warmup,
        measures=[WAIT],
    )
    row = runs.summary[0]
    long_run = 0.95 / (1 - 0.95) * 1.5 / 2  # half the M/M/1 wait
    return row["mean"], row["half_width"], long_run


# --8<-- [end:warmup]


# --8<-- [start:pairs]
def one_run_each():
    """Packing 1.45 s against 1.5 s: how often a single run of each says the faster design is not faster."""
    arrival = exponential(1 / 1.8)
    slow = waits(arrival, constant(1.5), 3, n=20, seed=1, until=3600)
    fast = waits(arrival, constant(1.45), 3, n=20, seed=2, until=3600)
    wrong = sum(s <= f for s, f in zip(slow, fast))
    return wrong, len(slow), mean(slow), mean(fast)


# --8<-- [end:pairs]


if __name__ == "__main__":
    fixed, random_gaps = means()
    print(
        f"constant gaps and times: mean wait {fixed:.2f} s; exponential gaps: {random_gaps:.2f} s"
    )
    assert fixed == 0 and abs(random_gaps - 2.45) < 0.005

    share = negative_share()
    print(f"normal(1.5, 1.0) draws a negative packing time {share:.1%} of the time")
    assert abs(share - 0.067) < 0.0005

    low, high, average = spread_of_runs()
    print(
        f"10 runs of the busy hour: {low:.2f} to {high:.2f} s, mean {average:.2f} s;"
        f" {1 - low / average:.0%} below and {high / average - 1:.0%} above the mean"
    )
    assert abs(low - 1.52) < 0.005 and abs(high - 2.75) < 0.005
    assert abs(average - 2.11) < 0.005
    assert round(1 - low / average, 2) == 0.28 and round(high / average - 1, 2) == 0.30

    assert round(1.5 / 0.95, 2) == 1.58  # the gap that keeps one packer 95% busy
    value, half_width, long_run = empty_start()
    print(
        f"30 minutes from empty: {value:.1f} ± {half_width:.1f} s; long run {long_run:.2f} s"
    )
    assert abs(value - 11.4) < 0.05 and abs(half_width - 2.1) < 0.05
    assert abs(long_run - 14.25) < 1e-9
    assert value + half_width < long_run

    # --8<-- [start:warmup-fix]
    value, half_width, long_run = empty_start(until=20_000, warmup=4000)
    print(
        f"20,000 s runs leaving out the first 4,000 s: {value:.1f} ± {half_width:.1f} s"
    )
    # --8<-- [end:warmup-fix]
    assert abs(value - 14.2) < 0.05 and abs(half_width - 1.3) < 0.05
    assert abs(value - long_run) <= half_width

    wrong, pairs, slow, fast = one_run_each()
    print(
        f"single runs picked the slower design in {wrong} of {pairs} pairs; means {slow:.2f} and {fast:.2f} s"
    )
    assert (wrong, pairs) == (4, 20)
    assert abs(slow - 1.99) < 0.005 and abs(fast - 1.51) < 0.005

"""The runnable checks of the Queueing Basics page.

Each check runs the shop and compares a reading with the formula the page states. Every
number the page states is asserted here: a measured one against the value printed on the
page, a formula one against its closed form, so a wrong number on the page fails this test.
The shop packs orders with `packers` packers; a service time averages 1.5 seconds. Every run
is unpaced, and the whole file takes a few seconds.
"""

import math
from pathlib import Path

from desflow import SimulationContext, replicate

SERVICE = 1.5  # mean seconds to pack one order


# --8<-- [start:shop]
def shop(arrival: dict, service: dict, packers: int) -> SimulationContext:
    """The shop with the given arrival gaps and packing times, and `packers` packers."""
    app = SimulationContext(sim_id="Shop", factor=0, random_seed=42)
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


# Exponential gaps and times are the M of Kendall's notation, constant ones the D
def exponential(mean: float) -> dict:
    return {"dist": "exponential", "rate": 1 / mean}


def constant(mean: float) -> dict:
    return {"dist": "constant", "value": mean}


# --8<-- [end:shop]


# --8<-- [start:little]
UNTIL = 20_000  # seconds, long enough that the start and end of the run hardly count


def little():
    """One run of the busy hour with 3 packers; returns L, lambda and W for the queue and the shop."""
    app = shop(exponential(1 / 1.8), constant(SERVICE), packers=3)
    app.run(until=UNTIL)
    r = app.env.stats.readings()
    arrival_rate = r["Shop.stats.flows.orders.started"] / UNTIL
    in_queue = r["Shop.stats.resources.packers.queue_length_avg"]
    in_service = 3 * r["Shop.stats.resources.packers.utilization_avg"] / 100
    wait = r["Shop.stats.tasks.pack_order.wait_mean"]
    service = r["Shop.stats.tasks.pack_order.service_mean"]
    return arrival_rate, (in_queue, wait), (in_queue + in_service, wait + service)


# --8<-- [end:little]


# --8<-- [start:utilisation]
def utilisation():
    """One run of the busy hour; returns utilisation from the formula and as measured."""
    app = shop(exponential(1 / 1.8), constant(SERVICE), packers=3)
    app.run(until=UNTIL)
    r = app.env.stats.readings()
    rate = r["Shop.stats.flows.orders.started"] / UNTIL
    return rate * SERVICE / 3, r["Shop.stats.resources.packers.utilization_avg"] / 100


# --8<-- [end:utilisation]


# --8<-- [start:theory]
def mm1_wait(utilisation: float) -> float:
    """Mean wait in the queue of an M/M/1 queue: utilisation / (1 - utilisation) x service."""
    return utilisation / (1 - utilisation) * SERVICE


def erlang_c(units: int, load: float) -> float:
    """The share of arrivals that find all `units` busy, for a load in packers' worth of work."""
    last = load**units / math.factorial(units) / (1 - load / units)
    return last / (sum(load**k / math.factorial(k) for k in range(units)) + last)


def mmc_wait(units: int, arrival_rate: float) -> float:
    """Mean wait in the queue of an M/M/c queue: Erlang C divided by (c / service - lambda)."""
    return erlang_c(units, arrival_rate * SERVICE) / (units / SERVICE - arrival_rate)


def kingman_wait(utilisation: float, arrival_cv: float, service_cv: float) -> float:
    """Kingman's formula for the mean wait in a one-server queue."""
    variability = (arrival_cv**2 + service_cv**2) / 2
    return utilisation / (1 - utilisation) * variability * SERVICE


# --8<-- [end:theory]


# --8<-- [start:measure]
def measured_wait(arrival: dict, service: dict, packers: int, until: int):
    """Mean wait over 10 runs, with its 95% half-width; each run leaves out its first 2,000 s."""
    result = replicate(
        lambda: shop(arrival, service, packers),
        10,
        seed=42,
        until=until,
        warmup=2000,
        measures=["tasks.pack_order.wait_mean"],
    )
    row = result.summary[0]
    return row["mean"], row["half_width"]


# --8<-- [end:measure]


def check(
    label: str,
    theory: float,
    measured: tuple[float, float],
    shown: tuple[float, float, float],
) -> None:
    """Print a check, assert the formula lies in the interval and that the page's figures are right: the formula value, the mean and the half-width."""
    mean, half_width = measured
    print(
        f"{label}: formula {theory:.2f} s, mean of 10 runs {mean:.2f} ± {half_width:.2f} s"
    )
    assert abs(mean - theory) <= half_width, label
    assert abs(theory - shown[0]) < 0.006, label
    assert abs(mean - shown[1]) < 0.006 and abs(half_width - shown[2]) < 0.006, label


if __name__ == "__main__":
    rate, queue, system = little()
    print(f"arrival rate {rate:.3f} orders per second")
    assert abs(rate - 1.782) < 0.0005
    for name, (count, time), shown in (
        ("queue", queue, (3.417, 3.417)),
        ("shop", system, (6.089, 6.090)),
    ):
        print(f"{name}: L {count:.3f}, lambda x W {rate * time:.3f}")
        assert abs(count - rate * time) < 0.01 * count, name
        assert abs(count - shown[0]) < 0.0006 and abs(rate * time - shown[1]) < 0.0006

    # --8<-- [start:utilisation-check]
    formula, measured = utilisation()
    print(f"utilisation {formula:.3f} from the formula, {measured:.3f} measured")
    assert abs(formula - 0.891) < 0.0005 and abs(measured - formula) < 0.005
    # --8<-- [end:utilisation-check]

    # --8<-- [start:formulas]
    for rho, exponential_wait in ((0.5, 1.5), (0.75, 4.5), (0.9, 13.5), (0.95, 28.5)):
        wait = mm1_wait(rho)
        print(
            f"utilisation {rho}: mean wait M/M/1 {wait:5.2f} s, M/D/1 {wait / 2:5.2f} s"
        )
        assert math.isclose(wait, exponential_wait)
        assert math.isclose(kingman_wait(rho, 1, 0), exponential_wait / 2)
    assert mm1_wait(0.95) > 2 * mm1_wait(
        0.9
    )  # from 0.9 to 0.95 the wait more than doubles
    one_packer = mm1_wait(0.8)
    print(f"one packer at utilisation 0.8: mean wait {one_packer:.1f} s")
    assert math.isclose(one_packer, 6.0)
    three_packers = mmc_wait(
        3, 1.6
    )  # three packers at the same utilisation, 1.6 / (3 / 1.5)
    print(f"three packers at utilisation 0.8: mean wait {three_packers:.2f} s")
    assert abs(three_packers - 1.62) < 0.005
    # --8<-- [end:formulas]

    # --8<-- [start:checks]
    for rho, figures in ((0.5, (1.50, 1.47, 0.06)), (0.75, (4.50, 4.46, 0.33))):
        arrival = exponential(SERVICE / rho)
        check(
            f"M/M/1 at utilisation {rho}",
            mm1_wait(rho),
            measured_wait(arrival, exponential(SERVICE), 1, 20_000),
            figures,
        )
    check(
        "M/M/3 at utilisation 0.8",
        mmc_wait(3, 1.6),
        measured_wait(exponential(1 / 1.6), exponential(SERVICE), 3, 30_000),
        (1.62, 1.64, 0.06),
    )
    arrival = exponential(SERVICE / 0.75)
    check(
        "M/D/1 at utilisation 0.75, Kingman",
        kingman_wait(0.75, 1, 0),
        measured_wait(arrival, constant(SERVICE), 1, 20_000),
        (2.25, 2.23, 0.14),
    )
    # --8<-- [end:checks]

    # --8<-- [start:kendall]
    # The busy hour is M/D/3: exponential gaps (M), constant service (D), three packers.
    # The runs are compared with half the M/M/3 wait at this load.
    check(
        "M/D/3 at utilisation 0.9, half the M/M/3 wait",
        mmc_wait(3, 1.8) / 2,
        measured_wait(exponential(1 / 1.8), constant(SERVICE), 3, 20_000),
        (2.04, 2.05, 0.13),
    )
    # --8<-- [end:kendall]

    # --8<-- [start:dm1]
    mean, half_width = measured_wait(
        constant(SERVICE / 0.75), exponential(SERVICE), 1, 20_000
    )
    kingman = kingman_wait(0.75, 0, 1)
    print(
        f"D/M/1 at utilisation 0.75: Kingman {kingman:.2f} s, mean of 10 runs {mean:.2f} ± {half_width:.2f} s"
    )
    # --8<-- [end:dm1]
    assert abs(kingman - 2.25) < 0.005
    assert abs(mean - 1.78) < 0.006 and abs(half_width - 0.09) < 0.006
    assert abs(mean - kingman) > half_width  # the approximation is outside the interval

    # --8<-- [start:flow]
    # Orders arrive at 1.6 a second, and a tenth of the checked orders go back to packing,
    # so the rate into packing solves: rate = 1.6 + 0.1 x rate.
    flow_rate = 1.6 / (1 - 0.1)
    packers = flow_rate * 1.5 / 4  # four packers, 1.5 seconds an order
    checker = flow_rate * 0.4  # one checker, 0.4 seconds an order on average
    print(f"rate into packing and into checking: {flow_rate:.3f} orders a second")
    print(f"utilisation: packers {packers:.3f}, checker {checker:.3f}")
    # desflow explain works out the same figures from the blueprint
    network = Path(__file__).parents[1] / "yaml" / "queues_network.yaml"
    explanation = SimulationContext.from_yaml(str(network)).analyze()
    print(f"desflow explain names the bottleneck: {explanation.bottleneck}")
    # --8<-- [end:flow]
    by_name = {r.name: r for r in explanation.resources}
    assert abs(flow_rate - 1.778) < 0.0005
    assert abs(packers - 0.667) < 0.0005 and abs(checker - 0.711) < 0.0005
    assert math.isclose(by_name["packers"].utilization, packers)
    assert math.isclose(by_name["checkers"].utilization, checker)
    assert explanation.bottleneck == "checkers"

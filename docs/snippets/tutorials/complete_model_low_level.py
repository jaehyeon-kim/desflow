"""The Complete Model tutorial, low-level: which shop design should be recommended?

Fits the gaps between orders and the picking and packing times of 300 observed
orders, builds the shop's busy hour from the fits, checks it against the queueing
formulas worked by hand, compares three warm-ups, and runs the experiment that
compares the designs, checks them and writes the report.
"""

import csv
import itertools
import logging
from pathlib import Path

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    LowLevelModel,
    Sampler,
    SimParameter,
    fit_column,
    replicate,
    run_experiment,
)

# Replications and the experiment log only their warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

HERE = Path(__file__).resolve().parent
OBSERVED = HERE / "complete_model_observed.csv"


def fitted() -> dict:
    """Each input fitted to its column of the observed orders."""
    # --8<-- [start:fit]
    with OBSERVED.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    columns = {
        "orders": "gap_seconds",
        "picking": "picking_seconds",
        "packing": "packing_seconds",
    }
    # Every column is in seconds, and no time can be below 0
    return {
        name: fit_column([float(row[column]) for row in rows], unit="s", lower=0)
        for name, column in columns.items()
    }
    # --8<-- [end:fit]


# The best fit of each input: dist and its parameters
FITS = {name: result.settings() for name, result in fitted().items()}

# --8<-- [start:inputs]
PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        # The fitted gaps between orders
        "orders": DistributionConfig(**FITS["orders"]),
        # A delivery of 1,000 boxes every 10 minutes
        "deliveries": DistributionConfig(dist="constant", value=600),
    },
    # The fitted picking and packing times
    service={
        "picking": DistributionConfig(**FITS["picking"]),
        "packing": DistributionConfig(**FITS["packing"]),
    },
    # One picker and three packers, as the shop has today
    resources={
        "pickers": CapacityConfig(current_cap=1, max_cap=4),
        "packers": CapacityConfig(current_cap=3, max_cap=10),
    },
    # Shipping boxes: 1,000 on the shelf at the start, and room for 2,000
    containers={"stock": ContainerConfig(current_cap=2000, max_cap=2000, level=1000)},
)
# --8<-- [end:inputs]

# Records and statistics start after the first 2 minutes
WARMUP = 120.0

# --8<-- [start:flow]
# Each task's service, resource and the task after it; None where it has none
TASKS = {
    "pick_order": ("picking", "pickers", "pack_order"),
    "pack_order": ("packing", "packers", "ship_order"),
    "ship_order": (None, None, None),
}
EVENTS = {
    "pick_order": "order_picked",
    "pack_order": "order_packed",
    "ship_order": "order_shipped",
}


def task(env, sampler, shop, name, key, task_id, counters):
    """Runs one task of an order, then starts the task after it."""
    service, resource, following = TASKS[name]
    status = {"path_id": f"Shop.service.{service}"} if service else {}

    def publish(value):
        env.publish_event(key, value, task=name, instance=task_id)

    if name == "pick_order":
        env.stats.flow_started("orders")  # the order enters the shop
    publish({**status, "status": "queued"})
    env.stats.task_queued(name, task_id)
    if resource is not None:
        with shop[resource].request() as request:
            yield request
            publish({**status, "status": "started"})
            env.stats.task_started(name, task_id)
            yield env.timeout(
                sampler.sample(env.registry.get_config(status["path_id"]))
            )
            publish({"event_type": EVENTS[name]})
            env.stats.task_finished(name, task_id)
    else:
        # Each parcel leaves in a box from the shelf; waits while the shelf is empty
        yield shop["stock"].get(1)
        publish({"status": "started"})
        env.stats.task_started(name, task_id)
        publish({"event_type": EVENTS[name]})
        env.stats.task_finished(name, task_id)
    if following is None:
        env.stats.flow_finished("orders")  # the order leaves the shop
        return
    instance = next(counters[following])
    env.process(task(env, sampler, shop, following, key, instance, counters))


def restock(env, shop, key, task_id):
    """One delivery: 1,000 boxes onto the shelf, waiting while there is no room."""
    env.stats.flow_started("deliveries")
    yield shop["stock"].put(1000)
    env.publish_event(
        key, {"event_type": "stock_delivered"}, task="restock", instance=task_id
    )
    env.stats.task_finished("restock", task_id)
    env.stats.flow_finished("deliveries")


# --8<-- [end:flow]


def order_generator(env, sampler, shop, counters):
    """Starts picking for every order, keyed orders-1, orders-2 and so on."""
    gap = env.registry.get_config("Shop.arrival.orders")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        instance = next(counters["pick_order"])
        env.process(
            task(
                env, sampler, shop, "pick_order", f"orders-{number}", instance, counters
            )
        )


def delivery_generator(env, sampler, shop):
    gap = env.registry.get_config("Shop.arrival.deliveries")
    for number in itertools.count(1):
        yield env.timeout(sampler.sample(gap))
        env.process(restock(env, shop, f"deliveries-{number}", number - 1))


def model(env, sampler):
    """The busy hour in a given environment: the runner sets the statistics."""
    env.registry.register_sim_parameter(PARAMS)
    shop = {
        "pickers": DynamicResource(env, "Shop", "pickers"),
        "packers": DynamicResource(env, "Shop", "packers"),
        "stock": DynamicContainer(env, "Shop", "stock"),
    }
    # One counter per task, shared by the arrivals and the next task
    counters = {name: itertools.count() for name in TASKS}
    env.process(order_generator(env, sampler, shop, counters))
    env.process(delivery_generator(env, sampler, shop))


def run(egress=(), until=3600):
    """One busy hour from the seed 42, its records sent to `egress`."""
    env = DynamicRealtimeEnvironment(factor=0.0)
    # Waits, utilisation and the stock's level, published at the end
    env.setup_stats("Shop", "typical", interval=3600)
    env.setup_egress(list(egress), warmup=WARMUP)
    model(env, Sampler(seed=42))
    try:
        env.run(until=until)
    finally:
        env.teardown()
    return env


SHOP = LowLevelModel(model, sim_id="Shop")
WAITS = ["tasks.pick_order.wait_mean", "tasks.pack_order.wait_mean"]
DESIGNS = ["current", "second-picker", "fourth-packer"]


def by_hand() -> dict:
    """The picker's utilisation and mean wait from the formulas, and the packers'
    utilisation; no formula gives the wait of three packers with gamma times."""
    # --8<-- [start:by-hand]
    rate = FITS["orders"]["rate"]  # orders a second
    picking, packing = FITS["picking"], FITS["packing"]
    pick_mean = picking["shape"] * picking["scale"]  # a gamma's mean
    pick_use = rate * pick_mean  # one picker
    # One server with Poisson arrivals, where Kingman's formula is exact; a gamma's
    # squared coefficient of variation is 1 / shape
    pick_wait = pick_use / (1 - pick_use) * (1 + 1 / picking["shape"]) / 2 * pick_mean
    pack_use = rate * packing["shape"] * packing["scale"] / 3  # three packers
    # --8<-- [end:by-hand]
    return {
        "resources.pickers.utilization_avg": pick_use * 100,
        "tasks.pick_order.wait_mean": pick_wait,
        "resources.packers.utilization_avg": pack_use * 100,
    }


def check():
    """10 replications of the busy hour beside the formulas worked by hand."""
    # --8<-- [start:check]
    formulas = by_hand()
    runs = replicate(
        SHOP, 10, seed=42, until=3600, warmup=WARMUP, measures=list(formulas)
    )
    for row in runs.summary:
        low, high = row["mean"] - row["half_width"], row["mean"] + row["half_width"]
        agrees = low <= formulas[row["measure"]] <= high
        print(
            f"{row['measure']}: formula {formulas[row['measure']]:.4f}, "
            f"runs {row['mean']:.4f} ± {row['half_width']:.4f}, "
            f"{'agrees' if agrees else 'differs'}"
        )
    # --8<-- [end:check]
    return runs


def warmups() -> dict:
    """The mean waits of 40 replications of today's shop, after three candidate warm-ups."""
    # --8<-- [start:warmups]
    return {
        warmup: replicate(SHOP, 40, seed=42, until=3600, warmup=warmup, measures=WAITS)
        for warmup in (0, WARMUP, 600)
    }
    # --8<-- [end:warmups]


EXPERIMENT = {
    "experiment": "complete-model",
    "question": "Which design keeps both mean waits of the busy hour under 1 second?",
    # --8<-- [start:model]
    # The busy hour as a LowLevelModel, and its 2-minute warm-up
    "model": SHOP,
    "warmup": WARMUP,
    "until": 3600,
    "replications": 40,
    "seed": 42,
    "measures": [
        "tasks.pick_order.wait_mean",
        "tasks.pack_order.wait_mean",
        "resources.packers.utilization_avg",
        "tasks.ship_order.wait_mean",
    ],
    # --8<-- [end:model]
    # Values made from the model's own runs to show the method, not shop records;
    # the real shop runs the current design
    "observed": {
        "file": str(HERE / "complete_model_outputs.csv"),
        "variant": "current",
    },
    "pairing": "common",
    "compare": [
        {"baseline": "current", "variant": "second-picker"},
        {"baseline": "current", "variant": "fourth-packer"},
    ],
    "variants": {
        "current": {},
        "second-picker": {"Shop.resources.pickers.current_cap": 2},
        "fourth-packer": {"Shop.resources.packers.current_cap": 4},
        "fourth-packer-busier": {
            "Shop.resources.packers.current_cap": 4,
            "Shop.arrival.orders.rate": 1.65791,
        },
    },
    "checks": [
        {"measure": "tasks.pick_order.wait_mean", "variant": "fourth-packer", "max": 1},
        {"measure": "tasks.pack_order.wait_mean", "variant": "fourth-packer", "max": 1},
        {"measure": "tasks.ship_order.wait_mean", "variant": "current", "max": 0},
        {"measure": "tasks.ship_order.wait_mean", "variant": "second-picker", "max": 0},
        {"measure": "tasks.ship_order.wait_mean", "variant": "fourth-packer", "max": 0},
        {
            "measure": "tasks.pick_order.wait_mean",
            "variant": "fourth-packer-busier",
            "max": 1,
        },
        {
            "measure": "tasks.pack_order.wait_mean",
            "variant": "fourth-packer-busier",
            "max": 1,
        },
    ],
    "assumptions": [
        "Orders arrive at random, at a steady rate, through the busy hour.",
        "One picker and three packers work the whole hour; nobody takes a break.",
        "Every order is one parcel in one shipping box.",
    ],
    "inputs": {
        "Shop.arrival.orders": "gaps between 300 orders observed in the shop, "
        "fitted by fit_column",
        "Shop.service.picking": "picking times of the same 300 orders, "
        "fitted by fit_column",
        "Shop.service.packing": "packing times of the same 300 orders, "
        "fitted by fit_column",
        "Shop.arrival.deliveries": "the supplier's schedule, a delivery every 10 "
        "minutes",
    },
    "limits": [
        "One busy hour only; the rest of the day is not modelled.",
        "The observed values were made from the model's own runs, so comparing "
        "with them shows the method only.",
    ],
    "output": "results/complete-model",
}


def experiment(output=None):
    """Runs the designs; raises CheckFailed when a check fails, after writing every file."""
    return run_experiment(EXPERIMENT, output=output)


def recommended(result) -> list:
    """The designs whose two mean waits both have an upper end under 1 second; the
    sensitivity variant is not a design."""
    upper: dict = {}
    for row in result.summary:
        if row["design"] in DESIGNS and row["statistic"] in WAITS:
            high = row["mean"] + row["half_width"]
            upper[row["design"]] = max(upper.get(row["design"], 0), high)
    return [design for design, high in upper.items() if high < 1]


if __name__ == "__main__":
    check()
    for warmup, result in warmups().items():
        for row in result.summary:
            print(
                f"warm-up {warmup} s: {row['measure']} {row['mean']:.3f} ± {row['half_width']:.3f}"
            )
    print(f"recommended: {recommended(experiment())}")

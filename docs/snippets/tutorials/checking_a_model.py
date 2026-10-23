"""The Checking a Model tutorial: checks of the busy hour, written as tests, builder.

A run of the shop with fixed inputs is compared with a trace worked by hand. An
experiment then runs the busy hour of Trusting Results with one, three and ten packers
and with four packers at a higher rate, checks each against a bound, and places the
three-packer design beside the values observed in the real shop. Run the checks with:
pytest checking_a_model.py
"""

import logging
from pathlib import Path

from desflow import SimulationContext, run_experiment
from trusting_results import shop

# The imported model logs at INFO; the checks log only their warnings
logging.getLogger("desflow").setLevel(logging.WARNING)

HERE = Path(__file__).resolve().parent


# --8<-- [start:fixed]
def fixed_shop() -> SimulationContext:
    """Three packers, an order every second and 4 seconds to pack each, for 10 s."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=3, max_cap=10)
        .add_arrival("orders", dist="constant", value=1)
        .add_service("packing", dist="constant", value=4)
        .with_stats("typical", interval=3600)
    )
    # --8<-- [end:fixed]

    @app.task(service_id="packing", resource_id="packers")
    def pack_order(task_id: int):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_generator(context: SimulationContext):
        task_id = 0
        while True:
            order = yield context.wait_for_arrival("orders")
            context.spawn(pack_order(task_id, entity=order))
            task_id += 1

    return app


def fixed_run() -> dict:
    """The statistics of the fixed shop after 10 seconds, without the Shop.stats. prefix."""
    app = fixed_shop()
    app.run(until=10)
    readings = app.env.stats.readings()
    return {key.removeprefix("Shop.stats."): value for key, value in readings.items()}


EXPERIMENT = {
    "experiment": "checking-a-model",
    "question": "Is the busy hour model adequate for choosing the number of packers?",
    # --8<-- [start:model]
    # The busy hour of Trusting Results, with its 2-minute warm-up
    "model": shop,
    "until": "1 h",
    "replications": 20,
    "seed": 42,
    "measures": [
        "tasks.pack_order.wait_mean",
        "resources.packers.utilization_avg",
        "resources.packers.queue_length_avg",
    ],
    # --8<-- [end:model]
    # --8<-- [start:extremes]
    # One packer cannot keep up, and ten are mostly idle
    "variants": {
        "three": {},
        "one": {"Shop.resources.packers.current_cap": 1},
        "ten": {"Shop.resources.packers.current_cap": 10},
        # --8<-- [end:extremes]
        # --8<-- [start:sensitivity]
        # The design chosen in Comparing Designs, at a rate 6% above the estimate
        "four-busier": {
            "Shop.resources.packers.current_cap": 4,
            "Shop.arrival.orders.rate": 1.9,
        },
        # --8<-- [end:sensitivity]
    },
    # --8<-- [start:extreme-checks]
    "checks": [
        # One packer is busy all the time: the lower end of the interval is 99.9% or more
        {
            "measure": "resources.packers.utilization_avg",
            "variant": "one",
            "min": 99.9,
        },
        # With ten packers almost no order waits
        {"measure": "tasks.pack_order.wait_mean", "variant": "ten", "max": 0.01},
        # --8<-- [end:extreme-checks]
        # --8<-- [start:sensitivity-check]
        # Four packers keep the mean wait under half a second at the higher rate
        {
            "measure": "tasks.pack_order.wait_mean",
            "variant": "four-busier",
            "max": 0.5,
        },
        # --8<-- [end:sensitivity-check]
    ],
    # --8<-- [start:observed]
    # Illustrative values for the real shop, which has three packers;
    # a real check reads the shop's own records
    "observed": {
        "file": str(HERE / "checking_a_model_observed.csv"),
        "variant": "three",
    },
    # --8<-- [end:observed]
    "output": "results/checking-a-model",
}


def experiment(output=None):
    """Runs the checks; raises CheckFailed when one fails, after writing every file."""
    return run_experiment(EXPERIMENT, output=output)


# --8<-- [start:little]
HORIZON = 3600 - 120  # the measured part of the hour, after the warm-up, in seconds


def littles_law(result, design):
    """The mean queue, and the arrival rate and mean wait whose product it should be."""
    means = {r["statistic"]: r["mean"] for r in result.summary if r["design"] == design}
    rate = means["flows.orders.started"] / HORIZON
    return (
        means["resources.packers.queue_length_avg"],
        rate,
        means["tasks.pack_order.wait_mean"],
    )


# --8<-- [end:little]
# --8<-- [start:tests]
def test_fixed_inputs_match_the_hand_trace():
    readings = fixed_run()
    assert readings["tasks.pack_order.finished"] == 4
    assert readings["tasks.pack_order.in_progress"] == 5
    assert readings["tasks.pack_order.wait_mean"] == 5 / 7
    assert readings["tasks.pack_order.wait_max"] == 2


def test_extremes_sensitivity_and_littles_law(tmp_path):
    result = experiment(output=tmp_path)  # a failed check raises CheckFailed
    queue, rate, wait = littles_law(result, "three")
    assert abs(queue - rate * wait) < 0.05 * queue


# --8<-- [end:tests]
if __name__ == "__main__":
    print(fixed_run())
    result = experiment()
    for design in ("three", "one"):
        queue, rate, wait = littles_law(result, design)
        print(
            f"{design}: queue {queue:.3f}, {rate:.4f} x {wait:.3f} = {rate * wait:.3f}"
        )
    for row in result.observed:
        print(f"observed {row['measure']}: {row['value']} inside={row['inside']}")

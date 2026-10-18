"""Local studies: run a study from Python, read its result and stop on a failed check."""

import logging

from desflow import CheckFailed, SimulationContext, run_study

logging.basicConfig(
    level=logging.WARNING, format="%(levelname)s [%(asctime)s] %(message)s"
)


def shop() -> SimulationContext:
    """A new shop each time it is called."""
    app = (
        SimulationContext(sim_id="Shop", factor=0)
        .add_resource("packers", current_cap=2, max_cap=4)
        .add_service("packing", dist="gamma", shape=4, scale=1)
        .add_arrival("orders", dist="exponential", rate=0.4)
    )

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id):
        return {"event_type": "order_packed"}

    @app.arrival_loop("orders")
    def order_loop(context):
        task_id = 0
        while True:
            entity = yield context.wait_for_arrival("orders")
            context.spawn(pack(task_id, entity=entity))
            task_id += 1

    return app


# --8<-- [start:run]
STUDY = {
    "study": "packers",
    "model": shop,  # a blueprint path, a builder factory or a LowLevelModel
    "until": "1 h",
    "warmup": "3 min",
    "replications": 10,
    "seed": 42,
    "measures": ["tasks.pack.wait_mean"],
    "variants": {"two": {}, "three": {"Shop.resources.packers.current_cap": 3}},
    # A sweep would run every variant at each rate: 6 designs
    # "sweep": {"Shop.arrival.orders.rate": [0.35, 0.4, 0.45]},
    "compare": [{"baseline": "two", "variant": "three"}],
    "checks": [{"measure": "tasks.pack.wait_mean", "variant": "two", "max": 2}],
}
try:
    result = run_study(STUDY)  # writes results/packers, then checks
except CheckFailed as failed:
    result = failed.result  # every file is written before it is raised
    print(failed)
# --8<-- [end:run]

# --8<-- [start:read]
for row in result.summary:  # one row per design and statistic, counts included
    print(row["design"], row["statistic"], row["mean"], row["half_width"])
for row in result.comparisons:  # the variant minus the baseline, seed by seed
    print(row["measure"], row["difference"], row["half_width"])
print([row["design"] for row in result.failed])  # the failed checks
print(result.output / "report.md")
# --8<-- [end:read]

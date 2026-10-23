"""The Choosing Inputs tutorial: the busy hour with four candidate packing times.

Each candidate has a mean of 1.5 seconds. The script runs the busy hour once per
candidate, unpaced, and prints the mean wait of each run.
"""

from desflow import SimulationContext

CANDIDATES = ["constant", "triangular", "empirical", "exponential"]


def shop(packing: str) -> SimulationContext:
    """The busy hour with 3 packers, packing with the service called `packing`."""
    app = (
        SimulationContext(sim_id="Shop", factor=0, random_seed=42)
        .add_resource("packers", current_cap=3, max_cap=10)
        # Each order starts the task pack_order
        .add_arrival("orders", dist="exponential", rate=1.8, spawn="pack_order")
        # Four candidates for packing, each with a mean of 1.5 seconds
        .add_service("constant", dist="constant", value=1.5)
        .add_service("exponential", dist="exponential", rate=0.6667)  # 1 / 1.5 s
    )
    # --8<-- [start:triangular]
    # An expert's estimate: at least 1 s, usually 1.25 s, at most 2.25 s
    app.add_service("triangular", dist="triangular", low=1, mode=1.25, high=2.25)
    # --8<-- [end:triangular]
    # --8<-- [start:empirical]
    # 40 packing times measured at the bench, in seconds
    measured = [
        1.5, 2.5, 2.4, 1.2, 1.3, 1.2, 1.8, 1.4, 2.0, 0.7,
        2.7, 1.4, 1.9, 1.4, 1.3, 1.8, 2.0, 1.4, 1.4, 1.9,
        1.1, 0.8, 1.7, 1.1, 0.7, 1.1, 1.2, 0.9, 0.8, 1.5,
        2.1, 1.3, 1.1, 1.7, 1.9, 1.3, 1.8, 2.2, 1.4, 1.1,
    ]  # fmt: skip
    app.add_service("empirical", dist="empirical", values=measured)

    # --8<-- [end:empirical]
    # --8<-- [start:compare]
    # The task packs with the candidate; the mean wait is read at the end
    @app.task(service_id=packing, resource_id="packers")
    def pack_order(task_id):
        return {"event_type": "order_packed"}

    app.with_stats("typical", interval=3600)
    # --8<-- [end:compare]

    return app


def mean_wait(packing: str) -> float:
    """Runs the busy hour with one candidate and returns the mean wait."""
    app = shop(packing)
    app.run(until="1 h")
    return app.env.stats.readings()["Shop.stats.tasks.pack_order.wait_mean"]


if __name__ == "__main__":
    for packing in CANDIDATES:
        print(f"{packing:<12} mean wait {mean_wait(packing):5.2f} s")

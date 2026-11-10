"""Bank Renege: SimPy's bank_renege.py example, builder API.

One counter serves five customers. The first arrives at 0 and the others about 10
seconds apart (exponential gaps). Each customer waits for the counter for a patience
drawn between 1 and 3 seconds, and goes away (reneged) if the counter is not free by
then. A customer at the counter is served for about 12 seconds (exponential).

SimPy's example counts seconds, as desflow does. Each value is drawn from desflow's
random sequence for its place in the model, so the numbers differ from those SimPy
prints.

Needs no containers. Runs unpaced and ends when the last customer leaves.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import generator


def build() -> SimulationContext:
    """Builds the bank."""
    app = SimulationContext(sim_id="Bank", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    # --8<-- [start:customers-arrive]
    app.add_resource("counter", current_cap=1)
    app.add_arrival(
        "customers",
        "exponential",
        rate=0.1,
        first_at=0,
        max_events=5,
        key=lambda n: "Customer{:02d}".format(n - 1),
        spawn="customer",
    )
    # --8<-- [end:customers-arrive]

    # --8<-- [start:customer-waits-or-reneges]
    @app.task(
        name="customer",
        resource_id="counter",
        # Waits for the counter at most this long, then goes away
        patience=generator({"gen": "uniform", "low": 1, "high": 3}),
        service_time=generator(
            {"gen": "sample", "dist": "exponential", "rate": 0.08333333333333333}
        ),
    )
    def customer(task_id):
        """Is served at the counter."""
        return {"event_type": "finished"}

    # --8<-- [end:customer-waits-or-reneges]

    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs until the last customer leaves."""
    model.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

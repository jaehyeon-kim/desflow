"""Carwash: SimPy's carwash.py example, builder API.

A carwash has two washing machines. Four cars wait when it opens. A car takes a
machine, keeps it while the wash runs (a task of its own that removes 50 to 99 percent
of the dirt), and then leaves.

SimPy's example counts minutes, and here each of its minutes is one second. So a car
arrives every 5 to 9 seconds, a wash takes 5 seconds, and the run ends after 20
seconds, SimPy's 20 minutes. Each value is drawn from desflow's random sequence for its
place in the model, so the numbers differ from those SimPy prints.

Needs no containers. Runs unpaced and ends after 20 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator


def build(size: int = 4) -> SimulationContext:
    """Builds the carwash, with `size` cars waiting when it opens."""
    app = SimulationContext(sim_id="Carwash", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    app.add_resource("machine", current_cap=2)
    app.add_service("wash", dist="constant", value=5)
    app.add_arrival(
        "cars",
        gap=generator({"gen": "integer", "bounds": [5, 9]}),
        key=lambda n: "Car {}".format(n - 1),
        initial=size,
        spawn="car",
    )

    @app.task(name="car", seize="machine", next="washing")
    def car(task_id):
        """Takes a machine and keeps it."""
        return {"event_type": "enters"}

    # The car keeps its machine until the wash, a task of its own, has finished
    @app.task(name="washing", wait={"task": "wash"}, release="machine")
    def washing(task_id):
        """Leaves once its wash has finished."""
        return {"event_type": "leaves"}

    wash_payload = Template(
        {
            "event_type": "washed",
            "dirt_removed_pct": {"gen": "integer", "bounds": [50, 99]},
        },
        # The name gives the random sequence the YAML version's payload draws from
        name="tasks.wash",
    )

    @app.task(name="wash", service_id="wash")
    def wash(task_id):
        """Washes the car."""
        return wash_payload

    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs 20 simulation seconds."""
    model.run(until=20)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

"""Gas Station Refueling: SimPy's gas_station_refuel.py example, builder API.

A gas station has two pumps and a tank of 200 litres. A car arrives every 30 to 300
seconds with 5 to 25 litres left in its 50 litre tank. It waits for a pump, takes the
fuel it needs from the station's tank and refuels at 2 litres a second. A control task
checks the tank every 10 seconds, and when it is below 25 percent it calls a tank
truck, which arrives 300 seconds later and fills the tank.

Each value is drawn from desflow's random sequence for its place in the model, so the
numbers differ from those SimPy prints.

Needs no containers. Runs unpaced and ends after 1000 simulation seconds.
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator


def build() -> SimulationContext:
    """Builds the gas station."""
    app = SimulationContext(sim_id="GasStation", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    app.add_resource("pumps", current_cap=2)
    app.add_container("tank", current_cap=200, level=200)
    app.add_initial_arrival("control", 1, spawn="control")
    app.add_arrival(
        "cars",
        gap=generator({"gen": "integer", "bounds": [30, 300]}),
        key=lambda n: "Car {}".format(n - 1),
        attributes={"level": generator({"gen": "integer", "bounds": [5, 25]})},
        spawn="car",
    )
    car_payload = Template(
        {
            "event_type": "refueled",
            "litres": {"gen": "expr", "expr": "50 - attr.level"},
        },
        name="car",
    )

    @app.task(
        name="car",
        resource_id="pumps",
        get={"container": "tank", "amount": "50 - attr.level"},
        service_time="(50 - attr.level) / 2",
    )
    def car(task_id):
        """Refuels at a pump."""
        return car_payload

    @app.task(
        name="control",
        next={
            "when": [
                {"condition": "level.tank / 200 * 100 < 25", "task": "call_truck"}
            ],
            "default": "pause",
        },
    )
    def control(task_id):
        """Checks the level of the tank."""
        return {"event_type": "checked"}

    @app.task(name="call_truck", next="await_truck")
    def call_truck(task_id):
        """Calls the tank truck."""
        return {"event_type": "truck_called"}

    @app.task(name="await_truck", wait={"task": "truck"}, next="pause")
    def await_truck(task_id):
        """Waits for the tank truck."""
        return {"event_type": "truck_returned"}

    truck_payload = Template(
        {
            "event_type": "truck_arrived",
            "litres": {"gen": "expr", "expr": "200 - level.tank"},
        },
        name="truck",
    )

    @app.task(
        name="truck",
        service_time=300,
        put={"container": "tank", "amount": "200 - level.tank"},
    )
    def truck(task_id):
        """Fills the tank on arrival."""
        return truck_payload

    @app.task(name="pause", service_time=10, next="control")
    def pause(task_id):
        """Waits until the next check."""
        return {"event_type": "waited"}

    return app


app = build()


def run(model: SimulationContext = app) -> None:
    """Runs 1000 simulation seconds."""
    model.run(until=1000)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    run()

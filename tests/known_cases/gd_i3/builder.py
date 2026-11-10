"""GD-I3: A driver interrupts a car that catches the interrupt.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Car", factor=0, random_seed=42)
    app.add_service("charge", dist="constant", value=5)
    app.add_service("drive", dist="constant", value=2)
    app.add_initial_arrival("car", size, spawn="park_and_charge")
    app.add_arrival("driver", "constant", value=3)

    @app.task(
        name="park_and_charge",
        wait={"task": "charge_task"},
        on_interrupt="abandon",
        on_interrupt_next="drive",
        next="drive",
    )
    def park_and_charge(task_id):
        return {"event_type": "charged_and_parked"}

    @app.task(name="charge_task", service_id="charge")
    def charge_task(task_id):
        return {"event_type": "charge_done"}

    @app.task(name="drive", service_id="drive", next="park_and_charge")
    def drive(task_id):
        return {"event_type": "drove"}

    @app.task(name="interrupt_car", interrupt={"task": "park_and_charge"})
    def interrupt_car(task_id):
        return {"event_type": "interrupted"}

    @app.arrival_loop("driver")
    def driver_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("driver")
            context.spawn(
                interrupt_car(context.next_instance("interrupt_car"), entity=entity)
            )

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=15)

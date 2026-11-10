"""GD-S1: The SimPy 2 verbs written for SimPy 4.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="S", factor=0, random_seed=42)
    app.add_resource("r", current_cap=1)
    app.add_container("q", current_cap=10, level=0)
    app.add_signal("ev", once=False)
    app.add_initial_arrival("a", size, spawn="hold")
    app.add_arrival("b", "constant", value=2)

    @app.task(name="hold", service_time=1, next="passivate")
    def hold(task_id):
        return {"event_type": "hold"}

    @app.task(name="passivate", wait={"signal": "ev"}, next="request")
    def passivate(task_id):
        return {"event_type": "reactivated"}

    @app.task(name="request", resource_id="r", service_time=1, next="get")
    def request(task_id):
        return {"event_type": "used"}

    @app.task(name="get", get={"container": "q", "amount": 5}, next="either")
    def get(task_id):
        return {"event_type": "got"}

    @app.task(
        name="either",
        wait={"any": {"t": {"timeout": 1}, "e": {"signal": "ev"}}},
        next="both",
    )
    def either(task_id):
        return {"event_type": "first_of_two"}

    @app.task(
        name="both",
        wait={"all": {"t": {"timeout": 1}, "e": {"signal": "ev"}}},
        next="all_of",
    )
    def both(task_id):
        return {"event_type": "both"}

    @app.task(
        name="all_of",
        wait={"all": {"a": {"timeout": 1}, "b": {"timeout": 2}, "c": {"timeout": 3}}},
        next="any_of",
    )
    def all_of(task_id):
        return {"event_type": "all_of_three"}

    @app.task(
        name="any_of",
        wait={"any": {"a": {"timeout": 3}, "b": {"timeout": 1}, "c": {"timeout": 2}}},
        next="process",
    )
    def any_of(task_id):
        return {"event_type": "any_of_three"}

    @app.task(name="process", wait={"task": "calculation"})
    def process(task_id):
        return {"event_type": "process_done"}

    @app.task(name="calculation", service_time=1)
    def calculation(task_id):
        return {"event_type": "calculated"}

    @app.task(name="put_it", put={"container": "q", "amount": 5}, signal="ev")
    def put_it(task_id):
        return {"event_type": "put"}

    @app.arrival_loop("b")
    def b_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("b")
            context.spawn(put_it(context.next_instance("put_it"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

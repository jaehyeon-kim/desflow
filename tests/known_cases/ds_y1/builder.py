"""DS-Y1: Resources, stores and signals in a computed number.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator
from desflow.builder.task import NO_RECORD


def build(size: int = 4) -> SimulationContext:
    app = SimulationContext(sim_id="Y1", factor=0, random_seed=42)
    app.add_resource(
        "desk",
        family=True,
        members=generator({"gen": "expr", "expr": "registry['Y1.variables.n']"}),
        current_cap="registry['Y1.variables.k']",
    )
    app.add_store(
        "q",
    )
    app.add_variable("n", 3)
    app.add_variable("k", 1)
    app.add_signal("reply", family=True, once=True)
    app.add_initial_arrival(
        "jobs",
        size,
        spawn="job",
        attributes={"i": generator({"gen": "sequence", "start": 0})},
    )
    app.add_arrival("c0", "constant", value=1, first_at=0, attributes={"i": 0})
    app.add_arrival("c1", "constant", value=1, first_at=0, attributes={"i": 1})
    app.add_arrival("srv", "constant", value=1, first_at=0)
    job_payload = Template(
        {"event_type": "got", "i": {"gen": "attr", "name": "i"}}, name="job"
    )

    @app.task(
        name="job", resource_id={"of": "desk", "key": "attr.i % 3"}, service_time=2
    )
    def job(task_id):
        return job_payload

    client_payload = Template(
        {"event_type": "put", "i": {"gen": "attr", "name": "i"}}, name="client"
    )

    @app.task(
        name="client",
        put={"store": "q", "item": generator({"gen": "attr", "name": "i"})},
        next={
            "when": [{"condition": "attr.i == 1", "task": "late"}],
            "default": "waits",
        },
    )
    def client(task_id):
        return client_payload

    @app.task(name="late", wait={"timeout": 5}, next="waits")
    def late(task_id):
        return NO_RECORD

    waits_payload = Template(
        {
            "event_type": "waits",
            "i": {"gen": "attr", "name": "i"},
            "at": {"gen": "expr", "expr": "t"},
        },
        name="waits",
    )

    @app.task(name="waits", next="hear")
    def waits(task_id):
        return waits_payload

    hear_payload = Template(
        {
            "event_type": "reply",
            "i": {"gen": "attr", "name": "i"},
            "v": {"gen": "expr", "expr": "wait.r.value"},
            "at": {"gen": "expr", "expr": "t"},
        },
        name="hear",
    )

    @app.task(
        name="hear", wait={"all": {"r": {"signal": {"of": "reply", "key": "attr.i"}}}}
    )
    def hear(task_id):
        return hear_payload

    @app.task(name="server", get={"store": "q"}, next="server_fire")
    def server(task_id):
        return NO_RECORD

    server_fire_payload = Template(
        {
            "event_type": "server_fired",
            "i": {"gen": "expr", "expr": "item"},
            "at": {"gen": "expr", "expr": "t"},
        },
        name="server_fire",
    )

    @app.task(
        name="server_fire",
        wait={"timeout": 1},
        on=[
            {
                "fire": {
                    "signal": {"of": "reply", "key": "item"},
                    "value": generator({"gen": "expr", "expr": "item * 10"}),
                }
            }
        ],
        next="server",
    )
    def server_fire(task_id):
        return server_fire_payload

    @app.arrival_loop("c0")
    def c0_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("c0")
            context.spawn(client(context.next_instance("client"), entity=entity))

    @app.arrival_loop("c1")
    def c1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("c1")
            context.spawn(client(context.next_instance("client"), entity=entity))

    @app.arrival_loop("srv")
    def srv_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("srv")
            context.spawn(server(context.next_instance("server"), entity=entity))

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=20)

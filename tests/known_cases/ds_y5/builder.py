"""DS-Y5: Lists and mappings in expressions.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator
from desflow.builder.task import NO_RECORD


def build() -> SimulationContext:
    app = SimulationContext(sim_id="Y5", factor=0, random_seed=42)
    app.add_store("jobs", items=[{"v": 1}, {"v": 2}])
    app.add_variable("seen", {})
    app.add_variable("q", [])
    app.add_arrival("c1", "constant", value=1, first_at=1, attributes={"k": "a"})
    app.add_arrival("c2", "constant", value=1, first_at=1, attributes={"k": "b"})
    app.add_arrival("c3", "constant", value=1, first_at=2, attributes={"k": "a"})
    app.add_arrival("l0", "constant", value=1, first_at=0, attributes={"i": 0})
    app.add_arrival("l1", "constant", value=1, first_at=1, attributes={"i": 1})
    app.add_arrival("l2", "constant", value=1, first_at=2, attributes={"i": 2})
    app.add_arrival("s", "constant", value=1, first_at=0)

    @app.task(
        name="cnt",
        on=[
            {
                "path": "Y5.variables.seen",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "set_key(registry['Y5.variables.seen'], attr.k, get_key(registry['Y5.variables.seen'], attr.k, 0) + 1)",
                    }
                ),
            }
        ],
        next="cnt_log",
    )
    def cnt(task_id):
        return NO_RECORD

    cnt_log_payload = Template(
        {
            "event_type": "cnt",
            "k": {"gen": "expr", "expr": "attr.k"},
            "seen": {"gen": "expr", "expr": "registry['Y5.variables.seen']"},
            "has_k": {"gen": "expr", "expr": "attr.k in registry['Y5.variables.seen']"},
            "has_z": {"gen": "expr", "expr": "'z' in registry['Y5.variables.seen']"},
        },
        name="cnt_log",
    )

    @app.task(name="cnt_log")
    def cnt_log(task_id):
        return cnt_log_payload

    @app.task(
        name="lst",
        on=[
            {
                "path": "Y5.variables.q",
                "value": generator(
                    {
                        "gen": "expr",
                        "expr": "registry['Y5.variables.q'] + [attr.i * 10]",
                    }
                ),
            }
        ],
        next="lst_log",
    )
    def lst(task_id):
        return NO_RECORD

    lst_log_payload = Template(
        {
            "event_type": "lst",
            "q": {"gen": "expr", "expr": "registry['Y5.variables.q']"},
            "first": {"gen": "expr", "expr": "registry['Y5.variables.q'][0]"},
            "last": {"gen": "expr", "expr": "registry['Y5.variables.q'][-1]"},
            "n": {"gen": "expr", "expr": "len(registry['Y5.variables.q'])"},
            "total": {"gen": "expr", "expr": "sum(registry['Y5.variables.q'])"},
            "ordered": {"gen": "expr", "expr": "sorted(registry['Y5.variables.q'])"},
            "plus_one": {
                "gen": "expr",
                "expr": "[x + 1 for x in registry['Y5.variables.q'] if x > 0]",
            },
            "rest": {"gen": "expr", "expr": "registry['Y5.variables.q'][1:]"},
        },
        name="lst_log",
    )

    @app.task(name="lst_log")
    def lst_log(task_id):
        return lst_log_payload

    peek_store_payload = Template(
        {
            "event_type": "peek_store",
            "first_v": {"gen": "expr", "expr": "items(jobs)[0].v"},
            "n": {"gen": "expr", "expr": "len(items(jobs))"},
            "values": {"gen": "expr", "expr": "[x.v for x in items(jobs)]"},
        },
        name="peek_store",
    )

    @app.task(name="peek_store")
    def peek_store(task_id):
        return peek_store_payload

    @app.arrival_loop("c1")
    def c1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("c1")
            context.spawn(cnt(context.next_instance("cnt"), entity=entity))

    @app.arrival_loop("c2")
    def c2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("c2")
            context.spawn(cnt(context.next_instance("cnt"), entity=entity))

    @app.arrival_loop("c3")
    def c3_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("c3")
            context.spawn(cnt(context.next_instance("cnt"), entity=entity))

    @app.arrival_loop("l0")
    def l0_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("l0")
            context.spawn(lst(context.next_instance("lst"), entity=entity))

    @app.arrival_loop("l1")
    def l1_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("l1")
            context.spawn(lst(context.next_instance("lst"), entity=entity))

    @app.arrival_loop("l2")
    def l2_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("l2")
            context.spawn(lst(context.next_instance("lst"), entity=entity))

    @app.arrival_loop("s")
    def s_arrivals(context):
        for _ in range(1):
            entity = yield context.wait_for_arrival("s")
            context.spawn(
                peek_store(context.next_instance("peek_store"), entity=entity)
            )

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

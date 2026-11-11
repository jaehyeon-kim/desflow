"""DS-Y12: A rank on the waiting gets and puts of a store and a container.

Builder form of `model.yaml`.
"""

from desflow import SimulationContext
from desflow.values import Template, generator
from desflow.builder.task import NO_RECORD


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(sim_id="Y12", factor=0, random_seed=42)
    app.add_store("sA", get_queue={"rank": "attr.rank", "ties": "fifo"})
    app.add_store("sB", get_queue={"rank": "attr.rank", "ties": "fifo"})
    app.add_store("shelf", current_cap=1, put_queue={"rank": "attr.rank"})
    app.add_container("c", get_queue={"rank": "attr.rank"}, level=0)
    app.add_arrival(
        "ga0",
        "constant",
        value=1,
        first_at=0,
        attributes={"i": 0, "rank": 5},
        spawn="getA",
        max_events=1,
    )
    app.add_arrival(
        "ga1",
        "constant",
        value=1,
        first_at=0,
        attributes={"i": 1, "rank": 1},
        spawn="getA",
        max_events=1,
    )
    app.add_arrival(
        "ga2",
        "constant",
        value=1,
        first_at=0,
        attributes={"i": 2, "rank": 3},
        spawn="getA",
        max_events=1,
    )
    app.add_arrival("pa", "constant", value=1, first_at=0, spawn="putA", max_events=1)
    app.add_arrival(
        "gb0",
        "constant",
        value=1,
        first_at=0,
        attributes={"i": 0, "rank": 1},
        spawn="getB",
        max_events=1,
    )
    app.add_arrival(
        "gb1",
        "constant",
        value=1,
        first_at=0,
        attributes={"i": 1, "rank": 1},
        spawn="getB",
        max_events=1,
    )
    app.add_arrival(
        "gb2",
        "constant",
        value=1,
        first_at=0,
        attributes={"i": 2, "rank": 0},
        spawn="getB",
        max_events=1,
    )
    app.add_arrival("pb", "constant", value=1, first_at=0, spawn="putB", max_events=1)
    app.add_initial_arrival(
        "wf", size, spawn="shelve", attributes={"rank": 0, "name": "first"}
    )
    app.add_initial_arrival(
        "wl", 1, spawn="shelve", attributes={"rank": 5, "name": "low"}
    )
    app.add_initial_arrival(
        "wh", 1, spawn="shelve", attributes={"rank": 1, "name": "high"}
    )
    app.add_arrival("tk", "constant", value=1, first_at=1, spawn="take", max_events=3)
    app.add_arrival(
        "cbig",
        "constant",
        value=1,
        first_at=0,
        attributes={"rank": 0, "amount": 5, "name": "big, rank 0"},
        spawn="cget",
        max_events=1,
    )
    app.add_arrival(
        "csmall",
        "constant",
        value=1,
        first_at=0,
        attributes={"rank": 1, "amount": 1, "name": "small, rank 1"},
        spawn="cget",
        max_events=1,
    )
    app.add_arrival("cp", "constant", value=1, first_at=0, spawn="cput", max_events=1)
    getA_payload = Template(
        {
            "event_type": "got",
            "i": {"gen": "attr", "name": "i"},
            "item": {"gen": "expr", "expr": "item"},
        },
        name="tasks.getA",
    )

    @app.task(name="getA", get={"store": "sA"})
    def getA(task_id):
        return getA_payload

    @app.task(name="putA", wait={"timeout": 1}, next="putA1")
    def putA(task_id):
        return NO_RECORD

    @app.task(name="putA1", put={"store": "sA", "item": "a"}, next="putA2")
    def putA1(task_id):
        return {"event_type": "put"}

    @app.task(name="putA2", put={"store": "sA", "item": "b"}, next="putA3")
    def putA2(task_id):
        return {"event_type": "put"}

    @app.task(name="putA3", put={"store": "sA", "item": "c"})
    def putA3(task_id):
        return {"event_type": "put"}

    getB_payload = Template(
        {
            "event_type": "got",
            "i": {"gen": "attr", "name": "i"},
            "item": {"gen": "expr", "expr": "item"},
        },
        name="tasks.getB",
    )

    @app.task(name="getB", get={"store": "sB"})
    def getB(task_id):
        return getB_payload

    @app.task(name="putB", wait={"timeout": 1}, next="putB1")
    def putB(task_id):
        return NO_RECORD

    @app.task(name="putB1", put={"store": "sB", "item": "a"}, next="putB2")
    def putB1(task_id):
        return {"event_type": "put"}

    @app.task(name="putB2", put={"store": "sB", "item": "b"}, next="putB3")
    def putB2(task_id):
        return {"event_type": "put"}

    @app.task(name="putB3", put={"store": "sB", "item": "c"})
    def putB3(task_id):
        return {"event_type": "put"}

    shelve_payload = Template(
        {"event_type": "in", "name": {"gen": "attr", "name": "name"}},
        name="tasks.shelve",
    )

    @app.task(
        name="shelve",
        put={"store": "shelf", "item": generator({"gen": "attr", "name": "name"})},
    )
    def shelve(task_id):
        return shelve_payload

    take_payload = Template(
        {"event_type": "out", "name": {"gen": "expr", "expr": "item"}},
        name="tasks.take",
    )

    @app.task(name="take", get={"store": "shelf"})
    def take(task_id):
        return take_payload

    cget_payload = Template(
        {
            "event_type": "got",
            "name": {"gen": "attr", "name": "name"},
            "level": {"gen": "expr", "expr": "level.c"},
        },
        name="tasks.cget",
    )

    @app.task(name="cget", get={"container": "c", "amount": "attr.amount"})
    def cget(task_id):
        return cget_payload

    @app.task(name="cput", wait={"timeout": 1}, next="cput1")
    def cput(task_id):
        return NO_RECORD

    @app.task(name="cput1", put={"container": "c", "amount": 2}, next="cput2")
    def cput1(task_id):
        return NO_RECORD

    @app.task(name="cput2", wait={"timeout": 1}, next="cput3")
    def cput2(task_id):
        return NO_RECORD

    @app.task(name="cput3", put={"container": "c", "amount": 3})
    def cput3(task_id):
        return NO_RECORD

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run()

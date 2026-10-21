"""Tasks in a flow: routing, entity keys and attributes, timed steps, priority, and
tasks that get from and put into stores and containers."""

import asyncio
import queue
import textwrap

import pytest

from desflow import Router, SimulationContext, Upstream
from desflow.blueprint import BlueprintError


class _Capture:
    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _run(app, until):
    capture = _Capture()
    app.add_egress(capture)
    app.run(until=until)
    return [r for r in capture.records if r["stream_type"] == "event"]


def _arrivals(app, arrival, task, count=None):
    """An arrival loop that spawns `task` for each entity, `count` times at most."""

    def loop(context):
        number = 0
        while count is None or number < count:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(number, context, entity=entity))
            number += 1

    app.add_process(loop)


def test_conditions_route_by_payload_and_attribute_and_the_flow_ends():
    app = (
        SimulationContext("Shop", factor=0, random_seed=1)
        .add_arrival("orders", "constant", value=1, attributes={"vip": 0})
        .add_arrival(
            "vips",
            "constant",
            value=1,
            attributes={"vip": 1},
            key=lambda n: f"VIP{n:03d}",
        )
    )
    totals = iter([50, 5, 500])

    @app.task(
        next={
            "when": [
                {"condition": "attr.vip == 1", "task": "concierge"},
                {"condition": "payload.total > 100", "task": "review"},
            ]
        },
        set={"checked": True},
    )
    def place(task_id, context):
        return {"total": next(totals, 10)}

    @app.task()
    def review(task_id, context, upstream):
        # upstream is passed because this function declares it
        return {"from": upstream.task_id, "total": upstream.payload["total"]}

    @app.task(next=lambda done: None)
    def concierge(task_id, context, entity):
        return {"checked": entity.attributes["checked"]}

    _arrivals(app, "orders", place, count=2)
    _arrivals(app, "vips", place, count=1)
    events = _run(app, 5)

    assert [(r["key"], r["task"], r["value"]) for r in events] == [
        # No rule holds and there is no default, so the flow ends
        ("orders-1", "place", {"total": 50}),
        # The attribute rule comes first; set applied before the next task
        ("VIP001", "place", {"total": 5}),
        ("VIP001", "concierge", {"checked": True}),
        ("orders-2", "place", {"total": 500}),
        ("orders-2", "review", {"from": 1, "total": 500}),
    ]


def test_weights_draw_from_the_tasks_own_sequence_and_repeat():
    def run(extra_arrival):
        app = SimulationContext("Shop", factor=0, random_seed=3).add_arrival(
            "orders", "constant", value=1
        )
        if extra_arrival:
            app.add_arrival("returns", "exponential", rate=1)

        @app.task(next={"weights": {"ship": 3, "pack": 1}})
        def pack(task_id, context):
            return {}

        @app.task()
        def ship(task_id, context):
            return {}

        _arrivals(app, "orders", pack, count=40)
        return [(r["key"], r["task"], r["instance"]) for r in _run(app, 100)]

    first = run(False)
    assert first == run(False)
    # Rework: a route back to pack runs it again, so it finishes more than 40 times
    assert sum(task == "pack" for _, task, _ in first) > 40
    assert sum(task == "ship" for _, task, _ in first) == 40
    # Another distribution does not change the routing draws
    assert run(True) == first


def test_a_timed_step_has_no_queued_event_and_waits_nothing():
    app = (
        SimulationContext("Shop", factor=0, random_seed=1)
        .add_arrival("orders", "constant", value=10)
        .add_service("shipping", "constant", value=4)
        .with_stats("typical", interval=100)
    )

    @app.task(service_id="shipping")
    def ship(task_id, context):
        return {"status": "shipped"}

    _arrivals(app, "orders", ship, count=1)
    events = _run(app, 20)
    assert [(r["sim_ts"], r["value"]) for r in events] == [
        (10.0, {"path_id": "Shop.service.shipping", "status": "started"}),
        (14.0, {"status": "shipped"}),
    ]
    readings = app.env.stats.readings()
    assert readings["Shop.stats.tasks.ship.wait_max"] == 0
    assert readings["Shop.stats.tasks.ship.service_mean"] == 4


def test_priority_comes_from_the_entity_and_needs_a_priority_queue():
    app = (
        SimulationContext("Shop", factor=0, random_seed=1)
        .add_resource("packers", 1, 1, queue="priority")
        .add_service("packing", "constant", value=5)
        .add_arrival("orders", "constant", value=1, attributes={"rank": 2})
        .add_arrival("express", "constant", value=2.5, attributes={"rank": 1})
    )

    @app.task(service_id="packing", resource_id="packers", priority="attr.rank")
    def pack(task_id, context):
        return {}

    _arrivals(app, "orders", pack, count=3)
    _arrivals(app, "express", pack, count=1)
    events = _run(app, 30)
    started = [r["key"] for r in events if r["value"].get("status") == "started"]
    # orders-1 holds the packer; express-1 then goes before orders-2 and orders-3
    assert started == ["orders-1", "express-1", "orders-2", "orders-3"]

    fifo = SimulationContext("Shop").add_resource("packers", 1, 1)
    fifo.add_service("packing", "constant", value=1)
    fifo.task(service_id="packing", resource_id="packers", priority=1)(lambda i: {})
    with pytest.raises(ValueError, match="queue='priority'"):
        fifo.run(until=1)


def test_tasks_take_stock_and_pass_items_through_a_store():
    app = (
        SimulationContext("Shop", factor=0, random_seed=1)
        .add_container("stock", current_cap=10, max_cap=10, level=2)
        .add_store("parcels", current_cap=5, max_cap=5)
        .add_arrival("orders", "constant", value=1)
    )

    @app.task(
        get={"container": "stock", "amount": "payload.quantity"},
        put={"store": "parcels"},
        next=None,
    )
    def pack(task_id, context, upstream):
        return {"parcel": upstream.key}

    @app.task(next="pack")
    def order(task_id, context):
        return {"quantity": 1 if task_id == 0 else 2}

    @app.task(get={"store": "parcels"})
    def collect(task_id, context, item):
        return {"collected": item["parcel"]}

    def restock(context):
        yield context.env.timeout(10)
        context.env.registry.update("Shop.containers.stock.level", 10)

    def couriers(context):
        for number in range(2):
            context.spawn(collect(number, context))
        yield context.env.timeout(0)

    _arrivals(app, "orders", order, count=3)
    app.add_process(restock)
    app.add_process(couriers)
    events = _run(app, 20)

    started = [
        (r["sim_ts"], r["key"])
        for r in events
        if r["task"] == "pack" and r["value"].get("status") == "started"
    ]
    # One box for orders-1, then two each: orders-2 waits for the restock at 10
    assert started == [(1.0, "orders-1"), (10.0, "orders-2"), (10.0, "orders-3")]
    collected = [
        r["value"]
        for r in events
        if r["task"] == "collect" and "collected" in r["value"]
    ]
    assert collected == [{"collected": "orders-1"}, {"collected": "orders-2"}]
    assert app.get_container("stock").level == 6.0


@pytest.mark.parametrize(
    "rule,fragment",
    [
        (
            {"next": {"when": [{"condition": "level.shelf > 0", "task": "pack"}]}},
            "level.shelf",
        ),
        ({"priority": "level.shelf"}, "level.shelf"),
        ({"get": {"container": "stock", "amount": "level.shelf"}}, "level.shelf"),
        (
            {
                "next": {
                    "when": [{"condition": 'registry["Shop.x"] > 0', "task": "pack"}]
                }
            },
            'registry["Shop.x"]',
        ),
        ({"priority": 'registry["Shop.x"]'}, 'registry["Shop.x"]'),
        (
            {"put": {"container": "stock", "amount": 'registry["Shop.x"]'}},
            'registry["Shop.x"]',
        ),
    ],
    ids=[
        "condition-level",
        "priority-level",
        "amount-level",
        "condition-registry",
        "priority-registry",
        "amount-registry",
    ],
)
def test_run_refuses_a_name_an_expression_reads_before_the_clock_starts(rule, fragment):
    app = (
        SimulationContext("Shop", factor=0, random_seed=1)
        .add_resource("packers", 1, 1, queue="priority")
        .add_service("packing", "constant", value=1)
        .add_container("stock", 5, 5)
    )
    app.task(service_id="packing", resource_id="packers", **rule)(lambda task_id: {})
    # A path the run registers is accepted
    app.task(
        name="pack",
        service_id="packing",
        resource_id="packers",
        priority='registry["Shop.service.packing.value"]',
    )(lambda task_id: {})
    with pytest.raises(ValueError, match="not a registry path|no store or") as error:
        app.run(until=1)
    assert fragment in str(error.value)
    assert app._env is None  # refused before the environment exists


def test_a_router_with_weights_needs_a_generator():
    router = Router({"weights": {"ship": 1}})
    with pytest.raises(ValueError, match="random generator"):
        router.choose(Upstream(0, "orders-1", {}, {}))
    with pytest.raises(ValueError, match="not both"):
        Router({"weights": {"ship": 1}, "default": "pack"})


BLUEPRINT = """\
simulation: {sim_id: Shop, factor: 0, random_seed: 1}
resources:
  packers: {current_cap: 1, max_cap: 1}
stores:
  parcels: {current_cap: 5, max_cap: 5}
containers:
  stock: {current_cap: 5, max_cap: 5, level: 5}
services:
  packing: {dist: constant, value: 1}
arrivals:
  orders: {dist: constant, value: 1, spawn: pick, attributes: {gift: 1}}
tasks:
  pick:
    payload: {event_type: picked}
    attributes: [gift]
    next:
      when:
        - {condition: "level.stock > 0", task: pack}
  pack:
    service: packing
    resource: packers
    get: {container: stock}
    payload: {event_type: packed}
run: {until: 3}
"""


def test_a_task_started_by_an_arrival_and_by_next_never_repeats_an_instance(
    tmp_path,
):
    # Arrival a spawns work; arrival b spawns first, whose next is work
    app = SimulationContext.from_yaml(
        _write(
            tmp_path,
            """\
            simulation: {sim_id: Shop, factor: 0, random_seed: 1}
            resources:
              packers: {current_cap: 1, max_cap: 1}
            services:
              packing: {dist: constant, value: 15}
            arrivals:
              a: {dist: constant, value: 10, spawn: work}
              b: {dist: constant, value: 10, spawn: first}
            tasks:
              first: {payload: {}, next: work}
              work: {service: packing, resource: packers, payload: {}}
            stats: {level: typical, interval: 1000}
            """,
        )
    )
    events = [r for r in _run(app, 100) if r["task"] == "work"]
    owners = {}
    for r in events:
        assert owners.setdefault(r["instance"], r["key"]) == r["key"]
    status = [(r["key"], r["sim_ts"], r["value"].get("status")) for r in events]
    queued = {key: at for key, at, state in status if state == "queued"}
    waits = [at - queued[key] for key, at, state in status if state == "started"]
    assert len(waits) > 4
    readings = app.env.stats.readings()
    assert readings["Shop.stats.tasks.work.wait_mean"] == pytest.approx(
        sum(waits) / len(waits)
    )


def _write(tmp_path, text):
    path = tmp_path / "flow.yaml"
    path.write_text(textwrap.dedent(text), encoding="utf-8")
    return path


def test_yaml_routes_and_includes_only_the_attributes_it_names(tmp_path):
    app = SimulationContext.from_yaml(_write(tmp_path, BLUEPRINT))
    events = _run(app, 3)
    assert [(r["key"], r["task"], r["value"]) for r in events][:4] == [
        ("orders-1", "pick", {"event_type": "picked", "gift": 1}),
        ("orders-1", "pack", {"path_id": "Shop.service.packing", "status": "queued"}),
        ("orders-1", "pack", {"path_id": "Shop.service.packing", "status": "started"}),
        ("orders-2", "pick", {"event_type": "picked", "gift": 1}),
    ]


@pytest.mark.parametrize(
    "old,new,line,fragment",
    [
        ("task: pack}", "task: ship}", 18, "routes to 'ship'"),
        ("level.stock > 0", "level.shelf > 0", 18, "level.shelf names no store"),
        ("level.stock > 0", 'registry[\\"Shop.x\\"] > 0', 18, "not a registry path"),
        ("level.stock > 0", "stock > 0", 18, "the name 'stock'"),
        ("{container: stock}", "{store: shelf}", 22, "store 'shelf'"),
        (
            "    resource: packers\n",
            "    resource: packers\n    priority: 1\n",
            22,
            "queue: priority",
        ),
        ("    service: packing\n    resource", "    resource", 19, "needs a service"),
        ("stores:\n  parcels:", "stores:\n  stock:", 5, "both a store and a container"),
    ],
)
def test_yaml_flow_errors_name_the_line(tmp_path, old, new, line, fragment):
    assert old in BLUEPRINT
    path = _write(tmp_path, BLUEPRINT.replace(old, new))
    with pytest.raises(BlueprintError) as error:
        SimulationContext.from_yaml(path)
    message = str(error.value)
    assert message.startswith(f"{path}:{line}:"), message
    assert fragment in message


def test_a_child_flow_that_raises_gives_its_unit_back():
    """A child flow whose payload cannot be published raises; the wait that started
    it fails, and the unit the child held goes back, as a `with` block gives it
    back, so the next order is packed."""
    from desflow import SimulationContext

    import asyncio
    import queue

    class Sink:
        async def run(self, egress_queue):
            while True:
                try:
                    egress_queue.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(0.01)

    app = SimulationContext("Shop", factor=0).add_egress(Sink())
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("packing", dist="constant", value=1)

    @app.task(service_id="packing", resource_id="packers")
    def broken(task_id):
        return {"label": object()}  # not a JSON value, so publishing it raises

    @app.task(wait={"task": "broken"})
    def order(task_id):
        return {"event": "ordered"}

    @app.task(service_id="packing", resource_id="packers")
    def pack(task_id):
        return {"event": "packed"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(5)
        context.spawn(pack(0))

    app.add_process(script)
    app.start(until=20)
    try:
        app.env.run(until=20)
        packers = app.get_resource("packers")
        assert packers.in_use == 0
    finally:
        app.close()


class _Keep:
    """An egress that keeps every record."""

    def __init__(self):
        self.records = []

    async def run(self, egress_queue):
        while True:
            try:
                self.records.extend(egress_queue.get_nowait())
            except queue.Empty:
                await asyncio.sleep(0.01)


def _statuses(records):
    return [
        (r["sim_ts"], r["value"].get("status", r["value"].get("event")))
        for r in records
        if r["stream_type"] == "event"
    ]


def test_a_wait_and_a_patience_that_end_at_the_same_instant():
    """The patience timer was scheduled first, so it wins the tie and the order
    reneges, as `yield wait | patience` does in SimPy; the wait's own branch,
    triggered at that instant but not yet processed, is not read."""
    keep = _Keep()
    app = SimulationContext("Shop", factor=0).add_egress(keep)

    @app.task(wait={"timeout": 5}, patience=5)
    def order(task_id):
        return {"event": "went_on"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=10)
    assert _statuses(keep.records) == [(0.0, "queued"), (5.0, "reneged")]


def test_a_resumed_wait_waits_again_for_the_same_child_flow():
    """An order waits for its inspection, a child flow of 10 s; an interrupt at 3
    with `resume` makes it wait again for that same inspection, which runs once
    and ends at 10, when the order goes on."""
    keep = _Keep()
    app = SimulationContext("Shop", factor=0).add_egress(keep)
    app.add_service("inspecting", dist="constant", value=10)

    @app.task(service_id="inspecting")
    def inspect(task_id):
        return {"event": "inspected"}

    @app.task(wait={"task": "inspect"}, on_interrupt="resume")
    def order(task_id):
        return {"event": "ordered"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(3)
        context.interrupt("order", cause="recount")

    app.add_process(script)
    app.run(until=30)
    events = [
        (r["sim_ts"], r["task"], r["value"].get("status", r["value"].get("event")))
        for r in keep.records
        if r["stream_type"] == "event"
    ]
    assert [e for e in events if e[2] == "inspected"] == [
        (10.0, "inspect", "inspected")
    ]
    assert (10.0, "order", "ordered") in events


def test_a_restarted_wait_starts_the_child_flow_again():
    """An interrupt with `restart` during a wait for a child flow starts that child
    flow a second time, where `resume` waits for the same one."""
    keep = _Keep()
    app = SimulationContext("Shop", factor=0).add_egress(keep)
    app.add_service("inspecting", dist="constant", value=10)

    @app.task(service_id="inspecting")
    def inspect(task_id):
        return {"event": "inspected"}

    @app.task(wait={"task": "inspect"}, on_interrupt="restart")
    def order(task_id):
        return {"event": "ordered"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(3)
        context.interrupt("order", cause="recount")

    app.add_process(script)
    app.run(until=40)
    events = [
        (r["sim_ts"], r["task"], r["value"].get("event"))
        for r in keep.records
        if r["stream_type"] == "event"
    ]
    inspected = sorted(t for t, task, event in events if event == "inspected")
    # The first inspection started at 0 keeps running to 10; restart at 3 starts a
    # second one, which ends at 13, when the order goes on.
    assert inspected == [10.0, 13.0]
    assert (13.0, "order", "ordered") in events


def test_a_negative_wait_timeout_is_refused():
    """A wait timeout is a duration of 0 or more, so a negative one is refused."""
    from desflow.core.flow import Wait

    with pytest.raises(ValueError, match="must be 0 or more"):
        Wait({"timeout": -1})


@pytest.mark.parametrize(
    "spec",
    [
        {"running": {"task": "inspect"}},
        {"any": {"a": {"running": {"task": "inspect"}}}},
    ],
)
def test_running_branch_parses(spec):
    """A `running` branch parses, named after its task, in a single branch and inside
    a group, and `targets("running")` and `targets("task")` name its task."""
    from desflow.core.flow import Wait

    wait = Wait(spec)
    assert wait.targets("running") == {"inspect"}
    assert "inspect" in wait.targets("task")


def test_at_least_above_its_branches_is_refused():
    """A non-empty `at_least` whose count is above its branch number never triggers
    in SimPy, so desflow refuses it at load."""
    from desflow.core.flow import Wait

    with pytest.raises(ValueError, match="above the"):
        Wait({"at_least": 3, "of": {"a": {"timeout": 1}, "b": {"timeout": 2}}})


def test_a_single_timeout_branch_is_read_as_wait_timeout_done():
    """A single timeout branch is named after its kind, so `wait.timeout.done` reads
    it, and it is true once the timeout fires."""
    keep = _Keep()
    app = SimulationContext("Shop", factor=0).add_egress(keep)

    @app.task(
        wait={"timeout": 5},
        next={"when": [{"condition": "wait.timeout.done", "task": "after"}]},
        name="start",
    )
    def start(task_id):
        return {"event": "started"}

    @app.task(name="after")
    def after(task_id):
        return {"event": "after"}

    def script(context):
        context.spawn(start(0))
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=20)
    events = [
        (r["sim_ts"], r["task"], r["value"].get("event"))
        for r in keep.records
        if r["stream_type"] == "event"
    ]
    assert (5.0, "start", "started") in events
    assert (5.0, "after", "after") in events


def test_a_preempted_task_gives_a_kept_unit_back_and_the_stat_counts_it():
    """A task preempted while keeping a second unit for its next task gives that kept
    unit back before its preempted record, so a task waiting for it starts, and the
    preempted statistic counts the preemption."""
    keep = _Keep()
    app = SimulationContext("Shop", factor=0).add_egress(keep)
    app.with_stats("basic", interval=1000)
    app.add_resource(
        "machine", current_cap=1, max_cap=1, queue="priority", preempt=True
    )
    app.add_resource("tray", current_cap=1, max_cap=1, queue="priority")
    app.add_service("working", dist="constant", value=10)
    app.add_service("finishing", dist="constant", value=1)
    app.add_service("grabbing", dist="constant", value=1)
    app.add_service("rushing", dist="constant", value=1)

    @app.task(
        service_id="working",
        resource_id=["tray", "machine"],
        keep=["tray"],
        next="finish",
        priority=5,
        on_interrupt="resume",
        name="work",
    )
    def work(task_id):
        return {"event": "worked"}

    @app.task(service_id="finishing", resource_id="tray", name="finish")
    def finish(task_id):
        return {"event": "finished"}

    @app.task(service_id="grabbing", resource_id="tray", name="grab")
    def grab(task_id):
        return {"event": "grabbed"}

    @app.task(service_id="rushing", resource_id="machine", priority=0, name="rush")
    def rush(task_id):
        return {"event": "rushed"}

    def script(context):
        context.spawn(work(0))  # holds the machine and the tray, keeps the tray
        context.spawn(grab(0))  # waits for the tray
        yield context.env.timeout(3)
        context.spawn(rush(0))  # preempts work on the machine

    app.add_process(script)
    app.run(until=40)
    events = [
        (r["sim_ts"], r["task"], r["value"].get("status", r["value"].get("event")))
        for r in keep.records
        if r["stream_type"] == "event"
    ]
    assert (3.0, "work", "preempted") in events
    grab_started = [t for t, task, s in events if task == "grab" and s == "started"]
    assert grab_started == [3.0]
    # The kept unit is given back before the preempted record, so the record is
    # published first and grab starts in the next step.
    preempted_index = events.index((3.0, "work", "preempted"))
    started_index = next(
        i for i, (t, task, s) in enumerate(events) if task == "grab" and s == "started"
    )
    assert preempted_index < started_index
    assert app.env.stats.readings()["Shop.stats.tasks.work.preempted"] == 1


def test_a_losing_child_flow_that_fails_later_does_not_stop_the_run():
    """A child flow a wait branch started keeps running when another branch wins; its
    later failure is defused, so the run goes on past it."""
    keep = _Keep()
    app = SimulationContext("Shop", factor=0).add_egress(keep)
    app.add_service("slow", dist="constant", value=10)

    @app.task(service_id="slow", name="inspect")
    def inspect(task_id):
        return {"label": object()}  # not a JSON value, so publishing it fails the child

    @app.task(
        wait={"any": {"quick": {"timeout": 5}, "deep": {"task": "inspect"}}},
        name="order",
    )
    def order(task_id):
        return {"event": "ordered"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(0)

    app.add_process(script)
    app.run(until=30)
    events = [
        (r["sim_ts"], r["task"], r["value"].get("event"))
        for r in keep.records
        if r["stream_type"] == "event"
    ]
    # The timeout branch wins at 5, so the order goes on; the inspect child keeps
    # running and fails at 10, which does not stop the run.
    assert (5.0, "order", "ordered") in events
    assert app.env.now == 30.0

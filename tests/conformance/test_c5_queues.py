"""C5 queue lengths: a condition can wait for putters or getters to queue up.

A plain SimPy `Store` or `Container` subclass records the time its put or get queue
first holds the threshold; a watcher in every API must resume at that time. A task
that gives up while it waits (`patience`) or is interrupted (`abandon`) leaves the
queue, and a condition on the lower count sees that.
"""

import simpy

from desflow import (
    CapacityConfig,
    ContainerConfig,
    DynamicContainer,
    DynamicStore,
    SimParameter,
)
from desflow.core.context import SimulationContext
from tests.conformance.harness import (
    run_builder,
    run_low_level,
    run_yaml_text,
    times,
)

UNTIL = 14
WATCHER_AT = 0.5
THRESHOLD = 2


def _simpy_classes(side):
    queue = "put_queue" if side == "putters" else "get_queue"

    def _note(self):
        if self.first is None and len(getattr(self, queue)) >= THRESHOLD:
            if self._env.now >= WATCHER_AT:
                self.first = self._env.now

    class WatchedStore(simpy.Store):
        first = None

        def _trigger_put(self, get_event):
            super()._trigger_put(get_event)
            _note(self)

        def _trigger_get(self, put_event):
            super()._trigger_get(put_event)
            _note(self)

    class WatchedContainer(simpy.Container):
        first = None

        def _trigger_put(self, get_event):
            super()._trigger_put(get_event)
            _note(self)

        def _trigger_get(self, put_event):
            super()._trigger_get(put_event)
            _note(self)

    return WatchedStore, WatchedContainer


def _reference(kind, side, put_gap, get_gap, capacity, amount):
    store_cls, container_cls = _simpy_classes(side)
    env = simpy.Environment()
    holder = (
        store_cls(env, capacity=capacity)
        if kind == "store"
        else container_cls(env, capacity=capacity, init=0)
    )

    def put(n):
        yield holder.put(f"item-{n}") if kind == "store" else holder.put(amount)

    def get():
        yield holder.get() if kind == "store" else holder.get(amount)

    def source(gap, make):
        n = 0
        while True:
            yield env.timeout(gap)
            n += 1
            env.process(make(n))

    env.process(source(put_gap, put))
    env.process(source(get_gap, lambda n: get()))
    env.run(until=UNTIL)
    return holder.first


def _params(kind, capacity):
    if kind == "store":
        return SimParameter(
            sim_id="C5",
            stores={"shelf": CapacityConfig(current_cap=capacity, max_cap=capacity)},
        )
    return SimParameter(
        sim_id="C5",
        containers={
            "shelf": ContainerConfig(current_cap=capacity, max_cap=capacity, level=0)
        },
    )


def _low_level(kind, side, put_gap, get_gap, capacity, amount):
    def start(env):
        holder = (
            DynamicStore(env, "C5", "shelf")
            if kind == "store"
            else DynamicContainer(env, "C5", "shelf")
        )
        waiting = (
            (lambda: holder.putters_waiting)
            if side == "putters"
            else (lambda: holder.getters_waiting)
        )

        def watcher():
            yield env.timeout(WATCHER_AT)
            done = env.event()

            def check():
                if waiting() >= THRESHOLD and not done.triggered:
                    done.succeed()
                    env.unwatch_changes(check)

            env.watch_changes(check, queues=True)
            check()
            yield done
            env.publish_event("w-0", {"event": "resumed"}, task="watcher", instance=0)

        def put(n):
            yield holder.put(f"item-{n}") if kind == "store" else holder.put(amount)

        def get():
            yield holder.get() if kind == "store" else holder.get(amount)

        def source(gap, make):
            n = 0
            while True:
                yield env.timeout(gap)
                n += 1
                env.process(make(n))

        env.process(watcher())
        env.process(source(put_gap, put))
        env.process(source(get_gap, lambda n: get()))

    records, _env = run_low_level(_params(kind, capacity), start, UNTIL)
    return records


def _builder(kind, side, put_gap, get_gap, capacity, amount):
    app = SimulationContext("C5", factor=0.0)
    if kind == "store":
        app.add_store("shelf", current_cap=capacity, max_cap=capacity)
        spec = {"store": "shelf"}
    else:
        app.add_container("shelf", current_cap=capacity, max_cap=capacity)
        spec = {"container": "shelf", "amount": amount}
    app.add_arrival("puts", dist="constant", value=put_gap)
    app.add_arrival("gets", dist="constant", value=get_gap)

    @app.task(name="put", put=spec)
    def put(task_id):
        return {"event": "put"}

    @app.task(name="get", get=spec)
    def get(task_id):
        return {"event": "get"}

    @app.task(name="watcher", wait={"condition": f"{side}.shelf >= {THRESHOLD}"})
    def watcher(task_id):
        return {"event": "resumed"}

    for arrival, task, name in (("puts", put, "put"), ("gets", get, "get")):

        def loop(context, arrival=arrival, task=task, name=name):
            while True:
                entity = yield context.wait_for_arrival(arrival)
                context.spawn(task(context.next_instance(name), entity=entity))

        app.arrival_loop(arrival)(loop)

    def later(context):
        yield context.env.timeout(WATCHER_AT)
        context.spawn(watcher(0))

    app.add_process(later)
    return run_builder(app, UNTIL)


def _yaml(tmp_path, kind, side, put_gap, get_gap, capacity, amount):
    if kind == "store":
        holder = f"stores:\n  shelf: {{current_cap: {capacity}, max_cap: {capacity}}}"
        put = "put: {store: shelf}"
        get = "get: {store: shelf}"
    else:
        holder = (
            f"containers:\n  shelf: {{current_cap: {capacity}, max_cap: {capacity}}}"
        )
        put = f"put: {{container: shelf, amount: {amount}}}"
        get = f"get: {{container: shelf, amount: {amount}}}"
    text = f"""
simulation: {{sim_id: C5, factor: 0}}
{holder}
arrivals:
  puts: {{dist: constant, value: {put_gap}, spawn: put}}
  gets: {{dist: constant, value: {get_gap}, spawn: get}}
  watchers: {{replay: {{path: watchers.csv}}, spawn: watcher}}
tasks:
  put:
    {put}
    payload: {{event: put}}
  get:
    {get}
    payload: {{event: get}}
  watcher:
    wait: {{condition: "{side}.shelf >= {THRESHOLD}"}}
    payload: {{event: resumed}}
run: {{until: {UNTIL}}}
"""
    return run_yaml_text(tmp_path, text, {"watchers": [WATCHER_AT]}).records


def _check(tmp_path, kind, side, put_gap, get_gap, capacity, amount):
    args = (kind, side, put_gap, get_gap, capacity, amount)
    first = _reference(*args)
    assert first is not None
    for records in (
        _low_level(*args),
        _builder(*args),
        _yaml(tmp_path, *args),
    ):
        assert times(records, "watcher") == [first]
    return first


def test_c5_store_put_and_get_queues(tmp_path):
    """Puts every 1 s into a store of 1, gets every 4 s: the third put waits at 3, so
    two putters queue and a watcher on `putters.shelf >= 2` resumes there. With puts
    every 4 s and gets every 1 s, the second get waits at 2 for `getters.shelf >= 2`."""
    assert _check(tmp_path, "store", "putters", 1, 4, 1, 1) == 3
    assert _check(tmp_path, "store", "getters", 4, 1, 1, 1) == 2


def test_c5_container_put_and_get_queues(tmp_path):
    """The same for a container: puts of 2 every 1 s into a capacity of 2, gets of 2
    every 4 s; then puts every 4 s and gets every 1 s."""
    assert _check(tmp_path, "container", "putters", 1, 4, 2, 2) == 3
    assert _check(tmp_path, "container", "getters", 4, 1, 2, 2) == 2


def _reference_leaving():
    """Plain SimPy: a getter at 1 gives up after 1.5 s (gone at 2.5) and one at 2 is
    interrupted at 4. The get queue length at 1.5, 3 and 4.5."""
    env = simpy.Environment()
    store = simpy.Store(env, capacity=1)
    seen = []

    def patient():
        yield env.timeout(1)
        request = store.get()
        result = yield request | env.timeout(1.5)
        if request not in result:
            request.cancel()

    def interrupted():
        yield env.timeout(2)
        request = store.get()
        try:
            yield request
        except simpy.Interrupt:
            request.cancel()

    def look():
        for moment in (1.5, 3, 4.5):
            yield env.timeout(moment - env.now)
            seen.append((moment, len(store.get_queue)))

    env.process(patient())
    waiter = env.process(interrupted())
    env.process(look())

    def recall():
        yield env.timeout(4)
        waiter.interrupt("recall")

    env.process(recall())
    env.run(until=6)
    return seen


def test_c5_queue_count_falls_when_a_waiter_leaves():
    """A getter that gives up (`patience`) or is interrupted (`abandon`) leaves the
    get queue, so `getters.shelf` is 1, then 2, then 1, then 0, and a condition on
    `getters.shelf == 0` resumes when the last one has left, at 4."""
    assert _reference_leaving() == [(1.5, 1), (3, 1), (4.5, 0)]

    app = SimulationContext("C5", factor=0.0)
    app.add_store("shelf", current_cap=1, max_cap=1)

    @app.task(name="patient", get={"store": "shelf"}, patience=1.5)
    def patient(task_id):
        return {"event": "got"}

    @app.task(name="interrupted", get={"store": "shelf"}, on_interrupt="abandon")
    def interrupted(task_id):
        return {"event": "got"}

    @app.task(name="watcher", wait={"condition": "t >= 3 and getters.shelf == 0"})
    def watcher(task_id):
        return {"event": "resumed"}

    def script(context):
        yield context.env.timeout(1)
        context.spawn(patient(0))
        yield context.env.timeout(1)
        context.spawn(interrupted(0))
        context.spawn(watcher(0))
        yield context.env.timeout(2)
        context.interrupt("interrupted", cause="recall")

    app.add_process(script)
    records = run_builder(app, 6)
    assert times(records, "watcher") == [4.0]

"""Nested and mixed conditions (C5) and a custom at-least rule (C6).

Each test compares a plain SimPy model with the same wait in the low-level, builder
and YAML interfaces. The comparison is the simulation time the wait resolves at, read
from the published event trace. The order is spawned once at time 0 in every API, so
the resolve times are directly comparable.
"""

import pytest
import simpy

from desflow import Entity, SimulationContext
from tests.conformance.harness import (
    run_builder,
    run_low_level,
    run_yaml,
    run_yaml_text,
    times,
)


def _reference_nested():
    """`(fast | slow) & inspect`: done when inspect (3) and the first of fast (1) and
    slow (10) are both done, so at 3."""
    env = simpy.Environment()
    seen = []

    def order():
        fast, slow, inspect = env.timeout(1), env.timeout(10), env.timeout(3)
        yield (fast | slow) & inspect
        seen.append(env.now)

    env.process(order())
    env.run()
    return seen


def _low_level_nested():
    def start(env):
        def order():
            fast, slow, inspect = env.timeout(1), env.timeout(10), env.timeout(3)
            yield (fast | slow) & inspect
            env.publish_event("o-0", {"event": "went_on"}, task="order", instance=0)

        env.process(order())

    return run_low_level(None, start, 20)[0]


NESTED_WAIT = {
    "all": {
        "look": {"timeout": 3},
        "either": {"any": {"fast": {"timeout": 1}, "slow": {"timeout": 10}}},
    }
}


def _builder_nested():
    app = SimulationContext("C2", factor=0.0)

    @app.task(wait=NESTED_WAIT, name="order")
    def order(task_id):
        return {"event": "went_on"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(0)

    app.add_process(script)
    return run_builder(app, 20)


def test_c2_nested_and_mixed_conditions():
    """A wait nesting `any` inside `all` resolves at SimPy's time: the inner `any`
    is done at 1 and the `look` timeout at 3, so the `all` resolves at 3."""
    assert _reference_nested() == [3]
    for records in (
        _low_level_nested(),
        _builder_nested(),
        run_yaml("c2_nested.yaml")[0],
    ):
        assert times(records, "order") == [3.0]


def _reference_at_least(count):
    """`Condition(count >= n)` over timeouts at 2, 5 and 9: done at the n-th."""
    env = simpy.Environment()
    seen = []

    def order():
        events = [env.timeout(t) for t in (2, 5, 9)]
        yield simpy.events.Condition(env, lambda evs, c, n=count: c >= n, events)
        seen.append(env.now)

    env.process(order())
    env.run()
    return seen


def _low_level_at_least(count):
    def start(env):
        def order():
            events = [env.timeout(t) for t in (2, 5, 9)]
            yield simpy.events.Condition(env, lambda evs, c, n=count: c >= n, events)
            env.publish_event("o-0", {"event": "went_on"}, task="order", instance=0)

        env.process(order())

    return run_low_level(None, start, 20)[0]


def _builder_at_least(count):
    app = SimulationContext("C2", factor=0.0)

    @app.task(
        wait={
            "at_least": count,
            "of": {"a": {"timeout": 2}, "b": {"timeout": 5}, "c": {"timeout": 9}},
        },
        name="order",
    )
    def order(task_id):
        return {"event": "went_on"}

    def script(context):
        context.spawn(order(0))
        yield context.env.timeout(0)

    app.add_process(script)
    return run_builder(app, 20)


def test_c2_condition_with_a_custom_rule():
    """`at_least: 2` of three timeouts at 2, 5 and 9 resolves at the second, 5, as
    SimPy's `Condition` with a count rule does."""
    assert _reference_at_least(2) == [5]
    for records in (
        _low_level_at_least(2),
        _builder_at_least(2),
        run_yaml("c2_at_least.yaml")[0],
    ):
        assert times(records, "order") == [5.0]


def test_c2_custom_rule_equal_to_the_branch_count_is_all():
    """`at_least` equal to the branch count behaves as `all`, resolving at the last
    timeout, 9, in SimPy and the builder."""
    assert _reference_at_least(3) == [9]
    assert times(_builder_at_least(3), "order") == [9.0]


def _reference_request():
    env = simpy.Environment()
    machine = simpy.Resource(env, capacity=1)
    seen = []

    def order(n):
        with machine.request() as req:
            yield req
            yield env.timeout(5)
        seen.append((n, round(env.now, 3)))

    env.process(order(0))
    env.process(order(1))
    env.run()
    return seen


def _builder_request():
    app = SimulationContext("C2", factor=0.0)
    app.add_resource("m", current_cap=1, max_cap=1)
    app.add_service("work", dist="constant", value=5)

    @app.task(wait={"request": "m"}, service_id="work", name="order")
    def order(task_id):
        return {"event": "done"}

    def script(context):
        context.spawn(order(0, entity=Entity("o-0", "o", {})))
        context.spawn(order(1, entity=Entity("o-1", "o", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    return run_builder(app, 20)


def test_c2_request_as_a_wait_branch():
    """A request wait branch holds its won unit through the service and gives it back
    when the task ends, so the second order is served at 5 and ends at 10, as SimPy's
    `with resource.request()` does."""
    assert _reference_request() == [(0, 5), (1, 10)]
    for records in (_builder_request(), run_yaml("c2_request.yaml")[0]):
        assert times(records, "order") == [5.0, 10.0]


def _builder_get():
    app = SimulationContext("C2", factor=0.0)
    app.add_store("parcels", current_cap=5, max_cap=5, items=["box"])

    @app.task(
        wait={"get": {"store": "parcels"}},
        next={"when": [{"condition": "wait.get.value == 'box'", "task": "got"}]},
        name="order",
    )
    def order(task_id):
        return {"event": "took"}

    @app.task(name="got")
    def got(task_id):
        return {"event": "got"}

    def script(context):
        context.spawn(order(0, entity=Entity("o-0", "o", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    return run_builder(app, 5)


def test_c2_get_as_a_wait_branch():
    """A get wait branch takes an item from the store, and `wait.<branch>.value` is
    that item, so the order routes on it to `got`."""
    for records in (_builder_get(), run_yaml("c2_get.yaml")[0]):
        assert times(records, "got") == [0.0]


def _builder_put():
    app = SimulationContext("C2", factor=0.0)
    app.add_store("parcels", current_cap=5, max_cap=5)

    @app.task(wait={"put": {"store": "parcels"}}, name="order")
    def order(task_id):
        return {"sku": "box"}

    @app.task(get={"store": "parcels"}, name="taker")
    def taker(task_id):
        return {"event": "taken"}

    def script(context):
        context.spawn(order(0, entity=Entity("o-0", "o", {})))
        yield context.env.timeout(1)
        context.spawn(taker(0, entity=Entity("t-0", "t", {})))

    app.add_process(script)
    return run_builder(app, 5)


def test_c2_put_as_a_wait_branch():
    """A put wait branch puts the order's payload into the store, so a later taker
    gets it."""
    for records in (_builder_put(), run_yaml("c2_put.yaml")[0]):
        assert times(records, "order")  # the order put its payload and went on


def test_c2_a_wait_branch_on_a_missing_store_is_refused():
    """A get, put or request wait branch on a resource, store or container that is not
    added is refused when the run starts, not left to crash at run time."""
    app = SimulationContext("C2", factor=0.0)

    @app.task(wait={"get": {"store": "nope"}}, name="order")
    def order(task_id):
        return {}

    def script(context):
        context.spawn(order(0, entity=Entity("o-0", "o", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    with pytest.raises(ValueError, match="which is not added"):
        app.run(until=5)


def test_c2_wait_branch_in_a_failing_all():
    """When an `all` wait fails because a member signal fails, a request branch that
    won its unit gives the unit back and leaves no reservation."""
    app = SimulationContext("C2", factor=0.0).add_signal("restocked")
    app.add_resource("m", current_cap=1, max_cap=1)

    @app.task(
        wait={"all": {"req": {"request": "m"}, "boom": {"signal": "restocked"}}},
        name="order",
    )
    def order(task_id):
        return {"event": "went"}

    @app.task(
        signal="restocked", fail="payload.x == 1", fail_reason="nope", name="boom"
    )
    def boom(task_id):
        return {"x": 1}

    def script(context):
        context.spawn(order(0, entity=Entity("o-0", "o", {})))
        yield context.env.timeout(1)
        context.spawn(boom(0, entity=Entity("b-0", "b", {})))

    app.add_process(script)
    records = run_builder(app, 10)
    assert times(records, "order", "failed") == [1.0]
    assert app.get_resource("m").in_use == 0
    assert app.get_resource("m").queue == []


def test_c2_request_wait_branch_cleaned_on_renege():
    """A request wait branch that is still waiting when the patience runs out leaves
    the queue, so a later free unit is not reserved for the gone task."""
    app = SimulationContext("C2", factor=0.0)
    app.add_resource("m", current_cap=1, max_cap=1)
    app.add_service("w", dist="constant", value=100)

    @app.task(service_id="w", resource_id="m", name="hold")
    def hold(task_id):
        return {}

    @app.task(wait={"request": "m"}, patience=2, name="waiter")
    def waiter(task_id):
        return {"event": "went"}

    def script(context):
        context.spawn(hold(0, entity=Entity("h", "h", {})))
        context.spawn(waiter(0, entity=Entity("w", "w", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    records = run_builder(app, 10)
    assert times(records, "waiter", "reneged") == [2.0]
    assert app.get_resource("m").queue == []


def test_c2_request_wait_branch_reused_across_a_resume_in_the_wait():
    """An interrupt with resume while a request wait branch is still waiting reuses
    the same request, so the queue never holds a second one for the one waiter."""
    app = SimulationContext("C2", factor=0.0)
    app.add_resource("m", current_cap=1, max_cap=1)
    app.add_service("w", dist="constant", value=100)

    @app.task(service_id="w", resource_id="m", name="hold")
    def hold(task_id):
        return {}

    @app.task(wait={"request": "m"}, on_interrupt="resume", name="waiter")
    def waiter(task_id):
        return {"event": "went"}

    def script(context):
        context.spawn(hold(0, entity=Entity("h", "h", {})))
        context.spawn(waiter(0, entity=Entity("w", "w", {})))
        yield context.env.timeout(3)
        context.interrupt("waiter")
        yield context.env.timeout(3)
        context.interrupt("waiter")
        yield context.env.timeout(0)

    app.add_process(script)
    run_builder(app, 20)
    assert len(app.get_resource("m").queue) == 1


def test_c2_request_wait_branch_kept_across_a_resume_in_the_service():
    """A won request unit is held through a service that is interrupted and resumed,
    and given back only when the task ends."""
    app = SimulationContext("C2", factor=0.0)
    app.add_resource("m", current_cap=1, max_cap=1)
    app.add_service("w", dist="constant", value=10)

    @app.task(
        wait={"request": "m"}, service_id="w", on_interrupt="resume", name="order"
    )
    def order(task_id):
        return {"event": "done"}

    seen = {}

    def script(context):
        context.spawn(order(0, entity=Entity("o", "o", {})))
        yield context.env.timeout(5)
        context.interrupt("order")
        yield context.env.timeout(0.01)
        seen["mid"] = app.get_resource("m").in_use

    app.add_process(script)
    records = run_builder(app, 30)
    assert seen["mid"] == 1
    assert times(records, "order")
    assert app.get_resource("m").in_use == 0


def _reference_nested_failure():
    env = simpy.Environment()
    seen = []

    def order():
        a, b = env.event(), env.timeout(5)

        def boom():
            yield env.timeout(2)
            a.fail(ValueError("nope"))

        env.process(boom())
        try:
            yield env.all_of([a | env.timeout(10), b])
            seen.append(("ok", env.now))
        except Exception:
            seen.append(("failed", round(env.now, 3)))

    env.process(order())
    env.run()
    return seen


def _builder_nested_failure():
    app = SimulationContext("C2", factor=0.0).add_signal("stock")

    @app.task(
        wait={
            "all": {
                "inner": {"any": {"s": {"signal": "stock"}, "cap": {"timeout": 10}}},
                "limit": {"timeout": 5},
            }
        },
        name="order",
    )
    def order(task_id):
        return {"event": "went"}

    @app.task(signal="stock", fail="payload.bad == 1", fail_reason="nope", name="boom")
    def boom(task_id):
        return {"bad": 1}

    def script(context):
        context.spawn(order(0, entity=Entity("o", "o", {})))
        yield context.env.timeout(2)
        context.spawn(boom(0, entity=Entity("b", "b", {})))

    app.add_process(script)
    return run_builder(app, 20)


def test_c2_nested_condition_with_a_failing_member():
    """A failing member inside a nested group fails the whole wait, at the time it
    fails, as SimPy raises from the condition then."""
    assert _reference_nested_failure() == [("failed", 2)]
    assert times(_builder_nested_failure(), "order", "failed") == [2.0]


def _reference_custom_rule_tie():
    env = simpy.Environment()
    seen = []

    def order():
        a, b, c = env.timeout(5), env.timeout(5), env.timeout(9)
        yield simpy.events.Condition(env, lambda evs, n: n >= 2, [a, b, c])
        seen.append(round(env.now, 3))

    env.process(order())
    env.run()
    return seen


def _builder_custom_rule_tie():
    app = SimulationContext("C2", factor=0.0)

    @app.task(
        wait={
            "at_least": 2,
            "of": {"a": {"timeout": 5}, "b": {"timeout": 5}, "c": {"timeout": 9}},
        },
        name="order",
    )
    def order(task_id):
        return {"event": "went"}

    def script(context):
        context.spawn(order(0, entity=Entity("o", "o", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    return run_builder(app, 20)


def test_c2_custom_rule_two_at_one_instant():
    """`at_least: 2` with two branches that fire at one instant is done then, as
    SimPy's count rule counts both at that instant."""
    assert _reference_custom_rule_tie() == [5]
    assert times(_builder_custom_rule_tie(), "order") == [5.0]


def _reference_custom_rule_failure():
    env = simpy.Environment()
    seen = []

    def order():
        a, b, c = env.event(), env.timeout(9), env.timeout(9)

        def boom():
            yield env.timeout(2)
            a.fail(ValueError("nope"))

        env.process(boom())
        try:
            yield simpy.events.Condition(env, lambda evs, n: n >= 2, [a, b, c])
            seen.append(("ok", env.now))
        except Exception:
            seen.append(("failed", round(env.now, 3)))

    env.process(order())
    env.run()
    return seen


def _builder_custom_rule_failure():
    app = SimulationContext("C2", factor=0.0).add_signal("stock")

    @app.task(
        wait={
            "at_least": 2,
            "of": {
                "s": {"signal": "stock"},
                "b": {"timeout": 9},
                "c": {"timeout": 9},
            },
        },
        name="order",
    )
    def order(task_id):
        return {"event": "went"}

    @app.task(signal="stock", fail="payload.bad == 1", fail_reason="nope", name="boom")
    def boom(task_id):
        return {"bad": 1}

    def script(context):
        context.spawn(order(0, entity=Entity("o", "o", {})))
        yield context.env.timeout(2)
        context.spawn(boom(0, entity=Entity("b", "b", {})))

    app.add_process(script)
    return run_builder(app, 20)


def test_c2_custom_rule_with_a_failing_member():
    """A failing member fails an `at_least` wait at the time it fails, as SimPy's
    `Condition` raises from the failed member."""
    assert _reference_custom_rule_failure() == [("failed", 2)]
    assert times(_builder_custom_rule_failure(), "order", "failed") == [2.0]


def _builder_untriggered_group():
    app = SimulationContext("C2", factor=0.0)

    @app.task(
        wait={
            "any": {
                "fast": {"timeout": 1},
                "slow": {"all": {"x": {"timeout": 10}, "y": {"timeout": 20}}},
            }
        },
        next={
            "when": [
                {
                    "condition": "wait.fast.done and not wait.slow.done",
                    "task": "just_fast",
                }
            ]
        },
        name="order",
    )
    def order(task_id):
        return {"event": "resumed"}

    @app.task(name="just_fast")
    def just_fast(task_id):
        return {"event": "just_fast"}

    def script(context):
        context.spawn(order(0, entity=Entity("o", "o", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    return run_builder(app, 30)


def test_c2_nested_condition_holds_leaves_of_an_untriggered_group():
    """When an `any` is won by one branch while an inner group has not triggered, the
    won branch is done and the untriggered group is not, so the order routes on it."""
    records = _builder_untriggered_group()
    assert times(records, "just_fast") == [1.0]
    # The value of a group is a flat map of every done leaf under it, as SimPy's
    # `ConditionValue` is: `x` is done inside `g`, which is not, and `outer` holds it
    assert _reference_flat_group_value() == {"a", "x"}
    assert times(_builder_flat_group_value(), "flat") == [5.0]


def _reference_flat_group_value():
    env = simpy.Environment()
    seen = set()

    def order():
        a = env.timeout(5, "a")
        x, y = env.timeout(1, "x"), env.timeout(100, "y")
        outer = a | (x & y)
        yield env.timeout(0) & outer
        seen.update(event.value for event in outer.value.todict())

    env.process(order())
    env.run(until=10)
    return seen


def _builder_flat_group_value():
    app = SimulationContext("C2", factor=0.0)

    @app.task(
        wait={
            "all": {
                "z": {"timeout": 0},
                "outer": {
                    "any": {
                        "a": {"timeout": {"after": 5, "value": "a"}},
                        "g": {
                            "all": {
                                "x": {"timeout": {"after": 1, "value": "x"}},
                                "y": {"timeout": {"after": 100, "value": "y"}},
                            }
                        },
                    }
                },
            }
        },
        next={
            "when": [
                {
                    "condition": "wait.outer.value.a == 'a' and wait.outer.value.x == 'x' "
                    "and not wait.g.done",
                    "task": "flat",
                }
            ]
        },
        name="order",
    )
    def order(task_id):
        return {"event": "resumed"}

    @app.task(name="flat")
    def flat(task_id):
        return {"event": "flat"}

    def script(context):
        context.spawn(order(0, entity=Entity("o", "o", {})))
        yield context.env.timeout(0)

    app.add_process(script)
    return run_builder(app, 10)


def test_c2_abandon_during_a_wait_with_a_won_branch_is_clean():
    """An abandon while an `all` wait is still pending, with a get and a request branch
    already won, cancels the branches without error and gives the request unit back."""
    app = SimulationContext("C2", factor=0.0)
    app.add_store("parcels", current_cap=5, max_cap=5, items=["box"])
    app.add_resource("m", current_cap=1, max_cap=1)

    @app.task(
        wait={
            "all": {
                "g": {"get": {"store": "parcels"}},
                "r": {"request": "m"},
                "t": {"timeout": 100},
            }
        },
        on_interrupt="abandon",
        name="order",
    )
    def order(task_id):
        return {"event": "went"}

    def script(context):
        context.spawn(order(0, entity=Entity("o-0", "o", {})))
        yield context.env.timeout(5)
        context.interrupt("order")
        yield context.env.timeout(1)

    app.add_process(script)
    run_builder(app, 20)
    assert app.get_resource("m").in_use == 0
    assert app.get_resource("m").queue == []


def test_c2_request_wait_branch_reacquired_after_a_preemption():
    """A unit won by a request wait branch and then taken by a preemption is requested
    again on resume, so the task finishes holding it, as a `resource:` unit is."""
    app = SimulationContext("C2", factor=0.0)
    app.add_resource("m", current_cap=1, max_cap=1, queue="priority", preempt=True)
    app.add_service("w", dist="constant", value=10)
    app.add_service("r", dist="constant", value=1)

    @app.task(
        wait={"request": "m"}, service_id="w", on_interrupt="resume", name="order"
    )
    def order(task_id):
        return {"event": "done"}

    @app.task(service_id="r", resource_id="m", priority=-1, name="rush")
    def rush(task_id):
        return {"event": "rushed"}

    def script(context):
        context.spawn(order(0, entity=Entity("o", "o", {})))
        yield context.env.timeout(3)
        context.spawn(rush(0, entity=Entity("r", "r", {})))

    app.add_process(script)
    records = run_builder(app, 40)
    assert times(records, "order", "preempted") == [3.0]
    assert times(records, "order") == [11.0]
    assert app.get_resource("m").in_use == 0


# ------------------------------------------- a branch granted as the wait ends (D-C1)


def _reference_get_granted_as_the_wait_ends(patience=False):
    """A get that is granted at the instant its wait ends, after the condition was
    processed, with the idiom `if not event.triggered: event.cancel()`: the get was
    granted, so the waiter has the item. With `patience`, the wait's own limit is 50
    and a patience of 5, made before the get, ends it."""
    env = simpy.Environment()
    store = simpy.Store(env, 5)
    seen = []

    def waiter():
        deadline = env.timeout(5) if patience else None
        got = store.get()
        if deadline is None:
            yield got | env.timeout(5)
        else:
            yield (got | env.timeout(50)) | deadline
        if not got.triggered:
            got.cancel()
        seen.append((env.now, got.triggered, got.value if got.triggered else None))

    def putter():
        yield env.timeout(5)
        yield store.put("box")

    # The putter starts first, so the get is granted after the condition was
    # processed and before the waiter resumes
    env.process(putter())
    env.process(waiter())
    env.run(until=10)
    return seen, list(store.items)


def _builder_get_granted_as_the_wait_ends(patience=False):
    app = SimulationContext("C2", factor=0.0)
    app.add_store("s", current_cap=5, max_cap=5)
    limit = 50 if patience else 5

    @app.task(
        wait={"any": {"g": {"get": {"store": "s"}}, "limit": {"timeout": limit}}},
        patience=5 if patience else None,
        next={"when": [{"condition": "wait.g.done", "task": "got"}]},
        name="waiter",
    )
    def waiter(task_id):
        return {"event": "waited"}

    @app.task(name="got")
    def got(task_id):
        return {"event": "got"}

    @app.task(put={"store": "s"}, name="putter")
    def putter(task_id):
        return {"sku": "box"}

    def script(context):
        context.spawn(waiter(0, entity=Entity("w", "w", {})))
        yield context.env.timeout(5)
        context.spawn(putter(0, entity=Entity("p", "p", {})))

    app.add_process(script)
    return run_builder(app, 10), app


GET_GRANTED_AS_THE_WAIT_ENDS = """
simulation: {sim_id: C2, factor: 0}
stores:
  s: {current_cap: 5, max_cap: 5}
arrivals:
  waiters: {replay: {path: waiters.csv}, spawn: waiter}
  putters: {replay: {path: putters.csv}, spawn: putter}
tasks:
  waiter:
    wait: {any: {g: {get: {store: s}}, limit: {timeout: 5}}}
    payload: {event: waited}
    next: {when: [{condition: wait.g.done, task: got}]}
  got:
    payload: {event: got}
  putter:
    put: {store: s}
    payload: {sku: box}
run: {until: 10}
"""


def test_c2_a_get_granted_as_the_wait_ends_counts_as_won(tmp_path):
    """An item put at the instant a wait times out is given to the get branch, which
    was granted after the condition was processed. The branch is done and the store
    is empty, as with SimPy's `if not event.triggered: event.cancel()`; the item is
    not lost. The same holds when the task's patience ends the wait."""
    seen, items = _reference_get_granted_as_the_wait_ends()
    assert seen == [(5, True, "box")]
    assert items == []
    records, app = _builder_get_granted_as_the_wait_ends()
    assert times(records, "got") == [5.0]
    assert list(app.get_store("s").items) == []
    outcome = run_yaml_text(
        tmp_path, GET_GRANTED_AS_THE_WAIT_ENDS, {"waiters": [0], "putters": [5]}
    )
    assert outcome.error is None
    assert times(outcome.records, "got") == [5.0]
    assert list(outcome.app_or_env.get_store("s").items) == []
    # The same when the task's patience, not the wait's own limit, ends the wait
    assert _reference_get_granted_as_the_wait_ends(patience=True) == (seen, items)
    records, app = _builder_get_granted_as_the_wait_ends(patience=True)
    assert times(records, "got") == [5.0]
    assert list(app.get_store("s").items) == []
    outcome = run_yaml_text(
        tmp_path,
        GET_GRANTED_AS_THE_WAIT_ENDS.replace(
            "limit: {timeout: 5}}}", "limit: {timeout: 50}}}\n    patience: 5"
        ),
        {"waiters": [0], "putters": [5]},
    )
    assert outcome.error is None
    assert times(outcome.records, "got") == [5.0]
    assert list(outcome.app_or_env.get_store("s").items) == []


def _reference_request_granted_as_the_wait_ends():
    """A request granted at the instant its wait ends: the waiter keeps the unit
    through its 3 s of work, so the contender, who asks at 6, is served at 8."""
    env = simpy.Environment()
    machine = simpy.Resource(env, 1)
    served = []

    def holder():
        with machine.request() as req:
            yield req
            yield env.timeout(5)

    def waiter():
        req = machine.request()
        yield (req | env.timeout(5)) | env.timeout(100)
        if not req.triggered:
            req.cancel()
        yield env.timeout(3)
        if req.triggered:
            machine.release(req)

    def contender():
        yield env.timeout(6)
        with machine.request() as req:
            yield req
            served.append(env.now)

    env.process(holder())
    env.process(waiter())
    env.process(contender())
    env.run(until=20)
    return served


def _builder_request_granted_as_the_wait_ends():
    app = SimulationContext("C2", factor=0.0)
    app.add_resource("m", current_cap=1, max_cap=1)
    app.add_service("five", dist="constant", value=5)
    app.add_service("three", dist="constant", value=3)
    app.add_service("one", dist="constant", value=1)

    @app.task(service_id="five", resource_id="m", name="holder")
    def holder(task_id):
        return {"event": "held"}

    @app.task(
        wait={"any": {"req": {"request": "m"}, "limit": {"timeout": 5}}},
        patience=100,
        service_id="three",
        name="waiter",
    )
    def waiter(task_id):
        return {"event": "waited"}

    @app.task(service_id="one", resource_id="m", name="contender")
    def contender(task_id):
        return {"event": "served"}

    def script(context):
        context.spawn(holder(0, entity=Entity("h", "h", {})))
        context.spawn(waiter(0, entity=Entity("w", "w", {})))
        yield context.env.timeout(6)
        context.spawn(contender(0, entity=Entity("c", "c", {})))

    app.add_process(script)
    return run_builder(app, 20)


REQUEST_GRANTED_AS_THE_WAIT_ENDS = """
simulation: {sim_id: C2, factor: 0}
resources:
  m: {current_cap: 1, max_cap: 1}
services:
  five: {dist: constant, value: 5}
  three: {dist: constant, value: 3}
  one: {dist: constant, value: 1}
arrivals:
  holders: {replay: {path: holders.csv}, spawn: holder}
  waiters: {replay: {path: waiters.csv}, spawn: waiter}
  contenders: {replay: {path: contenders.csv}, spawn: contender}
tasks:
  holder:
    resource: m
    service: five
    payload: {event: held}
  waiter:
    wait: {any: {req: {request: m}, limit: {timeout: 5}}}
    patience: 100
    service: three
    payload: {event: waited}
  contender:
    resource: m
    service: one
    payload: {event: served}
run: {until: 20}
"""


def test_c2_a_request_granted_as_the_wait_ends_counts_as_won(tmp_path):
    """A unit granted at the instant a wait times out goes to the request branch,
    which was granted after the condition was processed. The task keeps it through
    its service, so a later request is served when the task ends, as with SimPy's
    `if not event.triggered: event.cancel()`; the unit is not given back at once."""
    assert _reference_request_granted_as_the_wait_ends() == [8]
    records = _builder_request_granted_as_the_wait_ends()
    assert times(records, "contender", "started") == [8.0]
    outcome = run_yaml_text(
        tmp_path,
        REQUEST_GRANTED_AS_THE_WAIT_ENDS,
        {"holders": [0], "waiters": [0], "contenders": [6]},
    )
    assert outcome.error is None
    assert times(outcome.records, "contender", "started") == [8.0]

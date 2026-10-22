import functools

import pytest
import simpy
from simpy import Environment
from simpy.resources.resource import SortedQueue

from desflow import CapacityConfig, SimParameter
from desflow.core.environment import _jsonable
from desflow.core.statistics import _resource_readings
from desflow.resources.resource import (
    DynamicResource,
    ResourceBase,
    check_resource_type,
)


def _free(res):
    """Units a new request could take now."""
    return max(0, res.capacity - res.in_use)


def flush_events(env):
    """Process all events scheduled for the current simulation time."""
    while env._queue and Environment.peek(env) == env.now:
        env._step_event()


def test_resource_initialization(env, registry, sample_params):
    """Verify resource initializes with correct registry values."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")

    # Initial capacity from sample_params: current_cap=2, max_cap=5
    assert res.capacity == 2
    assert _free(res) == 2


def test_basic_request_release(env, registry, sample_params):
    """Verify standard request and release cycle using context manager."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")

    def user():
        with res.request() as req:
            yield req
            yield env.timeout(10)
        # Release happens automatically after the 'with' block

    env.process(user())

    env.run(until=1)
    assert _free(res) == 1  # 1 token taken

    env.run(until=15)
    assert _free(res) == 2  # token returned automatically


def test_request_cancellation(env, registry, sample_params):
    """Verify that leaving the 'with' block before getting a token doesn't break capacity."""
    registry.register_sim_parameter(sample_params)
    registry.update("Shop.resources.packers.current_cap", 1)
    flush_events(env)

    res = DynamicResource(env, "Shop", "packers")

    def slow_user():
        with res.request() as req:
            yield req
            yield env.timeout(10)

    def impatient_user():
        with res.request() as req:
            # Wait for either the resource OR 5 seconds
            result = yield req | env.timeout(5)
            if req not in result:
                # We timed out! The context manager exits without ever getting the token.
                pass

    # 1. Slow user takes the only slot
    env.process(slow_user())
    env.run(until=1)

    # 2. Impatient user gets in line but leaves after 5 seconds (t=6)
    env.process(impatient_user())
    env.run(until=6)

    # 3. Slow user still has the token. Impatient user left the queue.
    assert _free(res) == 0

    # 4. Slow user finishes at t=10. The impatient user's request left the queue
    # when it gave up, so the unit stays free.
    env.run(until=12)
    assert _free(res) == 1
    assert res.capacity == 1


def test_priority_queuing(env, registry, sample_params):
    """Verify priority jumping with context manager."""
    registry.register_sim_parameter(sample_params)
    registry.update("Shop.resources.packers.current_cap", 1)
    flush_events(env)

    res = DynamicResource(env, "Shop", "packers", queue="priority")
    log = []

    def user(name, priority, duration):
        with res.request(priority=priority) as req:
            yield req
            log.append(name)
            yield env.timeout(duration)

    env.process(user("First", priority=10, duration=10))
    env.run(until=1)

    env.process(user("LowPrio", priority=5, duration=1))
    env.run(until=2)

    env.process(user("HighPrio", priority=1, duration=1))

    env.run()
    assert log == ["First", "HighPrio", "LowPrio"]


def test_dynamic_capacity_increase(env, registry, sample_params):
    """Verify that increasing current_cap in registry grows the pool."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # Capacity starts at 2

    registry.update("Shop.resources.packers.current_cap", 4)
    env.run(until=env.now + 0.1)

    assert res.capacity == 4
    assert _free(res) == 4


def test_dynamic_capacity_decrease_while_busy(env, registry, sample_params):
    """Verify capacity reduction waits for tasks to finish."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # Capacity 2

    def worker():
        with res.request() as req:
            yield req
            yield env.timeout(10)

    env.process(worker())
    env.process(worker())
    env.run(until=1)

    assert res.capacity == 2
    assert _free(res) == 0

    # Request drop to 1
    registry.update("Shop.resources.packers.current_cap", 1)
    env.run(until=env.now + 0.1)

    # Total capacity target is now 1, but level is still 0 because
    # the 1 remaining token is still in use by a worker.
    assert res.capacity == 1
    assert _free(res) == 0

    env.run(until=12)
    assert _free(res) == 1


def test_max_cap_increase_allows_larger_capacity(env, registry, sample_params):
    """Verify raising max_cap lets current_cap grow past the original limit."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # current 2, max 5

    registry.update("Shop.resources.packers.max_cap", 8)
    registry.update("Shop.resources.packers.current_cap", 7)
    env.run(until=env.now + 0.1)

    assert res.capacity == 7
    assert _free(res) == 7


def test_max_cap_decrease_lowers_capacity(env, registry, sample_params):
    """Verify lowering max_cap below current_cap shrinks the pool at once."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # current 2, max 5

    registry.update("Shop.resources.packers.max_cap", 1)
    env.run(until=env.now + 0.1)

    assert res.capacity == 1
    assert _free(res) == 1


def _set_cap(registry, value):
    registry.update("Shop.resources.packers.current_cap", value)


def test_shrink_while_busy_does_not_start_queued_task(env, registry, sample_params):
    """A release after a shrink is discarded, so a queued task does not start early."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # Capacity 2
    started = {}

    def worker(name, duration):
        with res.request() as req:
            yield req
            started[name] = env.now
            yield env.timeout(duration)

    env.process(worker("A", 10))
    env.process(worker("B", 20))
    env.run(until=1)
    env.process(worker("C", 5))  # Queued behind A and B
    env.run(until=2)

    _set_cap(registry, 1)
    env.run(until=30)

    # A's release at 10 brings the busy count down to the new capacity of 1,
    # so C must wait for B's release at 20.
    assert started["C"] == 20, f"C started at {started['C']}, expected 20"


def test_shrink_to_zero_after_queue_drains_blocks_new_request(
    env, registry, sample_params
):
    """No task starts while capacity is 0, after the queue has emptied (defect 8)."""
    registry.register_sim_parameter(sample_params)
    _set_cap(registry, 1)
    flush_events(env)
    res = DynamicResource(env, "Shop", "packers")
    started = {}

    def worker(name, duration):
        with res.request() as req:
            yield req
            started[name] = env.now
            yield env.timeout(duration)

    # A and B arrive together; C queues behind them
    env.process(worker("A", 10))
    env.process(worker("B", 10))
    env.run(until=1)
    env.process(worker("C", 10))
    env.run(until=40)  # All three have finished and the resource is idle

    _set_cap(registry, 0)
    env.run(until=50)
    env.process(worker("D", 10))
    env.run(until=100)
    assert "D" not in started, f"D started at {started.get('D')} while capacity was 0"

    _set_cap(registry, 1)
    env.run(until=101)
    assert started["D"] == 100


def test_request_and_shrink_to_zero_at_same_instant(env, registry, sample_params):
    """A request made at the same instant as a shrink to 0 does not start."""
    registry.register_sim_parameter(sample_params)
    _set_cap(registry, 1)
    flush_events(env)
    res = DynamicResource(env, "Shop", "packers")
    started = []

    def worker():
        yield env.timeout(5)
        # In the same step: the shrink is signalled, then the request is made,
        # before the watcher applies the shrink.
        _set_cap(registry, 0)
        req = res.request()
        yield req
        started.append(env.now)
        res.release()

    env.process(worker())
    env.run(until=20)
    assert started == [], f"task started at {started} while capacity was 0"
    assert res.in_use == 0


def test_grow_after_shrink_cancels_pending_shrink(env, registry, sample_params):
    """Growing back before busy units release restores the old capacity exactly."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # Capacity 2
    started = {}

    def worker(name, duration):
        with res.request() as req:
            yield req
            started[name] = env.now
            yield env.timeout(duration)

    env.process(worker("A", 10))
    env.process(worker("B", 20))
    env.run(until=1)
    env.process(worker("C", 5))
    env.run(until=2)

    _set_cap(registry, 1)
    env.run(until=3)
    _set_cap(registry, 2)
    env.run(until=30)

    # Capacity is 2 again, so C takes A's slot at 10
    assert started["C"] == 10
    env.run(until=40)
    assert _free(res) == 2
    assert res.in_use == 0


def test_in_use_counts_busy_units_above_lowered_capacity(env, registry, sample_params):
    """in_use reports every busy unit while busy units exceed a lowered capacity."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # Capacity 2

    def worker(duration):
        with res.request() as req:
            yield req
            yield env.timeout(duration)

    env.process(worker(10))
    env.process(worker(20))
    env.run(until=1)
    assert res.in_use == 2

    _set_cap(registry, 1)
    env.run(until=2)
    assert res.capacity == 1
    assert res.in_use == 2, f"in_use is {res.in_use} with 2 busy units"

    env.run(until=15)
    assert res.in_use == 1
    env.run(until=25)
    assert res.in_use == 0
    assert _free(res) == 1


def test_max_cap_decrease_lowers_capacity_and_a_rise_restores_it(
    env, registry, sample_params
):
    """Lowering max_cap lowers the capacity, and raising both restores it (defect 9)."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # current 2, max 5

    registry.update("Shop.resources.packers.max_cap", 1)
    env.run(until=env.now + 0.1)
    assert res.capacity == 1

    registry.update("Shop.resources.packers.max_cap", 4)
    registry.update("Shop.resources.packers.current_cap", 3)
    env.run(until=env.now + 0.1)
    assert res.capacity == 3
    assert _free(res) == 3


def test_max_cap_decrease_while_busy_keeps_running_tasks(env, registry, sample_params):
    """Lowering max_cap below the busy count lets running tasks finish and release."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # current 2, max 5
    finished = []

    def worker(name):
        with res.request() as req:
            yield req
            yield env.timeout(10)
        finished.append(name)

    env.process(worker("A"))
    env.process(worker("B"))
    env.run(until=1)

    registry.update("Shop.resources.packers.max_cap", 1)
    env.run(until=2)
    assert res.capacity == 1
    assert res.in_use == 2

    env.run(until=20)
    assert sorted(finished) == ["A", "B"]
    assert _free(res) == 1
    assert res.in_use == 0


@pytest.mark.parametrize(
    "queue,expected",
    [
        ("fifo", list(range(10))),
        ("lifo", list(reversed(range(10)))),
        # Equal priorities: first in, first out, where a plain heap mixed them up
        ("priority", list(range(10))),
    ],
)
def test_queue_order_with_equal_priorities(
    env, registry, sample_params, queue, expected
):
    registry.register_sim_parameter(sample_params)
    registry.update("Shop.resources.packers.current_cap", 1)
    flush_events(env)
    res = DynamicResource(env, "Shop", "packers", queue=queue)
    served = []

    def user(name):
        with res.request(priority=1) as req:
            yield req
            served.append(name)
            yield env.timeout(1)

    def holder():
        with res.request() as req:
            yield req
            yield env.timeout(5)

    env.process(holder())
    env.run(until=1)
    for name in range(10):
        env.process(user(name))
    env.run()
    assert served == expected


def test_priority_counts_only_under_the_priority_order(env, registry, sample_params):
    registry.register_sim_parameter(sample_params)
    registry.update("Shop.resources.packers.current_cap", 1)
    flush_events(env)
    res = DynamicResource(env, "Shop", "packers")
    served = []

    def user(name, priority):
        with res.request(priority=priority) as req:
            yield req
            served.append(name)
            yield env.timeout(1)

    env.process(user("first", 5))
    env.run(until=0.5)
    env.process(user("later", 5))
    env.process(user("urgent", 0))
    env.run()
    assert served == ["first", "later", "urgent"]
    with pytest.raises(ValueError, match="queue must be one of"):
        DynamicResource(env, "Shop", "operator", queue="random")


def test_cancel_leaves_the_queue_at_once_and_does_nothing_after_a_grant(
    env, registry, sample_params
):
    registry.register_sim_parameter(sample_params)
    _set_cap(registry, 1)
    flush_events(env)
    res = DynamicResource(env, "Shop", "packers")
    held = res.request()
    waiting = res.request()
    assert len(res.queue) == 1

    waiting.cancel()
    assert res.queue == []

    held.cancel()  # granted: SimPy's cancel releases nothing either
    assert res.in_use == 1
    res.release(held)
    env.run(until=1)
    assert res.in_use == 0 and not waiting.triggered


def test_a_second_release_or_one_never_granted_does_nothing(
    env, registry, sample_params
):
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers")  # capacity 2
    first, second = res.request(), res.request()
    third = res.request()  # waits
    res.release(first)
    res.release(first)
    res.release(third)
    env.run(until=1)
    # One unit came back, and it went to the waiting request
    assert res.users == [second, third]


def test_release_none_gives_back_the_oldest_user(env, registry, sample_params):
    """`release()` with no request gives back the oldest held unit and grants the
    head, the form 0.21.0 code without a request uses."""
    registry.register_sim_parameter(sample_params)
    _set_cap(registry, 1)
    flush_events(env)
    res = DynamicResource(env, "Shop", "packers")
    first = res.request()
    second = res.request()  # waits, capacity 1
    env.run(until=1)
    assert res.users == [first] and not second.triggered
    res.release()
    env.run(until=2)
    assert res.users == [second]


def test_no_preemption_while_units_are_owed_above_a_lowered_capacity(
    env, registry, sample_params
):
    """Lowering capacity below the busy count owes units; a preempting request then
    finds them not preemptible, so it waits and interrupts nobody."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers", queue="priority", preempt=True)
    interrupted = []

    def holder(priority):
        try:
            with res.request(priority=priority) as req:
                yield req
                yield env.timeout(100)
        except simpy.Interrupt:
            interrupted.append(env.now)

    env.process(holder(5))
    env.process(holder(5))
    env.run(until=1)
    assert res.in_use == 2
    _set_cap(registry, 1)
    flush_events(env)
    taker = res.request(priority=0, preempt=True)
    env.run(until=5)
    assert interrupted == [] and not taker.triggered


def _grant_order(make_resource, env):
    """Who is granted when: A holds until 5, B waits from 1, and C asks at 5
    in the same step as A's release, after it."""
    res = make_resource()
    order = []

    def user(name, start, hold):
        yield env.timeout(start)
        with res.request() as req:
            yield req
            order.append((name, env.now))
            yield env.timeout(hold)

    env.process(user("A", 0, 5))
    env.process(user("B", 1, 1))
    env.process(user("C", 5, 1))
    env.run(until=20)
    return order


def test_grant_order_matches_simpy(env, registry, sample_params):
    import simpy

    registry.register_sim_parameter(sample_params)
    _set_cap(registry, 1)
    flush_events(env)
    reference_env = simpy.Environment()
    expected = _grant_order(
        lambda: simpy.Resource(reference_env, capacity=1), reference_env
    )
    assert expected == [("A", 0), ("B", 5), ("C", 6)]
    assert _grant_order(lambda: DynamicResource(env, "Shop", "packers"), env) == (
        expected
    )


def test_two_failures_in_a_row_restart_the_failure_clock():
    """The failure clock restarts at each repair: down at 15 and 34, so with a run
    to 40 the machine fails twice and is down 8 seconds."""
    from desflow.core.context import SimulationContext
    from tests.conformance.harness import run_builder
    from tests.conformance.test_s1_breakdowns import _trace

    app = SimulationContext("S1", factor=0.0)
    app.add_resource("machine", current_cap=1, max_cap=1)
    app.add_breakdown(
        "machine",
        time_to_failure={"dist": "constant", "value": 15},
        repair={"dist": "constant", "value": 4},
    )
    app.with_stats("basic")
    records = run_builder(app, 40)
    assert _trace(records) == [
        (15.0, "machine", "down"),
        (19.0, "machine", "repaired"),
        (34.0, "machine", "down"),
        (38.0, "machine", "repaired"),
    ]
    readings = app.env.stats.readings()
    assert readings["S1.stats.resources.machine.failures"] == 2
    assert readings["S1.stats.resources.machine.downtime"] == 8
    assert readings["S1.stats.resources.machine.down"] == 0


def test_a_resource_with_no_registry_entry_registers_itself(env, registry):
    """A resource built with no entry for it has one unit, as `simpy.Resource(env)`,
    and the registry holds its paths."""
    res = DynamicResource(env, "Solo", "bench")
    assert res.capacity == 1 and res.count == 0
    assert registry.get("Solo.resources.bench.current_cap").value == 1
    assert registry.get("Solo.resources.bench.max_cap").value == 1


def test_a_resource_with_infinite_capacity_never_queues(env, registry):
    registry.register_sim_parameter(
        SimParameter(
            sim_id="Open",
            resources={
                "bench": CapacityConfig(current_cap=float("inf"), max_cap=float("inf"))
            },
        )
    )
    res = DynamicResource(env, "Open", "bench")
    requests = [res.request() for _ in range(50)]
    env.run(until=1)
    assert all(request.triggered for request in requests)
    assert res.capacity == float("inf") and res.queue == []
    readings = _resource_readings(res)
    assert readings["utilization"] == 0.0
    assert _jsonable(readings["capacity"]) is None


def test_the_queue_is_a_list_in_service_order(env, registry, sample_params):
    registry.register_sim_parameter(sample_params)
    _set_cap(registry, 0)
    flush_events(env)
    res = DynamicResource(env, "Shop", "packers", queue="priority")
    low, high, mid = res.request(priority=5), res.request(priority=1), res.request(3)
    assert list(res.queue) == [high, mid, low]
    assert res.queue is res.put_queue and res.queue[0] is high
    assert res.queue.remove(mid) is True and res.queue.remove(mid) is False
    assert list(res.queue) == [high, low]


def test_queue_items_still_returns_the_ranked_pairs_with_a_warning(
    env, registry, sample_params
):
    registry.register_sim_parameter(sample_params)
    _set_cap(registry, 0)
    flush_events(env)
    res = DynamicResource(env, "Shop", "packers")
    first, second = res.request(), res.request()
    with pytest.warns(FutureWarning, match="0.24.0"):
        pairs = res.queue.items
    assert [request for _rank, request in pairs] == [first, second]
    assert pairs[0][0] < pairs[1][0]


def test_a_full_waiting_line_refuses_a_request(env, registry, sample_params):
    registry.register_sim_parameter(sample_params)
    _set_cap(registry, 0)
    flush_events(env)
    res = DynamicResource(env, "Shop", "packers", queue_limit=2)
    first, second = res.request(), res.request()
    with pytest.raises(RuntimeError, match="Cannot append event. Queue is full."):
        res.request()
    assert list(res.queue) == [first, second]
    first.cancel()
    assert len(res.queue) == 1
    res.request()
    assert len(res.queue) == 2


def test_a_queue_limit_counts_waiting_requests_after_a_lowered_capacity(
    env, registry, sample_params
):
    """Requests that cannot be granted because the capacity was lowered wait, and
    the limit counts them."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Shop", "packers", queue_limit=1)
    res.request()
    res.request()
    _set_cap(registry, 1)
    flush_events(env)
    res.request()  # both units are busy, one is owed: it waits
    with pytest.raises(RuntimeError, match="Queue is full"):
        res.request()


def test_a_resource_type_and_queue_limit_are_checked(env, registry, sample_params):
    registry.register_sim_parameter(sample_params)
    with pytest.raises(ValueError, match="queue_limit must be 0 or more"):
        DynamicResource(env, "Shop", "packers", queue_limit=-1)

    class Own(ResourceBase):
        PutQueue = staticmethod(functools.partial(SortedQueue, maxlen=1))

    with pytest.raises(ValueError, match="custom PutQueue sets its own maxlen"):
        Own(env, "Shop", "packers", queue_limit=1)
    with pytest.raises(ValueError, match="subclass of ResourceBase"):
        check_resource_type(simpy.Resource)
    check_resource_type(Own)
    check_resource_type(None)


def test_a_subclass_changes_the_grant_with_do_put(env, registry, sample_params):
    """`_do_put` and `_do_get` are the hooks, as SimPy's: this resource grants no
    request while its count is odd."""
    registry.register_sim_parameter(sample_params)

    class Pairs(ResourceBase):
        def _do_put(self, request):
            if len(self.users) % 2 == 0 or len(self.put_queue) > 1:
                return super()._do_put(request)

    res = Pairs(env, "Shop", "packers")
    first, second = res.request(), res.request()
    third = res.request()
    env.run(until=1)
    assert first.triggered and second.triggered and not third.triggered
    assert res.count == 2 and len(res.queue) == 1

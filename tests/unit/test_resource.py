from dynamic_des.resources.resource import DynamicResource


def flush_events(env):
    """Process all events scheduled for the current simulation time."""
    while env._queue and env.peek() == env.now:
        env.step()


def test_resource_initialization(env, registry, sample_params):
    """Verify resource initializes with correct registry values."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")

    # Initial capacity from sample_params: current_cap=2, max_cap=5
    assert res.capacity == 2
    assert res.pool.level == 2
    assert res.pool.capacity == 5


def test_basic_request_release(env, registry, sample_params):
    """Verify standard request and release cycle using context manager."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")

    def user():
        with res.request() as req:
            yield req
            yield env.timeout(10)
        # Release happens automatically after the 'with' block

    env.process(user())

    env.run(until=1)
    assert res.pool.level == 1  # 1 token taken

    env.run(until=15)
    assert res.pool.level == 2  # token returned automatically


def test_request_cancellation(env, registry, sample_params):
    """Verify that leaving the 'with' block before getting a token doesn't break capacity."""
    registry.register_sim_parameter(sample_params)
    registry.update("Line_A.resources.lathe.current_cap", 1)
    flush_events(env)

    res = DynamicResource(env, "Line_A", "lathe")

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
    assert res.pool.level == 0

    # 4. Slow user finishes at t=10.
    # The dispatcher will pull the impatient user's ticket, see it is cancelled,
    # and immediately return the token to the pool.
    env.run(until=12)
    assert res.pool.level == 1
    assert res.capacity == 1


def test_priority_queuing(env, registry, sample_params):
    """Verify priority jumping with context manager."""
    registry.register_sim_parameter(sample_params)
    registry.update("Line_A.resources.lathe.current_cap", 1)
    flush_events(env)

    res = DynamicResource(env, "Line_A", "lathe")
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
    res = DynamicResource(env, "Line_A", "lathe")  # Capacity starts at 2

    registry.update("Line_A.resources.lathe.current_cap", 4)
    env.run(until=env.now + 0.1)

    assert res.capacity == 4
    assert res.pool.level == 4


def test_dynamic_capacity_decrease_while_busy(env, registry, sample_params):
    """Verify capacity reduction waits for tasks to finish."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # Capacity 2

    def worker():
        with res.request() as req:
            yield req
            yield env.timeout(10)

    env.process(worker())
    env.process(worker())
    env.run(until=1)

    assert res.capacity == 2
    assert res.pool.level == 0

    # Request drop to 1
    registry.update("Line_A.resources.lathe.current_cap", 1)
    env.run(until=env.now + 0.1)

    # Total capacity target is now 1, but level is still 0 because
    # the 1 remaining token is still in use by a worker.
    assert res.capacity == 1
    assert res.pool.level == 0

    env.run(until=12)
    assert res.pool.level == 1


def test_max_cap_increase_allows_larger_capacity(env, registry, sample_params):
    """Verify raising max_cap lets current_cap grow past the original limit."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # current 2, max 5

    registry.update("Line_A.resources.lathe.max_cap", 8)
    registry.update("Line_A.resources.lathe.current_cap", 7)
    env.run(until=env.now + 0.1)

    assert res.capacity == 7
    assert res.pool.level == 7


def test_max_cap_decrease_lowers_capacity(env, registry, sample_params):
    """Verify lowering max_cap below current_cap shrinks the pool at once."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # current 2, max 5

    registry.update("Line_A.resources.lathe.max_cap", 1)
    env.run(until=env.now + 0.1)

    assert res.capacity == 1
    assert res.pool.level == 1


def _set_cap(registry, value):
    registry.update("Line_A.resources.lathe.current_cap", value)


def test_shrink_while_busy_does_not_start_queued_task(env, registry, sample_params):
    """A release after a shrink is discarded, so a queued task does not start early."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # Capacity 2
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
    res = DynamicResource(env, "Line_A", "lathe")
    started = {}

    def worker(name, duration):
        with res.request() as req:
            yield req
            started[name] = env.now
            yield env.timeout(duration)

    # A and B arrive together; C arrives while the dispatcher waits for A's token
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
    res = DynamicResource(env, "Line_A", "lathe")
    started = []

    def worker():
        yield env.timeout(5)
        # In the same step: the shrink is signalled, then the request is made. The
        # dispatcher takes the idle token before the watcher applies the shrink,
        # so the shrink arrives while that token is being handed over.
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
    res = DynamicResource(env, "Line_A", "lathe")  # Capacity 2
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
    assert res.pool.level == 2
    assert res.in_use == 0


def test_in_use_counts_busy_units_above_lowered_capacity(env, registry, sample_params):
    """in_use reports every busy unit while busy units exceed a lowered capacity."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # Capacity 2

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
    assert res.pool.level == 1


def test_max_cap_decrease_lowers_pool_capacity(env, registry, sample_params):
    """Lowering max_cap lowers the token pool capacity with it (defect 9)."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # current 2, max 5

    registry.update("Line_A.resources.lathe.max_cap", 1)
    env.run(until=env.now + 0.1)
    assert res.pool.capacity == 1, f"pool capacity is {res.pool.capacity}"

    registry.update("Line_A.resources.lathe.max_cap", 4)
    registry.update("Line_A.resources.lathe.current_cap", 3)
    env.run(until=env.now + 0.1)
    assert res.pool.capacity == 4
    assert res.capacity == 3
    assert res.pool.level == 3


def test_max_cap_decrease_while_busy_keeps_running_tasks(env, registry, sample_params):
    """Lowering max_cap below the busy count lets running tasks finish and release."""
    registry.register_sim_parameter(sample_params)
    res = DynamicResource(env, "Line_A", "lathe")  # current 2, max 5
    finished = []

    def worker(name):
        with res.request() as req:
            yield req
            yield env.timeout(10)
        finished.append(name)

    env.process(worker("A"))
    env.process(worker("B"))
    env.run(until=1)

    registry.update("Line_A.resources.lathe.max_cap", 1)
    env.run(until=2)
    assert res.capacity == 1
    assert res.in_use == 2

    env.run(until=20)
    assert sorted(finished) == ["A", "B"]
    assert res.pool.level == 1
    assert res.pool.capacity == 1
    assert res.in_use == 0

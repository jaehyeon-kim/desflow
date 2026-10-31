"""Wait for one or all, low-level API: SimPy's AnyOf and AllOf with desflow signals.

The low-level version of wait_for_one_or_all.py. `first(gap)` runs an order that
waits for a restock signal or 30 seconds, with the restock `gap` seconds later, and
returns which branches were done. `two_at_once()` fires two signals at the same
instant. `all_with_a_failure()` waits for a signal and a child process, and the
signal fails while the child still runs.
"""

from desflow import DynamicRealtimeEnvironment, Signal, SignalFailed


# --8<-- [start:first]
def first(gap: float) -> dict:
    """Which branches were done when the order's wait ended."""
    env = DynamicRealtimeEnvironment(factor=0)
    restocked = Signal(env, "restocked")
    seen = {}

    def order():
        branches = {"stock": restocked.wait(), "limit": env.timeout(30)}
        done = yield env.any_of(list(branches.values()))
        # The value of AnyOf holds the events that had happened
        seen.update({name: event in done for name, event in branches.items()})

    def restock():
        yield env.timeout(gap)
        restocked.fire({"boxes": 6})

    env.process(order())
    env.process(restock())
    env.run(until=60)
    env.teardown()
    return seen


# --8<-- [end:first]


# --8<-- [start:tie]
def two_at_once() -> dict:
    """Two signals fire at the same instant: both branches are done."""
    env = DynamicRealtimeEnvironment(factor=0)
    boxes, labels = Signal(env, "boxes"), Signal(env, "labels")
    seen = {}

    def order():
        branches = {"boxes": boxes.wait(), "labels": labels.wait()}
        done = yield env.any_of(list(branches.values()))
        seen.update({name: event in done for name, event in branches.items()})

    def deliver():
        yield env.timeout(5)
        boxes.fire({"item": "boxes"})
        labels.fire({"item": "labels"})

    env.process(order())
    env.process(deliver())
    env.run(until=10)
    env.teardown()
    return seen


# --8<-- [end:tie]


# --8<-- [start:all]
def all_with_a_failure() -> list:
    """(time, task, status) of the order and its inspection: the restock fails at 2,
    the order fails at once, and the inspection, still running, ends at 4."""
    env = DynamicRealtimeEnvironment(factor=0)
    restocked = Signal(env, "restocked")
    seen = []

    def inspect():
        yield env.timeout(4)
        seen.append((env.now, "inspect", "finished"))

    def order():
        seen.append((env.now, "order", "queued"))
        try:
            yield env.all_of([restocked.wait(), env.process(inspect())])
        except SignalFailed:
            # A failed branch fails the AllOf at once; the inspection runs on
            seen.append((env.now, "order", "failed"))

    def restock():
        yield env.timeout(2)
        restocked.fail("broken delivery")

    env.process(order())
    env.process(restock())
    env.run(until=10)
    env.teardown()
    return seen


# --8<-- [end:all]

# --8<-- [start:main]
if __name__ == "__main__":
    print("restock after 10 s:", first(10))
    print("restock after 50 s:", first(50))
    print("two at once:", two_at_once())
    print("all, with a failure:", all_with_a_failure())
# --8<-- [end:main]

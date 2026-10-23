"""The SimPy constructs of the SimPy and desflow page, each on the packing shop.

Every function is plain SimPy, with no desflow in it, and returns what the page shows.
The checks at the end assert what the page states about each one, so a wrong sentence on
the page fails this test. Times are seconds, and the whole file takes under a second.
"""

import math
import time

import simpy


# --8<-- [start:event_list]
def event_list():
    """Two events scheduled out of order are processed in time order."""
    env = simpy.Environment()
    env.timeout(2.6)  # order 2 arrives
    env.timeout(1.5)  # order 1 is packed
    seen = []
    while env.peek() != math.inf:  # peek is the time of the next event
        seen.append((env.peek(), env.now))
        env.step()  # take the earliest event, move the clock to its time
    return seen, env.now


# --8<-- [end:event_list]


# --8<-- [start:events]
def events():
    """An event is triggered once, and its callbacks and waiting processes run when it is processed."""
    env = simpy.Environment()
    packed = env.event()
    log = []
    packed.callbacks.append(
        lambda event: log.append(f"callback at {env.now}: {event.value}")
    )

    def courier(name):
        value = yield packed  # every process waiting on the event resumes with its value
        log.append(f"{name} at {env.now}: {value}")

    env.process(courier("courier 1"))
    env.process(courier("courier 2"))

    def packer():
        yield env.timeout(1.5)
        packed.succeed("order 1")  # triggers the event, which schedules it for now

    env.process(packer())
    env.run()
    return log, (packed.triggered, packed.processed, packed.ok)


# --8<-- [end:events]


# --8<-- [start:processes]
def processes():
    """A process is a generator; yielding another process waits for it and returns its value."""
    env = simpy.Environment()
    log = []

    def pack(order):
        yield env.timeout(1.5)
        return f"order {order} packed"

    def shop():
        for order in (1, 2):
            yield env.timeout(2)  # time to the next arrival
            result = yield env.process(pack(order))  # wait for the child
            log.append((env.now, result))

    env.process(shop())
    env.run()
    return log


# --8<-- [end:processes]


# --8<-- [start:interrupts]
def interrupts():
    """A packer is interrupted at 1.0 s and finds how much of the 1.5 s packing is left."""
    env = simpy.Environment()
    log = []

    def packer():
        left = 1.5
        while left > 0:
            started = env.now
            try:
                yield env.timeout(left)
                left = 0
            except (
                simpy.Interrupt
            ) as interrupt:  # raised inside the process, at its yield
                left -= env.now - started
                log.append((env.now, interrupt.cause, left))
                yield env.timeout(0.5)  # the break
        log.append((env.now, "packed", 0))

    process = env.process(packer())

    def supervisor():
        yield env.timeout(1.0)
        process.interrupt("break")  # the cause is any object

    env.process(supervisor())
    env.run()
    return log


# --8<-- [end:interrupts]


# --8<-- [start:conditions]
def conditions():
    """`|` waits for the first of several events, `&` for all of them."""
    env = simpy.Environment()
    log = []

    def courier():
        packed = env.timeout(2.0, value="packed")
        limit = env.timeout(1.0, value="limit")
        first = yield packed | limit  # AnyOf: holds every event processed so far
        log.append((env.now, packed in first, limit in first))
        both = yield packed & env.timeout(3.0)  # AllOf: waits for both
        log.append((env.now, len(both.todict())))

    env.process(courier())
    env.run()
    return log


# --8<-- [end:conditions]


# --8<-- [start:resource]
def resource():
    """Three orders at 0 s and one packer: each waits in the queue for the one before."""
    env = simpy.Environment()
    packers = simpy.Resource(env, capacity=1)
    log = []

    def order(name):
        with packers.request() as request:  # leaving the block releases the packer
            yield request
            log.append((name, env.now))
            yield env.timeout(1.5)

    for name in "ABC":
        env.process(order(name))
    env.run()
    return log


# --8<-- [end:resource]


# --8<-- [start:priority]
def priority():
    """The packer is busy at 0 s; a lower priority number is served first, ties in arrival order."""
    env = simpy.Environment()
    packers = simpy.PriorityResource(env, capacity=1)
    log = []

    def order(name, priority, arrives):
        yield env.timeout(arrives)
        with packers.request(priority=priority) as request:
            yield request
            log.append((name, env.now))
            yield env.timeout(1.5)

    env.process(order("first", priority=1, arrives=0))
    env.process(order("standard", priority=1, arrives=0.5))
    env.process(order("express", priority=0, arrives=1.0))
    env.run()
    return log


# --8<-- [end:priority]


# --8<-- [start:preemptive]
def preemptive():
    """An express order takes the packer from a standard one, which is interrupted."""
    env = simpy.Environment()
    packers = simpy.PreemptiveResource(env, capacity=1)
    log = []

    def standard():
        with packers.request(priority=1) as request:
            yield request
            try:
                yield env.timeout(1.5)
                log.append(("standard packed", env.now))
            except simpy.Interrupt as interrupt:
                cause = interrupt.cause  # a Preempted object
                log.append(("standard preempted", env.now, cause.usage_since))

    def express():
        yield env.timeout(1.0)
        with packers.request(priority=0) as request:  # preempt=True is the default
            yield request
            yield env.timeout(1.0)
            log.append(("express packed", env.now))

    env.process(standard())
    env.process(express())
    env.run()
    return log


# --8<-- [end:preemptive]


# --8<-- [start:container]
def container():
    """A shelf holds a quantity of boxes: a get for 3 waits until a put makes them available."""
    env = simpy.Environment()
    shelf = simpy.Container(env, capacity=10, init=2)
    log = []

    def packer():
        yield shelf.get(3)
        log.append(("took 3 boxes", env.now, shelf.level))

    def delivery():
        yield env.timeout(4)
        yield shelf.put(5)  # the waiting get is served as the boxes arrive

    env.process(packer())
    env.process(delivery())
    env.run()
    return log


# --8<-- [end:container]


# --8<-- [start:stores]
def stores():
    """A store holds distinct items; the filtered and priority kinds change which one a get returns."""
    env = simpy.Environment()
    parcels = simpy.Store(env)
    by_size = simpy.FilterStore(env)
    by_urgency = simpy.PriorityStore(env)
    got = {}

    def run():
        for name, size in (("p1", "small"), ("p2", "large"), ("p3", "small")):
            yield parcels.put(name)
            yield by_size.put((name, size))
        yield by_urgency.put(simpy.PriorityItem(2, "standard"))
        yield by_urgency.put(simpy.PriorityItem(1, "express"))
        got["store"] = yield parcels.get()  # first in, first out
        got["filter"] = yield by_size.get(
            lambda item: item[1] == "large"
        )  # first match
        got["priority"] = (yield by_urgency.get()).item  # smallest priority first

    env.process(run())
    env.run()
    return got


# --8<-- [end:stores]


# --8<-- [start:realtime]
def realtime():
    """One simulated second takes 0.2 wall-clock seconds; the run lasts at least 2 x 0.2 s."""
    env = simpy.rt.RealtimeEnvironment(factor=0.2)  # strict=True is the default

    def packer():
        yield env.timeout(2)

    env.process(packer())
    started = time.monotonic()
    env.run()
    return env.now, time.monotonic() - started


# --8<-- [end:realtime]


if __name__ == "__main__":
    seen, now = event_list()
    print(f"event list: {seen}, clock at {now}")
    assert seen == [(1.5, 0), (2.6, 1.5)] and now == 2.6

    log, state = events()
    print(*log, sep="\n")
    print(f"triggered, processed, ok: {state}")
    assert log[0] == "callback at 1.5: order 1" and len(log) == 3
    assert state == (True, True, True)

    log = processes()
    print(log)
    assert log == [(3.5, "order 1 packed"), (7.0, "order 2 packed")]

    log = interrupts()
    print(log)
    assert log == [(1.0, "break", 0.5), (2.0, "packed", 0)]

    log = conditions()
    print(log)
    assert log == [(1.0, False, True), (4.0, 2)]

    log = resource()
    print(log)
    assert log == [("A", 0), ("B", 1.5), ("C", 3.0)]

    log = priority()
    print(log)
    assert log == [("first", 0), ("express", 1.5), ("standard", 3.0)]

    log = preemptive()
    print(log)
    assert log == [("standard preempted", 1.0, 0), ("express packed", 2.0)]

    log = container()
    print(log)
    assert log == [("took 3 boxes", 4, 4)]

    got = stores()
    print(got)
    assert got == {"store": "p1", "filter": ("p2", "large"), "priority": "express"}

    now, wall = realtime()
    print(f"clock {now}, wall clock at least 0.4 s: {wall >= 0.4}")
    assert now == 2 and wall >= 0.4

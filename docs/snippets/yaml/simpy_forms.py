"""SimPy processes that a blueprint runs where its keys do not give SimPy's behaviour yet.

Each function is a SimPy process: it takes `context` and its `kwargs`, and uses
`context.env` as it would use a SimPy environment. A process publishes no event
record of its own; `context.publish` sends a telemetry value. Each one ends by calling
a task, whose records are true because the work has happened by then.
"""

from simpy import Interrupt


# --8<-- [start:computed-time]
def fill(context, car, start, litres):
    """A car that fills for half its litres in seconds, so the time is computed."""
    env = context.env
    yield env.timeout(start)
    with context.get_resource("pump").request() as request:
        yield request
        yield env.timeout(litres / 2)
    yield context.call("filled", car=car)


# --8<-- [end:computed-time]


# --8<-- [start:held]
def repair(context, job, start, priority, first, second):
    """A job that keeps its unit of the machine through two steps."""
    env = context.env
    yield env.timeout(start)
    with context.get_resource("machine").request(priority=priority) as request:
        yield request
        yield env.timeout(first)
        yield env.timeout(second)
    yield context.call("repaired", job=job)


# --8<-- [end:held]


# --8<-- [start:arrivals]
def customers(context, count, gap):
    """Starts `count` customers, one every `gap` seconds, and then stops."""
    for _ in range(count):
        yield context.env.timeout(gap)
        context.call("serve")


# --8<-- [end:arrivals]


# --8<-- [start:sub-process]
def order(context, boxes):
    """Asks the task `pack` for `boxes` boxes and reads what it returns."""
    packed = yield context.call("pack", boxes=boxes)
    yield context.call("ship", total=packed["total"] + 1)


# --8<-- [end:sub-process]


# --8<-- [start:wait-again]
def pause(env, target, at):
    """Interrupts `target` at time `at`."""
    yield env.timeout(at)
    target.interrupt("pause")


def wait_again(context, job, delay, interrupt_at):
    """A job that waits for a timeout, is interrupted, and waits for the same
    timeout again, so the timeout ends at its first time."""
    env = context.env
    context.spawn(pause(env, env.active_process, interrupt_at))
    timeout = env.timeout(delay)
    while True:
        try:
            yield timeout
            break
        except Interrupt:
            pass
    yield context.call("done", job=job)


# --8<-- [end:wait-again]


# --8<-- [start:queue-again]
def queue_again(context, job, start, time, interrupt_at=None):
    """A job that queues for the machine, is interrupted, and yields the same
    request again, so it keeps its place in the queue."""
    env = context.env
    yield env.timeout(start)
    machine = context.get_resource("machine")
    if interrupt_at is not None:
        context.spawn(pause(env, env.active_process, interrupt_at))
    request = machine.request()
    while True:
        try:
            yield request
            break
        except Interrupt:
            pass
    yield env.timeout(time)
    machine.release(request)
    yield context.call("done", job=job)


# --8<-- [end:queue-again]


# --8<-- [start:whole-flow]
def two_steps(context):
    """The flow: the task `relay`, then the task `more`, in one process."""
    yield context.call("relay")
    yield context.call("more")


def watch_flow(context):
    """Waits for the whole flow to end, as `yield process` does."""
    flow = context.spawn(two_steps(context))
    yield flow
    yield context.call("done")


# --8<-- [end:whole-flow]


# The tasks of the blueprints take these arguments
def filled(task_id, context, car):
    return {"car": car}


def repaired(task_id, context, job):
    return {"job": job}


def pack(task_id, context, boxes):
    return {"total": boxes * 3}


def shipped(task_id, context, total):
    return {"total": total}


def done(task_id, context, job):
    return {"job": job}

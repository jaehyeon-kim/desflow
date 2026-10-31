"""SimPy processes that a blueprint runs where its keys do not give SimPy's behaviour yet.

Each function is a SimPy process: it takes `context` and its `kwargs`, and uses
`context.env` as it would use a SimPy environment. A process publishes no event
record of its own; `context.publish` sends a telemetry value. Each one ends by calling
a task, whose records are true because the work has happened by then.
"""


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


# The tasks of the four blueprints take these arguments
def filled(task_id, context, car):
    return {"car": car}


def repaired(task_id, context, job):
    return {"job": job}


def pack(task_id, context, boxes):
    return {"total": boxes * 3}


def shipped(task_id, context, total):
    return {"total": total}

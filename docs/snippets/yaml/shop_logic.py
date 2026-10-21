"""Python for the blueprints beside it: a payload, a process with arguments, a
predicate, a custom reading and a resource type."""

from desflow import ResourceBase


def choose_delivery(task_id: int, context):
    """Picks each order's delivery from the shared generator, so a seed repeats it."""
    delivery = "standard" if context.sampler.rng.random() < 0.9 else "express"
    return {"status": "finished", "order_id": task_id, "delivery": delivery}


def breaks(context, resource: str, every: float, duration: float):
    """Waits `every` seconds, then takes every packer off for a break of `duration`."""
    path = f"{context.sim_id}.resources.{resource}.current_cap"
    registry = context.env.registry
    while True:
        yield context.env.timeout(every)
        normal = registry.get(path).value
        registry.update(path, 0)
        context.env.publish_event(f"{resource}-break", {"status": "down"})
        yield context.env.timeout(duration)
        registry.update(path, normal)
        context.env.publish_event(f"{resource}-break", {"status": "up"})


def events_only(record: dict) -> bool:
    """Egress predicate: keeps events and drops telemetry."""
    return record["stream_type"] == "event"


# --8<-- [start:packers-report]
def packers_report(context):
    """A custom reading: orders waiting per packer, and the live arrival rate."""
    packers = context.get_resource("packers")
    waiting = len(packers.queue)
    context.publish("waiting_per_packer", waiting / max(packers.capacity, 1))
    rate = context.env.registry.get(f"{context.sim_id}.arrival.orders.rate").value
    context.publish("arrival_rate", rate)  # as Shop.arrival_rate


# --8<-- [end:packers-report]


# --8<-- [start:logged-packers]
class LoggedPackers(ResourceBase):
    """A resource that keeps how long each request waited before it was granted."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.waits = []

    def _do_put(self, request):
        super()._do_put(request)
        if request.triggered:  # the request was granted just now
            self.waits.append(self.env.now - request.time)


# --8<-- [end:logged-packers]

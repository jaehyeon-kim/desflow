"""Python for hybrid.yaml: a payload, a process with arguments and a predicate."""


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

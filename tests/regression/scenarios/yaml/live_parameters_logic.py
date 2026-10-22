"""The process of the live_parameters scenario in YAML: updates at fixed times."""

# Each change: the simulation time, the registry path and the new value. No time is a
# multiple of the 30 s statistics interval, so no change ties with a reading.
CHANGES = [
    (145, "Shop.arrival.orders.rate", 0.9),
    (145, "Shop.resources.packers.current_cap", 4),
    (305, "Shop.service.packing.mean", 4.5),
    (455, "Shop.arrival.orders.rate", 0.7),
    (455, "Shop.resources.packers.current_cap", 2),
    (605, "Shop.service.packing.mean", 3.5),
    (605, "Shop.service.packing.std", 0.8),
]


def steer(context):
    """Applies each change at its time, as an ingress connector would."""
    env = context.env
    for at, path, value in CHANGES:
        yield env.timeout(at - env.now)
        env.registry.update(path, value)

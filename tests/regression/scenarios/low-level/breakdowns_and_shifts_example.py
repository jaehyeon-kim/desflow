"""Breakdowns and shifts: a station that breaks down and interrupts the task holding
it, capacity changed at fixed times, and signals that open and close the shop,
low-level API.

The low-level twin of `builder/breakdowns_and_shifts_example.py`, a regression
scenario (see scripts/regression.py), not an example. A day task opens the shop every
150 seconds and a second task closes it 90 seconds later; an order that arrives while
the shop is closed waits up to 60 seconds for it to open, and is turned away if it
stays closed.
"""

import itertools

import simpy

from desflow import (
    BreakdownConfig,
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Sampler,
    Signal,
    SimParameter,
    run_breakdowns,
    run_scenario,
)

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        "orders": DistributionConfig(dist="exponential", rate=0.25),
        "days": DistributionConfig(dist="constant", value=150),
    },
    service={
        "packing": DistributionConfig(dist="triangular", low=2, mode=4, high=8),
        "wrapping": DistributionConfig(dist="uniform", low=2, high=4),
    },
    resources={
        "station": CapacityConfig(current_cap=2, max_cap=4),
        "packers": CapacityConfig(current_cap=2, max_cap=4),
    },
    breakdowns={
        "station": BreakdownConfig(
            time_to_failure=DistributionConfig(dist="exponential", rate=0.01),
            repair=DistributionConfig(dist="uniform", low=8, high=20),
            work="interrupt",
        )
    },
)
# Shifts: more staff for the rush, fewer after it
SCHEDULE = [
    {"at": 130, "path": "Shop.resources.packers.current_cap", "value": 3},
    {"at": 130, "path": "Shop.resources.station.current_cap", "value": 3},
    {"at": 310, "path": "Shop.resources.packers.current_cap", "value": 1},
    {"at": 310, "path": "Shop.resources.station.current_cap", "value": 1},
    {"at": 470, "path": "Shop.resources.packers.current_cap", "value": 2},
    {"at": 470, "path": "Shop.resources.station.current_cap", "value": 2},
]


class Shop:
    """The shop's parts, its sampler and the task counters its processes share."""

    def __init__(self, env, sampler):
        self.env, self.sampler = env, sampler
        self.station = DynamicResource(env, "Shop", "station")
        self.packers = DynamicResource(env, "Shop", "packers")
        self.opened = Signal(env, "opened")
        self.closed = Signal(env, "closed")
        self.ids = {}

    def is_open(self):
        return self.opened.count > self.closed.count

    def step(self, key, task, service=None):
        """The publish and service-time helpers of one task instance."""
        task_id = next(self.ids.setdefault(task, itertools.count()))
        path_id = f"Shop.service.{service}"

        def publish(value):
            self.env.publish_event(key, value, task=task, instance=task_id)

        def status(state, **extra):
            value = {"path_id": path_id} if service else {}
            publish({**value, "status": state, **extra})

        def duration():
            return self.sampler.sample(self.env.registry.get_config(path_id))

        return task_id, publish, status, duration


def open_shop(shop, key):
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "open_shop")
    stats.flow_started("days")
    payload = {"event_type": "shop_opened"}
    publish(payload)
    stats.task_finished("open_shop", task_id)
    env.notify_change()
    shop.opened.fire(payload)
    env.process(close_up(shop, key))
    yield env.timeout(0)


def close_up(shop, key):
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "close_up")
    status("queued")
    stats.task_queued("close_up", task_id)
    yield env.all_of([env.timeout(90)])
    status("started")
    stats.task_started("close_up", task_id)
    payload = {"event_type": "shop_closed"}
    publish(payload)
    stats.task_finished("close_up", task_id)
    env.notify_change()
    shop.closed.fire(payload)
    stats.flow_finished("days")


def serve(shop, key):
    """Waits for the shop to be open, for at most 60 seconds."""
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "serve")
    stats.flow_started("orders")
    status("queued")
    stats.task_queued("serve", task_id)
    opening = env.event()

    def check():
        if not opening.triggered and shop.is_open():
            opening.succeed(True)
            env.unwatch_changes(check)

    check()
    if not opening.triggered:
        env.watch_changes(check)
    joined = env.any_of([opening, env.timeout(60)])
    try:
        yield joined
    finally:
        env.unwatch_changes(check)
    status("started")
    stats.task_started("serve", task_id)
    publish({"event_type": "order_received"})
    stats.task_finished("serve", task_id)
    env.notify_change()
    if opening in joined.value:
        env.process(pack(shop, key))
    else:
        env.process(turned_away(shop, key))


def pack(shop, key):
    """Packs on the station; a breakdown sends it back to the queue with what it
    had left."""
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "pack", "packing")
    status("queued")
    stats.task_queued("pack", task_id)
    remaining, started = None, False
    while True:
        request = shop.station.request()
        since = None
        try:
            yield request
            if started:
                status("resumed")
                stats.task_resumed("pack", task_id)
            else:
                status("started")
                stats.task_started("pack", task_id)
                started = True
            if remaining is None:
                remaining = duration()
            since = env.now
            yield env.timeout(remaining)
            publish({"event_type": "order_packed"})
            stats.task_finished("pack", task_id)
            env.notify_change()
            shop.station.release(request)
            break
        except simpy.Interrupt as interrupt:
            request.cancel()
            shop.station.release(request)
            if since is not None:
                remaining -= env.now - since
            status("interrupted", cause=interrupt.cause)
            stats.task_interrupted("pack", task_id)
    env.process(wrap(shop, key))


def wrap(shop, key):
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "wrap", "wrapping")
    status("queued")
    stats.task_queued("wrap", task_id)
    request = shop.packers.request()
    yield request
    status("started")
    stats.task_started("wrap", task_id)
    yield env.timeout(duration())
    publish({"event_type": "order_wrapped"})
    stats.task_finished("wrap", task_id)
    env.notify_change()
    shop.packers.release(request)
    stats.flow_finished("orders")


def turned_away(shop, key):
    env, stats = shop.env, shop.env.stats
    task_id, publish, status, duration = shop.step(key, "turned_away")
    publish({"event_type": "order_turned_away"})
    stats.task_finished("turned_away", task_id)
    env.notify_change()
    stats.flow_finished("orders")
    yield env.timeout(0)


def generator(shop, arrival, start):
    """Starts a task for every arrival, keyed orders-1, days-1 and so on."""
    env = shop.env
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(shop.sampler.sample(gap))
        env.process(start(shop, f"{arrival}-{number}"))


def build(env, sampler):
    return Shop(env, sampler)


def processes(env, sampler, shop):
    """The breakdowns, the arrivals and the schedule, in the builder's order."""
    return [
        run_breakdowns(env, shop.station, sampler, work="interrupt"),
        generator(shop, "orders", serve),
        generator(shop, "days", open_shop),
        run_scenario(env, SCHEDULE, until=600),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    env.setup_stats("Shop", "basic", interval=70.0)
    sampler = Sampler(seed=42)
    shop = build(env, sampler)
    for process in processes(env, sampler, shop):
        env.process(process)
    try:
        env.run(until=600)
    finally:
        env.teardown()


if __name__ == "__main__":
    run()

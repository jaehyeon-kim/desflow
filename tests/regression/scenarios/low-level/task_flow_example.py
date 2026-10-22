"""Task flow: a branching flow with conditions and expressions, waits on groups of
signals and tasks, labels, and entities with many attributes, low-level API.

The low-level twin of `builder/task_flow_example.py`, a regression scenario (see
scripts/regression.py), not an example. Orders, gifts and rush orders enter
check_order, which branches on the stock level and on their attributes. A packed order
waits for two child tasks, then for a courier, a truck or a time limit, and is sent by
whichever came first. A `Router` per task takes the same rules as the builder's `next`.
"""

import itertools

from desflow import (
    CapacityConfig,
    ConsoleEgress,
    ContainerConfig,
    DistributionConfig,
    DynamicContainer,
    DynamicRealtimeEnvironment,
    DynamicResource,
    Entity,
    Router,
    Sampler,
    Signal,
    SimParameter,
    Upstream,
    run_scenario,
)
from desflow.core.flow import make_scope

PARAMS = SimParameter(
    sim_id="Shop",
    arrival={
        "orders": DistributionConfig(dist="exponential", rate=0.12),
        "gifts": DistributionConfig(dist="exponential", rate=0.04),
        "rush": DistributionConfig(dist="exponential", rate=0.02),
        "deliveries": DistributionConfig(dist="constant", value=90),
        "couriers": DistributionConfig(dist="constant", value=50),
        "trucks": DistributionConfig(dist="constant", value=70),
    },
    service={
        "packing": DistributionConfig(dist="triangular", low=1, mode=2, high=4),
        "wrapping": DistributionConfig(dist="constant", value=3),
        "supplying": DistributionConfig(dist="constant", value=30),
        "inspecting": DistributionConfig(dist="constant", value=2),
        "labelling": DistributionConfig(dist="uniform", low=1, high=2),
        "rechecking": DistributionConfig(dist="constant", value=2),
    },
    resources={"packers": CapacityConfig(current_cap=2, max_cap=4)},
    containers={"stock": ContainerConfig(current_cap=100, max_cap=100, level=12)},
)
SCHEDULE = [
    {"at": 200, "record": {"fault": "rush_hour"}},
    {
        "at": 330,
        "path": "Shop.service.packing.high",
        "value": 6,
        "record": {"fault": "slow_packing"},
    },
    {
        "at": 470,
        "path": "Shop.service.packing.high",
        "value": 4,
        "record": {"fault": "packing_recovered"},
    },
]
ATTRIBUTES = {
    "orders": {
        "gift": 0,
        "express": 0,
        "region": "north",
        "items": 2,
        "tier": 1,
        "weight": 1.5,
    },
    "gifts": {
        "gift": 1,
        "express": 0,
        "region": "south",
        "items": 4,
        "tier": 2,
        "weight": 2.5,
    },
    "rush": {
        "gift": 0,
        "express": 1,
        "region": "north",
        "items": 1,
        "tier": 3,
        "weight": 0.5,
    },
}
KEPT = ["gift", "region", "tier", "backordered"]

# Each task: its service, its payload, what it sets, the attributes its payload carries
# and the signal it fires. Packing, settling and dispatching have their own functions.
TASKS = {
    "check_order": {"payload": {"total": 80}},
    "backorder": {
        "service": "supplying",
        "payload": {"event_type": "order_backordered"},
        "set": {"backordered": 1},
    },
    "wrap_gift": {"service": "wrapping", "payload": {"event_type": "gift_wrapped"}},
    "inspect": {"service": "inspecting", "payload": {"event_type": "inspected"}},
    "recheck": {"service": "rechecking", "payload": {"event_type": "rechecked"}},
    "print_label": {"service": "labelling", "payload": {"event_type": "label_printed"}},
    "by_courier": {"payload": {"event_type": "sent_by_courier"}, "attributes": KEPT},
    "by_truck": {"payload": {"event_type": "sent_by_truck"}, "attributes": KEPT},
    "by_post": {"payload": {"event_type": "sent_by_post"}, "attributes": KEPT},
    "restock": {"payload": {"event_type": "stock_delivered"}},
    "courier": {"payload": {"event_type": "courier_left"}, "signal": "courier_left"},
    "truck": {"payload": {"event_type": "truck_left"}, "signal": "truck_ready"},
}


def routes():
    """A Router per task, with the rules of the builder's `next`."""
    return {
        "check_order": Router(
            {
                "when": [
                    {"condition": "level.stock == 0", "task": "backorder"},
                    {
                        "condition": "payload.total >= 100 or attr.gift == 1",
                        "task": "wrap_gift",
                    },
                ],
                "default": "pack_order",
            }
        ),
        "backorder": Router("check_order"),
        "wrap_gift": Router("pack_order"),
        "pack_order": Router({"weights": {"settle": 9, "pack_order": 1}}),
        "settle": Router("dispatch"),
        "dispatch": Router(
            {
                "when": [
                    {"condition": "wait.courier.done", "task": "by_courier"},
                    {"condition": "wait.truck.done", "task": "by_truck"},
                ],
                "default": "by_post",
            }
        ),
    }


class Shop:
    """The shop's parts, its routes and the counters its processes share."""

    def __init__(self, env, sampler):
        self.env, self.sampler = env, sampler
        self.packers = DynamicResource(env, "Shop", "packers")
        self.stock = DynamicContainer(env, "Shop", "stock")
        self.signals = {
            "courier_left": Signal(env, "courier_left"),
            "truck_ready": Signal(env, "truck_ready"),
        }
        self.routes = routes()
        # One random sequence of its own for pack_order's routing, as the builder uses
        self.routes["pack_order"].rng = sampler.generator("route.pack_order")
        self.ids = {}

    def next_id(self, task):
        return next(self.ids.setdefault(task, itertools.count()))

    def start(self, task, entity, child=False):
        """Starts a task of an entity and returns its process."""
        run = {"pack_order": pack_order, "settle": settle, "dispatch": dispatch}
        return self.env.process(
            run.get(task, timed)(self, task, entity, self.next_id(task), child)
        )

    def step(self, entity, task, task_id, service=None):
        """The publish and service-time helpers of one task instance."""
        path_id = f"Shop.service.{service}"

        def publish(value):
            self.env.publish_event(entity.key, value, task=task, instance=task_id)

        def status(state, **extra):
            value = {"path_id": path_id} if service else {}
            publish({**value, "status": state, **extra})

        def duration():
            return self.sampler.sample(self.env.registry.get_config(path_id))

        return publish, status, duration

    def leave(self, task, entity, task_id, payload, waits=None):
        """The task's own end: its attributes, then its route. Returns the next task."""
        attributes = TASKS.get(task, {}).get("set", {})
        entity.attributes.update(attributes)
        handed = Upstream(task_id, entity.key, entity.attributes, payload)
        scope = make_scope(
            handed,
            entity.attributes,
            self.env,
            {"stock": self.stock},
            waits=waits,
            signals=self.signals,
        )
        return handed, self.routes[task].choose(handed, self.env, scope=scope)


def timed(shop, task, entity, task_id, child):
    """A task with no resource: an optional service time, then its payload."""
    env, stats, spec = shop.env, shop.env.stats, TASKS[task]
    service = spec.get("service")
    publish, status, duration = shop.step(entity, task, task_id, service)
    if service:
        status("started")
        stats.task_started(task, task_id)
        yield env.timeout(duration())
    if task == "restock":
        yield shop.stock.put(20)
    if task == "inspect" and entity.attributes["tier"] == 3:
        reason = "rush needs a second look"
        status("failed", reason=reason)
        stats.task_failed(task, task_id)
        return (yield shop.start("recheck", entity, child=True))
    payload = dict(spec["payload"])
    for name in spec.get("attributes", []):
        payload[name] = entity.attributes.get(name)
    publish(payload)
    stats.task_finished(task, task_id)
    env.notify_change()
    if "signal" in spec:
        shop.signals[spec["signal"]].fire(payload)
    if child:
        return payload
    following = None
    if task in shop.routes:
        _, following = shop.leave(task, entity, task_id, payload)
    if following is None:
        stats.flow_finished(entity.arrival)
    else:
        shop.start(following, entity)


def pack_order(shop, task, entity, task_id, child):
    env, stats = shop.env, shop.env.stats
    publish, status, duration = shop.step(entity, task, task_id, "packing")
    status("queued")
    stats.task_queued(task, task_id)
    request = shop.packers.request()
    yield request
    yield shop.stock.get(1)
    status("started")
    stats.task_started(task, task_id)
    yield env.timeout(duration())
    payload = {"event_type": "order_packed"}
    publish(payload)
    stats.task_finished(task, task_id)
    env.notify_change()
    shop.packers.release(request)
    _, following = shop.leave(task, entity, task_id, payload)
    shop.start(following, entity)


def settle(shop, task, entity, task_id, child):
    """Starts inspect and print_label and waits for both."""
    env, stats = shop.env, shop.env.stats
    publish, status, _ = shop.step(entity, task, task_id)
    status("queued")
    stats.task_queued(task, task_id)
    children = {
        "inspected": shop.start("inspect", entity, child=True),
        "labelled": shop.start("print_label", entity, child=True),
    }
    yield env.all_of(list(children.values()))
    status("started")
    stats.task_started(task, task_id)
    payload = {"event_type": "order_settled"}
    publish(payload)
    stats.task_finished(task, task_id)
    env.notify_change()
    _, following = shop.leave(task, entity, task_id, payload)
    shop.start(following, entity)


def dispatch(shop, task, entity, task_id, child):
    """The courier's departure, the truck's, or 40 seconds, whichever comes first."""
    env, stats = shop.env, shop.env.stats
    publish, status, _ = shop.step(entity, task, task_id)
    status("queued")
    stats.task_queued(task, task_id)
    events = {
        "courier": shop.signals["courier_left"].wait(),
        "truck": shop.signals["truck_ready"].wait(),
        "limit": env.timeout(40),
    }
    joined = env.any_of(list(events.values()))
    yield joined
    waits = {
        name: {
            "done": event in joined.value,
            "ok": event.ok if event in joined.value else None,
            "value": event.value if event in joined.value else None,
        }
        for name, event in events.items()
    }
    status("started")
    stats.task_started(task, task_id)
    payload = {"event_type": "dispatched"}
    publish(payload)
    stats.task_finished(task, task_id)
    env.notify_change()
    _, following = shop.leave(task, entity, task_id, payload, waits)
    shop.start(following, entity)


def generator(shop, arrival, first):
    """Starts an entity for every arrival, keyed orders-1, gifts-1 and so on."""
    env = shop.env
    gap = env.registry.get_config(f"Shop.arrival.{arrival}")
    for number in itertools.count(1):
        yield env.timeout(shop.sampler.sample(gap))
        entity = Entity(
            f"{arrival}-{number}", arrival, dict(ATTRIBUTES.get(arrival, {}))
        )
        env.stats.flow_started(arrival)
        shop.start(first, entity)


def build(env, sampler):
    return Shop(env, sampler)


def processes(env, sampler, shop):
    """The arrivals and the schedule, in the builder's order."""
    return [
        generator(shop, "orders", "check_order"),
        generator(shop, "gifts", "check_order"),
        generator(shop, "rush", "check_order"),
        generator(shop, "deliveries", "restock"),
        generator(shop, "couriers", "courier"),
        generator(shop, "trucks", "truck"),
        run_scenario(env, SCHEDULE, until=600),
    ]


def run():
    env = DynamicRealtimeEnvironment(factor=0)
    env.registry.register_sim_parameter(PARAMS)
    env.setup_egress([ConsoleEgress()])
    env.setup_stats("Shop", "basic", interval=60.0)
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

"""Task flow: a branching flow with conditions and expressions, waits on groups of
signals and tasks, labels, and entities with many attributes, builder API.

A regression scenario (see scripts/regression.py), not an example. Orders, gifts and
rush orders enter check_order, which branches on the stock level and on their
attributes. A packed order waits for two child tasks, then for a courier, a truck or a
time limit, and is sent by whichever came first.
"""

from desflow import ConsoleEgress, SimulationContext

KEPT = ("gift", "region", "tier", "backordered")

app = (
    SimulationContext(sim_id="Shop", factor=0, random_seed=42)
    .add_egress(ConsoleEgress())
    .add_resource("packers", current_cap=2, max_cap=4)
    .add_container("stock", current_cap=100, max_cap=100, level=12)
    .add_signal("courier_left")
    .add_signal("truck_ready")
    .add_service("packing", dist="triangular", low=1, mode=2, high=4)
    .add_service("wrapping", dist="constant", value=3)
    .add_service("supplying", dist="constant", value=30)
    .add_service("inspecting", dist="constant", value=2)
    .add_service("labelling", dist="uniform", low=1, high=2)
    .add_service("rechecking", dist="constant", value=2)
    .add_arrival(
        "orders",
        dist="exponential",
        rate=0.12,
        attributes={
            "gift": 0,
            "express": 0,
            "region": "north",
            "items": 2,
            "tier": 1,
            "weight": 1.5,
        },
    )
    .add_arrival(
        "gifts",
        dist="exponential",
        rate=0.04,
        attributes={
            "gift": 1,
            "express": 0,
            "region": "south",
            "items": 4,
            "tier": 2,
            "weight": 2.5,
        },
    )
    .add_arrival(
        "rush",
        dist="exponential",
        rate=0.02,
        attributes={
            "gift": 0,
            "express": 1,
            "region": "north",
            "items": 1,
            "tier": 3,
            "weight": 0.5,
        },
    )
    .add_arrival("deliveries", dist="constant", value=90)
    .add_arrival("couriers", dist="constant", value=50)
    .add_arrival("trucks", dist="constant", value=70)
    .add_scenario(
        [
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
    )
    .with_stats("basic", interval=60.0)
)


@app.task(
    next={
        "when": [
            {"condition": "level.stock == 0", "task": "backorder"},
            {
                "condition": "payload.total >= 100 or attr.gift == 1",
                "task": "wrap_gift",
            },
        ],
        "default": "pack_order",
    }
)
def check_order(task_id, context):
    return {"total": 80}


@app.task(service_id="supplying", set={"backordered": 1}, next="check_order")
def backorder(task_id, context):
    return {"event_type": "order_backordered"}


@app.task(service_id="wrapping", next="pack_order")
def wrap_gift(task_id, context):
    return {"event_type": "gift_wrapped"}


@app.task(
    service_id="packing",
    resource_id="packers",
    get={"container": "stock", "amount": 1},
    next={"weights": {"settle": 9, "pack_order": 1}},
)
def pack_order(task_id, context):
    return {"event_type": "order_packed"}


@app.task(
    wait={
        "all": {"inspected": {"task": "inspect"}, "labelled": {"task": "print_label"}}
    },
    next="dispatch",
)
def settle(task_id, context):
    return {"event_type": "order_settled"}


@app.task(
    service_id="inspecting",
    fail="attr.tier == 3",
    fail_reason="rush needs a second look",
    on_fail="recheck",
)
def inspect(task_id, context):
    return {"event_type": "inspected"}


@app.task(service_id="rechecking")
def recheck(task_id, context):
    return {"event_type": "rechecked"}


@app.task(service_id="labelling")
def print_label(task_id, context):
    return {"event_type": "label_printed"}


@app.task(
    wait={
        "any": {
            "courier": {"signal": "courier_left"},
            "truck": {"signal": "truck_ready"},
            "limit": {"timeout": 40},
        }
    },
    next={
        "when": [
            {"condition": "wait.courier.done", "task": "by_courier"},
            {"condition": "wait.truck.done", "task": "by_truck"},
        ],
        "default": "by_post",
    },
)
def dispatch(task_id, context):
    return {"event_type": "dispatched"}


def sent(event_type, entity):
    """A payload with the attributes the order is sent with."""
    return {
        "event_type": event_type,
        **{name: entity.attributes.get(name) for name in KEPT},
    }


@app.task()
def by_courier(task_id, context, entity):
    return sent("sent_by_courier", entity)


@app.task()
def by_truck(task_id, context, entity):
    return sent("sent_by_truck", entity)


@app.task()
def by_post(task_id, context, entity):
    return sent("sent_by_post", entity)


@app.task(put={"container": "stock", "amount": 20})
def restock(task_id, context):
    return {"event_type": "stock_delivered"}


@app.task(signal="courier_left")
def courier(task_id, context):
    return {"event_type": "courier_left"}


@app.task(signal="truck_ready")
def truck(task_id, context):
    return {"event_type": "truck_left"}


def spawn_loop(arrival, task, name):
    """Spawns `task` for every arrival of `arrival`."""

    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            context.spawn(task(context.next_instance(name), context, entity=entity))

    return loop


for arrival, task, name in (
    ("orders", check_order, "check_order"),
    ("gifts", check_order, "check_order"),
    ("rush", check_order, "check_order"),
    ("deliveries", restock, "restock"),
    ("couriers", courier, "courier"),
    ("trucks", truck, "truck"),
):
    app.arrival_loop(arrival)(spawn_loop(arrival, task, name))


def run():
    app.run(until=600)


if __name__ == "__main__":
    run()

"""The flows of the Task Flow page: builder and low-level.

tests/docs runs this file: the builder model runs unpaced for 2 minutes, and the
low-level part routes an entity with a Router and checks a refused expression.
"""

from desflow import (
    Entity,
    Router,
    Sampler,
    SimulationContext,
    Upstream,
)
from desflow.core.expressions import Expression

app = (
    SimulationContext(sim_id="Shop", factor=0.0, random_seed=42)
    .add_resource("packers", current_cap=2, max_cap=4)
    .add_container("stock", current_cap=100, max_cap=100, level=10)
    .add_service("packing", dist="constant", value=1.5)
    .add_service("wrapping", dist="constant", value=3)
    .add_service("supplying", dist="constant", value=30)
    .add_arrival("deliveries", dist="constant", value=30)
)
# --8<-- [start:builder-attributes]
# Each order starts with these attributes, read as attr.gift
# An order every 2 s on average (rate 0.5)
app.add_arrival("orders", dist="exponential", rate=0.5, attributes={"gift": 0})
# A gift order every 10 s on average (rate 0.1)
app.add_arrival("gifts", dist="exponential", rate=0.1, attributes={"gift": 1})
# --8<-- [end:builder-attributes]
# --8<-- [start:builder-key]
# Keys ret-00001, ret-00002 instead of returns-1, from the counter, which starts at 1
# A return every 20 s on average (rate 0.05)
app.add_arrival("returns", dist="exponential", rate=0.05, key=lambda n: f"ret-{n:05d}")
# --8<-- [end:builder-key]


# --8<-- [start:builder-condition]
# Rules are tried in order; the first match wins. Otherwise, use the default.
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
def check_order(task_id: int):
    return {"total": 80}


# --8<-- [end:builder-condition]


# --8<-- [start:builder-set]
# `set` is applied to the order's attributes when the task finishes
@app.task(service_id="supplying", set={"backordered": 1}, next="check_order")
def backorder(task_id: int):
    return {"event_type": "order_backordered"}


# --8<-- [end:builder-set]


@app.task(service_id="wrapping", next="pack_order")
def wrap_gift(task_id: int):
    return {"event_type": "gift_wrapped"}


# --8<-- [start:builder-weights]
# A task drawn in proportion to its weight: one in ten is packed again
@app.task(
    service_id="packing",
    resource_id="packers",
    get={"container": "stock", "amount": 1},
    next={"weights": {"ship_order": 9, "pack_order": 1}},
)
def pack_order(task_id: int):
    return {"event_type": "order_packed"}


# --8<-- [end:builder-weights]


# --8<-- [start:builder-upstream]
# A function declaring `upstream` receives what the previous task handed on,
# and `entity` the order itself; a payload includes attributes only when it asks
@app.task()
def ship_order(task_id: int, upstream, entity):
    return {
        "event_type": "order_shipped",
        "packed_by": upstream.task_id,  # the instance of pack_order before it
        "gift": entity.attributes.get("gift"),
        "backordered": entity.attributes.get("backordered"),
    }


# A rule can also be a function of the Upstream, returning a task name or None
@app.task(next=lambda upstream: "check_order" if upstream.payload["ok"] else None)
def inspect_return(task_id: int):
    return {"ok": False}


# --8<-- [end:builder-upstream]


@app.task(put={"container": "stock", "amount": 20})
def restock(task_id: int):
    return {"event_type": "stock_delivered"}


def spawn(arrival, task, name):
    def loop(context):
        while True:
            entity = yield context.wait_for_arrival(arrival)
            # One counter per task, shared by every arrival and by next
            context.spawn(task(context.next_instance(name), entity=entity))

    return loop


app.arrival_loop("orders")(spawn("orders", check_order, "check_order"))
app.arrival_loop("gifts")(spawn("gifts", check_order, "check_order"))
app.arrival_loop("deliveries")(spawn("deliveries", restock, "restock"))
app.arrival_loop("returns")(spawn("returns", inspect_return, "inspect_return"))

# --8<-- [start:low-level-condition]
# The same rules as next, as a Router; choose() returns the next task's name
route = Router(
    {
        "when": [{"condition": "attr.gift == 1", "task": "wrap_gift"}],
        "default": "pack_order",
    }
)
order = Entity("orders-1", "orders", {"gift": 0})  # key, arrival, attributes
finished = Upstream(0, order.key, order.attributes, {"total": 80})
assert route.choose(finished, env=None) == "pack_order"
# --8<-- [end:low-level-condition]

# --8<-- [start:low-level-weights]
# Weights draw from a generator: the routing task's own stream, as the builder's do
rework = Router({"weights": {"ship_order": 9, "pack_order": 1}})
rework.rng = Sampler(seed=42).generator("route.pack_order")
choices = [rework.choose(finished) for _ in range(1000)]
# --8<-- [end:low-level-weights]

# --8<-- [start:refused]
for text in ("payload.total ** 2", "x > 1", "open('orders.csv')"):
    try:
        Expression(text)
    except ValueError as error:
        print(error)
# --8<-- [end:refused]

if __name__ == "__main__":
    assert 0.05 < choices.count("pack_order") / len(choices) < 0.15
    app.run(until="2 min")

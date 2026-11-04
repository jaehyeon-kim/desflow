"""DX-1: An order with 1 to 5 random items and a total.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. The draws are made with `Sampler`, from the streams the
blueprint's generators use.
"""

import itertools

from desflow import DistributionConfig, DynamicRealtimeEnvironment, Sampler

UNTIL = 6


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    gap = DistributionConfig(dist="exponential", rate=1.0)
    gap.stream = "arrival.orders"  # the random sequence the blueprint's draw uses
    times = sampler.generator("tasks.order.set.lines.times")
    price = sampler.generator("tasks.order.set.lines.value.price")
    quantity = sampler.generator("tasks.order.set.lines.value.quantity")
    product = sampler.generator("tasks.item.payload.product_id")
    customer = sampler.generator("tasks.items.payload.customer_id")
    counts = {"item": 0}  # items made so far: their instance number and their id

    def order(key, task_id):
        order_id = task_id + 1
        env.publish_event(key, {"event_type": "drawn"}, task="order", instance=task_id)
        lines = [
            {
                "price": round(float(price.uniform(10.0, 50.0)), 2),
                "quantity": int(quantity.integers(1, 4)),
            }
            for _ in range(int(times.integers(1, 6)))
        ]
        env.publish_event(key, {"status": "queued"}, task="items", instance=task_id)
        for line in lines:
            counts["item"] += 1
            env.publish_event(
                key,
                {
                    "__table__": "order_items",
                    "order_item_id": counts["item"],
                    "order_id": order_id,
                    "product_id": int(product.integers(1, 51)),
                    "quantity": line["quantity"],
                    "unit_price": line["price"],
                },
                task="item",
                instance=counts["item"] - 1,
            )
        env.publish_event(key, {"status": "started"}, task="items", instance=task_id)
        total = round(sum(line["price"] * line["quantity"] for line in lines), 2)
        env.publish_event(
            key,
            {
                "__table__": "orders",
                "order_id": order_id,
                "customer_id": int(customer.integers(1, 101)),
                "total_amount": total,
                "status": "pending",
            },
            task="items",
            instance=task_id,
        )
        yield env.timeout(0)

    def source():
        for number in itertools.count(1):
            yield env.timeout(sampler.sample(gap))
            env.process(order(f"orders-{number}", number - 1))

    env.process(source())

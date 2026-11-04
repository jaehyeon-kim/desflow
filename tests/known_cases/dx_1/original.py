"""DX-1: An order with 1 to 5 random items and a total.

The original is the example's own Python: `original.yaml` runs `order_generator` of
examples/yaml/advanced/postgres_orders_logic.py as a process. The test runs it, and the YAML form
publishes orders of the same shape. Both draw from a seeded stream, but not the same one,
so `compare` states what holds for each: the same number of orders arrive at the same
times, each has 1 to 5 items, the ids run on from 1, each value lies in the range
`order_generator` draws it from, the status is `pending`, and an order's total is the sum
of its items.
"""

PYTHON_FORM = "tests/known_cases/dx_1/original.yaml"
LOGIC = "examples/yaml/advanced"  # the folder of postgres_orders_logic.py


def _tables(records):
    rows = [
        r for r in records if r["stream_type"] == "event" and "__table__" in r["value"]
    ]
    orders = [r for r in rows if r["value"]["__table__"] == "orders"]
    items = [r for r in rows if r["value"]["__table__"] == "order_items"]
    return orders, items


def _drawn_as_the_example_draws(order, items):
    """Each value lies in the range `order_generator` draws it from."""
    assert 1 <= order["customer_id"] <= 100
    assert order["status"] == "pending"
    for item in items:
        assert 10.0 <= item["unit_price"] <= 50.0
        assert round(item["unit_price"], 2) == item["unit_price"]
        assert 1 <= item["quantity"] <= 3
        assert 1 <= item["product_id"] <= 50


def compare(python_records, yaml_records):
    """The two runs publish the same number of orders, of one shape and one range."""
    orders_p, items_p = _tables(python_records)
    orders_y, items_y = _tables(yaml_records)
    assert orders_y and orders_p
    # the arrival draws the same gaps from the same stream, so the orders come together
    assert [r["sim_ts"] for r in orders_p] == [r["sim_ts"] for r in orders_y]
    for orders, items in ((orders_p, items_p), (orders_y, items_y)):
        assert [r["value"]["order_id"] for r in orders] == list(
            range(1, len(orders) + 1)
        )
        assert [r["value"]["order_item_id"] for r in items] == list(
            range(1, len(items) + 1)
        )
        for order in orders:
            mine = [
                r["value"]
                for r in items
                if r["value"]["order_id"] == order["value"]["order_id"]
            ]
            assert 1 <= len(mine) <= 5
            _drawn_as_the_example_draws(order["value"], mine)
            total = round(sum(i["unit_price"] * i["quantity"] for i in mine), 2)
            assert abs(order["value"]["total_amount"] - total) < 0.011
            assert all(
                set(i)
                == {
                    "__table__",
                    "order_item_id",
                    "order_id",
                    "product_id",
                    "quantity",
                    "unit_price",
                }
                for i in mine
            )
            assert set(order["value"]) == {
                "__table__",
                "order_id",
                "customer_id",
                "total_amount",
                "status",
            }

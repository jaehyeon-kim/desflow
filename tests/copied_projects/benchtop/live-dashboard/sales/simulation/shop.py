"""The shop's rules, apart from time: who signs up, what a basket holds, a status change.

Every choice comes from the random generator passed in, so a seeded run repeats.
"""

import uuid

from numpy.random import Generator
from pydantic import BaseModel

from sales.core.models import Order, OrderItem, Product, User
from sales.simulation.catalogue import COUNTRIES, ITEM_COUNTS, TRAFFIC_SOURCES


def choose(rng: Generator, weights: dict):
    """
    Picks one key of `weights`, in proportion to its weight.

    Args:
        rng (Generator): The random generator.
        weights (dict): The choices and their weights.

    Returns:
        Any: The key chosen.
    """
    keys, total = list(weights), sum(weights.values())
    return keys[int(rng.choice(len(keys), p=[w / total for w in weights.values()]))]


def new_id(rng: Generator) -> str:
    """Returns a UUID drawn from the generator, so a seeded run repeats its ids."""
    return str(uuid.UUID(bytes=rng.bytes(16), version=4))


def new_user(rng: Generator, now: str) -> User:
    """
    Signs up a new user.

    Args:
        rng (Generator): The random generator.
        now (str): The time, as ISO 8601 text.

    Returns:
        User: The user.
    """
    return User(
        id=new_id(rng),
        age=int(rng.integers(12, 71)),
        gender=choose(rng, {"M": 1, "F": 1}),
        country=choose(rng, COUNTRIES),
        traffic_source=choose(rng, TRAFFIC_SOURCES),
        created_at=now,
    )


def basket(
    rng: Generator, user: User, catalogue: list[Product], now: str
) -> tuple[Order, list[OrderItem]]:
    """
    Places an order of one to four different products.

    Args:
        rng (Generator): The random generator.
        user (User): The buyer.
        catalogue (list[Product]): The products on sale.
        now (str): The time, as ISO 8601 text.

    Returns:
        tuple[Order, list[OrderItem]]: The order and its items, all `Processing`.
    """
    count = choose(rng, ITEM_COUNTS)
    order = Order(
        id=new_id(rng),
        user_id=user.id,
        status="Processing",
        num_of_item=count,
        created_at=now,
    )
    picked = rng.choice(len(catalogue), size=count, replace=False)
    items = [
        OrderItem(
            id=new_id(rng),
            order_id=order.id,
            user_id=user.id,
            product_id=catalogue[int(i)].id,
            status="Processing",
            sale_price=catalogue[int(i)].retail_price,
            created_at=now,
        )
        for i in picked
    ]
    return order, items


def with_status(rows: list[BaseModel], status: str) -> list[BaseModel]:
    """
    Returns copies of an order and its items with a new status.

    Args:
        rows (list[BaseModel]): The order and its items.
        status (str): The new status, such as `Shipped`.

    Returns:
        list[BaseModel]: The copies, with the same ids.
    """
    return [row.model_copy(update={"status": status}) for row in rows]

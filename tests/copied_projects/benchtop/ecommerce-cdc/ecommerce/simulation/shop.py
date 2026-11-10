"""The shop's rules as plain Python: new users and orders, status changes, moves and visits.

Every choice draws from the random generator it is given, so a seeded run repeats. The
simulation in `ecommerce.simulation.run` decides when each rule applies.
"""

import uuid

from faker import Faker
from numpy.random import Generator

from ecommerce.core.models import Order, OrderItem, Product, User
from ecommerce.simulation.catalogue import CITIES

# The statuses an order can move to from each one. An order is cancelled only before it
# ships, and returned only after it is delivered.
NEXT_STATUS = {
    "Processing": {"Shipped", "Cancelled"},
    "Shipped": {"Delivered"},
    "Delivered": {"Returned"},
    "Cancelled": set(),
    "Returned": set(),
}
_WHERE = ("country", "state", "city", "postal_code", "latitude", "longitude")
_CITY_WEIGHTS = [c.population / sum(c.population for c in CITIES) for c in CITIES]


def new_id(rng: Generator) -> str:
    """Returns a new id drawn from `rng`, so a seeded run repeats its ids."""
    return str(uuid.UUID(bytes=rng.bytes(16), version=4))


def city(rng: Generator) -> dict:
    """
    Chooses a city in proportion to its population.

    Args:
        rng (Generator): The simulation's random number generator.

    Returns:
        dict: The city's country, state, city, postal code, latitude and longitude.
    """
    chosen = CITIES[int(rng.choice(len(CITIES), p=_CITY_WEIGHTS))]
    return {k: getattr(chosen, k) for k in _WHERE}


def new_user(fake: Faker, rng: Generator, now: str) -> User:
    """
    Builds a newly registered user, living in a city chosen by population.

    Args:
        fake (Faker): The seeded generator of names and addresses.
        rng (Generator): The simulation's random number generator.
        now (str): The time of registering.

    Returns:
        User: The user.
    """
    gender = str(rng.choice(["M", "F"]))
    first = fake.first_name_male() if gender == "M" else fake.first_name_female()
    last = fake.last_name()
    return User(
        id=new_id(rng),
        first_name=first,
        last_name=last,
        email=f"{first.lower()}.{last.lower()}@{fake.safe_domain_name()}",
        age=int(rng.integers(12, 71)),
        gender=gender,
        street_address=fake.street_address(),
        traffic_source=str(rng.choice(["Organic", "Facebook", "Search", "Email"])),
        created_at=now,
        updated_at=now,
        **city(rng),
    )


def moved(user: User, fake: Faker, rng: Generator, now: str) -> User:
    """
    Returns the user after a move to a new address in a city chosen by population.

    Args:
        user (User): The user.
        fake (Faker): The seeded generator of addresses.
        rng (Generator): The simulation's random number generator.
        now (str): The time of the move.

    Returns:
        User: The user with the new address.
    """
    change = {"street_address": fake.street_address(), "updated_at": now, **city(rng)}
    return user.model_copy(update=change)


def new_order(
    rng: Generator, products: list[Product], user: User, now: str
) -> tuple[Order, list[OrderItem]]:
    """
    Builds a new order of one to four products, in the Processing status.

    Args:
        rng (Generator): The simulation's random number generator.
        products (list[Product]): The catalogue.
        user (User): The buyer.
        now (str): The time of ordering.

    Returns:
        tuple[Order, list[OrderItem]]: The order and its items.
    """
    size = int(rng.choice([1, 2, 3, 4], p=[0.7, 0.2, 0.05, 0.05]))
    order = Order(
        id=new_id(rng),
        user_id=user.id,
        status="Processing",
        num_of_items=size,
        created_at=now,
        updated_at=now,
    )
    items = [
        OrderItem(
            id=new_id(rng),
            order_id=order.id,
            product_id=p.id,
            status="Processing",
            quantity=int(rng.integers(1, 4)),
            sale_price=p.retail_price,
            created_at=now,
            updated_at=now,
        )
        for p in (products[i] for i in rng.integers(len(products), size=size))
    ]
    return order, items


def advance(
    order: Order, items: list[OrderItem], status: str, now: str
) -> tuple[Order, list[OrderItem]]:
    """
    Moves an order and its items to a new status, and stamps the status's time column.

    Args:
        order (Order): The order.
        items (list[OrderItem]): Its items.
        status (str): Shipped, Delivered, Cancelled or Returned.
        now (str): The time of the change.

    Returns:
        tuple[Order, list[OrderItem]]: The changed order and items.

    Raises:
        ValueError: If the order cannot move to `status` from its current status.
    """
    if status not in NEXT_STATUS[order.status]:
        raise ValueError(f"an order cannot move from {order.status} to {status}")
    change = {"status": status, "updated_at": now, f"{status.lower()}_at": now}
    return order.model_copy(update=change), [i.model_copy(update=change) for i in items]


def pages(rng: Generator, products: list[Product], buys: bool) -> list[tuple[str, str]]:
    """
    Chooses the pages a visitor views: the home page, one to four products, and the
    cart and purchase pages when the visitor buys.

    Args:
        rng (Generator): The simulation's random number generator.
        products (list[Product]): The catalogue.
        buys (bool): Whether the visitor buys.

    Returns:
        list[tuple[str, str]]: Each page's type and address, in the order viewed.
    """
    viewed = (
        products[i] for i in rng.integers(len(products), size=int(rng.integers(1, 5)))
    )
    visit = [("home", "/home"), *(("product", f"/product/{p.id}") for p in viewed)]
    return visit + [("cart", "/cart"), ("purchase", "/purchase")] if buys else visit

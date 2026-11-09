"""Generates the simulated users and writes them to `users.csv`."""

import csv
import dataclasses
import logging
from collections import OrderedDict

from faker import Faker

from recommender.core.config import NUM_USERS, USERS
from recommender.core.models import User
from recommender.data.locations import Locations, load_locations

logger = logging.getLogger(__name__)

# How users arrive, and the share of each.
_TRAFFIC = OrderedDict(
    zip(
        ["Organic", "Facebook", "Search", "Email", "Display"],
        [0.15, 0.06, 0.7, 0.05, 0.04],
    )
)


def new_user(user_id: int, locations: Locations, fake: Faker) -> User:
    """
    Draws one user's profile.

    Args:
        user_id (int): The user's id.
        locations (Locations): Where users live.
        fake (Faker): The seeded Faker that draws every value.

    Returns:
        User: The new user.
    """
    gender = fake.random_element(elements=("M", "F"))
    first_name = fake.first_name_male() if gender == "M" else fake.first_name_female()
    last_name = fake.last_name_nonbinary()
    location = locations.pick(fake)
    traffic_source = fake.random_choices(elements=_TRAFFIC, length=1)[0]
    return User(
        user_id=user_id,
        first_name=first_name,
        last_name=last_name,
        email=f"{first_name.lower()}.{last_name.lower()}@{fake.safe_domain_name()}",
        age=fake.random_int(min=16, max=70),
        gender=gender,
        street_address=fake.street_address(),
        traffic_source=traffic_source,
        **location,
    )


def generate_users(fake: Faker) -> None:
    """
    Generates `NUM_USERS` users and writes them to `users.csv`.

    Args:
        fake (Faker): The seeded Faker that draws every value.
    """
    logger.info(f"Generating {NUM_USERS} synthetic users...")
    locations = load_locations()
    users = [
        dataclasses.asdict(new_user(i + 1, locations, fake)) for i in range(NUM_USERS)
    ]
    with open(USERS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(users[0]))
        writer.writeheader()
        writer.writerows(users)
    logger.info(f"Saved raw users to: {USERS}")

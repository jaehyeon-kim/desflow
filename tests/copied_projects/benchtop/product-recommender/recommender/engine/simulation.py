"""Simulates users: the time of a visit, whether they click, and a live visit.

A hidden formula decides clicks, so the bandit has a pattern to learn: coffee sells in the
morning, pizza and burgers at the weekend, young users avoid expensive items, and users
from search click more.
"""

import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
from faker import Faker

from recommender.core.config import ANCHOR_DATE
from recommender.core.models import User
from recommender.data.features import user_features


def time_context(dt: datetime) -> dict:
    """
    Turns a visit time into the time-of-day and day-of-week features.

    Args:
        dt (datetime): The visit time, in local wall-clock time.

    Returns:
        dict: `is_morning`, `is_afternoon`, `is_evening`, `is_weekend` and `is_weekday`,
            each 0 or 1.
    """
    is_weekend = 1 if dt.weekday() >= 5 else 0
    return {
        "is_morning": 1 if 6 <= dt.hour < 12 else 0,
        "is_afternoon": 1 if 12 <= dt.hour < 18 else 0,
        "is_evening": 1 if 18 <= dt.hour < 24 else 0,
        "is_weekend": is_weekend,
        "is_weekday": 1 - is_weekend,
    }


def click_probability(user_ctx: dict, item_ctx: dict) -> float:
    """
    Returns the chance that a user clicks an item: the hidden formula the bandit learns.

    Args:
        user_ctx (dict): The user and time features.
        item_ctx (dict): The product features.

    Returns:
        float: The click probability, from 0 to 1.
    """
    score = -2.5  # the base logit, a low probability

    # Rule 1: Morning Coffee
    if user_ctx.get("is_morning") == 1 and item_ctx.get("is_coffee") == 1:
        score += 2.5

    # Rule 2: Weekend Comfort Food
    if user_ctx.get("is_weekend") == 1 and (
        item_ctx.get("cat_Pizzas") == 1 or item_ctx.get("cat_Burgers & Sandwiches") == 1
    ):
        score += 1.8

    # Rule 3: Budget Constraint (age and price are scaled from 0 to 1)
    if user_ctx.get("age", 0.5) < 0.25 and item_ctx.get("price", 0.5) > 0.8:
        score -= 3.0

    # Rule 4: Traffic Bias
    if user_ctx.get("traffic_source_Search") == 1:
        score += 0.5

    return 1 / (1 + np.exp(-score))


def will_click(user_ctx: dict, item_ctx: dict, fake: Faker) -> int:
    """
    Decides whether a user clicks an item, with the formula's probability.

    Args:
        user_ctx (dict): The user and time features.
        item_ctx (dict): The product features.
        fake (Faker): The seeded Faker, whose random generator decides.

    Returns:
        int: 1 for a click, 0 for none.
    """
    return 1 if fake.random.random() < click_probability(user_ctx, item_ctx) else 0


@dataclass
class Visit:
    """
    One simulated visit: who came when, what they were shown, and what they clicked.

    Attributes:
        user (User): The visiting user.
        time (datetime): The simulated visit time.
        context (dict): The user and time features the recommendation used.
        recommendations (list[int]): The recommended product ids, best first.
        chosen (int): The clicked product, or the first recommendation if none.
        reward (int): 1 for a click, 0 for none.
    """

    user: User
    time: datetime
    context: dict
    recommendations: list[int]
    chosen: int
    reward: int

    def __str__(self) -> str:
        recs = ", ".join(str(r).rjust(3, "0") for r in self.recommendations)
        chosen = str(self.chosen).rjust(3, "0")
        result = (
            f"Clicked: {chosen}  (✅), Reward: 1 on {chosen}"
            if self.reward
            else f"Clicked: None (❌), Reward: 0 on {chosen}"
        )
        return (
            f"User {str(self.user.user_id).rjust(4, '0')} ({self.user.age} yo) @ {self.time.strftime('%a %H:%M')} "
            f"-> Recs: [{recs}] -> {result}"
        )  # fmt: skip


def visit(
    users: list[dict],
    products: dict[int, dict],
    artifacts: dict,
    recommend: Callable[[dict], list[int]],
    fake: Faker,
) -> Visit:
    """
    Simulates one visit: a random user arrives, gets recommendations and reacts.

    The user reads the recommendations in order and clicks the first one the hidden
    formula makes them click, or none.

    Args:
        users (list[dict]): The users, as rows of `users.csv`.
        products (dict[int, dict]): Each product's features, by product id.
        artifacts (dict): The fitted transforms, from `load_artifacts`.
        recommend (Callable[[dict], list[int]]): Returns the product ids to show,
            best first, for the user and time features.
        fake (Faker): The seeded Faker that draws the time and the clicks.

    Returns:
        Visit: What happened.
    """
    user = User(**random.choice(users))
    visit_time = fake.date_time_between(
        start_date=ANCHOR_DATE - timedelta(days=7), end_date=ANCHOR_DATE
    )
    context = {**user_features(user, artifacts), **time_context(visit_time)}
    recommendations = recommend(context)
    chosen, reward = recommendations[0], 0
    for item_id in recommendations:
        if will_click(context, products[item_id], fake):
            chosen, reward = item_id, 1
            break
    return Visit(user, visit_time, context, recommendations, chosen, reward)

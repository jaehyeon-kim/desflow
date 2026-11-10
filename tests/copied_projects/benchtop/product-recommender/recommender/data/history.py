"""Generates the click history that the models are first trained on."""

import logging
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

from recommender.core.config import (
    HISTORY_END,
    NUM_PAST_EVENTS,
    PRODUCT_FEATURES,
    TRAINING_LOG,
    USER_FEATURES,
)
from recommender.engine.simulation import time_context, will_click

logger = logging.getLogger(__name__)


def generate_history(fake: Faker) -> None:
    """
    Writes `training_log.csv`: random users shown random products over 90 days.

    Args:
        fake (Faker): The seeded Faker that draws every user, product, time and click.
    """
    users = pd.read_csv(USER_FEATURES).to_dict("records")
    products = pd.read_csv(PRODUCT_FEATURES).to_dict("records")
    logger.info(f"Loaded {len(users)} users and {len(products)} products.")
    logger.info(f"Generating {NUM_PAST_EVENTS} events...")
    end = datetime.strptime(HISTORY_END, "%Y-%m-%d")  # noqa: DTZ007 (local wall-clock time)
    rows = []
    for event_id in range(1, NUM_PAST_EVENTS + 1):
        user = fake.random_element(elements=users)
        item = fake.random_element(elements=products)
        visit_time = fake.date_time_between(
            start_date=end - timedelta(days=90), end_date=end
        )
        context = {
            "event_id": event_id,
            **{k: v for k, v in user.items() if k != "user_id"},
            **time_context(visit_time),
        }
        response = will_click(context, item, fake)
        rows.append({**context, "product_id": item["product_id"], "response": response})
    df = pd.DataFrame(rows)
    df.to_csv(TRAINING_LOG, index=False)
    logger.info(f"Done. Saved Training Log to {TRAINING_LOG}")
    logger.info(f"Avg Click Rate: {df['response'].mean():.2%}")

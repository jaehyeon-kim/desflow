"""Settings shared by the entry points: paths, the seed, and the odctl service addresses.

The service addresses use 127.0.0.1 rather than localhost: on an IPv6-first host,
localhost resolves to ::1 first, and the schema registry client fails outright.
"""

import logging
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
USERS = DATA_DIR / "users.csv"
PRODUCTS = DATA_DIR / "products.csv"
LOCATIONS = DATA_DIR / "world_pop.csv"
USER_FEATURES = DATA_DIR / "user_features.csv"
PRODUCT_FEATURES = DATA_DIR / "product_features.csv"
TRAINING_LOG = DATA_DIR / "training_log.csv"
ARTIFACTS = DATA_DIR / "preprocessing_artifacts.pkl"

SEED = 1237
NUM_USERS = 1000
CITY = ("Australia", "Melbourne")  # the country and city the users live in
TEXT_COMPONENTS = 10  # SVD components for the product text embedding
NUM_PAST_EVENTS = 10_000
HISTORY_END = "2026-01-01"  # the training log covers the 90 days before this date
ANCHOR_DATE = datetime(2026, 1, 1)  # noqa: DTZ001 (the simulation uses local wall-clock time)
LOCAL_VISITS = 30
TOP_K = 5
ALPHA = 1.0  # LinUCB's exploration weight
TIME_COLUMNS = ["is_morning", "is_afternoon", "is_evening", "is_weekend", "is_weekday"]

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
SCHEMA_REGISTRY = "http://127.0.0.1:8081"
FEEDBACK_TOPIC = "feedback-events"
VALKEY = {"host": "127.0.0.1", "port": 6379, "username": "user", "password": "password"}
# The Valkey key of a product's model, which the Flink job writes.
MODEL_KEY = "linucb:{}"
FLINK_URL = "http://127.0.0.1:8082"
FLINK_JOB = "RecommenderParameterUpdate"  # the job name the Kotlin trainer sets


def setup_logging() -> None:
    """Logs at INFO with the timestamped format the posts show."""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)-8s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

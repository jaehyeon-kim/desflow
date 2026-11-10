"""The simulated user, and the feedback event the Flink job learns from."""

import json
from dataclasses import dataclass


@dataclass
class User:
    """
    A simulated user, as stored in `users.csv`.

    Attributes:
        user_id (int): The user's id, from 1.
        first_name (str): The first name.
        last_name (str): The last name.
        email (str): The email address.
        age (int): The age, from 16 to 70.
        gender (str): `M` or `F`.
        street_address (str): The street address.
        postal_code (str): The postal code.
        city (str): The city.
        state (str): The state.
        country (str): The country.
        latitude (float): The latitude of the postal area.
        longitude (float): The longitude of the postal area.
        traffic_source (str): How the user arrived, such as `Search` or `Email`.
    """

    user_id: int
    first_name: str
    last_name: str
    email: str
    age: int
    gender: str
    street_address: str
    postal_code: str
    city: str
    state: str
    country: str
    latitude: float
    longitude: float
    traffic_source: str


@dataclass
class FeedbackEvent:
    """
    A user's reaction to a recommended product, sent to Kafka for the Flink job.

    Attributes:
        event_id (str): A unique id, `evt_` and the send time in milliseconds.
        product_id (str): The product the user reacted to.
        reward (int): 1 for a click, 0 for none.
        context_vector (list[float]): The user and time features, in the model's order.
        timestamp (int): The simulated visit time, in milliseconds since the Unix epoch.
    """

    event_id: str
    product_id: str
    reward: int
    context_vector: list[float]
    timestamp: int


# The Avro schema the Flink job reads the feedback events with.
FEEDBACK_SCHEMA = json.dumps(
    {
        "namespace": "me.jaehyeon",
        "type": "record",
        "name": "FeedbackEvent",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "product_id", "type": "string"},
            {"name": "reward", "type": "int"},
            {"name": "context_vector", "type": {"type": "array", "items": "double"}},
            {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
        ],
    }
)

"""BT-PR1 to BT-PR10: product-recommender

Builder form of `model.yaml`. The ranking of BT-PR8 moved after 1.0, so `rank` shows a drawn product.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from desflow import KafkaEgress, ParquetStorageEgress, SimulationContext
from desflow.values import Template

from tests.known_cases.bt_support import deep

HERE = Path(__file__).parent

SERVICES = {}

RESOURCES = {}

CONTAINERS = {}

VARIABLES = {}

ARRIVALS = {
    "users": {"dist": "constant", "value": 1, "spawn": "user", "max_events": 1000},
    "history": {
        "dist": "exponential",
        "rate": 0.001286,
        "spawn": "event",
        "max_events": 10000,
    },
    "live": {"dist": "constant", "value": 0.1, "spawn": "rank", "max_events": 50},
}

SCENARIO = []

TASKS = {
    "user": {
        "vars": {
            "place": {
                "gen": "row",
                "file": str(HERE / "world_pop.csv"),
                "where": "country == 'Australia' and city == 'Melbourne'",
                "weight": "population",
            },
            "person": {
                "gen": "one_of",
                "choices": [
                    {
                        "gender": "F",
                        "first": {"gen": "fake", "method": "first_name_female"},
                    },
                    {
                        "gender": "M",
                        "first": {"gen": "fake", "method": "first_name_male"},
                    },
                ],
            },
            "last": {"gen": "fake", "method": "last_name_nonbinary"},
        },
        "payload": {
            "__table__": "users",
            "user_id": {"gen": "sequence", "start": 1},
            "first_name": {"gen": "var", "name": "person", "path": "first"},
            "last_name": {"gen": "var", "name": "last"},
            "email": {
                "gen": "format",
                "template": "{f}.{l}@{d}",
                "names": {
                    "f": {
                        "gen": "lower",
                        "value": {"gen": "var", "name": "person", "path": "first"},
                    },
                    "l": {"gen": "lower", "value": {"gen": "var", "name": "last"}},
                    "d": {"gen": "fake", "method": "safe_domain_name"},
                },
            },
            "age": {"gen": "integer", "bounds": [16, 70]},
            "gender": {"gen": "var", "name": "person", "path": "gender"},
            "street_address": {"gen": "fake", "method": "street_address"},
            "postal_code": {"gen": "var", "name": "place", "path": "postal_code"},
            "city": {"gen": "var", "name": "place", "path": "city"},
            "state": {"gen": "var", "name": "place", "path": "state"},
            "country": {"gen": "var", "name": "place", "path": "country"},
            "latitude": {"gen": "var", "name": "place", "path": "latitude"},
            "longitude": {"gen": "var", "name": "place", "path": "longitude"},
            "traffic_source": {
                "gen": "weighted",
                "options": {
                    "Organic": 0.15,
                    "Facebook": 0.06,
                    "Search": 0.7,
                    "Email": 0.05,
                    "Display": 0.04,
                },
            },
        },
    },
    "event": {
        "vars": {
            "u": {"gen": "lookup", "task": "user"},
            "uage": {"gen": "var", "name": "u", "path": "age"},
            "usrc": {"gen": "var", "name": "u", "path": "traffic_source"},
            "epoch_s": {"gen": "format_time", "format": "epoch_s"},
            "hour": {"gen": "format_time", "format": "%H", "time": {"gen": "now"}},
            "product": {"gen": "row", "file": str(HERE / "products.csv")},
            "pid": {"gen": "var", "name": "product", "path": "product_id"},
            "pcat": {"gen": "var", "name": "product", "path": "category"},
            "pprice": {"gen": "var", "name": "product", "path": "price"},
            "morning": {
                "gen": "expr",
                "expr": "1 if int(hour) >= 6 and int(hour) < 12 else 0",
            },
            "evening": {"gen": "expr", "expr": "1 if int(hour) >= 18 else 0"},
            "weekend": {
                "gen": "expr",
                "expr": "1 if ((epoch_s // 86400 + 3) % 7) >= 5 else 0",
            },
            "score": {
                "gen": "expr",
                "expr": "-2.5 + (2.5 if morning == 1 and pid >= 189 and pid <= "
                "194 else 0) + (1.8 if weekend == 1 and (pcat == "
                "'Pizzas' or pcat == 'Burgers & Sandwiches') else 0) - "
                "(3.0 if (uage - 16) / 54 < 0.25 and (pprice - 3.0) / "
                "35.99 > 0.8 else 0) + (0.5 if usrc == 'Search' else 0)",
            },
            "p": {"gen": "expr", "expr": "1 / (1 + exp(-score))"},
        },
        "payload": {
            "__table__": "training_log",
            "user": {"gen": "var", "name": "u", "path": "user_id"},
            "product": {"gen": "var", "name": "pid"},
            "morning": {"gen": "var", "name": "morning"},
            "evening": {"gen": "var", "name": "evening"},
            "weekend": {"gen": "var", "name": "weekend"},
            "click": {"gen": "boolean", "p": {"gen": "var", "name": "p"}},
        },
    },
    "rank": {
        "vars": {
            "product": {"gen": "row", "file": str(HERE / "products.csv")},
            "pid": {"gen": "var", "name": "product", "path": "product_id"},
            "reward": {"gen": "boolean", "p": 0.2},
        },
        "payload": {
            "event_id": {
                "gen": "format",
                "template": "evt_{ms}",
                "names": {
                    "ms": {
                        "gen": "format_time",
                        "format": "epoch_ms",
                        "time": {"gen": "now"},
                    }
                },
            },
            "product_id": {"gen": "expr", "expr": "str(pid)"},
            "reward": {"gen": "expr", "expr": "1 if reward else 0"},
            "context_vector": [0.5, 0.25, 0.0, 1.0, 0.0],
            "timestamp": {
                "gen": "format_time",
                "format": "epoch_ms",
                "time": {"gen": "now"},
            },
        },
        "key_field": "product_id",
    },
}

KEYS = {
    "service": "service_id",
    "resource": "resource_id",
}  # YAML keys the builder names otherwise


def _returns(payload: Any, task: str) -> Any:
    """The function of a task that returns `payload` as a template named for the task,
    so its generators draw from the streams the document's `payload` does."""
    template = Template(payload, name=f"tasks.{task}")

    def body(task_id):
        return template

    body.__name__ = task
    return body


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="Rec",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2025, 10, 3, 0, 0, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_egress(ParquetStorageEgress(default_path="out/pr.parquet"))
    app.add_egress(
        KafkaEgress(
            bootstrap_servers="127.0.0.1:9092",
            event_topic="feedback-events",
            serializer={
                "type": "confluent_avro",
                "registry_url": "http://127.0.0.1:8081",
                "schema_file": str(Path(__file__).parent / "feedback.avsc"),
            },
        ),
        when=lambda record: (
            record["task"] == "rank" and record["value"].get("event_id") is not None
        ),
        envelope=False,
    )
    for name, spec in SERVICES.items():
        app.add_service(name, **spec)
    for name, spec in RESOURCES.items():
        app.add_resource(name, **spec)
    for name, spec in CONTAINERS.items():
        app.add_container(name, **spec)
    for name, value in VARIABLES.items():
        app.add_variable(name, value)
    for name, spec in ARRIVALS.items():
        spec = dict(spec)
        app.add_arrival(name, spec.pop("dist", None), **deep(spec))
    for name, spec in TASKS.items():
        spec = dict(spec)
        payload = spec.pop("payload", {})
        app.task(name=name, **{KEYS.get(k, k): deep(v) for k, v in spec.items()})(
            _returns(deep(payload), name)
        )
    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=7776000)

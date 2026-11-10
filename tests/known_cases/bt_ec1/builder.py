"""BT-EC1 to BT-EC9: ecommerce-cdc.

Builder form of `model.yaml`. The centres are the arrival of `size` entities at the start.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from desflow import KafkaIngress, SimulationContext
from desflow.values import Template

from tests.known_cases.bt_support import deep

HERE = Path(__file__).parent

SERVICES = {
    "page_view": {"dist": "lognormal", "mean": 4.0, "std": 2.0},
    "patience": {"dist": "lognormal", "mean": 120.0, "std": 60.0},
    "pick": {"dist": "lognormal", "mean": 5.0, "std": 2.0},
    "transit": {"dist": "lognormal", "mean": 60.0, "std": 20.0},
    "return_after": {"dist": "lognormal", "mean": 60.0, "std": 30.0},
}

VARIABLES = {"buy": 0.3, "return": 0.1}

ARRIVALS = {
    "centers": {"initial": 10, "spawn": "center"},
    "users": {"initial": 100, "spawn": "new_user"},
    "visitor": {
        "dist": "exponential",
        "rate": 1.0,
        "spawn": "session_start",
        "attributes": {
            "n": 0,
            "seq": 0,
            "sid": "",
            "pages": {"gen": "integer", "bounds": [1, 4]},
        },
    },
    "move": {"dist": "exponential", "rate": 0.05, "spawn": "move"},
}

TASKS = {
    "center": {
        "payload": {"__table__": "dist_centers", "id": {"gen": "sequence", "start": 1}}
    },
    "new_user": {
        "vars_once": {
            "city": {
                "gen": "row",
                "file": str(HERE / "cities.csv"),
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
            "last": {"gen": "fake", "method": "last_name"},
        },
        "payload": {
            "__table__": "users",
            "id": {"gen": "sequence"},
            "first_name": {"gen": "var", "name": "person", "path": "first"},
            "last_name": {"gen": "var", "name": "last"},
            "email": {
                "gen": "lower",
                "value": {
                    "gen": "format",
                    "template": "{f}.{l}@example.com",
                    "names": {
                        "f": {"gen": "var", "name": "person", "path": "first"},
                        "l": {"gen": "var", "name": "last"},
                    },
                },
            },
            "age": {"gen": "integer", "bounds": [12, 70]},
            "gender": {"gen": "var", "name": "person", "path": "gender"},
            "street_address": {"gen": "fake", "method": "street_address"},
            "postal_code": {
                "gen": "expr",
                "expr": "str(pc)",
                "names": {"pc": {"gen": "var", "name": "city", "path": "postal_code"}},
            },
            "latitude": {"gen": "var", "name": "city", "path": "latitude"},
            "longitude": {"gen": "var", "name": "city", "path": "longitude"},
            "city": {"gen": "var", "name": "city", "path": "city"},
            "state": {"gen": "var", "name": "city", "path": "state"},
            "country": {"gen": "var", "name": "city", "path": "country"},
            "traffic_source": {
                "gen": "one_of",
                "choices": ["Organic", "Facebook", "Search", "Email"],
            },
            "created_at": {"gen": "now"},
            "updated_at": {"gen": "now"},
        },
    },
    "move": {
        "payload": {
            "__table__": "users",
            "id": {
                "gen": "lookup",
                "task": "new_user",
                "path": "id",
                "latest_by": "id",
            },
            "street_address": {"gen": "fake", "method": "street_address"},
            "updated_at": {"gen": "now"},
        }
    },
    "session_start": {
        "vars_once": {
            "session_id": {"gen": "uuid"},
            "browser": {"gen": "one_of", "choices": ["Chrome", "Safari", "Firefox"]},
        },
        "payload": {
            "__table__": "events",
            "event_type": "home",
            "ip_address": {"gen": "fake", "method": "ipv4"},
            "sequence_number": 1,
            "session_id": {"gen": "var", "name": "session_id"},
            "browser": {"gen": "var", "name": "browser"},
        },
        "set": {"seq": 1, "sid": {"gen": "var", "name": "session_id"}},
        "next": "browse",
    },
    "browse": {
        "service_time": {"gen": "sample", "service": "page_view"},
        "vars": {"s": {"gen": "expr", "expr": "attr.seq + 1"}},
        "payload": {
            "__table__": "events",
            "event_type": "product",
            "ip_address": {"gen": "fake", "method": "ipv4"},
            "session_id": {"gen": "attr", "name": "sid"},
            "sequence_number": {"gen": "var", "name": "s"},
        },
        "set": {
            "n": {"gen": "expr", "expr": "attr.n + 1"},
            "seq": {"gen": "var", "name": "s"},
        },
        "next": {
            "when": [{"condition": "attr.n < attr.pages", "task": "browse"}],
            "default": "buy_choice",
        },
    },
    "buy_choice": {
        "payload": {"event_type": "pages_done"},
        "next": {
            "weights": {
                "cart": "registry['ecommerce.variables.buy']",
                "leave": "1 - registry['ecommerce.variables.buy']",
            }
        },
    },
    "leave": {"payload": {"event_type": "left"}},
    "cart": {
        "service_time": {"gen": "sample", "service": "page_view"},
        "vars": {"s": {"gen": "expr", "expr": "attr.seq + 1"}},
        "payload": {
            "__table__": "events",
            "event_type": "cart",
            "ip_address": {"gen": "fake", "method": "ipv4"},
            "session_id": {"gen": "attr", "name": "sid"},
            "sequence_number": {"gen": "var", "name": "s"},
        },
        "set": {"seq": {"gen": "var", "name": "s"}},
        "next": "purchase",
    },
    "purchase": {
        "service_time": {"gen": "sample", "service": "page_view"},
        "vars": {"s": {"gen": "expr", "expr": "attr.seq + 1"}},
        "payload": {
            "__table__": "events",
            "event_type": "purchase",
            "ip_address": {"gen": "fake", "method": "ipv4"},
            "session_id": {"gen": "attr", "name": "sid"},
            "sequence_number": {"gen": "var", "name": "s"},
        },
        "spawn": {"task": "order_row"},
    },
    "order_row": {
        "vars": {
            "oid": {"gen": "uuid"},
            "created": {"gen": "now"},
            "uid": {"gen": "lookup", "task": "new_user", "path": "id"},
            "n": {
                "gen": "expr",
                "expr": "1 if u < 0.7 else 2 if u < 0.9 else 3 if u < 0.95 else 4",
                "names": {"u": {"gen": "uniform", "low": 0, "high": 1}},
            },
        },
        "set": {
            "order_id": {"gen": "var", "name": "oid"},
            "created_at": {"gen": "var", "name": "created"},
            "user": {"gen": "var", "name": "uid"},
            "n": {"gen": "var", "name": "n"},
        },
        "payload": {
            "__table__": "orders",
            "id": {"gen": "var", "name": "oid"},
            "user_id": {"gen": "var", "name": "uid"},
            "status": "Processing",
            "num_of_items": {"gen": "var", "name": "n"},
            "created_at": {"gen": "var", "name": "created"},
            "updated_at": {"gen": "now"},
            "shipped_at": {"gen": "expr", "expr": "None"},
            "delivered_at": {"gen": "expr", "expr": "None"},
            "cancelled_at": {"gen": "expr", "expr": "None"},
            "returned_at": {"gen": "expr", "expr": "None"},
        },
        "next": "pick_order",
    },
    "pick_order": {
        "resource": "pickers",
        "service": "pick",
        "patience": {"gen": "sample", "service": "patience"},
        "on_renege": "cancel_order",
        "set": {"shipped_at": {"gen": "now"}},
        "payload": {
            "__table__": "orders",
            "id": {"gen": "attr", "name": "order_id"},
            "user_id": {"gen": "attr", "name": "user"},
            "status": "Shipped",
            "num_of_items": {"gen": "attr", "name": "n"},
            "created_at": {"gen": "attr", "name": "created_at"},
            "updated_at": {"gen": "now"},
            "shipped_at": {"gen": "now"},
            "delivered_at": {"gen": "expr", "expr": "None"},
            "cancelled_at": {"gen": "expr", "expr": "None"},
            "returned_at": {"gen": "expr", "expr": "None"},
        },
        "next": "deliver_order",
    },
    "cancel_order": {
        "payload": {
            "__table__": "orders",
            "id": {"gen": "attr", "name": "order_id"},
            "user_id": {"gen": "attr", "name": "user"},
            "status": "Cancelled",
            "num_of_items": {"gen": "attr", "name": "n"},
            "created_at": {"gen": "attr", "name": "created_at"},
            "updated_at": {"gen": "now"},
            "shipped_at": {"gen": "expr", "expr": "None"},
            "delivered_at": {"gen": "expr", "expr": "None"},
            "cancelled_at": {"gen": "now"},
            "returned_at": {"gen": "expr", "expr": "None"},
        }
    },
    "deliver_order": {
        "service": "transit",
        "vars": {
            "sent_back": {
                "gen": "boolean",
                "p": {"gen": "param", "path": "ecommerce.variables.return"},
            }
        },
        "set": {"delivered_at": {"gen": "now"}},
        "payload": {
            "__table__": "orders",
            "id": {"gen": "attr", "name": "order_id"},
            "user_id": {"gen": "attr", "name": "user"},
            "status": "Delivered",
            "num_of_items": {"gen": "attr", "name": "n"},
            "created_at": {"gen": "attr", "name": "created_at"},
            "updated_at": {"gen": "now"},
            "shipped_at": {"gen": "attr", "name": "shipped_at"},
            "delivered_at": {"gen": "now"},
            "cancelled_at": {"gen": "expr", "expr": "None"},
            "returned_at": {"gen": "expr", "expr": "None"},
        },
        "next": {"when": [{"condition": "sent_back", "task": "return_order"}]},
    },
    "return_order": {
        "service": "return_after",
        "payload": {
            "__table__": "orders",
            "id": {"gen": "attr", "name": "order_id"},
            "user_id": {"gen": "attr", "name": "user"},
            "status": "Returned",
            "num_of_items": {"gen": "attr", "name": "n"},
            "created_at": {"gen": "attr", "name": "created_at"},
            "updated_at": {"gen": "now"},
            "shipped_at": {"gen": "attr", "name": "shipped_at"},
            "delivered_at": {"gen": "attr", "name": "delivered_at"},
            "cancelled_at": {"gen": "expr", "expr": "None"},
            "returned_at": {"gen": "now"},
        },
    },
}

KEYS = {
    "service": "service_id",
    "resource": "resource_id",
}  # YAML keys the builder names otherwise
KEPT = ("new_user",)  # the tasks a `lookup` reads, which keep their rows


def _returns(payload: Any, task: str) -> Any:
    """The function of a task that returns `payload` as a template named for the task,
    so its generators draw from the streams the document's `payload` does."""
    template = Template(payload, name=f"tasks.{task}")

    def body(task_id):
        return template

    body.__name__ = task
    return body


def build(size: int = 10) -> SimulationContext:
    app = SimulationContext(
        sim_id="ecommerce",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 9, 1, 0, 0, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_ingress(
        KafkaIngress(
            bootstrap_servers="127.0.0.1:9092",
            topic="ecommerce-control",
            topic_policy={"partitions": 1},
        )
    )
    for name, spec in SERVICES.items():
        app.add_service(name, **spec)
    app.add_resource("pickers", current_cap=3, max_cap=10)
    for name, value in VARIABLES.items():
        app.add_variable(name, value)
    for name, spec in ARRIVALS.items():
        spec = dict(spec)
        if "initial" in spec:
            count = spec.pop("initial")
            count = size if name == "centers" else count
            app.add_initial_arrival(name, count, **spec)
        else:
            app.add_arrival(name, spec.pop("dist", None), **deep(spec))
    for name, spec in TASKS.items():
        spec = dict(spec)
        payload = spec.pop("payload", {})
        history = 1_000_000 if name in KEPT else None
        app.task(
            name=name,
            history=history,
            **{KEYS.get(k, k): deep(v) for k, v in spec.items()},
        )(_returns(deep(payload), name))
    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=1200)

"""BT-LD1 to BT-LD17: live-dashboard.

Builder form of `model.yaml`. The tables of the four egress entries and the parameter
table of the ingress are the sinks of the document; the test replaces them with its
recording egress, so no database is needed.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from desflow import NO_RECORD, PostgresEgress, PostgresIngress, SimulationContext
from desflow.values import Template

from tests.known_cases.bt_support import deep

HERE = Path(__file__).parent
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"

SERVICES = {
    "page_view": {"dist": "lognormal", "mean": 4.0, "std": 2.0},
    "pick": {"dist": "lognormal", "mean": 5.0, "std": 2.0},
    "transit": {"dist": "lognormal", "mean": 60.0, "std": 20.0},
    "return_after": {"dist": "lognormal", "mean": 60.0, "std": 30.0},
    "patience": {"dist": "lognormal", "mean": 120.0, "std": 60.0},
}

VARIABLES = {"returning": 0.6, "buy": 0.3, "return": 0.1, "users": 0}

RESOURCES = {"pickers": {"current_cap": 10, "max_cap": 20}}

SCENARIO = [
    {"at": 100.1, "path": "sales.arrival.visitor.rate", "value": 6},
    {"at": 150.1, "path": "sales.resources.pickers.current_cap", "value": 6},
    {"at": 200.1, "path": "sales.service.pick.mean", "value": 8},
    {"at": 250.1, "path": "sales.variables.buy", "value": 0.4},
]

ARRIVALS = {
    "catalogue": {"initial": 260, "spawn": "product_row"},
    "visitor": {
        "dist": "exponential",
        "rate": 4,
        "spawn": "catalogue_check",
        "attributes": {"n": 0, "pages": {"gen": "integer", "bounds": [2, 5]}},
    },
}

TASKS = {
    "product_row": {
        "vars": {
            "p": {
                "gen": "row",
                "file": str(HERE / "products.csv"),
                "order": "sequential",
            }
        },
        "payload": {
            "__table__": "products",
            "id": {"gen": "var", "name": "p", "path": "id"},
            "name": {"gen": "var", "name": "p", "path": "name"},
            "category": {"gen": "var", "name": "p", "path": "category"},
            "department": {"gen": "var", "name": "p", "path": "department"},
            "retail_price": {"gen": "var", "name": "p", "path": "retail_price"},
            "cost": {"gen": "var", "name": "p", "path": "cost"},
        },
    },
    "catalogue_check": {
        "payload": {"event_type": "visit"},
        "next": {
            "weights": {
                "pick_user": "registry['sales.variables.returning'] "
                "if "
                "registry['sales.variables.users'] "
                "> 0 else 0",
                "no_user": "1 - "
                "(registry['sales.variables.returning'] "
                "if "
                "registry['sales.variables.users'] "
                "> 0 else 0)",
            }
        },
    },
    "pick_user": {
        "payload": None,
        "set": {"user": {"gen": "lookup", "task": "new_user", "path": "id"}},
        "next": "pages",
    },
    "no_user": {"payload": None, "set": {"user": ""}, "next": "pages"},
    "pages": {
        "service_time": {"gen": "sample", "service": "page_view"},
        "payload": None,
        "set": {"n": {"gen": "expr", "expr": "attr.n + 1"}},
        "next": {
            "when": [{"condition": "attr.n < attr.pages", "task": "pages"}],
            "default": "buy_choice",
        },
    },
    "buy_choice": {
        "payload": {"event_type": "pages_done"},
        "next": {
            "weights": {
                "buyer": "registry['sales.variables.buy']",
                "walk_away": "1 - registry['sales.variables.buy']",
            }
        },
    },
    "walk_away": {"payload": {"event_type": "left"}},
    "buyer": {
        "payload": None,
        "next": {
            "when": [{"condition": "attr.user != ''", "task": "returning"}],
            "default": "new_user",
        },
    },
    "returning": {"payload": {"event_type": "returning"}, "next": "order"},
    "new_user": {
        "on": [
            {
                "path": "sales.variables.users",
                "value": {
                    "gen": "expr",
                    "expr": "registry['sales.variables.users'] + 1",
                },
            }
        ],
        "vars": {"uid": {"gen": "uuid"}},
        "set": {"user": {"gen": "var", "name": "uid"}},
        "payload": {
            "__table__": "users",
            "id": {"gen": "var", "name": "uid"},
            "age": {"gen": "integer", "bounds": [12, 70]},
            "gender": {"gen": "one_of", "choices": ["F", "M"]},
            "country": {
                "gen": "weighted",
                "options": {
                    "China": 7319934,
                    "Brasil": 7294477,
                    "South Korea": 1493470,
                    "France": 1458801,
                    "United States": 1157308,
                    "United Kingdom": 1024009,
                },
            },
            "traffic_source": {
                "gen": "weighted",
                "options": {
                    "Search": 0.7,
                    "Organic": 0.15,
                    "Facebook": 0.06,
                    "Email": 0.05,
                    "Display": 0.04,
                },
            },
            "created_at": {"gen": "now"},
        },
        "next": "order",
    },
    "order": {
        "vars": {
            "oid": {"gen": "uuid"},
            "created": {"gen": "now"},
            "n": {
                "gen": "expr",
                "expr": "1 if u < 0.7 else 2 if u < 0.9 else 3 if u < 0.95 else 4",
                "names": {"u": {"gen": "uniform", "low": 0, "high": 1}},
            },
            "ids": {
                "gen": "repeat",
                "times": {"gen": "var", "name": "n"},
                "value": {"gen": "uuid"},
            },
        },
        "set": {
            "order_id": {"gen": "var", "name": "oid"},
            "created_at": {"gen": "var", "name": "created"},
            "n": {"gen": "var", "name": "n"},
            "iids": {"gen": "var", "name": "ids"},
            "products": {
                "gen": "repeat",
                "times": {"gen": "var", "name": "n"},
                "value": {"gen": "lookup", "task": "product_row", "distinct": True},
            },
        },
        "payload": {
            "__table__": "orders",
            "id": {"gen": "var", "name": "oid"},
            "user_id": {"gen": "attr", "name": "user"},
            "status": "Processing",
            "num_of_item": {"gen": "var", "name": "n"},
            "created_at": {"gen": "var", "name": "created"},
        },
        "next": "items",
    },
    "items": {
        "wait": {
            "each": "range(attr.n)",
            "branch": {
                "task": "order_item",
                "args": {
                    "i": {"gen": "expr", "expr": "each.value"},
                    "status": "Processing",
                },
            },
        },
        "payload": {"event_type": "items_written"},
        "next": "pick",
    },
    "order_item": {
        "vars": {"p": {"gen": "expr", "expr": "attr.products[arg.i]"}},
        "payload": {
            "__table__": "order_items",
            "id": {"gen": "expr", "expr": "attr.iids[arg.i]"},
            "order_id": {"gen": "attr", "name": "order_id"},
            "user_id": {"gen": "attr", "name": "user"},
            "product_id": {"gen": "var", "name": "p", "path": "id"},
            "status": {"gen": "expr", "expr": "arg.status"},
            "sale_price": {"gen": "var", "name": "p", "path": "retail_price"},
            "created_at": {"gen": "attr", "name": "created_at"},
        },
    },
    "pick": {
        "resource": "pickers",
        "service": "pick",
        "patience": {"gen": "sample", "service": "patience"},
        "on_renege": "cancelled",
        "payload": {
            "__table__": "orders",
            "id": {"gen": "attr", "name": "order_id"},
            "user_id": {"gen": "attr", "name": "user"},
            "status": "Shipped",
            "num_of_item": {"gen": "attr", "name": "n"},
            "created_at": {"gen": "attr", "name": "created_at"},
        },
        "next": "items_shipped",
    },
    "items_shipped": {
        "wait": {
            "each": "range(attr.n)",
            "branch": {
                "task": "order_item",
                "args": {
                    "i": {"gen": "expr", "expr": "each.value"},
                    "status": "Shipped",
                },
            },
        },
        "payload": {"event_type": "items_shipped"},
        "next": "transit",
    },
    "cancelled": {
        "payload": {
            "__table__": "orders",
            "id": {"gen": "attr", "name": "order_id"},
            "user_id": {"gen": "attr", "name": "user"},
            "status": "Cancelled",
            "num_of_item": {"gen": "attr", "name": "n"},
            "created_at": {"gen": "attr", "name": "created_at"},
        },
        "next": "items_cancelled",
    },
    "items_cancelled": {
        "wait": {
            "each": "range(attr.n)",
            "branch": {
                "task": "order_item",
                "args": {
                    "i": {"gen": "expr", "expr": "each.value"},
                    "status": "Cancelled",
                },
            },
        },
        "payload": {"event_type": "items_cancelled"},
    },
    "transit": {
        "service": "transit",
        "payload": {
            "__table__": "orders",
            "id": {"gen": "attr", "name": "order_id"},
            "user_id": {"gen": "attr", "name": "user"},
            "status": "Complete",
            "num_of_item": {"gen": "attr", "name": "n"},
            "created_at": {"gen": "attr", "name": "created_at"},
        },
        "next": "items_complete",
    },
    "items_complete": {
        "vars": {
            "sent_back": {
                "gen": "boolean",
                "p": {"gen": "param", "path": "sales.variables.return"},
            }
        },
        "wait": {
            "each": "range(attr.n)",
            "branch": {
                "task": "order_item",
                "args": {
                    "i": {"gen": "expr", "expr": "each.value"},
                    "status": "Complete",
                },
            },
        },
        "payload": {"event_type": "items_complete"},
        "next": {"when": [{"condition": "sent_back", "task": "returned"}]},
    },
    "returned": {
        "service": "return_after",
        "payload": {
            "__table__": "orders",
            "id": {"gen": "attr", "name": "order_id"},
            "user_id": {"gen": "attr", "name": "user"},
            "status": "Returned",
            "num_of_item": {"gen": "attr", "name": "n"},
            "created_at": {"gen": "attr", "name": "created_at"},
        },
        "next": "items_returned",
    },
    "items_returned": {
        "wait": {
            "each": "range(attr.n)",
            "branch": {
                "task": "order_item",
                "args": {
                    "i": {"gen": "expr", "expr": "each.value"},
                    "status": "Returned",
                },
            },
        },
        "payload": {"event_type": "items_returned"},
    },
}

EGRESS = [
    {
        "table_name": "users",
        "server_settings": {"search_path": "dashboard"},
        "tables": {
            "dashboard.users": {
                "columns": {
                    "id": "TEXT",
                    "age": "INT",
                    "gender": "TEXT",
                    "country": "TEXT",
                    "traffic_source": "TEXT",
                    "created_at": "TEXT",
                },
                "primary_key": "id",
            }
        },
    },
    {
        "table_name": "orders",
        "server_settings": {"search_path": "dashboard"},
        "upsert_keys": ["id"],
        "tables": {
            "dashboard.orders": {
                "columns": {
                    "id": "TEXT",
                    "user_id": "TEXT",
                    "status": "TEXT",
                    "num_of_item": "INT",
                    "created_at": "TEXT",
                },
                "primary_key": "id",
            }
        },
    },
    {
        "table_name": "order_items",
        "server_settings": {"search_path": "dashboard"},
        "upsert_keys": ["id"],
        "tables": {
            "dashboard.order_items": {
                "columns": {
                    "id": "TEXT",
                    "order_id": "TEXT",
                    "user_id": "TEXT",
                    "product_id": "BIGINT",
                    "status": "TEXT",
                    "sale_price": "FLOAT8",
                    "created_at": "TEXT",
                },
                "primary_key": "id",
            }
        },
    },
    {
        "table_name": "products",
        "server_settings": {"search_path": "dashboard"},
        "tables": {
            "dashboard.products": {
                "columns": {
                    "id": "BIGINT",
                    "name": "TEXT",
                    "category": "TEXT",
                    "department": "TEXT",
                    "retail_price": "FLOAT8",
                    "cost": "FLOAT8",
                },
                "primary_key": "id",
            }
        },
    },
]

KEYS = {
    "service": "service_id",
    "resource": "resource_id",
}  # YAML keys the builder names otherwise
KEPT = ("new_user", "product_row")  # the tasks a `lookup` reads, which keep their rows


def _returns(payload: Any, task: str) -> Any:
    """The function of a task that returns `payload` as a template named for the task,
    so its generators draw from the streams the document's `payload` does. A task with
    `payload: null` publishes nothing."""
    template = None if payload is None else Template(payload, name=f"tasks.{task}")

    def body(task_id):
        return NO_RECORD if template is None else template

    body.__name__ = task
    return body


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="sales",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_ingress(PostgresIngress(DSN, table_name="dashboard.params"))
    app.with_batching(batch_size=4, flush_interval=1.0)
    for config in EGRESS:
        app.add_egress(PostgresEgress(DSN, **config))
    for name, spec in SERVICES.items():
        app.add_service(name, **spec)
    for name, spec in RESOURCES.items():
        app.add_resource(name, **spec)
    for name, value in VARIABLES.items():
        app.add_variable(name, value)
    app.add_scenario(deep(SCENARIO))
    for name, spec in ARRIVALS.items():
        spec = dict(spec)
        if "initial" in spec:
            count = spec.pop("initial")
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
    return app.run(until=300)

"""BT-WL1 to BT-WL12: web-and-logs, visits of each domain, their requests and the
reference data.

Builder form of `model.yaml`. `size` is the number of times the mappings are written at
the start.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from desflow import NO_RECORD, KafkaEgress, PostgresEgress, SimulationContext
from desflow.values import Template

from tests.known_cases.bt_support import deep

HERE = Path(__file__).parent
BROKER = "localhost:9092"
DSN = "postgresql://user:password@127.0.0.1:5432/odctl"

VARIABLES = {
    "tenants": ["acme", "globex"],
    "domains": {"acme": ["acme.com", "shop.acme.com"], "globex": ["globex.io"]},
    "services": {
        "acme.com": ["acme-api", "acme-web", "acme-pay"],
        "shop.acme.com": ["acme-web", "acme-pay"],
        "globex.io": ["globex-api", "globex-web"],
    },
    "paths": {
        "acme-api": ["/"],
        "acme-web": ["/"],
        "acme-pay": ["/checkout", "/confirmation"],
        "globex-api": ["/", "/checkout", "/confirmation"],
        "globex-web": ["/"],
    },
    "ratio": {"acme": 2, "globex": 1},
    "error_rate": {
        "acme-api": 0.02,
        "acme-web": 0.005,
        "acme-pay": 0.01,
        "globex-api": 0.02,
        "globex-web": 0.005,
    },
    "level_of": {"200": "INFO", "500": "ERROR"},
}

TABLES = ("tenant_domains", "domain_services", "service_paths")

ARRIVALS = {
    "acme_com": {
        "dist": "exponential",
        "rate": 1,
        "spawn": "hit",
        "attributes": {"tenant": "acme", "domain": "acme.com"},
    },
    "shop_acme_com": {
        "dist": "exponential",
        "rate": 0.5,
        "spawn": "hit",
        "attributes": {"tenant": "acme", "domain": "shop.acme.com"},
    },
    "globex_io": {
        "dist": "exponential",
        "rate": 0.5,
        "spawn": "hit",
        "attributes": {"tenant": "globex", "domain": "globex.io"},
    },
    "bots": {"dist": "exponential", "rate": 0.2, "spawn": "bot_hit"},
}

SCENARIO = [
    {
        "at": 600,
        "path": "Web.variables.error_rate",
        "value": {**VARIABLES["error_rate"], "acme-pay": 0.4},
    },
    {
        "at": 600,
        "record": {
            "event_type": "ground_truth",
            "type": "service_fault",
            "start": 600,
            "end": 700,
            "tenants": ["acme"],
            "domains": ["acme.com", "shop.acme.com"],
            "services": ["acme-pay"],
        },
    },
]

VISIT = {
    "tenant": {"gen": "attr", "name": "tenant"},
    "domain": {"gen": "attr", "name": "domain"},
}
SERVING = (
    "[s for s in registry['Web.variables.services'][attr.domain] "
    "if attr.path in registry['Web.variables.paths'][s]]"
)


def _mapping(variable: str, table: str, after: str | None) -> dict[str, Any]:
    """The task that writes one mapping variable to its table, a row for each key."""
    spec: dict[str, Any] = {
        "payload": None,
        "wait": {
            "each": f"[k for k in registry['Web.variables.{variable}']]",
            "branch": {
                "task": "mapping_row",
                "args": {
                    "table": table,
                    "name": {"gen": "expr", "expr": "each.value"},
                    "items": {
                        "gen": "expr",
                        "expr": f"registry['Web.variables.{variable}'][each.value]",
                    },
                },
            },
        },
    }
    if after:
        spec["next"] = after
    return spec


TASKS = {
    "hit": {
        "vars_once": {
            "path": {"gen": "weighted", "options": {"/": 5, "/checkout": 1}},
            "goes_on": {"gen": "boolean", "p": 0.5},
        },
        "payload": {
            "event_type": "hit",
            **VISIT,
            "path": {"gen": "var", "name": "path"},
            "__topic__": "web_events",
        },
        "set": {
            "path": {"gen": "var", "name": "path"},
            "goes_on": {"gen": "var", "name": "goes_on"},
        },
        "next": "requests",
    },
    "requests": {
        "payload": None,
        "wait": {
            "each": SERVING,
            "branch": {
                "task": "calls",
                "args": {"service": {"gen": "expr", "expr": "each.value"}},
            },
        },
        "next": {
            "when": [
                {
                    "condition": "attr.path == '/checkout' and attr.goes_on",
                    "task": "confirmation",
                }
            ]
        },
    },
    "calls": {
        "payload": None,
        "wait": {
            "each": "range(registry['Web.variables.ratio'][attr.tenant])",
            "branch": {
                "task": "request",
                "args": {"service": {"gen": "expr", "expr": "arg.service"}},
            },
        },
    },
    "request": {
        "vars": {
            "err": {
                "gen": "boolean",
                "p": {
                    "gen": "param",
                    "path": "Web.variables.error_rate",
                    "field": {"gen": "expr", "expr": "arg.service"},
                },
            },
            "code": {"gen": "expr", "expr": "500 if err else 200"},
        },
        "payload": {
            "event_type": "request",
            **VISIT,
            "path": {"gen": "attr", "name": "path"},
            "service": {"gen": "expr", "expr": "arg.service"},
            "status": {"gen": "var", "name": "code"},
            "level": {
                "gen": "param",
                "path": "Web.variables.level_of",
                "field": {"gen": "expr", "expr": "str(code)"},
            },
            "__topic__": "logs",
        },
    },
    "confirmation": {
        "payload": {
            "event_type": "hit",
            **VISIT,
            "path": "/confirmation",
            "__topic__": "web_events",
        },
        "set": {"path": "/confirmation"},
        "next": "requests",
    },
    "bot_hit": {
        "payload": {"event_type": "hit", "label": "bot", "__topic__": "web_events"}
    },
    "tenant_domains": _mapping("domains", "tenant_domains", "domain_services"),
    "domain_services": _mapping("services", "domain_services", "service_paths"),
    "service_paths": _mapping("paths", "service_paths", None),
    "mapping_row": {
        "payload": {
            "__table__": {"gen": "expr", "expr": "arg.table"},
            "name": {"gen": "expr", "expr": "arg.name"},
            "items": {"gen": "expr", "expr": "arg.items"},
        }
    },
}


def _returns(payload: Any, task: str) -> Any:
    """The function of a task that returns `payload` as a template named for the task,
    so its generators draw from the streams the document's `payload` does. A task with
    `payload: null` publishes nothing."""
    template = None if payload is None else Template(payload, name=f"tasks.{task}")

    def body(task_id):
        return NO_RECORD if template is None else template

    body.__name__ = task
    return body


def build(size: int = 1) -> SimulationContext:
    app = SimulationContext(
        sim_id="Web",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        timezone="UTC",
    )
    app.add_egress(
        KafkaEgress(
            bootstrap_servers=BROKER,
            serializer={
                "type": "confluent_avro",
                "registry_url": "http://localhost:8081",
                "schema_file": str(HERE / "web.avsc"),
            },
        )
    )
    app.add_egress(
        KafkaEgress(bootstrap_servers=BROKER, event_topic="ground_truth"),
        when="labels",
    )
    for table in TABLES:
        app.add_egress(
            PostgresEgress(
                DSN,
                table_name=table,
                tables={
                    table: {
                        "columns": {"name": "TEXT", "items": "TEXT[]"},
                        "primary_key": "name",
                    }
                },
            )
        )
    for name, value in VARIABLES.items():
        app.add_variable(name, value)
    for name, spec in ARRIVALS.items():
        spec = dict(spec)
        app.add_arrival(name, spec.pop("dist"), **deep(spec))
    app.add_initial_arrival("reference", size, spawn="tenant_domains")
    app.add_scenario(deep(SCENARIO))
    for name, spec in TASKS.items():
        spec = dict(spec)
        payload = spec.pop("payload", {})
        app.task(name=name, **{k: deep(v) for k, v in spec.items()})(
            _returns(deep(payload), name)
        )
    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=700)

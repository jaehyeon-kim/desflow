"""Each request becomes a trace of nested spans, and each child span names its parent.

The Builder form of requests_become_traces_of_nested_spans.yaml, with the same records.
The gateway span covers an auth check and a database call that run inside it, so each
child starts and ends within its parent's time.

Needs: nothing
Run it with: python examples/gallery/requests_become_traces_of_nested_spans_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template, generator

UNTIL = 120


def duration(low, high):
    """The task's `duration` var, drawn once for each span, in milliseconds."""
    return {"duration": generator({"gen": "integer", "bounds": [low, high]})}


def child_span(task, name):
    """The payload of a span inside the gateway span, whose id is its parent's id."""
    return Template(
        {
            "trace_id": {"gen": "attr", "name": "trace_id"},
            "span_id": {"gen": "uuid"},
            "parent_span_id": {"gen": "attr", "name": "gateway_span"},
            "name": name,
            "start": {"gen": "expr", "expr": "t - duration / 1000"},
            "duration_ms": {"gen": "var", "name": "duration"},
        },
        name=f"tasks.{task}",
    )


gateway_payload = Template(
    {
        "trace_id": {"gen": "attr", "name": "trace_id"},
        "span_id": {"gen": "attr", "name": "gateway_span"},
        "parent_span_id": None,
        "name": "GET /orders",
        "start": {"gen": "attr", "name": "began"},
        "duration_ms": {"gen": "expr", "expr": "round((t - attr.began) * 1000, 3)"},
    },
    name="tasks.gateway",
)


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Api",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_arrival(
            "requests",
            "exponential",
            rate=0.5,
            attributes={
                "trace_id": generator({"gen": "uuid"}),
                "gateway_span": generator({"gen": "uuid"}),
                "began": generator({"gen": "expr", "expr": "t"}),
            },
            spawn="auth",
        )
    )

    # A span's record is published when it ends, so the children come before the gateway
    auth_payload = child_span("auth", "check token")
    orders_db_payload = child_span("orders_db", "SELECT orders")

    @app.task(
        name="auth",
        vars=duration(5, 15),
        service_time="duration / 1000",
        next="orders_db",
    )
    def auth(task_id):
        return auth_payload

    @app.task(
        name="orders_db",
        vars=duration(10, 120),
        service_time="duration / 1000",
        next="gateway",
    )
    def orders_db(task_id):
        return orders_db_payload

    @app.task(name="gateway", vars=duration(20, 60), service_time="duration / 1000")
    def gateway(task_id):
        return gateway_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

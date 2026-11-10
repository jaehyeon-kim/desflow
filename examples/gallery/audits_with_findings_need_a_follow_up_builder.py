"""An audit that finds an issue needs a follow-up, which waits for the same auditors.

The Builder form of audits_with_findings_need_a_follow_up.yaml, with the same records.
An audit of one of 150 merchants is due about every 3 hours and takes an auditor about 4
hours, and 15 percent find an issue. A follow-up of about an hour goes to the back of
the same queue.

Needs: nothing
Run it with: python examples/gallery/audits_with_findings_need_a_follow_up_builder.py
"""

import logging
from datetime import datetime

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 14 * 24 * 3600


def build() -> SimulationContext:
    """Builds the model."""
    app = (
        SimulationContext(
            sim_id="Compliance",
            factor=0,
            random_seed=42,
            logical_start_time=datetime(2026, 1, 1),
        )
        .add_egress(ConsoleEgress())
        .add_resource("auditors", current_cap=2, max_cap=2)
        .add_service("audit", dist="normal", mean="4 hours", std="1 hour")
        .add_service("follow_up", dist="normal", mean="1 hour", std="15 min")
        .add_arrival("audits_due", "exponential", rate=0.0001, spawn="audit")
    )

    audit_payload = Template(
        {
            "merchant": {"gen": "integer", "bounds": [1, 150]},
            "finding": {"gen": "weighted", "options": {"clean": 85, "issue": 15}},
        },
        name="tasks.audit",
    )
    follow_up_payload = Template(
        {
            "merchant": {"gen": "upstream", "path": "merchant"},
            "event_type": "follow_up_done",
        },
        name="tasks.follow_up",
    )

    @app.task(
        name="audit",
        service_id="audit",
        resource_id="auditors",
        next={
            "when": [{"condition": "payload.finding == 'issue'", "task": "follow_up"}]
        },
    )
    def audit(task_id):
        return audit_payload

    @app.task(name="follow_up", service_id="follow_up", resource_id="auditors")
    def follow_up(task_id):
        return follow_up_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

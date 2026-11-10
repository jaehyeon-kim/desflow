"""The Add Reneging and Balking guide: an order that leaves the line goes on to a
follow-up task, Builder form of docs/snippets/yaml/reneging_follow_up.yaml.

Run it with: python reneging_follow_up.py
"""

import logging

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 40


def build() -> SimulationContext:
    """Builds the shop."""
    app = SimulationContext("Shop", factor=0, random_seed=42)
    app.add_egress(ConsoleEgress())
    app.add_resource("packers", current_cap=1, max_cap=1)
    app.add_service("packing", dist="constant", value=3)
    app.add_arrival("orders", dist="constant", value=1, spawn="pack", max_events=12)

    # --8<-- [start:tasks]
    @app.task(
        name="pack",
        service_id="packing",
        resource_id="packers",
        patience=4,
        balk="t >= 8",
        # Where an order goes after it leaves the line, by reneging or by balking
        on_renege="follow_up",
        on_balk="follow_up",
    )
    def pack(task_id):
        """Packs one order."""
        return {"event_type": "order_packed"}

    # reneged or balked: the status of the task the order came from
    follow_up_payload = Template(
        {"event_type": "order_left", "how": {"gen": "expr", "expr": "upstream.status"}},
        name="tasks.follow_up",
    )

    @app.task(name="follow_up")
    def follow_up(task_id):
        """Records how the order left the line."""
        return follow_up_payload

    # --8<-- [end:tasks]
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

"""A machine slows down as it wears, then is repaired, builder API.

The builder version of `machine_slows_as_it_wears.yaml`: a press with one station,
whose mean stamping time rises in steps at 1, 2 and 3 hours and returns to 10 s at 4.

Needs: nothing
Run it with: python examples/gallery/machine_slows_as_it_wears_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext

UNTIL = 18000


def build() -> SimulationContext:
    """Builds the press, with the wear and the repair as scenario steps."""
    app = SimulationContext(
        sim_id="Press",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_resource("station", current_cap=1)
    app.add_service("stamp", dist="normal", mean=10, std=1)
    app.add_arrival("blanks", "exponential", rate=0.05, spawn="blank")
    app.add_scenario(
        [
            {"at": "1 hour", "path": "Press.service.stamp.mean", "value": 12},
            {"at": "2 hours", "path": "Press.service.stamp.mean", "value": 14},
            {"at": "3 hours", "path": "Press.service.stamp.mean", "value": 16},
            {"at": "4 hours", "path": "Press.service.stamp.mean", "value": 10},
        ]
    )

    @app.task(name="blank", resource_id="station", service_id="stamp")
    def blank(task_id):
        """Stamps one blank."""
        return {"event_type": "stamped"}

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

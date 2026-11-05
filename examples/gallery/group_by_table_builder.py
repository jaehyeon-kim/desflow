"""A table for group-by benchmarks, builder API.

The builder version of `group_by_table.yaml`: 1,000 rows, one a second, each with the
keys id1 to id6 and the measures v1 to v3.

Needs: nothing
Run it with: python examples/gallery/group_by_table_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

# The 1,000th row is at 1,000 s, and a run stops before the events due at its end
# time, so it ends 1 s later.
UNTIL = 1001


def build() -> SimulationContext:
    """Builds the table."""
    app = SimulationContext(
        sim_id="Table",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("rows", "constant", value=1, spawn="row", max_events=1000)
    row_payload = Template(
        {
            "id1": {
                "gen": "format",
                "template": "id{n:03d}",
                "names": {"n": {"gen": "integer", "bounds": [1, 100]}},
            },
            "id2": {
                "gen": "format",
                "template": "id{n:03d}",
                "names": {"n": {"gen": "integer", "bounds": [1, 100]}},
            },
            "id3": {
                "gen": "format",
                "template": "id{n:010d}",
                "names": {"n": {"gen": "integer", "bounds": [1, 100000]}},
            },
            "id4": {"gen": "integer", "bounds": [1, 100]},
            "id5": {"gen": "integer", "bounds": [1, 100]},
            "id6": {"gen": "integer", "bounds": [1, 100000]},
            "v1": {"gen": "integer", "bounds": [1, 5]},
            "v2": {"gen": "integer", "bounds": [1, 15]},
            "v3": {"gen": "uniform", "low": 0, "high": 100, "decimals": 6},
        },
        name="tasks.row",
    )

    @app.task(name="row")
    def row(task_id):
        """Gives one row."""
        return row_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

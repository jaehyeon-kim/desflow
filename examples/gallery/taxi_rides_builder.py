"""Taxi rides with a pickup, a drop-off, a distance and a fare, builder API.

The builder version of `taxi_rides.yaml`: a ride about every 10 seconds, for 10 minutes.

Needs: nothing
Run it with: python examples/gallery/taxi_rides_builder.py
"""

import logging
from datetime import datetime, timezone

from desflow import ConsoleEgress, SimulationContext
from desflow.values import Template

UNTIL = 600


def build() -> SimulationContext:
    """Builds the taxi company."""
    app = SimulationContext(
        sim_id="Taxi",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    app.add_egress(ConsoleEgress())
    app.add_arrival("rides", "exponential", rate=0.1, spawn="ride")
    ride_payload = Template(
        {
            "ride_id": {"gen": "sequence", "start": 1},
            "pickup_time": {"gen": "now"},
            "dropoff_time": {
                "gen": "add",
                "args": [{"gen": "now"}, {"gen": "expr", "expr": "minutes * 60"}],
            },
            "passengers": {
                "gen": "weighted",
                "options": {1: 70, 2: 15, 3: 5, 4: 5, 5: 5},
            },
            "pickup_latitude": {
                "gen": "uniform",
                "low": 40.7,
                "high": 40.8,
                "decimals": 5,
            },
            "pickup_longitude": {
                "gen": "uniform",
                "low": -74.02,
                "high": -73.93,
                "decimals": 5,
            },
            "distance_km": {"gen": "var", "name": "distance"},
            "fare": {
                "gen": "expr",
                "expr": "round(3 + 2.2 * distance + 0.4 * minutes, 2)",
            },
            "payment": {
                "gen": "weighted",
                "options": {"card": 65, "cash": 33, "dispute": 2},
            },
        },
        name="tasks.ride",
        vars={
            "distance": {"gen": "uniform", "low": 0.5, "high": 20, "decimals": 2},
            "minutes": {"gen": "uniform", "low": 3, "high": 40, "decimals": 1},
        },
    )

    @app.task(name="ride")
    def ride(task_id):
        """Gives one ride."""
        return ride_payload

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    build().run(until=UNTIL)

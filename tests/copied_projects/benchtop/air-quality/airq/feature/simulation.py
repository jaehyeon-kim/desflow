"""Runs the desflow simulation that writes all four tables, for the backfill and the daily run.

Each simulated hour publishes that hour's forecasts and reading. When a day ends, it
also publishes that day's daily weather and air quality, computed from the same hourly
rows. desflow's Iceberg writer sends each row to its table: the backfill appends
into new tables, and the daily run upserts on each table's keys, so running a day again
replaces it.
"""

from collections.abc import Iterator
from datetime import datetime, timedelta

from desflow import IcebergStorageEgress, SimulationContext
from pydantic import BaseModel

from airq.core.config import TABLES
from airq.core.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast
from airq.feature.features import daily_features, in_window

_HOUR = 3600.0
_MODELS = {model.__name__: model for model in TABLES}

# The columns that identify a row in each table, for the daily run's upserts.
UPSERT_KEYS = {
    WeatherForecast: ["location_id", "forecast_for", "lead_days"],
    Observation: ["location_id", "measured_at"],
    DailyWeather: ["location_id", "day", "lead_days"],
    DailyAirQuality: ["location_id", "day"],
}


def route(r: dict) -> str:
    """
    Returns the table for a published event, and turns the event back into a row.

    desflow publishes each row as `model_dump(mode="json")`, so its timestamps
    arrive as strings. Validating the row with its model turns them back into datetimes.
    The row replaces the contents of `r`, because the egress writes the same dict it
    passes here.

    Args:
        r (dict): The published event. `key` holds the model name and `value` the row.

    Returns:
        str: The identifier of the Iceberg table the row belongs to.
    """
    model = _MODELS[r["key"]]
    row = model.model_validate(r["value"]).model_dump()
    r.clear()
    r.update(row)
    return TABLES[model]


class HourlyPublisher:
    """
    Decides which rows each simulated hour publishes.

    Every hour publishes its own forecasts and reading. The last hour of a day also
    publishes that day's daily weather (the forecasts issued on it) and its air quality
    row. The air quality row needs the day before for its previous-day mean, so the
    readings of the day before are kept.

    Attributes:
        previous (list[Observation]): The readings of the day before the current one.
    """

    def __init__(self, previous: list[Observation] | None = None):
        """
        Initializes the publisher.

        Args:
            previous (list[Observation], optional): The readings of the day before the
                first simulated day. Without them, the first day has no air quality row.
        """
        self.previous: list[Observation] = list(previous or [])
        self._forecasts: list[WeatherForecast] = []
        self._observations: list[Observation] = []

    def step(self, rows: list[BaseModel]) -> list[BaseModel]:
        """
        Returns the rows to publish for one hour.

        Args:
            rows (list[BaseModel]): The hour's seven forecasts, then its reading, as
                `generate` yields them.

        Returns:
            list[BaseModel]: The hour's rows, followed by the day's daily rows when the
                hour is the day's last.
        """
        self._forecasts += rows[:-1]
        self._observations.append(rows[-1])
        hour = rows[-1].measured_at
        if (hour + timedelta(hours=1)).hour != 0:
            return list(rows)
        start = hour.replace(hour=0)
        end = start + timedelta(days=1)
        weather, air_quality = daily_features(
            self._forecasts, self.previous + self._observations
        )
        daily = [r for r in [*weather, *air_quality] if in_window(r, start, end)]
        self.previous, self._forecasts, self._observations = self._observations, [], []
        return [*rows, *daily]


def simulate(
    cat,
    records: Iterator[list[BaseModel]],
    start: datetime,
    hours: int,
    upsert: bool,
    previous: list[Observation] | None = None,
) -> None:
    """
    Simulates `hours` hours from `start` and writes every row through desflow.

    Args:
        cat: The Iceberg catalog.
        records (Iterator[list[BaseModel]]): The generator's hourly rows, positioned at
            `start`.
        start (datetime): The first simulated hour, in UTC.
        hours (int): How many hours to simulate.
        upsert (bool): Whether to upsert on each table's keys, rather than append.
        previous (list[Observation], optional): The readings of the day before `start`.
    """
    app = SimulationContext(
        "air_quality", factor=0.0, logical_start_time=start.replace(tzinfo=None)
    )
    publisher = HourlyPublisher(previous)

    @app.telemetry_loop(interval=_HOUR)
    def hourly(ctx):
        # desflow publishes a lag metric every simulated second by default,
        # which is 63 million records over two years at factor=0.
        ctx.env.egress_lag_monitor_interval = _HOUR
        for row in publisher.step(next(records)):
            ctx.env.publish_event(type(row).__name__, row)

    upsert_keys = {TABLES[m]: k for m, k in UPSERT_KEYS.items()} if upsert else None
    # One buffer for the whole run, so one Iceberg commit per table.
    app.add_egress(
        IcebergStorageEgress(catalog=cat, table_router=route, upsert_keys=upsert_keys),
        when=lambda r: r["stream_type"] == "event",
        batch_size=500_000,
    )
    app.run(until=hours * _HOUR)

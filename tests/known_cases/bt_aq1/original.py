"""BT-AQ1 to BT-AQ13: the air-quality project's own generator.

The original is the project's `airq.feature` package (`~/personal/benchtop/air-quality`,
read only, at `DESFLOW_BENCHTOP`; the test skips without it):

- `generate` runs the project's `generator.generate` with a random number generator that
  gives fixed draws (`_FixedRng`), then the project's `HourlyPublisher` over the hours of
  the window, as `simulation.simulate` does, and `load.advance` for a rerun from the
  stored first hour, so every row of the four tables is the project's;
- `fix` gives the YAML the same draws, so the YAML and the project run the same weather
  and `compare` holds the rows of each table equal. The project makes the rows of an
  hour in the hour they are issued, and the YAML makes a forecast in the hour it is for,
  so the rows are compared by their table and key, not by their order.

The fixed draws are a function of what a draw is for: a hidden weather state counts its
draws (`state`), a forecast has the hour it is for and its lead, and a reading has its
hour. A hundred and eighty hours of storms push the wind past both ends of its range
(the wind speed is never below 0, and a very windy hour gives a PM2.5 below 0, which the
reading clamps), and a run of draws takes the wind direction past 0 and 360, so each
clamp and the wrap of the direction are reached. The draw for a wet hour is below 0.06
about once in 17 hours and above 0.78 about once in 5, so a dry hour turns wet and a wet
hour turns dry or stays wet.

What is checked beside the rows: AQ1 (the hours start at the stored first hour, one
every 3,600 s), AQ13 (one lag reading every 3,600 s), and AQ9 to AQ11 (the sink the
document describes has the project's tables, upsert keys and timestamp type).
"""

import contextlib
import os
import types
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from desflow import IcebergStorageEgress, SimulationContext

from tests.known_cases import harness
from tests.known_cases.bt_support import in_project

HERE = Path(__file__).parent
ORIGIN = datetime(2024, 10, 1, tzinfo=UTC)
SEED = 42
GOLDEN = 0.6180339887
PHASE = {"wet": 0.7548776662, "rain": 0.5698402909}
FORECAST_STEP = 0.113
FIELD_STEP = 0.37
READING_PHASE = 0.91
STORM = 60  # hours in a block of the storm schedule
BLOCKS = 5
VARIABLES = {"HOURS": "432", "RUN_HOURS": "600"}
CONFIGS = [
    {
        # a backfill: the window is the whole run from the first hour (AQ1)
        "variables": {"HOURS": "432", "RUN_HOURS": "600", "WARMUP_HOURS": "0"},
        "args": {"hours": 432, "warmup": 0},
    },
    {
        # a rerun from the stored first hour, writing from day 5 on (AQ12)
        "variables": {"HOURS": "432", "RUN_HOURS": "600", "WARMUP_HOURS": "120"},
        "args": {"hours": 432, "warmup": 120},
    },
]


def _storm(block: int, kind: str) -> float | None:
    """The fixed draw a block of the storm schedule gives: a wind of +1.5 or -1.5
    standard deviations (the wind is then past both ends of its range), or a wind
    direction that moves one deviation each hour."""
    if kind == "wind":
        return {1: 1.5, 2: -1.5}.get(block)
    return {3: 1.0, 4: -1.0}.get(block)


def z_python(kind: str, k: int, field: int, lead: int = 0) -> float:
    """The fixed draw of a normal, in standard deviations, for hour `k`.

    Args:
        kind: `state` (the hidden state of hour `k`), `forecast` (a forecast for hour
            `k`) or `reading` (the reading of hour `k`).
        k: The hour, counted from the first hour (for a state, the number of the draw).
        field: Which normal of the kind: a state has the temperature (1), the wind (2)
            and the direction (3); a forecast has four.
        lead: The lead of a forecast, in days.

    Returns:
        float: A number from -1.5 to 1.5.
    """
    if kind == "state":
        storm = _storm(
            (k // STORM) % BLOCKS, {2: "wind", 3: "direction"}.get(field, "")
        )
        if field in (2, 3) and storm is not None:
            return storm
        return 2 * ((k * GOLDEN + field * FIELD_STEP) % 1.0) - 1
    if kind == "forecast":
        return 2 * ((k * GOLDEN + lead * FORECAST_STEP + field * FIELD_STEP) % 1.0) - 1
    return 2 * ((k * GOLDEN + READING_PHASE) % 1.0) - 1


def _z_expression(kind: str, k: str, field: int, lead: str = "0") -> str:
    """The YAML expression of `z_python`, with `k` and `lead` as expressions."""
    if kind == "state":
        block = f"(({k}) // {STORM}) % {BLOCKS}"
        base = f"2 * ((({k}) * {GOLDEN} + {field * FIELD_STEP!r}) % 1.0) - 1"
        if field == 2:
            return f"1.5 if {block} == 1 else -1.5 if {block} == 2 else {base}"
        if field == 3:
            return f"1.0 if {block} == 3 else -1.0 if {block} == 4 else {base}"
        return base
    if kind == "forecast":
        return (
            f"2 * ((({k}) * {GOLDEN} + ({lead}) * {FORECAST_STEP} + "
            f"{field * FIELD_STEP!r}) % 1.0) - 1"
        )
    return f"2 * ((({k}) * {GOLDEN} + {READING_PHASE}) % 1.0) - 1"


def _hour(k: str = "t") -> str:
    return f"int(({k}) // 3600)"


def fix(document: dict[str, Any]) -> dict[str, Any]:
    """The document with its random draws fixed: every `normal`, the wet draw and the
    rain amount an expression of the fixed draw the project's stand-in generator gives.

    Args:
        document: The loaded `model.yaml`.

    Returns:
        dict[str, Any]: The same document with the draws fixed.
    """
    weather = document["tasks"]["weather"]["vars"]
    state = f"{_hour()} + 1"
    for field, name in ((1, "n_temp"), (2, "n_wind"), (3, "n_direction")):
        draw = weather[name]["names"]["e"]
        assert draw["mean"] == 0, draw
        weather[name]["names"]["e"] = {
            "gen": "expr",
            "expr": f"{draw['mean']} + {draw['std']!r} * "
            f"({_z_expression('state', state, field)})",
        }
    chance = weather["n_wet"]["p"]["expr"]
    weather["n_wet"] = {
        "gen": "expr",
        "expr": f"(({state}) * {PHASE['wet']}) % 1.0 < ({chance})",
    }
    # the exponential of the document has the project's mean of 0.6 (to 4 digits)
    rate = weather["n_rain"]["names"]["r"]["rate"]
    assert abs(1 / rate - 0.6) < 1e-4, rate
    weather["n_rain"]["names"]["r"] = {
        "gen": "expr",
        "expr": f"0.6 * (0.1 + 1.8 * ((({state}) * {PHASE['rain']}) % 1.0))",
    }
    noise = weather["pm"]["names"]["e"]
    assert noise["mean"] == 0, noise
    weather["pm"]["names"]["e"] = {
        "gen": "expr",
        "expr": f"{noise['mean']} + {noise['std']!r} * "
        f"({_z_expression('reading', _hour(), 0)})",
    }
    forecast = document["tasks"]["forecast"]["vars"]
    for field, name in (
        (1, "t2m"),
        (2, "precipitation"),
        (3, "wind_speed"),
        (4, "wind_direction"),
    ):
        std = forecast[name]["names"]["e"]["std"]["expr"]
        forecast[name]["names"]["e"] = {
            "gen": "expr",
            "expr": f"0 + ({std}) * ({_z_expression('forecast', _hour(), field, 'arg.lead')})",
        }
    return document


class _FixedRng:
    """A `numpy` generator that gives the fixed draws: `where` says what the next draws
    are for, which the wrappers of the project's three functions set."""

    def __init__(self) -> None:
        self.where: tuple[Any, ...] = ()
        self.calls = 0
        self.states = 0

    def random(self) -> float:
        return (self.where[1] * PHASE["wet"]) % 1.0

    def exponential(self, scale: float) -> float:
        return scale * (0.1 + 1.8 * ((self.where[1] * PHASE["rain"]) % 1.0))

    def normal(self, mean: float, std: float) -> float:
        self.calls += 1
        kind, k, *rest = self.where
        z = (
            z_python(kind, k, self.calls, *rest)
            if kind != "reading"
            else z_python(kind, k, 0)
        )
        return mean + std * z


@contextlib.contextmanager
def _unchanged_environment() -> Iterator[None]:
    """The project's `airq.core.config` sets service addresses in `os.environ` when it is
    imported; the test leaves the environment as it found it."""
    before = dict(os.environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(before)


def _project() -> types.SimpleNamespace:
    """The project's modules, imported from its folder."""
    with _unchanged_environment():
        from airq.core import config, models
        from airq.feature import generator, load, simulation
        from airq.stores import iceberg

    return types.SimpleNamespace(
        config=config,
        models=models,
        generator=generator,
        load=load,
        simulation=simulation,
        iceberg=iceberg,
    )


@in_project("air-quality")
def generate(config: dict[str, Any]) -> types.SimpleNamespace:
    """Runs the project's generator with the fixed draws for the hours of the window.

    Args:
        config: An entry of `CONFIGS`; `args` has `hours` (the end of the window, in
            hours from the first hour) and `warmup` (the hours before the window).

    Returns:
        types.SimpleNamespace: `rows` (the project's rows in the order it publishes
            them), `project` (its modules), `states` (the hidden states, in order).
    """
    project = _project()
    generator = project.generator
    hours, warmup = config["args"]["hours"], config["args"]["warmup"]
    rng = _FixedRng()
    states: list[dict[str, Any]] = []
    real_next, real_forecast, real_observation = (
        generator.next_hour,
        generator.weather_forecast,
        generator.observation,
    )

    def index(when: datetime) -> int:
        return int((when - ORIGIN).total_seconds() // 3600)

    def next_hour(rng: _FixedRng, last: dict[str, Any]) -> dict[str, Any]:
        rng.states += 1
        rng.where, rng.calls = ("state", rng.states), 0
        states.append(real_next(rng, last))
        return states[-1]

    def forecast(
        rng: _FixedRng, issued: datetime, lead: int, truth: dict[str, Any]
    ) -> Any:
        rng.where, rng.calls = ("forecast", index(issued) + 24 * lead, lead), 0
        return real_forecast(rng, issued, lead, truth)

    def observation(rng: _FixedRng, hour: datetime, truth: dict[str, Any]) -> Any:
        rng.where, rng.calls = ("reading", index(hour)), 0
        return real_observation(rng, hour, truth)

    rows: list[Any] = []
    with (
        mock.patch.object(generator.np.random, "default_rng", lambda seed: rng),
        mock.patch.object(generator, "next_hour", next_hour),
        mock.patch.object(generator, "weather_forecast", forecast),
        mock.patch.object(generator, "observation", observation),
    ):
        records = generator.generate(ORIGIN, SEED)
        start = ORIGIN + timedelta(hours=warmup)
        previous = project.load.advance(records, start) if warmup else []
        publisher = project.simulation.HourlyPublisher(previous)
        for _ in range(hours - warmup):
            rows += publisher.step(next(records))
    return types.SimpleNamespace(rows=rows, project=project, states=states)


def _a_tie_of_the_mean(
    row: Any, found: Any, rows: list[Any], project: types.SimpleNamespace
) -> None:
    """A daily mean the YAML and the project round to different hundredths is a mean that
    lies on the middle of two: the YAML adds its 24 readings newest first and the project
    with NumPy's pairwise sum, so the last bits differ and a tie rounds either way.

    Args:
        row: The project's row.
        found: The YAML's row, which differs from it.
        rows: Every row the project made.
        project: The project's modules.

    Raises:
        AssertionError: When the rows differ in anything but such a mean.
    """
    assert isinstance(row, project.models.DailyWeather), (row, found)
    differing = {
        name
        for name in type(row).model_fields
        if getattr(row, name) != getattr(found, name)
    }
    assert differing <= {"temperature_2m", "wind_speed_10m"}, (row, found)
    hourly = [
        f
        for f in rows
        if isinstance(f, project.models.WeatherForecast)
        and f.forecast_for.date() == row.day
        and f.lead_days == row.lead_days
    ]
    assert len(hourly) == 24
    for name in differing:
        mean = sum(getattr(f, f"{name}") for f in hourly) / 24
        assert abs(mean * 100 % 1 - 0.5) < 1e-6, (name, mean, row, found)
        assert abs(getattr(row, name) - getattr(found, name)) <= 0.0101


def _rows(records: list[dict[str, Any]], table: str) -> list[dict[str, Any]]:
    return [
        {k: v for k, v in r["value"].items() if k != "__table__"}
        for r in records
        if r["stream_type"] == "event"
        and isinstance(r["value"], dict)
        and r["value"].get("__table__") == table
    ]


def compare(
    records: list[dict[str, Any]], result: types.SimpleNamespace, config: dict[str, Any]
) -> None:
    """Asserts that the YAML's rows and the project's rows are the same set in each of the
    four tables, and that the checks the rows name hold, for BT-AQ1 to BT-AQ13.

    Args:
        records: The records of the YAML with the fixed draws.
        result: What `generate` returned.
        config: The entry of `CONFIGS` both ran.

    Raises:
        AssertionError: At the first check that fails.
    """
    project = result.project
    tables = project.config.TABLES
    hours, warmup = config["args"]["hours"], config["args"]["warmup"]
    window = hours - warmup
    # AQ9: four tables, and the YAML's rows are the project's models, equal in every field
    # of every row, found by the key the table's upsert uses (AQ10)
    keys = {
        tables[model]: columns
        for model, columns in project.simulation.UPSERT_KEYS.items()
    }
    for model, table in tables.items():
        wanted = [row for row in result.rows if type(row) is model]
        got = [model.model_validate(row) for row in _rows(records, table)]
        key = keys[table]
        by_key = {tuple(getattr(row, c) for c in key): row for row in got}
        assert len(by_key) == len(got), f"{table}: a key is written twice"
        assert set(by_key) == {tuple(getattr(r, c) for c in key) for r in wanted}, table
        for row in wanted:
            found = by_key[tuple(getattr(row, c) for c in key)]
            if found != row:
                _a_tie_of_the_mean(row, found, result.rows, project)
    forecasts = [
        r for r in result.rows if isinstance(r, project.models.WeatherForecast)
    ]
    readings = [r for r in result.rows if isinstance(r, project.models.Observation)]
    daily_weather = [
        r for r in result.rows if isinstance(r, project.models.DailyWeather)
    ]
    daily_air = [
        r for r in result.rows if isinstance(r, project.models.DailyAirQuality)
    ]
    # AQ6 and AQ7: seven forecasts and a reading for each hour of the window
    assert len(forecasts) == 7 * window and len(readings) == window
    assert {f.lead_days for f in forecasts} == set(range(1, 8))
    # AQ8: a daily weather row for each day and lead issued in the window, and a daily air
    # quality row for each day that has the day before
    days = window // 24
    assert len(daily_weather) == 7 * days
    assert len(daily_air) == days - (1 if warmup == 0 else 0)
    # every branch the draws are for is reached
    assert {r.is_weekend for r in daily_air} == {True, False}  # AQ5
    assert any(r.pm2_5 == 0.0 for r in readings)  # AQ4: the reading's clamp at 0
    assert any(r.pm2_5 > 5 for r in readings)
    assert any(f.wind_speed_10m == 0.0 for f in forecasts)  # AQ2: the wind's clamp at 0
    assert any(f.precipitation == 0.0 for f in forecasts) and any(
        f.precipitation > 0 for f in forecasts
    )  # AQ3
    wet = [s["wet"] for s in result.states]
    assert {(a, b) for a, b in zip(wet, wet[1:])} == {
        (False, False),
        (False, True),
        (True, False),
        (True, True),
    }  # AQ2: the wet or dry chain
    turns = [s["direction"] for s in result.states]
    assert any(abs(a - b) > 180 for a, b in zip(turns, turns[1:]))  # AQ2: the wrap
    assert any(w > 0 for w in (d.wet_hours for d in daily_weather)) and any(
        d.wet_hours == 0 for d in daily_weather
    )
    # AQ1: an hour every 3,600 s from the first hour, and a run of the stated length
    hourly = [
        r for r in records if r["stream_type"] == "event" and r["task"] == "weather"
    ]
    run_hours = int(config["variables"]["RUN_HOURS"])
    if warmup == 0:
        assert [r["sim_ts"] for r in hourly] == [3600.0 * n for n in range(run_hours)]
        first = _rows(records, tables[project.models.Observation])[0]
        assert first["measured_at"].startswith("2024-10-01T00:00:00")
    # AQ13: a lag reading every 3,600 s, not every second
    lag = [r["sim_ts"] for r in records if str(r.get("path_id")) in harness.WALL_PATHS]
    assert lag, "the run has no lag readings"
    assert all(t % 3600 == 0 for t in lag), lag[:5]
    assert len(lag) <= run_hours + 1, len(lag)
    # AQ9 to AQ11: the sink of the document has the project's upsert keys, and a
    # timestamp column of the project's type
    app = SimulationContext.from_yaml(
        HERE / "model.yaml", variables=config["variables"]
    )
    sink = app._egress_providers[0]
    assert isinstance(sink, IcebergStorageEgress)
    assert sink.upsert_keys == keys
    pinned = sink.schemas[tables[project.models.Observation]]
    assert pinned.field("measured_at").type == project.iceberg._TS

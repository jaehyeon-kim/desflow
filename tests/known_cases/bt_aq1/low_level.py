"""BT-AQ1 to BT-AQ13: air-quality hourly step.

Low-level form: plain SimPy on a `DynamicRealtimeEnvironment`, which publishes each
record with `publish_event`. One process steps the hours. The draws are the ones of
`builder.py`, given the place the document writes them at, and read through the
`ValueRuntime`, so they come from the random streams the document's draws come from. The
hidden weather is the attributes of one entity, as in the document. The daily rows are
means over the readings and forecasts the process keeps, taken newest first as the
`aggregate` generator takes them.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from desflow import (
    DistributionConfig,
    DynamicRealtimeEnvironment,
    Sampler,
    SimParameter,
)
from desflow.values import Template, ValueRuntime
from desflow.values.template import bind

from tests.known_cases.bt_aq1.builder import ARRIVAL, TASKS
from tests.known_cases.bt_support import Entity, deep

HOURS = 432
WARMUP_HOURS = 0
UNTIL = 600 * 3600
START = datetime(2024, 10, 1, tzinfo=timezone.utc)
ENVIRONMENT = {"logical_start_time": START, "timezone": "UTC"}
PARAMS = SimParameter(
    sim_id="air_quality",
    arrival={"hour": DistributionConfig(dist="constant", value=3600)},
)
TABLES = {
    "reading": "airq.observations",
    "forecast": "airq.weather_forecasts",
    "daily_weather": "airq.daily_weather",
    "daily_air_quality": "airq.daily_air_quality",
}
WEATHER_VARS = (
    "pm",
    "temperature",
    "wind",
    "n_temp",
    "n_wind",
    "n_wet",
    "n_rain",
    "n_direction",
)


class _Run:
    """What the value runtime asks of a running model: its id and no signals."""

    sim_id = "air_quality"

    def __init__(self) -> None:
        self.values: Any = None

    def _stop_run(self) -> None:
        pass


def _stamp(env: DynamicRealtimeEnvironment, behind: float = 0.0) -> datetime:
    return START + timedelta(seconds=env.now - behind)


def _iso(moment: datetime) -> str:
    return moment.isoformat(timespec="milliseconds")


def _mean(rows: list[dict[str, Any]], field: str, keep: Any = None) -> float:
    """The mean of `field` over the newest 24 rows that `keep` accepts, added newest
    first, as the `aggregate` generator reads them."""
    values: list[float] = []
    for row in reversed(rows):
        if keep is not None and not keep(row):
            continue
        values.append(row[field])
        if len(values) == 24:
            break
    return sum(values) / len(values)


def build(env: DynamicRealtimeEnvironment) -> None:
    sampler = Sampler(seed=42)
    run = _Run()
    values = ValueRuntime(42, env, sampler, run, stop=run._stop_run)
    run.values = values
    hidden = Entity("hour-1", **deep(ARRIVAL["attributes"]))
    weather = Template(
        {name: {"gen": "var", "name": name} for name in WEATHER_VARS},
        name="tasks.weather",
        vars=TASKS["weather"]["vars"],
    )
    forecast_vars = deep(
        {name: _bare(spec) for name, spec in TASKS["forecast"]["vars"].items()}
    )
    bind(forecast_vars, ("tasks", "forecast", "vars"))
    readings: list[dict[str, Any]] = []
    forecasts: list[dict[str, Any]] = []
    counts: dict[str, int] = {}

    def publish(task: str, payload: dict[str, Any]) -> None:
        env.publish_event(hidden.key, payload, task=task, instance=counts.get(task, 0))
        counts[task] = counts.get(task, 0) + 1

    def row(task: str, payload: dict[str, Any]) -> dict[str, Any]:
        payload = {**payload, "__table__": TABLES[task]}
        publish(task, payload)
        return payload

    def hour() -> Any:
        while True:
            at = round(env.now / 3600)
            now = _stamp(env)
            attributes = hidden.attributes
            drawn = weather.render(run, hidden)
            publish("weather", {})
            attributes.update(
                act_pm=drawn["pm"],
                act_temp=drawn["temperature"],
                act_wind=drawn["wind"],
                act_rain=attributes["rain"],
                act_direction=attributes["direction"],
                temp=drawn["n_temp"],
                wind=drawn["n_wind"],
                wet=drawn["n_wet"],
                rain=drawn["n_rain"],
                direction=drawn["n_direction"],
            )
            publish("fanout", {})
            if at < HOURS:
                readings.append(
                    row(
                        "reading",
                        {
                            "location_id": "station-1",
                            "measured_at": _iso(now),
                            "pm2_5": attributes["act_pm"],
                        },
                    )
                )
            for lead in range(
                max(1, (at - HOURS) // 24 + 1),
                min(7, (at - WARMUP_HOURS) // 24) + 1,
            ):
                names = values.evaluate_vars(
                    {
                        "lead": lead,
                        "temp": attributes["act_temp"],
                        "wind": attributes["act_wind"],
                        "rain": attributes["act_rain"],
                        "direction": attributes["act_direction"],
                        **forecast_vars,
                    },
                    values.scope_of(hidden),
                )
                forecasts.append(
                    row(
                        "forecast",
                        {
                            "location_id": "station-1",
                            "forecast_for": _iso(now),
                            "issued_at": _iso(_stamp(env, 86400 * lead)),
                            "lead_days": lead,
                            "temperature_2m": names["t2m"],
                            "precipitation": names["precipitation"],
                            "wind_speed_10m": names["wind_speed"],
                            "wind_direction_10m": names["wind_direction"],
                        },
                    )
                )
            if at == 23:
                publish("day_one", {})
                attributes["prev_mean"] = _mean(readings, "pm2_5")
            if at % 24 == 23 and 47 <= at < HOURS:
                mean = _mean(readings, "pm2_5")
                epoch_days = int(now.timestamp()) // 86400
                row(
                    "daily_air_quality",
                    {
                        "location_id": "station-1",
                        "day": now.strftime("%Y-%m-%d"),
                        "pm2_5": round(mean, 2),
                        "is_weekend": ((epoch_days + 3) % 7) >= 5,
                        "pm2_5_lag1": round(attributes["prev_mean"], 2),
                    },
                )
                attributes["prev_mean"] = mean
            day = int(env.now // 86400)
            if at % 24 == 23:
                today = now.strftime("%Y-%m-%d")
                for lead in range(
                    max(1, day - HOURS // 24 + 1),
                    min(7, day - WARMUP_HOURS // 24) + 1,
                ):

                    def keep(r: dict[str, Any], lead: int = lead) -> bool:
                        return (
                            r["lead_days"] == lead and r["forecast_for"][:10] == today
                        )

                    row(
                        "daily_weather",
                        {
                            "location_id": "station-1",
                            "day": today,
                            "lead_days": lead,
                            "issued_on": _stamp(env, 86400 * lead).strftime("%Y-%m-%d"),
                            "temperature_2m": round(
                                _mean(forecasts, "temperature_2m", keep), 2
                            ),
                            "wind_speed_10m": round(
                                _mean(forecasts, "wind_speed_10m", keep), 2
                            ),
                            "wet_hours": sum(
                                1
                                for r in forecasts
                                if keep(r) and r["precipitation"] > 0
                            ),
                        },
                    )
            yield env.timeout(3600)

    env.process(hour())


def _bare(spec: Any) -> Any:
    """The document's generator with `arg.<name>` read as the bare name `<name>`, which
    the low-level form supplies as a name."""
    if isinstance(spec, dict):
        return {key: _bare(value) for key, value in spec.items()}
    if isinstance(spec, list):
        return [_bare(value) for value in spec]
    if isinstance(spec, str):
        return spec.replace("arg.", "")
    return spec

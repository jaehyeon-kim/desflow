"""BT-AQ1 to BT-AQ13: air-quality hourly step.

Builder form of `model.yaml`. A blueprint's `count: 1` is one entity that starts the
`weather` task every hour and keeps the weather in its attributes, so the Builder runs
that loop itself with `arrival_loop` and starts every hour on the entity of the first.
"""

from datetime import datetime, timezone
from typing import Any

from desflow import IcebergStorageEgress, SimulationContext
from desflow.values import Template

from tests.known_cases.bt_support import deep

ARRIVAL = {
    "dist": "constant",
    "value": 3600,
    "count": 1,
    "first_at": 0,
    "spawn": "weather",
    "attributes": {
        "temp": 0,
        "wind": 0,
        "wet": False,
        "rain": 0,
        "direction": 180,
        "prev_mean": 0,
        "act_pm": 0,
        "act_temp": 0,
        "act_wind": 0,
        "act_rain": 0,
        "act_direction": 0,
    },
}

TASKS = {
    "weather": {
        "vars": {
            "epoch_s": {"gen": "format_time", "format": "epoch_s"},
            "temperature": {
                "gen": "expr",
                "expr": "18 + 5 * cos(2 * 3.141592653589793 * "
                "(epoch_s / 3600 - 360) / 8766) + 3 * "
                "cos(2 * 3.141592653589793 * (epoch_s / "
                "3600 - 4.5) / 24) + attr.temp",
            },
            "wind": {
                "gen": "expr",
                "expr": "max(10 + 2.4 * cos(2 * 3.141592653589793 * "
                "(epoch_s / 3600 - 5.5) / 24) + attr.wind, 0)",
            },
            "weekday": {
                "gen": "expr",
                "expr": "1 if ((epoch_s // 86400 + 3) % 7) < 5 else 0",
            },
            "pm": {
                "gen": "expr",
                "expr": "max(exp(2.05 + 0.4 * weekday + 0.12 * cos(2 * "
                "3.141592653589793 * (epoch_s / 3600 - 17) / 24) "
                "- 0.1 * (wind - 10) - 0.4 * (1 if attr.wet else "
                "0) + e) - 1, 0)",
                "names": {"e": {"gen": "normal", "mean": 0, "std": 0.1}},
                "decimals": 2,
            },
            "n_temp": {
                "gen": "expr",
                "expr": "0.96 * attr.temp + e",
                "names": {"e": {"gen": "normal", "mean": 0, "std": 0.8}},
            },
            "n_wind": {
                "gen": "expr",
                "expr": "0.9 * attr.wind + e",
                "names": {"e": {"gen": "normal", "mean": 0, "std": 1.8}},
            },
            "n_wet": {
                "gen": "boolean",
                "p": {"gen": "expr", "expr": "0.78 if attr.wet else 0.06"},
            },
            "n_rain": {
                "gen": "expr",
                "expr": "r if n_wet else 0",
                "names": {
                    "r": {"gen": "sample", "dist": "exponential", "rate": 1.6667}
                },
            },
            "n_direction": {
                "gen": "expr",
                "expr": "(attr.direction + e) % 360",
                "names": {"e": {"gen": "normal", "mean": 0, "std": 27}},
            },
        },
        "payload": {},
        "next": "fanout",
        "set": {
            "act_pm": {"gen": "var", "name": "pm"},
            "act_temp": {"gen": "var", "name": "temperature"},
            "act_wind": {"gen": "var", "name": "wind"},
            "act_rain": {"gen": "expr", "expr": "attr.rain"},
            "act_direction": {"gen": "expr", "expr": "attr.direction"},
            "temp": {"gen": "var", "name": "n_temp"},
            "wind": {"gen": "var", "name": "n_wind"},
            "wet": {"gen": "var", "name": "n_wet"},
            "rain": {"gen": "var", "name": "n_rain"},
            "direction": {"gen": "var", "name": "n_direction"},
        },
    },
    "fanout": {
        "open": {
            "readings": {
                "each": "1 if int(t // 3600) < 432 else 0",
                "branch": {
                    "task": "reading",
                    "args": {"pm": {"gen": "expr", "expr": "attr.act_pm"}},
                },
            },
            "forecasts": {
                "each": "range(max(1, (int(t // 3600) - 432) // 24 "
                "+ 1), min(7, (int(t // 3600) - 0) // 24) + "
                "1)",
                "branch": {
                    "task": "forecast",
                    "args": {
                        "lead": {"gen": "expr", "expr": "each.value"},
                        "temp": {"gen": "expr", "expr": "attr.act_temp"},
                        "wind": {"gen": "expr", "expr": "attr.act_wind"},
                        "rain": {"gen": "expr", "expr": "attr.act_rain"},
                        "direction": {"gen": "expr", "expr": "attr.act_direction"},
                    },
                },
            },
            "first_day": {
                "each": "1 if int(t // 3600) == 23 else 0",
                "branch": {"task": "day_one"},
            },
            "daily_air": {
                "each": "1 if int(t // 3600) % 24 == 23 and int(t "
                "// 3600) >= 47 and int(t // 3600) < 432 "
                "else 0",
                "branch": {"task": "daily_air_quality"},
            },
            "daily_weather": {
                "each": "range(max(1, int(t // 86400) - 432 // "
                "24 + 1), min(7, int(t // 86400) - 0 // "
                "24) + 1) if int(t // 3600) % 24 == 23 "
                "else []",
                "branch": {
                    "task": "daily_weather",
                    "args": {"lead": {"gen": "expr", "expr": "each.value"}},
                },
            },
        },
        "payload": {},
    },
    "reading": {
        "payload": {
            "location_id": "station-1",
            "measured_at": {"gen": "now"},
            "pm2_5": {"gen": "expr", "expr": "arg.pm"},
            "__table__": "airq.observations",
        }
    },
    "forecast": {
        "vars": {
            "t2m": {
                "gen": "expr",
                "expr": "arg.temp + e",
                "names": {
                    "e": {
                        "gen": "normal",
                        "mean": 0,
                        "std": {"gen": "expr", "expr": "1 + 0.2 * arg.lead"},
                    }
                },
                "decimals": 1,
            },
            "precipitation": {
                "gen": "expr",
                "expr": "arg.rain * exp(e)",
                "names": {
                    "e": {
                        "gen": "normal",
                        "mean": 0,
                        "std": {"gen": "expr", "expr": "0.3 + 0.1 * arg.lead"},
                    }
                },
                "decimals": 1,
            },
            "wind_speed": {
                "gen": "expr",
                "expr": "arg.wind * exp(e)",
                "names": {
                    "e": {
                        "gen": "normal",
                        "mean": 0,
                        "std": {"gen": "expr", "expr": "0.1 + 0.03 * arg.lead"},
                    }
                },
                "decimals": 1,
            },
            "wind_direction": {
                "gen": "expr",
                "expr": "round(arg.direction + e) % 360",
                "names": {
                    "e": {
                        "gen": "normal",
                        "mean": 0,
                        "std": {"gen": "expr", "expr": "35 + 5 * arg.lead"},
                    }
                },
            },
        },
        "payload": {
            "location_id": "station-1",
            "forecast_for": {"gen": "now"},
            "issued_at": {
                "gen": "now",
                "offset": {"gen": "expr", "expr": "-86400 * arg.lead"},
            },
            "lead_days": {"gen": "expr", "expr": "arg.lead"},
            "temperature_2m": {"gen": "var", "name": "t2m"},
            "precipitation": {"gen": "var", "name": "precipitation"},
            "wind_speed_10m": {"gen": "var", "name": "wind_speed"},
            "wind_direction_10m": {"gen": "var", "name": "wind_direction"},
            "__table__": "airq.weather_forecasts",
        },
    },
    "day_one": {
        "vars": {
            "m": {
                "gen": "aggregate",
                "task": "reading",
                "path": "pm2_5",
                "last": 24,
                "function": "mean",
            }
        },
        "payload": {},
        "set": {"prev_mean": {"gen": "var", "name": "m"}},
    },
    "daily_air_quality": {
        "vars": {
            "m": {
                "gen": "aggregate",
                "task": "reading",
                "path": "pm2_5",
                "last": 24,
                "function": "mean",
            },
            "epoch_s": {"gen": "format_time", "format": "epoch_s"},
        },
        "payload": {
            "location_id": "station-1",
            "day": {"gen": "format_time", "format": "%Y-%m-%d"},
            "pm2_5": {"gen": "expr", "expr": "round(m, 2)"},
            "is_weekend": {"gen": "expr", "expr": "((epoch_s // 86400 + 3) % 7) >= 5"},
            "pm2_5_lag1": {"gen": "expr", "expr": "round(attr.prev_mean, 2)"},
            "__table__": "airq.daily_air_quality",
        },
        "set": {"prev_mean": {"gen": "var", "name": "m"}},
    },
    "daily_weather": {
        "vars": {
            "day": {"gen": "format_time", "format": "%Y-%m-%d"},
            "temp_mean": {
                "gen": "aggregate",
                "task": "forecast",
                "path": "temperature_2m",
                "last": 24,
                "function": "mean",
                "where": "lead_days == arg.lead and forecast_for[:10] == day",
            },
            "wind_mean": {
                "gen": "aggregate",
                "task": "forecast",
                "path": "wind_speed_10m",
                "last": 24,
                "function": "mean",
                "where": "lead_days == arg.lead and forecast_for[:10] == day",
            },
            "wet": {
                "gen": "aggregate",
                "task": "forecast",
                "last": 24,
                "function": "count",
                "where": "lead_days == arg.lead and "
                "forecast_for[:10] == day and "
                "precipitation > 0",
            },
        },
        "payload": {
            "location_id": "station-1",
            "day": {"gen": "var", "name": "day"},
            "lead_days": {"gen": "expr", "expr": "arg.lead"},
            "issued_on": {
                "gen": "format_time",
                "time": {
                    "gen": "now",
                    "offset": {"gen": "expr", "expr": "-86400 * arg.lead"},
                },
                "format": "%Y-%m-%d",
            },
            "temperature_2m": {"gen": "expr", "expr": "round(temp_mean, 2)"},
            "wind_speed_10m": {"gen": "expr", "expr": "round(wind_mean, 2)"},
            "wet_hours": {"gen": "var", "name": "wet"},
            "__table__": "airq.daily_weather",
        },
    },
}

EGRESS = {
    "catalog": {
        "name": "odctl",
        "type": "rest",
        "uri": "http://127.0.0.1:8181",
        "warehouse": "s3://warehouse/",
    },
    "upsert_keys": {
        "airq.observations": ["location_id", "measured_at"],
        "airq.weather_forecasts": ["location_id", "forecast_for", "lead_days"],
        "airq.daily_weather": ["location_id", "day", "lead_days"],
        "airq.daily_air_quality": ["location_id", "day"],
    },
    "schemas": {"airq.observations": {"measured_at": "timestamptz"}},
}

HISTORY = 1_000_000  # rows a task keeps for the aggregates that read it


def _returns(payload: Any, task: str) -> Any:
    """The function of a task that returns `payload` as a template named for the task,
    so its generators draw from the streams the document's `payload` does."""
    template = Template(payload, name=f"tasks.{task}")

    def body(task_id):
        return template

    body.__name__ = task
    return body


def build() -> SimulationContext:
    app = SimulationContext(
        sim_id="air_quality",
        factor=0,
        random_seed=42,
        logical_start_time=datetime(2024, 10, 1, 0, 0, tzinfo=timezone.utc),
        timezone="UTC",
        lag_interval=3600,
        warmup=0,
    )
    app.add_egress(IcebergStorageEgress(**EGRESS))
    spec = dict(ARRIVAL)
    for key in ("count", "spawn"):  # the loop below starts the one entity's hours
        spec.pop(key)
    app.add_arrival("hour", spec.pop("dist"), **deep(spec))
    functions = {}
    for name, spec in TASKS.items():
        spec = dict(spec)
        payload = spec.pop("payload", {})
        history = HISTORY if name in ("reading", "forecast") else None
        functions[name] = app.task(
            name=name, history=history, **{k: deep(v) for k, v in spec.items()}
        )(_returns(deep(payload), name))

    @app.arrival_loop("hour")
    def hours(context):
        entity = None
        while True:
            arrived = yield context.wait_for_arrival("hour")
            entity = entity or arrived
            context.spawn(
                functions["weather"](context.next_instance("weather"), entity=entity)
            )

    return app


def run(app: SimulationContext):
    """Runs the model to the end its `run` section sets."""
    return app.run(until=600 * 3600)

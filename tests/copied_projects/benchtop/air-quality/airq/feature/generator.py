"""Generates the values in the hourly forecast and reading records.

The values only need to look realistic. Each hour's weather keeps part of the previous
hour's, and follows a daily and a yearly cycle. A forecast is the true value plus an
error that grows with the lead time.

`generate` produces every hour's records from a first hour and a seed. The same first
hour and seed always give the same values, so the backfill and the daily run write the
same values for the same hour.
"""

import math
from collections.abc import Iterator
from datetime import datetime, timedelta

import numpy as np
from numpy.random import Generator
from pydantic import BaseModel

from airq.core.config import LEADS, STATION
from airq.core.models import Observation, WeatherForecast

# Temperature and wind are deviations from their cycles.
_CALM = {"temperature": 0.0, "wind": 0.0, "wet": False, "rain": 0.0, "direction": 180.0}  # fmt: skip


def wave(t: datetime, period_hours: float, peak_hour: float) -> float:
    """
    Returns a cosine that repeats every `period_hours` hours.

    Args:
        t (datetime): The time to evaluate.
        period_hours (float): The length of one cycle, in hours.
        peak_hour (float): How many hours into each cycle the value is highest.

    Returns:
        float: A value between -1 and 1.
    """
    return math.cos(2 * math.pi * (t.timestamp() / 3600 - peak_hour) / period_hours)


def actual(hour: datetime, truth: dict) -> tuple[float, float]:
    """
    Returns the true temperature and wind speed in an hour.

    Each value is its daily and yearly cycle plus the hour's deviation from that cycle.
    The wind speed is never below zero.

    Args:
        hour (datetime): The hour.
        truth (dict): The hour's true state, from `next_hour`.

    Returns:
        tuple[float, float]: The temperature in °C and the wind speed in km/h.
    """
    temperature = 18 + 5 * wave(hour, 8766, 15 * 24) + 3 * wave(hour, 24, 4.5)
    wind = max(10 + 2.4 * wave(hour, 24, 5.5) + truth["wind"], 0.0)
    return temperature + truth["temperature"], wind


def next_hour(rng: Generator, last: dict) -> dict:
    """
    Returns the true state of the hour after `last`.

    The temperature and wind deviations keep most of the previous hour's values. An hour
    is much more likely to be wet when the previous hour was wet.

    Args:
        rng (Generator): The random number generator.
        last (dict): The true state of the previous hour.

    Returns:
        dict: The new state, with the keys `temperature`, `wind`, `wet`, `rain` and
            `direction`.
    """
    wet = bool(rng.random() < (0.78 if last["wet"] else 0.06))
    return {
        "temperature": 0.96 * last["temperature"] + rng.normal(0, 0.8),
        "wind": 0.9 * last["wind"] + rng.normal(0, 1.8),
        "wet": wet,
        "rain": float(rng.exponential(0.6)) if wet else 0.0,
        "direction": (last["direction"] + rng.normal(0, 27)) % 360,
    }


def weather_forecast(
    rng: Generator, issued: datetime, lead: int, truth: dict
) -> WeatherForecast:
    """
    Returns the forecast issued at `issued` for the hour `lead` days later.

    Each forecast value is the true value plus a random error that grows with the lead.

    Args:
        rng (Generator): The random number generator.
        issued (datetime): The hour the forecast is issued.
        lead (int): How many days ahead the forecast is for.
        truth (dict): The true state of the hour being forecast.

    Returns:
        WeatherForecast: The forecast row.
    """
    hour = issued + timedelta(days=lead)
    temperature, wind = actual(hour, truth)
    return WeatherForecast(
        location_id=STATION,
        forecast_for=hour,
        issued_at=issued,
        lead_days=lead,
        temperature_2m=round(temperature + rng.normal(0, 1 + 0.2 * lead), 1),
        precipitation=round(truth["rain"] * math.exp(rng.normal(0, 0.3 + 0.1 * lead)), 1),
        wind_speed_10m=round(wind * math.exp(rng.normal(0, 0.1 + 0.03 * lead)), 1),
        wind_direction_10m=round(truth["direction"] + rng.normal(0, 35 + 5 * lead)) % 360,
    )  # fmt: skip


def observation(rng: Generator, hour: datetime, truth: dict) -> Observation:
    """
    Returns the PM2.5 reading for an hour.

    PM2.5 is higher on weekdays and in the evening, and lower when it is windy or wet. A
    model on the weather forecast beats predicting yesterday's value, because the
    weather changes from day to day and the forecast sees it. The weekday effect does
    not show in the weather, and yesterday's value gets it wrong on Saturdays and
    Mondays. So a model that also knows the weekend beats both.

    Args:
        rng (Generator): The random number generator.
        hour (datetime): The hour measured.
        truth (dict): The true state of the hour.

    Returns:
        Observation: The reading.
    """
    _, wind = actual(hour, truth)
    log_pm = (2.05 + 0.4 * (hour.weekday() < 5) + 0.12 * wave(hour, 24, 17) - 0.1 * (wind - 10)
              - 0.4 * truth["wet"] + rng.normal(0, 0.1))  # fmt: skip
    pm = math.expm1(log_pm)
    return Observation(
        location_id=STATION,
        measured_at=hour,
        pm2_5=round(max(pm, 0.0), 2),
    )


def generate(origin: datetime, seed: int) -> Iterator[list[BaseModel]]:
    """
    Yields the records of every hour from `origin` onwards, without end.

    Args:
        origin (datetime): The first hour.
        seed (int): The seed for the generated values.

    Yields:
        list[BaseModel]: One hour's records: the seven forecasts issued in that hour,
            one per lead, then that hour's reading.
    """
    rng = np.random.default_rng(seed)
    hours = [
        _CALM
    ]  # true state per hour since origin, a week ahead of the current hour
    k = 0
    while True:
        now = origin + timedelta(hours=k)
        while len(hours) <= k + 24 * len(LEADS):
            hours.append(next_hour(rng, hours[-1]))
        forecasts = [
            weather_forecast(rng, now, lead, hours[k + 24 * lead]) for lead in LEADS
        ]
        yield [*forecasts, observation(rng, now, hours[k])]
        k += 1

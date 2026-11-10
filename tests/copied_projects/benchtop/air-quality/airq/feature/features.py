"""Turns the hourly rows into the daily rows that Feast reads.

A DailyWeather row averages the 24 hourly forecasts for one day, issued `lead_days`
before it. A DailyAirQuality row averages the day's 24 PM2.5 readings, and adds the
weekend flag and the previous day's mean.

The backfill and the daily run both call `daily_features` and `in_window`, so they write
the same rows for the same day.
"""

from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
from pydantic import BaseModel

from airq.core.config import STATION
from airq.core.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast

# The column that places each row on a day, for choosing and replacing a day's rows.
DAY_COLUMN = {
    WeatherForecast: "issued_at",
    Observation: "measured_at",
    DailyWeather: "issued_on",
    DailyAirQuality: "day",
}


def in_window(row: BaseModel, start: datetime, end: datetime) -> bool:
    """
    Returns whether a row falls between `start` and `end`.

    The row's day column, from `DAY_COLUMN`, decides. Timestamps are compared as they
    are, and dates are compared with the dates of `start` and `end`.

    Args:
        row (BaseModel): A row of any of the four tables.
        start (datetime): The start of the window, included.
        end (datetime): The end of the window, excluded.

    Returns:
        bool: Whether the row falls in the window.
    """
    value = getattr(row, DAY_COLUMN[type(row)])
    if isinstance(value, datetime):
        return start <= value < end
    return start.date() <= value < end.date()


def daily_features(
    forecasts: list[WeatherForecast], observations: list[Observation]
) -> tuple[list[DailyWeather], list[DailyAirQuality]]:
    """
    Returns the daily weather and air quality rows computed from hourly rows.

    A day and lead get a weather row only when all 24 hourly forecasts are present. A
    day gets an air quality row only when it and the day before both have all 24
    readings, because the row holds the previous day's mean.

    Args:
        forecasts (list[WeatherForecast]): The hourly weather forecasts.
        observations (list[Observation]): The hourly PM2.5 readings.

    Returns:
        tuple[list[DailyWeather], list[DailyAirQuality]]: The daily weather rows, then
            the daily air quality rows, both sorted by day.
    """
    weather = defaultdict(list)
    for row in forecasts:
        weather[row.forecast_for.date(), row.lead_days].append(row)
    daily_weather = [
        DailyWeather(
            location_id=STATION,
            day=day,
            lead_days=lead,
            issued_on=day - timedelta(days=lead),
            temperature_2m=round(float(np.mean([r.temperature_2m for r in rows])), 2),
            wind_speed_10m=round(float(np.mean([r.wind_speed_10m for r in rows])), 2),
            wet_hours=sum(r.precipitation > 0 for r in rows),
        )
        for (day, lead), rows in sorted(weather.items())
        if len(rows) == 24
    ]
    pm = defaultdict(list)
    for row in observations:
        pm[row.measured_at.date()].append(row.pm2_5)
    mean = {day: float(np.mean(v)) for day, v in pm.items() if len(v) == 24}
    daily_air_quality = [
        DailyAirQuality(
            location_id=STATION,
            day=day,
            pm2_5=round(mean[day], 2),
            is_weekend=day.weekday() >= 5,
            pm2_5_lag1=round(mean[day - timedelta(days=1)], 2),
        )
        for day in sorted(mean)
        if day - timedelta(days=1) in mean
    ]
    return daily_weather, daily_air_quality

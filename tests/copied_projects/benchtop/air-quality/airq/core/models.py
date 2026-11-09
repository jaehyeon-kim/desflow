"""Pydantic models for the rows of the Iceberg tables, one model per table.

The Iceberg schemas are derived from these models, and the generator and the feature
code build rows with them. The bounds on the fields check every row when it is built,
before anything is written.
"""

from datetime import date

from pydantic import AwareDatetime, BaseModel, Field

_PM2_5 = Field(ge=0, le=500)  # µg/m³
_LEAD = Field(ge=1, le=7)  # days


class Observation(BaseModel):
    """
    One hour of measured PM2.5 at one station.

    Attributes:
        location_id (str): The station.
        measured_at (AwareDatetime): The start of the hour measured.
        pm2_5 (float): The measured PM2.5 in µg/m³, from 0 to 500.
    """

    location_id: str
    measured_at: AwareDatetime
    pm2_5: float = _PM2_5


class WeatherForecast(BaseModel):
    """
    The weather forecast for one hour, issued `lead_days` before that hour.

    Attributes:
        location_id (str): The station.
        forecast_for (AwareDatetime): The hour being forecast.
        issued_at (AwareDatetime): When the forecast was made.
        lead_days (int): How many days before `forecast_for` the forecast was made, from
            1 to 7.
        temperature_2m (float): The air temperature 2 metres above the ground in °C,
            from -60 to 60.
        precipitation (float): The rain in the hour in mm, from 0 to 500.
        wind_speed_10m (float): The wind speed 10 metres above the ground in km/h, from
            0 to 400.
        wind_direction_10m (float): The wind direction in degrees, from 0 up to but not
            including 360.
    """

    location_id: str
    forecast_for: AwareDatetime
    issued_at: AwareDatetime
    lead_days: int = _LEAD
    temperature_2m: float = Field(ge=-60, le=60)  # °C
    precipitation: float = Field(ge=0, le=500)  # mm in an hour
    wind_speed_10m: float = Field(ge=0, le=400)  # km/h
    wind_direction_10m: float = Field(ge=0, lt=360)  # degrees


class DailyWeather(BaseModel):
    """
    The forecast weather for one day, issued `lead_days` before it.

    Attributes:
        location_id (str): The station.
        day (date): The day forecast.
        lead_days (int): How many days before `day` the forecast was made, from 1 to 7.
        issued_on (date): The day the forecast was made.
        temperature_2m (float): The mean of the day's 24 hourly temperatures in °C, from
            -60 to 60.
        wind_speed_10m (float): The mean of the day's 24 hourly wind speeds in km/h,
            from 0 to 400.
        wet_hours (int): The number of hours with any rain, from 0 to 24.
    """

    location_id: str
    day: date
    lead_days: int = _LEAD
    issued_on: date
    temperature_2m: float = Field(ge=-60, le=60)
    wind_speed_10m: float = Field(ge=0, le=400)
    wet_hours: int = Field(ge=0, le=24)


class DailyAirQuality(BaseModel):
    """
    The measured PM2.5 for one day, with the features known from the date and the day
    before.

    Attributes:
        location_id (str): The station.
        day (date): The day measured.
        pm2_5 (float): The mean of the day's 24 hourly readings in µg/m³, from 0 to 500.
            This is the target the models predict.
        is_weekend (bool): Whether the day is a Saturday or Sunday.
        pm2_5_lag1 (float): The mean PM2.5 of the day before in µg/m³, from 0 to 500.
    """

    location_id: str
    day: date
    pm2_5: float = _PM2_5
    is_weekend: bool
    pm2_5_lag1: float = _PM2_5


class Prediction(BaseModel):
    """
    The predicted PM2.5 for one day, from one model version.

    Attributes:
        location_id (str): The station.
        as_of (date): The date the inference run treated as today.
        day (date): The day predicted.
        lead_days (int): How many days after `as_of` the day is, from 1 to 7.
        pm2_5 (float): The predicted daily mean PM2.5 in µg/m³.
        model_version (str): The MLflow model version that made the prediction.
    """

    location_id: str
    as_of: date
    day: date
    lead_days: int = _LEAD
    pm2_5: float
    model_version: str

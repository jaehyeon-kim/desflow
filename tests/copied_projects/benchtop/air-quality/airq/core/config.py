"""Settings shared by the pipelines, the app and the tests.

On the host, importing this module sets environment variables that point the code at the
odctl services on 127.0.0.1. It avoids localhost, because with IPv6 enabled in Docker
some services reset connections to the IPv6 address of localhost. Inside a container it
changes nothing, because odctl already sets the container addresses there.
"""

import os

from pydantic import BaseModel

from airq.core.models import DailyAirQuality, DailyWeather, Observation, WeatherForecast

if not os.path.exists("/.dockerenv"):  # Docker creates this file in every container
    os.environ.update(
        {
            "PYICEBERG_CATALOG__ODCTL__TYPE": "rest",
            "PYICEBERG_CATALOG__ODCTL__URI": "http://127.0.0.1:8181",
            "PYICEBERG_CATALOG__ODCTL__WAREHOUSE": "s3://warehouse/",
            "PYICEBERG_CATALOG__ODCTL__S3__ENDPOINT": "http://127.0.0.1:8333",
            "PYICEBERG_CATALOG__ODCTL__S3__ACCESS_KEY_ID": "user",
            "PYICEBERG_CATALOG__ODCTL__S3__SECRET_ACCESS_KEY": "password",
            "PYICEBERG_CATALOG__ODCTL__S3__REGION": "us-east-1",
            "PYICEBERG_CATALOG__ODCTL__S3__PATH_STYLE_ACCESS": "true",
            "MLFLOW_TRACKING_URI": "http://127.0.0.1:5004",
            # The server hands out download links to http://seaweed:8333, which
            # the host cannot resolve; this sends model downloads through it.
            "MLFLOW_ENABLE_PROXY_MULTIPART_DOWNLOAD": "false",
            "FEAST_REGISTRY": "postgresql+psycopg://user:password@127.0.0.1:5432/feast",
        }
    )

CATALOG = "odctl"  # the PyIceberg catalog name the variables above configure
DEFAULT_SEED = 42
STATION = "station-1"
LEADS = range(1, 8)  # forecast lead times, in days

NAMESPACE = "airq"
# The feature pipeline's tables; the backfill recreates exactly these.
TABLES: dict[type[BaseModel], str] = {
    Observation: f"{NAMESPACE}.observations",
    WeatherForecast: f"{NAMESPACE}.weather_forecasts",
    DailyWeather: f"{NAMESPACE}.daily_weather",
    DailyAirQuality: f"{NAMESPACE}.daily_air_quality",
}
PREDICTIONS = f"{NAMESPACE}.predictions"  # written by the inference pipeline
# Table properties the backfill writes and the daily run reads, so the daily run
# generates from the same first hour with the same seed as the backfill did.
ORIGIN_PROPERTY = "airq.origin"
SEED_PROPERTY = "airq.seed"

FEAST_PROJECT = "airq"
EXPERIMENT = "airq"  # MLflow experiment of the sweep and training runs
MODEL_NAME = "airq_pm25"  # registered model in MLflow
CHAMPION = "champion"  # alias of the version inference serves
CHALLENGER = "challenger"  # alias of the version inference predicts beside it
# Airflow asset the daily run updates and the inference DAG is scheduled on.
DAILY_FEATURES_ASSET = "airq_daily_features"
# Airflow asset training updates; the inference DAG is scheduled on it too.
MODELS_ASSET = "airq_models"
# The DAGs in dags/, by id; the clean-up deletes their records in Airflow.
DAG_IDS = ("airq_features", "airq_training", "airq_inference")
AIRFLOW_URL = "http://127.0.0.1:8085"  # the Airflow API, from the host
AIRFLOW_USER, AIRFLOW_PASSWORD = "user", "password"

# The forecast assistant's model: an Ollama model that supports tool calls,
# pulled into the local Ollama server. Nothing is sent to a hosted model. The
# instruct variant answers straight away; plain qwen3:4b reasons first and is
# several times slower.
ASSISTANT_MODEL = os.environ.get("AIRQ_MODEL", "qwen3:4b-instruct")
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://127.0.0.1:11434")
APP_PORT = 8090  # the NiceGUI app, airq.app

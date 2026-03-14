"""
Centralized configuration for the NYC Taxi & Weather Pipeline.

Reads values from environment variables (set via .env file or Airflow Variables).
"""

import os
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# NYC TLC Taxi Data (Bulk Parquet Downloads)
# =============================================================================
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
TAXI_YEAR = int(os.getenv("TAXI_YEAR", "2023"))

# =============================================================================
# NOAA CDO API (Weather Data)
# =============================================================================
NOAA_API_TOKEN = os.getenv("NOAA_API_TOKEN", "")
NOAA_STATION_ID = os.getenv("NOAA_STATION_ID", "GHCND:USW00094728")
NOAA_DATASET_ID = os.getenv("NOAA_DATASET_ID", "GHCND")
NOAA_BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2"

# =============================================================================
# Google Cloud Storage
# =============================================================================
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "nyc-taxi-weather-pipeline")
GCS_RAW_TAXI_PREFIX = os.getenv("GCS_RAW_TAXI_PREFIX", "raw-taxi/")
GCS_RAW_WEATHER_PREFIX = os.getenv("GCS_RAW_WEATHER_PREFIX", "raw-weather/")

# =============================================================================
# MongoDB Atlas
# =============================================================================
MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "nyc_taxi_weather")

# How many taxi records to sample for MongoDB (free tier = 512 MB)
MONGO_SAMPLE_SIZE = int(os.getenv("MONGO_SAMPLE_SIZE", "700000"))

# Collection names
TAXI_COLLECTION = "taxi_trips"
WEATHER_COLLECTION = "weather_daily"
TIP_WEATHER_AGG_COLLECTION = "tip_by_weather"
HOURLY_STATS_COLLECTION = "hourly_trip_stats"

# =============================================================================
# Data Parameters
# =============================================================================
DATA_START_DATE = os.getenv("DATA_START_DATE", "2023-01-01")
DATA_END_DATE = os.getenv("DATA_END_DATE", "2023-12-31")

# Weather data types to fetch from NOAA
WEATHER_DATATYPES = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]

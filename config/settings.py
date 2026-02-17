"""
Centralized configuration for the NYC Taxi & Weather Pipeline.

Reads values from environment variables (set via .env file or Airflow Variables).
"""

import os
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Socrata API (NYC Open Data - Taxi Trips)
# =============================================================================
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")
SOCRATA_DOMAIN = os.getenv("SOCRATA_DOMAIN", "data.cityofnewyork.us")
TAXI_DATASET_ID = os.getenv("TAXI_DATASET_ID", "m6nq-qud6")

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

# Collection names
TAXI_COLLECTION = "taxi_trips"
WEATHER_COLLECTION = "weather_daily"
ENRICHED_COLLECTION = "enriched_trips"
TIP_WEATHER_AGG_COLLECTION = "tip_by_weather"
HOURLY_STATS_COLLECTION = "hourly_trip_stats"

# =============================================================================
# Data Parameters
# =============================================================================
DATA_START_DATE = os.getenv("DATA_START_DATE", "2024-01-01")
DATA_END_DATE = os.getenv("DATA_END_DATE", "2024-12-31")
TAXI_FETCH_LIMIT = int(os.getenv("TAXI_FETCH_LIMIT", "50000"))

# Weather data types to fetch from NOAA
WEATHER_DATATYPES = ["PRCP", "SNOW", "SNWD", "TMAX", "TMIN"]

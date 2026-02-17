"""
Fetch daily weather observations from the NOAA Climate Data Online (CDO) API.

Uses the NOAA CDO Web Services v2 API to pull daily weather data for
Central Park station (USW00094728).
"""

import logging
import time
from typing import Optional

import pandas as pd
import requests

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import (
    NOAA_API_TOKEN,
    NOAA_BASE_URL,
    NOAA_DATASET_ID,
    NOAA_STATION_ID,
    WEATHER_DATATYPES,
)

logger = logging.getLogger(__name__)


def fetch_weather_data(
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Fetch daily weather data from NOAA CDO for Central Park station.

    The API returns at most 1000 records per request, so we paginate
    and handle rate-limiting (5 requests/second cap).

    Args:
        start_date: YYYY-MM-DD inclusive.
        end_date:   YYYY-MM-DD inclusive.

    Returns:
        pandas DataFrame pivoted to one row per date with columns:
        DATE, PRCP, SNOW, SNWD, TMAX, TMIN.
    """
    logger.info("Fetching weather data from %s to %s", start_date, end_date)

    headers = {"token": NOAA_API_TOKEN}
    all_results = []
    offset = 1  # NOAA API uses 1-based offset

    while True:
        params = {
            "datasetid": NOAA_DATASET_ID,
            "stationid": NOAA_STATION_ID,
            "startdate": start_date,
            "enddate": end_date,
            "datatypeid": ",".join(WEATHER_DATATYPES),
            "units": "standard",
            "limit": 1000,
            "offset": offset,
        }

        logger.info("  NOAA request offset=%d", offset)
        resp = requests.get(
            f"{NOAA_BASE_URL}/data",
            headers=headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            break

        all_results.extend(results)
        metadata = data.get("metadata", {}).get("resultset", {})
        total_count = metadata.get("count", 0)

        offset += len(results)
        if offset > total_count:
            break

        # Respect rate limit
        time.sleep(0.25)

    logger.info("Fetched %d raw weather observations.", len(all_results))

    if not all_results:
        return pd.DataFrame(columns=["DATE", "PRCP", "SNOW", "SNWD", "TMAX", "TMIN"])

    df = pd.DataFrame(all_results)
    df = _pivot_weather(df)
    return df


def _pivot_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the long-form NOAA response into a wide table:
    one row per date, one column per data type.
    """
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    pivot = df.pivot_table(
        index="date",
        columns="datatype",
        values="value",
        aggfunc="first",
    ).reset_index()

    pivot.columns.name = None
    pivot = pivot.rename(columns={"date": "DATE"})

    for col in WEATHER_DATATYPES:
        if col not in pivot.columns:
            pivot[col] = None

    return pivot


def add_weather_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a human-readable 'weather_condition' column based on
    precipitation, snowfall, and temperature thresholds.
    """
    def _categorize(row):
        snow = row.get("SNOW", 0) or 0
        prcp = row.get("PRCP", 0) or 0
        tmax = row.get("TMAX", None)

        if snow > 0:
            return "Snow"
        if prcp > 0.5:
            return "Heavy Rain"
        if prcp > 0:
            return "Light Rain"
        if tmax is not None and tmax >= 90:
            return "Extreme Heat"
        if tmax is not None and tmax <= 32:
            return "Freezing"
        return "Clear/Normal"

    df["weather_condition"] = df.apply(_categorize, axis=1)
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = fetch_weather_data("2024-01-01", "2024-01-31")
    df = add_weather_category(df)
    print(df.head(10))
    print(f"Shape: {df.shape}")

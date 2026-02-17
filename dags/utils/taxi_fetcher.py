"""
Fetch NYC Yellow Taxi trip data from the Socrata Open Data API.

Uses the sodapy library to paginate through the NYC TLC dataset.
The data is returned as a list of dicts (JSON records).
"""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd
from sodapy import Socrata

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import (
    SOCRATA_APP_TOKEN,
    SOCRATA_DOMAIN,
    TAXI_DATASET_ID,
    TAXI_FETCH_LIMIT,
)

logger = logging.getLogger(__name__)


def fetch_taxi_data(
    start_date: str,
    end_date: str,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetch Yellow Taxi trip records between start_date and end_date.

    Args:
        start_date: Inclusive start date (YYYY-MM-DD).
        end_date:   Inclusive end date   (YYYY-MM-DD).
        limit:      Max rows to fetch.  None → TAXI_FETCH_LIMIT from config.

    Returns:
        pandas DataFrame with taxi trip records.
    """
    limit = limit or TAXI_FETCH_LIMIT
    logger.info(
        "Fetching up to %d taxi records from %s to %s",
        limit, start_date, end_date,
    )

    client = Socrata(
        SOCRATA_DOMAIN,
        SOCRATA_APP_TOKEN,
        timeout=60,
    )

    where_clause = (
        f"tpep_pickup_datetime >= '{start_date}T00:00:00' "
        f"AND tpep_pickup_datetime < '{end_date}T23:59:59'"
    )

    select_fields = (
        "tpep_pickup_datetime, tpep_dropoff_datetime, "
        "passenger_count, trip_distance, "
        "pulocationid, dolocationid, "
        "payment_type, fare_amount, tip_amount, total_amount"
    )

    all_records = []
    offset = 0
    batch_size = min(limit, 50000)

    while offset < limit:
        fetch_count = min(batch_size, limit - offset)
        logger.info("  Fetching batch at offset=%d, count=%d", offset, fetch_count)

        results = client.get(
            TAXI_DATASET_ID,
            where=where_clause,
            select=select_fields,
            limit=fetch_count,
            offset=offset,
            order="tpep_pickup_datetime ASC",
        )

        if not results:
            logger.info("  No more records returned. Done.")
            break

        all_records.extend(results)
        offset += len(results)

        if len(results) < fetch_count:
            logger.info("  Received fewer rows than requested. Done.")
            break

    client.close()
    logger.info("Fetched %d total taxi records.", len(all_records))

    df = pd.DataFrame(all_records)
    if not df.empty:
        df = _clean_taxi_df(df)

    return df


def _clean_taxi_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply basic type conversions and add a date column for joins."""
    datetime_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    numeric_cols = [
        "passenger_count", "trip_distance",
        "fare_amount", "tip_amount", "total_amount",
        "payment_type", "pulocationid", "dolocationid",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "tpep_pickup_datetime" in df.columns:
        df["pickup_date"] = df["tpep_pickup_datetime"].dt.strftime("%Y-%m-%d")
        df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    df = fetch_taxi_data("2024-01-01", "2024-01-07", limit=1000)
    print(df.head())
    print(f"Shape: {df.shape}")

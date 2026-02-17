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
    Fetch Yellow Taxi trip records spread across the full date range.

    Samples evenly across each month to ensure weather diversity
    in the joined dataset.

    Args:
        start_date: Inclusive start date (YYYY-MM-DD).
        end_date:   Inclusive end date   (YYYY-MM-DD).
        limit:      Max total rows to fetch.  None → TAXI_FETCH_LIMIT from config.

    Returns:
        pandas DataFrame with taxi trip records.
    """
    limit = limit or TAXI_FETCH_LIMIT

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Build monthly date ranges for even sampling across the year
    months = []
    current = start_dt.replace(day=1)
    while current < end_dt:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        month_end = min(next_month, end_dt)
        months.append((current, month_end))
        current = next_month

    per_month = max(limit // len(months), 500)
    logger.info(
        "Fetching ~%d taxi records/month across %d months (%s to %s), total target: %d",
        per_month, len(months), start_date, end_date, limit,
    )

    client = Socrata(
        SOCRATA_DOMAIN,
        SOCRATA_APP_TOKEN,
        timeout=120,
    )

    select_fields = (
        "tpep_pickup_datetime, tpep_dropoff_datetime, "
        "passenger_count, trip_distance, "
        "pulocationid, dolocationid, "
        "payment_type, fare_amount, tip_amount, total_amount"
    )

    all_records = []

    for month_start, month_end in months:
        ms = month_start.strftime("%Y-%m-%dT00:00:00")
        me = month_end.strftime("%Y-%m-%dT00:00:00")

        where_clause = (
            f"tpep_pickup_datetime >= '{ms}' "
            f"AND tpep_pickup_datetime < '{me}'"
        )

        logger.info("  Fetching %d records for %s ...", per_month, month_start.strftime("%Y-%m"))

        try:
            results = client.get(
                TAXI_DATASET_ID,
                where=where_clause,
                select=select_fields,
                limit=per_month,
            )
            if results:
                all_records.extend(results)
                logger.info("    Got %d records", len(results))
            else:
                logger.warning("    No records returned for %s", month_start.strftime("%Y-%m"))
        except Exception as e:
            logger.error("    Error fetching %s: %s", month_start.strftime("%Y-%m"), e)

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

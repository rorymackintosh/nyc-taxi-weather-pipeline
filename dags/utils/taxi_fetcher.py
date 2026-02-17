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

    Samples from 4 different days per month (1st, 8th, 15th, 22nd) to
    ensure coverage of all hours, diverse weather conditions, and
    weekday/weekend representation.

    Args:
        start_date: Inclusive start date (YYYY-MM-DD).
        end_date:   Inclusive end date   (YYYY-MM-DD).
        limit:      Max total rows to fetch.  None -> TAXI_FETCH_LIMIT from config.

    Returns:
        pandas DataFrame with taxi trip records.
    """
    from datetime import timedelta

    limit = limit or TAXI_FETCH_LIMIT
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Build sample days: 4 days per month (1st, 8th, 15th, 22nd)
    sample_days = [1, 8, 15, 22]
    date_ranges = []
    current = start_dt.replace(day=1)
    while current < end_dt:
        for day in sample_days:
            try:
                day_start = current.replace(day=day)
            except ValueError:
                continue
            if day_start < start_dt or day_start > end_dt:
                continue
            day_end = day_start + timedelta(days=1)
            date_ranges.append((day_start, day_end))

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1, day=1)
        else:
            current = current.replace(month=current.month + 1, day=1)

    per_day = max(limit // len(date_ranges), 200)
    logger.info(
        "Fetching ~%d records/day across %d sample days (%s to %s), total target: %d",
        per_day, len(date_ranges), start_date, end_date, limit,
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

    # For each sample day, fetch from 6 different offsets to cover all hours.
    # NYC has ~300K-400K taxi trips/day. Offsets spaced ~50K apart jump
    # roughly 3-4 hours through the day, giving us coverage of 0-23h.
    offsets = [0, 50000, 100000, 150000, 200000, 250000]
    per_offset = max(per_day // len(offsets), 50)

    for day_start, day_end in date_ranges:
        ds = day_start.strftime("%Y-%m-%dT00:00:00")
        de = day_end.strftime("%Y-%m-%dT00:00:00")
        day_str = day_start.strftime("%Y-%m-%d")
        day_total = 0

        where_clause = (
            f"tpep_pickup_datetime >= '{ds}' "
            f"AND tpep_pickup_datetime < '{de}'"
        )

        for ofs in offsets:
            try:
                results = client.get(
                    TAXI_DATASET_ID,
                    where=where_clause,
                    select=select_fields,
                    limit=per_offset,
                    offset=ofs,
                    order="tpep_pickup_datetime ASC",
                )
                if results:
                    all_records.extend(results)
                    day_total += len(results)
            except Exception as e:
                logger.error("    Error fetching %s offset %d: %s", day_str, ofs, e)

        logger.info("  %s: %d records (6 offsets across the day)", day_str, day_total)

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

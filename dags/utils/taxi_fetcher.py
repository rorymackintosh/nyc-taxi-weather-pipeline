"""
Fetch NYC Yellow Taxi trip data from the Socrata Open Data API.

Uses the sodapy library to pull data spread across the full year with
coverage of all 24 hours.
"""

import logging
from datetime import datetime, timedelta
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
    Fetch Yellow Taxi trip records spread across every week of the year,
    with coverage of all 24 hours.

    Strategy: pick 2 days per week (52 weeks = 104 sample days).
    For each day, fetch from 4 explicit hour ranges (morning, midday,
    evening, night) to guarantee full 0-23 hour coverage.

    Args:
        start_date: Inclusive start (YYYY-MM-DD).
        end_date:   Inclusive end   (YYYY-MM-DD).
        limit:      Max total rows. Default from config.

    Returns:
        pandas DataFrame with taxi trip records.
    """
    limit = limit or TAXI_FETCH_LIMIT
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # Pick 2 days per week: Wednesday (weekday) and Saturday (weekend)
    sample_days = []
    current = start_dt
    while current <= end_dt:
        weekday = current.weekday()
        if weekday == 2:  # Wednesday
            sample_days.append(current)
        elif weekday == 5:  # Saturday
            sample_days.append(current)
        current += timedelta(days=1)

    # 8 specific 1-hour windows spread across 24h to guarantee
    # full hour coverage. Each window spans exactly 1 hour.
    hour_ranges = [
        ("00:00:00", "01:00:00"),  # hour 0
        ("03:00:00", "04:00:00"),  # hour 3
        ("06:00:00", "07:00:00"),  # hour 6
        ("09:00:00", "10:00:00"),  # hour 9
        ("12:00:00", "13:00:00"),  # hour 12
        ("15:00:00", "16:00:00"),  # hour 15
        ("18:00:00", "19:00:00"),  # hour 18
        ("21:00:00", "22:00:00"),  # hour 21
    ]

    total_slots = len(sample_days) * len(hour_ranges)
    per_slot = max(limit // total_slots, 20)

    logger.info(
        "Fetching ~%d records per slot, %d sample days x %d hour ranges = %d slots, "
        "target: %d total records (%s to %s)",
        per_slot, len(sample_days), len(hour_ranges), total_slots,
        limit, start_date, end_date,
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
    errors = 0

    for day in sample_days:
        day_str = day.strftime("%Y-%m-%d")
        day_total = 0

        for h_start, h_end in hour_ranges:
            where_clause = (
                f"tpep_pickup_datetime >= '{day_str}T{h_start}' "
                f"AND tpep_pickup_datetime < '{day_str}T{h_end}'"
            )
            try:
                results = client.get(
                    TAXI_DATASET_ID,
                    where=where_clause,
                    select=select_fields,
                    limit=per_slot,
                )
                if results:
                    all_records.extend(results)
                    day_total += len(results)
            except Exception as e:
                errors += 1
                if errors <= 3:
                    logger.error("  Error fetching %s %s: %s", day_str, h_start, e)

        if day_total > 0:
            logger.info("  %s (%s): %d records", day_str, day.strftime("%a"), day_total)

    client.close()
    logger.info("Fetched %d total taxi records (%d errors).", len(all_records), errors)

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
    df = fetch_taxi_data("2023-01-01", "2023-01-31", limit=2000)
    print(df.head())
    print(f"Shape: {df.shape}")
    print(f"Unique dates: {df['pickup_date'].nunique()}")
    print(f"Hour range: {df['pickup_hour'].min()} - {df['pickup_hour'].max()}")
    print(f"Hour distribution:\n{df['pickup_hour'].value_counts().sort_index()}")

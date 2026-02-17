"""
Download NYC Yellow Taxi trip data from the TLC bulk Parquet files.

Downloads monthly Parquet files from the TLC CDN, then samples a
representative subset for MongoDB loading (to fit the free tier).
Full Parquet files are preserved for GCS upload and Spark processing.
"""

import logging
import os
from typing import Optional

import pandas as pd
import requests

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import TLC_BASE_URL, TAXI_YEAR, MONGO_SAMPLE_SIZE

logger = logging.getLogger(__name__)

# Columns to keep (matches our pipeline's expected schema)
KEEP_COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance",
    "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "tip_amount", "total_amount",
]

# Rename columns to match our lowercase convention
RENAME_MAP = {
    "PULocationID": "pulocationid",
    "DOLocationID": "dolocationid",
}


def download_taxi_parquets(
    year: int = None,
    data_dir: str = None,
) -> list[str]:
    """
    Download 12 monthly Yellow Taxi Parquet files from the TLC CDN.

    Args:
        year:     Year to download (default from config).
        data_dir: Local directory to save files (default: ./data/).

    Returns:
        List of local file paths to the downloaded Parquet files.
    """
    year = year or TAXI_YEAR
    if data_dir is None:
        data_dir = os.path.join(os.path.dirname(__file__), "..", "..", "data")
    os.makedirs(data_dir, exist_ok=True)

    paths = []
    for month in range(1, 13):
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"{TLC_BASE_URL}{filename}"
        local_path = os.path.join(data_dir, filename)

        if os.path.exists(local_path):
            size_mb = os.path.getsize(local_path) / (1024 * 1024)
            logger.info("  %s already exists (%.1f MB), skipping.", filename, size_mb)
            paths.append(local_path)
            continue

        logger.info("  Downloading %s ...", filename)
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        logger.info("    Saved %s (%.1f MB)", filename, size_mb)
        paths.append(local_path)

    logger.info("Downloaded %d Parquet files to %s", len(paths), data_dir)
    return paths


def sample_for_mongodb(
    parquet_paths: list[str],
    target_rows: Optional[int] = None,
) -> pd.DataFrame:
    """
    Read each monthly Parquet file, take a random sample, and
    concatenate into a single clean DataFrame for MongoDB loading.

    Args:
        parquet_paths: List of paths to monthly Parquet files.
        target_rows:   Total target rows (default from config).

    Returns:
        Cleaned pandas DataFrame ready for MongoDB insertion.
    """
    target_rows = target_rows or MONGO_SAMPLE_SIZE
    per_month = target_rows // len(parquet_paths)

    logger.info(
        "Sampling %d records/month from %d files (target: %d total)",
        per_month, len(parquet_paths), target_rows,
    )

    frames = []
    for path in parquet_paths:
        filename = os.path.basename(path)
        df = pd.read_parquet(path, columns=KEEP_COLUMNS)
        total = len(df)

        if total <= per_month:
            sample = df
        else:
            sample = df.sample(n=per_month, random_state=42)

        logger.info("  %s: %d total rows, sampled %d", filename, total, len(sample))
        frames.append(sample)

    combined = pd.concat(frames, ignore_index=True)
    combined = _clean_taxi_df(combined)

    logger.info("Combined sample: %d rows, %d columns", len(combined), len(combined.columns))
    return combined


def _clean_taxi_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply column renames, type conversions, and add date/hour fields."""
    df = df.rename(columns=RENAME_MAP)

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
    paths = download_taxi_parquets(2023)
    df = sample_for_mongodb(paths, target_rows=10000)
    print(f"\nShape: {df.shape}")
    print(f"Unique dates: {df['pickup_date'].nunique()}")
    print(f"Hour range: {df['pickup_hour'].min()} - {df['pickup_hour'].max()}")
    print(f"Columns: {list(df.columns)}")
    print(df.head())

"""
Full pipeline runner for Phase 2.

Executes all steps in sequence:
  1. Download 12 monthly Parquet files from TLC CDN
  2. Fetch weather data from NOAA API
  3. Upload Parquet files + weather CSV to GCS
  4. Sample taxi data and load into MongoDB; load weather into MongoDB
  5. Run aggregation pipelines (enriched_trips, tip_by_weather, hourly_stats)
  6. Drop enriched_trips to free MongoDB storage, verify collections

Usage:
    python scripts/run_full_pipeline.py
"""

import logging
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from config.settings import (
    DATA_START_DATE, DATA_END_DATE, TAXI_YEAR, MONGO_SAMPLE_SIZE,
    GCS_RAW_TAXI_PREFIX, GCS_RAW_WEATHER_PREFIX,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pipeline")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def main():
    start_time = time.time()

    # =========================================================================
    # STEP 1: Download TLC Parquet files
    # =========================================================================
    logger.info("=" * 60)
    logger.info("STEP 1: Downloading NYC Yellow Taxi Parquet files from TLC")
    logger.info("=" * 60)

    from dags.utils.taxi_downloader import download_taxi_parquets
    parquet_paths = download_taxi_parquets(year=TAXI_YEAR, data_dir=DATA_DIR)
    total_size = sum(os.path.getsize(p) for p in parquet_paths) / (1024 * 1024)
    logger.info("Total Parquet data: %.1f MB across %d files", total_size, len(parquet_paths))

    # =========================================================================
    # STEP 2: Fetch weather data
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Fetching NOAA weather data for Central Park")
    logger.info("=" * 60)

    from dags.utils.weather_fetcher import fetch_weather_data, add_weather_category
    weather_df = fetch_weather_data(DATA_START_DATE, DATA_END_DATE)
    weather_df = add_weather_category(weather_df)
    logger.info("Weather data: %d days, columns: %s", len(weather_df), list(weather_df.columns))

    weather_csv = os.path.join(DATA_DIR, "weather_daily.csv")
    os.makedirs(DATA_DIR, exist_ok=True)
    weather_df.to_csv(weather_csv, index=False)

    # =========================================================================
    # STEP 3: Upload to GCS
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Uploading data to Google Cloud Storage")
    logger.info("=" * 60)

    try:
        from dags.utils.gcs_helpers import upload_file_to_gcs, upload_df_to_gcs

        for path in parquet_paths:
            fname = os.path.basename(path)
            upload_file_to_gcs(path, f"{GCS_RAW_TAXI_PREFIX}{fname}")

        upload_df_to_gcs(weather_df, f"{GCS_RAW_WEATHER_PREFIX}weather_daily.csv", file_format="csv")
        logger.info("All data uploaded to GCS.")

    except Exception as e:
        logger.warning("GCS upload failed (non-fatal): %s", e)
        logger.warning("Continuing with MongoDB loading from local data...")

    # =========================================================================
    # STEP 4: Sample taxi data and load into MongoDB
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Loading data into MongoDB Atlas")
    logger.info("=" * 60)

    from dags.utils.taxi_downloader import sample_for_mongodb
    from dags.utils.mongo_helpers import load_taxi_data, load_weather_data

    taxi_df = sample_for_mongodb(parquet_paths, target_rows=MONGO_SAMPLE_SIZE)
    logger.info("Sampled %d taxi records for MongoDB", len(taxi_df))
    logger.info("  Unique dates: %d", taxi_df["pickup_date"].nunique())
    logger.info("  Hour range: %d - %d", taxi_df["pickup_hour"].min(), taxi_df["pickup_hour"].max())

    taxi_count = load_taxi_data(taxi_df)
    logger.info("Loaded %d taxi documents into MongoDB", taxi_count)

    weather_count = load_weather_data(weather_df)
    logger.info("Loaded %d weather documents into MongoDB", weather_count)

    # =========================================================================
    # STEP 5: Run aggregations
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: Running MongoDB aggregation pipelines")
    logger.info("=" * 60)

    from dags.utils.mongo_helpers import (
        create_enriched_trips,
        create_tip_by_weather_aggregate,
        create_hourly_trip_stats,
        get_database,
    )
    from config.settings import (
        TAXI_COLLECTION, WEATHER_COLLECTION, ENRICHED_COLLECTION,
        TIP_WEATHER_AGG_COLLECTION, HOURLY_STATS_COLLECTION,
    )

    logger.info("--- Aggregation 1: enriched_trips ($lookup join) ---")
    enriched_count = create_enriched_trips()
    logger.info("Result: %d enriched documents\n", enriched_count)

    logger.info("--- Aggregation 2: tip_by_weather (avg tip %% by condition) ---")
    tip_count = create_tip_by_weather_aggregate()
    logger.info("Result: %d weather condition groups\n", tip_count)

    logger.info("--- Aggregation 3: hourly_trip_stats ---")
    hourly_count = create_hourly_trip_stats()
    logger.info("Result: %d hourly stat documents\n", hourly_count)

    # =========================================================================
    # STEP 6: Drop enriched_trips to save MongoDB storage, then verify
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 6: Cleanup and verification")
    logger.info("=" * 60)

    db = get_database()

    # Drop enriched_trips to free ~200+ MB on MongoDB free tier.
    # The downstream aggregates (tip_by_weather, hourly_stats) are already
    # materialized and don't depend on enriched_trips existing.
    logger.info("Dropping enriched_trips to free MongoDB storage...")
    db[ENRICHED_COLLECTION].drop()
    logger.info("  enriched_trips dropped.")

    for name in [TAXI_COLLECTION, WEATHER_COLLECTION,
                 TIP_WEATHER_AGG_COLLECTION, HOURLY_STATS_COLLECTION]:
        count = db[name].count_documents({})
        logger.info("  %-25s %s documents", name, f"{count:,}")

    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE in %.1f seconds", elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

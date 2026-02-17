"""
Full pipeline runner for Phase 2.

Executes all steps in sequence:
  1. Fetch taxi data from Socrata API
  2. Fetch weather data from NOAA API
  3. Upload both to GCS
  4. Load both into MongoDB
  5. Run aggregation pipelines
  6. Run sample queries

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
    DATA_START_DATE, DATA_END_DATE, TAXI_FETCH_LIMIT,
    GCS_RAW_TAXI_PREFIX, GCS_RAW_WEATHER_PREFIX,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pipeline")


def main():
    start_time = time.time()

    # =========================================================================
    # STEP 1: Fetch taxi data
    # =========================================================================
    logger.info("=" * 60)
    logger.info("STEP 1: Fetching NYC Yellow Taxi data from Socrata API")
    logger.info("=" * 60)

    from dags.utils.taxi_fetcher import fetch_taxi_data
    taxi_df = fetch_taxi_data(DATA_START_DATE, DATA_END_DATE, TAXI_FETCH_LIMIT)
    logger.info("Taxi data shape: %s", taxi_df.shape)
    logger.info("Taxi data columns: %s", list(taxi_df.columns))
    logger.info("Sample:\n%s", taxi_df.head(3).to_string())

    # Save locally as backup
    taxi_csv = os.path.join(os.path.dirname(__file__), "..", "data", "taxi_trips.csv")
    os.makedirs(os.path.dirname(taxi_csv), exist_ok=True)
    taxi_df.to_csv(taxi_csv, index=False)
    logger.info("Saved taxi data locally to %s", taxi_csv)

    # =========================================================================
    # STEP 2: Fetch weather data
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Fetching NOAA weather data for Central Park")
    logger.info("=" * 60)

    from dags.utils.weather_fetcher import fetch_weather_data, add_weather_category
    weather_df = fetch_weather_data(DATA_START_DATE, DATA_END_DATE)
    weather_df = add_weather_category(weather_df)
    logger.info("Weather data shape: %s", weather_df.shape)
    logger.info("Weather data columns: %s", list(weather_df.columns))
    logger.info("Sample:\n%s", weather_df.head(5).to_string())

    weather_csv = os.path.join(os.path.dirname(__file__), "..", "data", "weather_daily.csv")
    weather_df.to_csv(weather_csv, index=False)
    logger.info("Saved weather data locally to %s", weather_csv)

    # =========================================================================
    # STEP 3: Upload to GCS
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Uploading data to Google Cloud Storage")
    logger.info("=" * 60)

    try:
        from dags.utils.gcs_helpers import upload_df_to_gcs

        taxi_uri = upload_df_to_gcs(
            taxi_df,
            f"{GCS_RAW_TAXI_PREFIX}taxi_trips.csv",
            file_format="csv",
        )
        logger.info("Taxi data uploaded to: %s", taxi_uri)

        weather_uri = upload_df_to_gcs(
            weather_df,
            f"{GCS_RAW_WEATHER_PREFIX}weather_daily.csv",
            file_format="csv",
        )
        logger.info("Weather data uploaded to: %s", weather_uri)

    except Exception as e:
        logger.warning("GCS upload failed (non-fatal): %s", e)
        logger.warning("Continuing with MongoDB loading from local data...")

    # =========================================================================
    # STEP 4: Load into MongoDB
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Loading data into MongoDB Atlas")
    logger.info("=" * 60)

    from dags.utils.mongo_helpers import load_taxi_data, load_weather_data

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
    # STEP 6: Verify
    # =========================================================================
    logger.info("\n" + "=" * 60)
    logger.info("STEP 6: Verifying all collections")
    logger.info("=" * 60)

    from dags.utils.mongo_helpers import get_database
    from config.settings import (
        TAXI_COLLECTION, WEATHER_COLLECTION, ENRICHED_COLLECTION,
        TIP_WEATHER_AGG_COLLECTION, HOURLY_STATS_COLLECTION,
    )

    db = get_database()
    for name in [TAXI_COLLECTION, WEATHER_COLLECTION, ENRICHED_COLLECTION,
                 TIP_WEATHER_AGG_COLLECTION, HOURLY_STATS_COLLECTION]:
        count = db[name].count_documents({})
        logger.info("  %-25s %s documents", name, f"{count:,}")

    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE in %.1f seconds", elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

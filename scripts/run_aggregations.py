"""
Standalone script to run MongoDB aggregation pipelines.

Use this to manually create the aggregated collections outside of Airflow.
Assumes that taxi_trips and weather_daily collections are already populated.

Usage:
    python scripts/run_aggregations.py
"""

import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dags.utils.mongo_helpers import (
    create_enriched_trips,
    create_tip_by_weather_aggregate,
    create_hourly_trip_stats,
    get_database,
)
from config.settings import TAXI_COLLECTION, WEATHER_COLLECTION

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    db = get_database()

    # Verify source collections exist and have data
    taxi_count = db[TAXI_COLLECTION].count_documents({})
    weather_count = db[WEATHER_COLLECTION].count_documents({})

    logger.info("Source data counts:")
    logger.info("  taxi_trips:    %d documents", taxi_count)
    logger.info("  weather_daily: %d documents", weather_count)

    if taxi_count == 0 or weather_count == 0:
        logger.error(
            "Source collections are empty. "
            "Run the Airflow DAG or load data first."
        )
        sys.exit(1)

    # --- Aggregation 1: Enriched trips (data fusion via $lookup) ---
    logger.info("=" * 60)
    logger.info("AGGREGATION 1: Creating enriched_trips collection")
    logger.info("  Joins taxi_trips with weather_daily on pickup_date = DATE")
    logger.info("=" * 60)
    enriched_count = create_enriched_trips()
    logger.info("Result: %d enriched trip documents\n", enriched_count)

    # --- Aggregation 2: Average tip by weather condition ---
    logger.info("=" * 60)
    logger.info("AGGREGATION 2: Creating tip_by_weather collection")
    logger.info("  Computes avg tip %% grouped by weather condition")
    logger.info("  (Credit card payments only)")
    logger.info("=" * 60)
    tip_count = create_tip_by_weather_aggregate()
    logger.info("Result: %d weather condition aggregates\n", tip_count)

    # --- Aggregation 3: Hourly trip statistics ---
    logger.info("=" * 60)
    logger.info("AGGREGATION 3: Creating hourly_trip_stats collection")
    logger.info("  Computes avg fare, tip, distance by hour & weather")
    logger.info("=" * 60)
    hourly_count = create_hourly_trip_stats()
    logger.info("Result: %d hourly stat documents\n", hourly_count)

    logger.info("All aggregations complete!")


if __name__ == "__main__":
    main()

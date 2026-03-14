"""
Phase 3 — Spark Analysis
NYC Taxi & Weather Pipeline

Loads taxi_trips and weather_daily from MongoDB Atlas into Spark DataFrames,
performs the join and analytics via SparkSQL, and benchmarks against the
equivalent MongoDB aggregation pipelines.

Usage:
    python scripts/spark_analysis.py

First run downloads the MongoDB Spark Connector JAR (~5 min).
Subsequent runs use the cached version.
"""

import sys
import os
import time
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from config.settings import MONGO_URI, MONGO_DB_NAME
from dags.utils.mongo_helpers import get_database

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"


# =============================================================================
# SparkSession
# =============================================================================

def create_spark_session():
    from pyspark.sql import SparkSession
    logger.info("Creating SparkSession (downloads JAR on first run)...")
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Weather Analysis")
        .config("spark.jars.packages", CONNECTOR)
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready.")
    return spark


# =============================================================================
# Load from MongoDB Atlas → Spark DataFrames
# =============================================================================

def load_collection(spark, collection_name):
    logger.info("Loading collection: %s", collection_name)
    t0 = time.time()
    df = (
        spark.read
        .format("mongodb")
        .option("database", MONGO_DB_NAME)
        .option("collection", collection_name)
        .load()
    )
    # Force evaluation
    count = df.count()
    elapsed = time.time() - t0
    logger.info("  Loaded %d documents in %.2fs", count, elapsed)
    return df, count, elapsed


# =============================================================================
# MongoDB benchmark queries (identical logic to SparkSQL queries below)
# =============================================================================

def mongo_query_tip_by_weather(db):
    pipeline = [
        {"$match": {"payment_type": 1, "fare_amount": {"$gt": 0}}},
        {
            "$group": {
                "_id": "$weather_condition",
                "avg_tip_pct": {
                    "$avg": {
                        "$multiply": [
                            {"$divide": ["$tip_amount", "$fare_amount"]},
                            100,
                        ]
                    }
                },
                "avg_tip_amount": {"$avg": "$tip_amount"},
                "total_trips": {"$sum": 1},
            }
        },
        {"$sort": {"total_trips": -1}},
    ]
    return list(db["enriched_trips"].aggregate(pipeline, allowDiskUse=True))


def mongo_query_hourly_stats(db):
    pipeline = [
        {"$match": {"fare_amount": {"$gt": 0}}},
        {
            "$group": {
                "_id": {
                    "pickup_hour": "$pickup_hour",
                    "weather_condition": "$weather_condition",
                },
                "avg_fare": {"$avg": "$fare_amount"},
                "avg_tip": {"$avg": "$tip_amount"},
                "trip_count": {"$sum": 1},
            }
        },
        {"$sort": {"_id.pickup_hour": 1}},
    ]
    return list(db["enriched_trips"].aggregate(pipeline, allowDiskUse=True))


# =============================================================================
# Main
# =============================================================================

def main():
    spark = create_spark_session()

    # -------------------------------------------------------------------------
    # 1. Load collections from Atlas into Spark DataFrames
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 1: Loading MongoDB collections into Spark DataFrames")
    print("=" * 70)

    taxi_df, taxi_count, taxi_load_time = load_collection(spark, "taxi_trips")
    weather_df, weather_count, weather_load_time = load_collection(spark, "weather_daily")

    print(f"\n  taxi_trips schema:")
    taxi_df.printSchema()
    print(f"  weather_daily schema:")
    weather_df.printSchema()

    # -------------------------------------------------------------------------
    # 2. Join in Spark (replicate the MongoDB $lookup enrichment)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 2: Joining taxi_trips with weather_daily in Spark")
    print("=" * 70)

    t0 = time.time()
    enriched_df = taxi_df.join(
        weather_df,
        taxi_df["pickup_date"] == weather_df["DATE"],
        how="inner"
    ).select(
        taxi_df["pickup_date"],
        taxi_df["pickup_hour"],
        taxi_df["trip_distance"],
        taxi_df["fare_amount"],
        taxi_df["tip_amount"],
        taxi_df["payment_type"],
        weather_df["PRCP"],
        weather_df["SNOW"],
        weather_df["SNWD"],
        weather_df["TMAX"],
        weather_df["weather_condition"],
    )

    enriched_df.createOrReplaceTempView("enriched_trips")
    enriched_count = enriched_df.count()
    spark_join_time = time.time() - t0
    print(f"  Joined {enriched_count:,} records in {spark_join_time:.2f}s")

    # -------------------------------------------------------------------------
    # 3. SparkSQL Query A — avg tip % by weather condition
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 3: SparkSQL Query A — avg tip %% by weather condition")
    print("=" * 70)

    t0 = time.time()
    spark_tip_weather = spark.sql("""
        SELECT
            weather_condition,
            ROUND(AVG(tip_amount / fare_amount) * 100, 2) AS avg_tip_pct,
            ROUND(AVG(tip_amount), 2)                     AS avg_tip_amount,
            COUNT(*)                                       AS total_trips
        FROM enriched_trips
        WHERE payment_type = 1
          AND fare_amount > 0
        GROUP BY weather_condition
        ORDER BY total_trips DESC
    """)
    spark_tip_weather.cache()
    spark_tip_weather_results = spark_tip_weather.collect()
    spark_query_a_time = time.time() - t0

    print(f"\n  Results (SparkSQL, {spark_query_a_time:.2f}s):")
    spark_tip_weather.show(truncate=False)

    # -------------------------------------------------------------------------
    # 4. SparkSQL Query B — hourly trip stats by weather
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 4: SparkSQL Query B — avg tip by hour and weather condition")
    print("=" * 70)

    t0 = time.time()
    spark_hourly = spark.sql("""
        SELECT
            pickup_hour,
            weather_condition,
            ROUND(AVG(fare_amount), 2)    AS avg_fare,
            ROUND(AVG(tip_amount), 2)     AS avg_tip,
            ROUND(AVG(trip_distance), 2)  AS avg_distance,
            COUNT(*)                      AS trip_count
        FROM enriched_trips
        WHERE fare_amount > 0
        GROUP BY pickup_hour, weather_condition
        ORDER BY pickup_hour, trip_count DESC
    """)
    spark_hourly.cache()
    spark_hourly_results = spark_hourly.collect()
    spark_query_b_time = time.time() - t0

    print(f"\n  Results (SparkSQL, {spark_query_b_time:.2f}s) — first 20 rows:")
    spark_hourly.show(20, truncate=False)

    # -------------------------------------------------------------------------
    # 5. MongoDB benchmark — rebuild enriched_trips temporarily and time queries
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("STEP 5: MongoDB benchmark (rebuilding enriched_trips for timing)")
    print("=" * 70)

    from dags.utils.mongo_helpers import create_enriched_trips
    db = get_database()

    logger.info("Rebuilding enriched_trips for benchmark...")
    create_enriched_trips()

    # Time Query A in MongoDB
    t0 = time.time()
    mongo_tip_weather_results = mongo_query_tip_by_weather(db)
    mongo_query_a_time = time.time() - t0
    print(f"\n  MongoDB Query A (tip by weather): {mongo_query_a_time:.2f}s")

    # Time Query B in MongoDB
    t0 = time.time()
    mongo_hourly_results = mongo_query_hourly_stats(db)
    mongo_query_b_time = time.time() - t0
    print(f"  MongoDB Query B (hourly stats):   {mongo_query_b_time:.2f}s")

    # Drop enriched_trips again to free storage
    db["enriched_trips"].drop()
    logger.info("enriched_trips dropped (storage freed).")

    # -------------------------------------------------------------------------
    # 6. Benchmark summary
    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("BENCHMARK RESULTS")
    print("=" * 70)
    print(f"\n  Data loading:")
    print(f"    Spark load taxi_trips ({taxi_count:,} docs):     {taxi_load_time:.2f}s")
    print(f"    Spark load weather_daily ({weather_count:,} docs): {weather_load_time:.2f}s")
    print(f"    Spark join (enriched, {enriched_count:,} docs):   {spark_join_time:.2f}s")
    print(f"\n  Query A — avg tip %% by weather condition:")
    print(f"    MongoDB:  {mongo_query_a_time:.2f}s")
    print(f"    SparkSQL: {spark_query_a_time:.2f}s")
    if mongo_query_a_time < spark_query_a_time:
        ratio = spark_query_a_time / mongo_query_a_time
        print(f"    Winner:   MongoDB ({ratio:.1f}x faster)")
    else:
        ratio = mongo_query_a_time / spark_query_a_time
        print(f"    Winner:   SparkSQL ({ratio:.1f}x faster)")

    print(f"\n  Query B — hourly trip stats by weather:")
    print(f"    MongoDB:  {mongo_query_b_time:.2f}s")
    print(f"    SparkSQL: {spark_query_b_time:.2f}s")
    if mongo_query_b_time < spark_query_b_time:
        ratio = spark_query_b_time / mongo_query_b_time
        print(f"    Winner:   MongoDB ({ratio:.1f}x faster)")
    else:
        ratio = mongo_query_b_time / spark_query_b_time
        print(f"    Winner:   SparkSQL ({ratio:.1f}x faster)")

    print("\n" + "=" * 70)
    print("Spark analysis complete.")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    main()

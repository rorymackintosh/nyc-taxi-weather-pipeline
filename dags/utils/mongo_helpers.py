"""
MongoDB helper functions.

Handles connections, bulk inserts, and aggregation pipelines for
the NYC Taxi & Weather Pipeline.
"""

import logging
from typing import Optional

import pandas as pd
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from pymongo.server_api import ServerApi

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    TAXI_COLLECTION,
    WEATHER_COLLECTION,
    ENRICHED_COLLECTION,
    TIP_WEATHER_AGG_COLLECTION,
    HOURLY_STATS_COLLECTION,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_mongo_client() -> MongoClient:
    """Return a MongoClient connected to Atlas with Stable API v1."""
    return MongoClient(MONGO_URI, server_api=ServerApi('1'))


def get_database(client: Optional[MongoClient] = None):
    """Return the project database handle."""
    client = client or get_mongo_client()
    return client[MONGO_DB_NAME]


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_taxi_data(df: pd.DataFrame) -> int:
    """
    Insert taxi trip records into the taxi_trips collection.
    Converts the DataFrame to a list of dicts and does a bulk insert.

    Returns:
        Number of documents inserted.
    """
    client = get_mongo_client()
    db = client[MONGO_DB_NAME]
    collection = db[TAXI_COLLECTION]

    records = df.to_dict(orient="records")
    if not records:
        logger.warning("No taxi records to insert.")
        return 0

    result = collection.insert_many(records, ordered=False)
    count = len(result.inserted_ids)
    logger.info("Inserted %d taxi trip documents.", count)

    # Create indexes for query performance
    collection.create_index("pickup_date")
    collection.create_index("tpep_pickup_datetime")
    collection.create_index("payment_type")

    client.close()
    return count


def load_weather_data(df: pd.DataFrame) -> int:
    """
    Upsert daily weather records into the weather_daily collection.
    Uses DATE as the unique key to avoid duplicates on re-runs.

    Returns:
        Number of documents upserted.
    """
    client = get_mongo_client()
    db = client[MONGO_DB_NAME]
    collection = db[WEATHER_COLLECTION]

    operations = []
    for record in df.to_dict(orient="records"):
        operations.append(
            UpdateOne(
                {"DATE": record["DATE"]},
                {"$set": record},
                upsert=True,
            )
        )

    if not operations:
        logger.warning("No weather records to upsert.")
        return 0

    try:
        result = collection.bulk_write(operations, ordered=False)
        count = result.upserted_count + result.modified_count
        logger.info("Upserted %d weather documents.", count)
    except BulkWriteError as e:
        logger.error("Bulk write error: %s", e.details)
        count = 0

    collection.create_index("DATE", unique=True)
    client.close()
    return count


# ---------------------------------------------------------------------------
# Aggregation Pipeline 1: Enriched Trips ($lookup join)
# ---------------------------------------------------------------------------

def create_enriched_trips() -> int:
    """
    Join taxi_trips with weather_daily using a $lookup on the date field.
    Stores the result in the enriched_trips collection.

    This is the core data fusion step: each taxi trip is enriched with
    the weather conditions on the day it occurred.

    Returns:
        Number of enriched documents created.
    """
    client = get_mongo_client()
    db = client[MONGO_DB_NAME]

    pipeline = [
        # Join with weather data on the date key
        {
            "$lookup": {
                "from": WEATHER_COLLECTION,
                "localField": "pickup_date",
                "foreignField": "DATE",
                "as": "weather",
            }
        },
        # Unwind the weather array (1:1 join since weather is daily)
        {"$unwind": {"path": "$weather", "preserveNullAndEmptyArrays": True}},
        # Project the fields we need
        {
            "$project": {
                "_id": 0,
                "tpep_pickup_datetime": 1,
                "tpep_dropoff_datetime": 1,
                "pickup_date": 1,
                "pickup_hour": 1,
                "passenger_count": 1,
                "trip_distance": 1,
                "fare_amount": 1,
                "tip_amount": 1,
                "total_amount": 1,
                "payment_type": 1,
                "pulocationid": 1,
                "dolocationid": 1,
                "PRCP": "$weather.PRCP",
                "SNOW": "$weather.SNOW",
                "SNWD": "$weather.SNWD",
                "TMAX": "$weather.TMAX",
                "TMIN": "$weather.TMIN",
                "weather_condition": "$weather.weather_condition",
            }
        },
        # Write to the enriched collection
        {"$out": ENRICHED_COLLECTION},
    ]

    logger.info("Running $lookup pipeline: taxi_trips ⟕ weather_daily → enriched_trips")
    db[TAXI_COLLECTION].aggregate(pipeline, allowDiskUse=True)

    count = db[ENRICHED_COLLECTION].count_documents({})
    logger.info("Enriched trips collection now has %d documents.", count)

    # Skip index creation on enriched_trips to conserve MongoDB free tier
    # storage. The downstream aggregation pipelines scan the full collection
    # anyway, so indexes are not needed for correctness.

    client.close()
    return count


# ---------------------------------------------------------------------------
# Aggregation Pipeline 2: Average Tip by Weather Condition
# ---------------------------------------------------------------------------

def create_tip_by_weather_aggregate() -> int:
    """
    Compute average tip percentage grouped by weather condition.
    Only considers credit card payments (payment_type == 1) since
    cash tips are not recorded in the dataset.

    Stores result in the tip_by_weather collection.

    Returns:
        Number of aggregate documents created.
    """
    client = get_mongo_client()
    db = client[MONGO_DB_NAME]

    pipeline = [
        # Filter to credit card payments only (cash tips not recorded)
        {"$match": {"payment_type": 1, "fare_amount": {"$gt": 0}}},
        # Group by weather condition
        {
            "$group": {
                "_id": "$weather_condition",
                "avg_tip_amount": {"$avg": "$tip_amount"},
                "avg_fare_amount": {"$avg": "$fare_amount"},
                "avg_tip_percentage": {
                    "$avg": {
                        "$multiply": [
                            {"$divide": ["$tip_amount", "$fare_amount"]},
                            100,
                        ]
                    }
                },
                "avg_trip_distance": {"$avg": "$trip_distance"},
                "total_trips": {"$sum": 1},
                "max_tip": {"$max": "$tip_amount"},
                "min_tip": {"$min": "$tip_amount"},
            }
        },
        # Rename _id to weather_condition for clarity
        {
            "$project": {
                "_id": 0,
                "weather_condition": "$_id",
                "avg_tip_amount": {"$round": ["$avg_tip_amount", 2]},
                "avg_fare_amount": {"$round": ["$avg_fare_amount", 2]},
                "avg_tip_percentage": {"$round": ["$avg_tip_percentage", 2]},
                "avg_trip_distance": {"$round": ["$avg_trip_distance", 2]},
                "total_trips": 1,
                "max_tip": 1,
                "min_tip": 1,
            }
        },
        {"$sort": {"total_trips": -1}},
        {"$out": TIP_WEATHER_AGG_COLLECTION},
    ]

    logger.info("Running aggregation: avg tip % by weather condition → tip_by_weather")
    db[ENRICHED_COLLECTION].aggregate(pipeline, allowDiskUse=True)

    count = db[TIP_WEATHER_AGG_COLLECTION].count_documents({})
    logger.info("tip_by_weather collection now has %d documents.", count)

    client.close()
    return count


# ---------------------------------------------------------------------------
# Aggregation Pipeline 3: Hourly Trip Statistics
# ---------------------------------------------------------------------------

def create_hourly_trip_stats() -> int:
    """
    Compute hourly trip statistics (avg fare, avg tip, trip count)
    grouped by pickup_hour and weather_condition.

    Stores result in the hourly_trip_stats collection.

    Returns:
        Number of aggregate documents created.
    """
    client = get_mongo_client()
    db = client[MONGO_DB_NAME]

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
                "avg_distance": {"$avg": "$trip_distance"},
                "trip_count": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "pickup_hour": "$_id.pickup_hour",
                "weather_condition": "$_id.weather_condition",
                "avg_fare": {"$round": ["$avg_fare", 2]},
                "avg_tip": {"$round": ["$avg_tip", 2]},
                "avg_distance": {"$round": ["$avg_distance", 2]},
                "trip_count": 1,
            }
        },
        {"$sort": {"pickup_hour": 1, "trip_count": -1}},
        {"$out": HOURLY_STATS_COLLECTION},
    ]

    logger.info("Running aggregation: hourly trip stats → hourly_trip_stats")
    db[ENRICHED_COLLECTION].aggregate(pipeline, allowDiskUse=True)

    count = db[HOURLY_STATS_COLLECTION].count_documents({})
    logger.info("hourly_trip_stats collection now has %d documents.", count)

    client.close()
    return count

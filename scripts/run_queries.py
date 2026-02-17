"""
Standalone script demonstrating MongoDB queries on both original
and aggregated collections.

Covers the Phase 2 requirement:
  "Query the data in MongoDB (both original and new aggregate data)
   using MongoDB queries."

Usage:
    python scripts/run_queries.py
"""

import logging
import sys
import os
import json
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dags.utils.mongo_helpers import get_database
from config.settings import (
    TAXI_COLLECTION,
    WEATHER_COLLECTION,
    ENRICHED_COLLECTION,
    TIP_WEATHER_AGG_COLLECTION,
    HOURLY_STATS_COLLECTION,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def pp(doc):
    """Pretty-print a MongoDB document (handles ObjectId, datetime)."""
    def default(o):
        if hasattr(o, "isoformat"):
            return o.isoformat()
        return str(o)
    return json.dumps(doc, indent=2, default=default)


def run_all_queries():
    db = get_database()

    # =========================================================================
    # QUERIES ON ORIGINAL COLLECTIONS
    # =========================================================================

    print("\n" + "=" * 70)
    print("QUERY 1: Count documents in each collection")
    print("=" * 70)
    for coll_name in [TAXI_COLLECTION, WEATHER_COLLECTION, ENRICHED_COLLECTION,
                      TIP_WEATHER_AGG_COLLECTION, HOURLY_STATS_COLLECTION]:
        count = db[coll_name].count_documents({})
        print(f"  {coll_name}: {count:,} documents")

    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY 2: Sample taxi trips (first 5)")
    print("=" * 70)
    for doc in db[TAXI_COLLECTION].find().limit(5):
        print(pp(doc))

    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY 3: Average fare and tip for trips > 5 miles")
    print("=" * 70)
    pipeline = [
        {"$match": {"trip_distance": {"$gt": 5}}},
        {
            "$group": {
                "_id": None,
                "avg_fare": {"$avg": "$fare_amount"},
                "avg_tip": {"$avg": "$tip_amount"},
                "count": {"$sum": 1},
            }
        },
    ]
    for doc in db[TAXI_COLLECTION].aggregate(pipeline):
        print(pp(doc))

    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY 4: Top 5 pickup locations by trip count")
    print("=" * 70)
    pipeline = [
        {"$group": {"_id": "$pulocationid", "trip_count": {"$sum": 1}}},
        {"$sort": {"trip_count": -1}},
        {"$limit": 5},
    ]
    for doc in db[TAXI_COLLECTION].aggregate(pipeline):
        print(pp(doc))

    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY 5: Weather data sample (first 5 days)")
    print("=" * 70)
    for doc in db[WEATHER_COLLECTION].find().sort("DATE", 1).limit(5):
        print(pp(doc))

    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY 6: Days with snowfall > 0")
    print("=" * 70)
    snow_days = list(
        db[WEATHER_COLLECTION]
        .find({"SNOW": {"$gt": 0}}, {"_id": 0, "DATE": 1, "SNOW": 1, "TMAX": 1})
        .sort("DATE", 1)
    )
    print(f"  Found {len(snow_days)} snow days")
    for doc in snow_days[:10]:
        print(f"  {pp(doc)}")

    # =========================================================================
    # QUERIES ON AGGREGATED / ENRICHED COLLECTIONS
    # =========================================================================

    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY 7: Tip percentage by weather condition (from tip_by_weather)")
    print("=" * 70)
    for doc in db[TIP_WEATHER_AGG_COLLECTION].find({}, {"_id": 0}).sort("total_trips", -1):
        print(pp(doc))

    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY 8: Enriched trips on snowy days (sample 5)")
    print("=" * 70)
    for doc in db[ENRICHED_COLLECTION].find(
        {"weather_condition": "Snow"},
        {"_id": 0, "pickup_date": 1, "trip_distance": 1,
         "fare_amount": 1, "tip_amount": 1, "SNOW": 1},
    ).limit(5):
        print(pp(doc))

    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY 9: Average tip by hour during rainy weather")
    print("=" * 70)
    for doc in db[HOURLY_STATS_COLLECTION].find(
        {"weather_condition": {"$in": ["Light Rain", "Heavy Rain"]}},
        {"_id": 0},
    ).sort("pickup_hour", 1):
        print(pp(doc))

    # -------------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("QUERY 10: Compare avg tip: Clear vs Snow vs Rain (enriched_trips)")
    print("=" * 70)
    pipeline = [
        {
            "$match": {
                "payment_type": 1,
                "fare_amount": {"$gt": 0},
                "weather_condition": {"$in": ["Clear/Normal", "Snow", "Light Rain", "Heavy Rain"]},
            }
        },
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
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
    ]
    for doc in db[ENRICHED_COLLECTION].aggregate(pipeline):
        print(pp(doc))

    print("\n" + "=" * 70)
    print("All queries complete!")
    print("=" * 70)


if __name__ == "__main__":
    run_all_queries()

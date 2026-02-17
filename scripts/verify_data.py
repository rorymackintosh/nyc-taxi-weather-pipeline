"""
Quick verification script to check the state of all MongoDB collections.

Prints document counts, sample documents, and basic statistics
for each collection in the pipeline.

Usage:
    python scripts/verify_data.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dags.utils.mongo_helpers import get_database
from config.settings import (
    TAXI_COLLECTION,
    WEATHER_COLLECTION,
    ENRICHED_COLLECTION,
    TIP_WEATHER_AGG_COLLECTION,
    HOURLY_STATS_COLLECTION,
)


def main():
    db = get_database()

    collections = [
        TAXI_COLLECTION,
        WEATHER_COLLECTION,
        ENRICHED_COLLECTION,
        TIP_WEATHER_AGG_COLLECTION,
        HOURLY_STATS_COLLECTION,
    ]

    print("\n" + "=" * 60)
    print("NYC Taxi & Weather Pipeline - Data Verification")
    print("=" * 60)

    for name in collections:
        coll = db[name]
        count = coll.count_documents({})
        print(f"\n--- {name} ---")
        print(f"  Document count: {count:,}")

        if count > 0:
            sample = coll.find_one({}, {"_id": 0})
            print(f"  Fields: {list(sample.keys())}")
            print(f"  Sample: {sample}")

            # Show indexes
            indexes = coll.index_information()
            print(f"  Indexes: {list(indexes.keys())}")

    # Show existing collections in the database
    print(f"\nAll collections in database: {db.list_collection_names()}")
    print("\nVerification complete.")


if __name__ == "__main__":
    main()

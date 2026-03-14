"""
Train a Random Forest model to predict tip_amount.

Steps:
  1. Connect to MongoDB Atlas
  2. Pull taxi_trips, weather_daily, tip_by_weather, hourly_trip_stats into Spark
  3. Join taxi_trips with weather_daily on date
  4. Engineer features from tip_by_weather and hourly_trip_stats aggregates
  5. Train a RandomForestRegressor and evaluate

Usage:
    python scripts/run_model.py
"""

import sys
import os

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

from config.settings import (
    MONGO_URI,
    MONGO_DB_NAME,
    TAXI_COLLECTION,
    WEATHER_COLLECTION,
    TIP_WEATHER_AGG_COLLECTION,
    HOURLY_STATS_COLLECTION,
)

def get_spark_session() -> SparkSession:
    """Create a local SparkSession."""
    return (
        SparkSession.builder
        .appName("NYC-Taxi-Tip-Prediction")
        .master("local[*]")
        .getOrCreate()
    )


def mongo_collection_to_spark_df(spark, db, collection_name, limit=0):
    """Read a MongoDB collection via pymongo → pandas → Spark DataFrame."""
    cursor = db[collection_name].find()
    if limit > 0:
        cursor = cursor.limit(limit)

    docs = list(cursor)
    if not docs:
        print(f"Collection '{collection_name}' is empty.")
        return None

    pdf = pd.DataFrame(docs)
    if "_id" in pdf.columns:
        pdf["_id"] = pdf["_id"].astype(str)

    return spark.createDataFrame(pdf)


def main():
    # 1. MongoDB connection
    if not MONGO_URI:
        print("ERROR: MONGO_URI is not set. Fill in your .env file.")
        sys.exit(1)

    print("Connecting to MongoDB …")
    client = MongoClient(MONGO_URI, server_api=ServerApi("1"))
    db = client[MONGO_DB_NAME]

    try:
        client.admin.command("ping")
        print("  ✓ MongoDB connection successful!\n")
    except Exception as e:
        print(f"  ✗ Connection failed: {e}")
        sys.exit(1)


    # 2. Pull data from MongoDB into Spark DataFrames
    spark = get_spark_session()

    print(f"Reading {TAXI_COLLECTION} …")
    taxi_df = mongo_collection_to_spark_df(spark, db, TAXI_COLLECTION)

    print(f"Reading {WEATHER_COLLECTION} …")
    weather_df = mongo_collection_to_spark_df(spark, db, WEATHER_COLLECTION)

    print(f"Reading {TIP_WEATHER_AGG_COLLECTION} …")
    tip_weather_df = mongo_collection_to_spark_df(spark, db, TIP_WEATHER_AGG_COLLECTION)

    print(f"Reading {HOURLY_STATS_COLLECTION} …")
    hourly_stats_df = mongo_collection_to_spark_df(spark, db, HOURLY_STATS_COLLECTION)

    client.close()

    if taxi_df is None or weather_df is None:
        print("ERROR: taxi_trips or weather_daily is empty. Load data first.")
        spark.stop()
        sys.exit(1)

    print(f"\n  taxi_trips:        {taxi_df.count():,} rows")
    print(f"  weather_daily:     {weather_df.count():,} rows")
    if tip_weather_df:
        print(f"  tip_by_weather:    {tip_weather_df.count():,} rows")
    if hourly_stats_df:
        print(f"  hourly_trip_stats: {hourly_stats_df.count():,} rows")

    # 3. Join taxi_trips with weather_daily
    #    Join key: DATE (weather) == dropoff date (taxi) 
    # Extract date string from tpep_dropoff_datetime to match weather DATE
    taxi_df = taxi_df.withColumn(
        "dropoff_date",
        F.to_date(F.col("tpep_dropoff_datetime")).cast("string"),
    )

    # Drop _id and DATE from weather side; use dropoff_date as the surviving key
    weather_join = weather_df.drop("_id").withColumnRenamed("DATE", "weather_date")

    joined_df = taxi_df.join(
        weather_join,
        taxi_df["dropoff_date"] == weather_join["weather_date"],
        "inner",
    ).drop("weather_date")

    print(f"Joined (taxi ⟕ weather): {joined_df.count():,} rows")

    # 4. Feature engineering
    #    a) Columns from tip_by_weather   → aggregate-level weather features
    #    b) Columns from hourly_trip_stats → aggregate-level hour features

    # 4a – Merge tip_by_weather features on weather_condition
    if tip_weather_df is not None:
        tip_weather_features = tip_weather_df.select(
            F.col("weather_condition").alias("tw_weather_condition"),
            F.col("avg_tip_percentage").alias("tw_avg_tip_pct"),
            F.col("avg_fare_amount").alias("tw_avg_fare"),
            F.col("avg_trip_distance").alias("tw_avg_distance"),
            F.col("total_trips").alias("tw_total_trips"),
        )
        joined_df = joined_df.join(
            tip_weather_features,
            joined_df["weather_condition"] == tip_weather_features["tw_weather_condition"],
            "left",
        ).drop("tw_weather_condition")

    # 4b – Merge hourly_trip_stats features on (pickup_hour, weather_condition)
    if hourly_stats_df is not None:
        hourly_features = hourly_stats_df.select(
            F.col("pickup_hour").alias("hs_pickup_hour"),
            F.col("weather_condition").alias("hs_weather_condition"),
            F.col("avg_fare").alias("hs_avg_fare"),
            F.col("avg_tip").alias("hs_avg_tip"),
            F.col("avg_distance").alias("hs_avg_distance"),
            F.col("trip_count").alias("hs_trip_count"),
        )
        joined_df = joined_df.join(
            hourly_features,
            (joined_df["pickup_hour"] == hourly_features["hs_pickup_hour"])
            & (joined_df["weather_condition"] == hourly_features["hs_weather_condition"]),
            "left",
        ).drop("hs_pickup_hour", "hs_weather_condition")

    # 5. Prepare features for ML

    # Resolve any duplicate column names that survived the joins
    # by renaming them with a _dup suffix, then we just ignore them.
    orig_cols = joined_df.columns
    new_names = []
    seen = {}
    for c in orig_cols:
        if c in seen:
            seen[c] += 1
            new_names.append(f"{c}_dup{seen[c]}")
        else:
            seen[c] = 0
            new_names.append(c)
    joined_df = joined_df.toDF(*new_names)

    # Columns to exclude (target, leakage, identifiers, raw datetimes)
    drop_cols = {
        "_id",
        "tip_amount",          # target – don't include as feature
        "total_amount",        # includes tip → leakage
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_date",
        "dropoff_date",
        "DATE",
        "weather_condition",   # will be string-indexed below
    }
    # Also exclude any _dup columns
    drop_cols.update(c for c in joined_df.columns if c.endswith("_dup1") or c.endswith("_dup2"))

    # Index the categorical weather_condition column
    if "weather_condition" in joined_df.columns:
        indexer = StringIndexer(
            inputCol="weather_condition",
            outputCol="weather_condition_idx",
            handleInvalid="keep",
        )
        joined_df = indexer.fit(joined_df).transform(joined_df)

    # Cast numeric columns to double and collect feature names
    feature_cols = []
    for col_name, dtype in joined_df.dtypes:
        if col_name in drop_cols:
            continue
        if dtype in ("double", "float", "int", "bigint", "long"):
            joined_df = joined_df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
            feature_cols.append(col_name)

    # Drop rows with nulls in feature / target columns
    model_df = joined_df.select(feature_cols + ["tip_amount"]).dropna()

    print(f"\n  Features ({len(feature_cols)}):")
    for fc in sorted(feature_cols):
        print(f"    • {fc}")
    print(f"\n  Modelling rows: {model_df.count():,}")

    # 6. Train / test split + Random Forest
    train_df, test_df = model_df.randomSplit([0.8, 0.2], seed=42)
    print(f"  Train: {train_df.count():,}  |  Test: {test_df.count():,}\n")

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="tip_amount",
        numTrees=100,
        maxDepth=8,
        seed=42,
    )

    pipeline = Pipeline(stages=[assembler, rf])

    print("Training Random Forest (100 trees, maxDepth=8) …")
    model = pipeline.fit(train_df)

    # 7. Evaluate
    predictions = model.transform(test_df)

    evaluators = {
        "RMSE": RegressionEvaluator(labelCol="tip_amount", predictionCol="prediction", metricName="rmse"),
        "MAE":  RegressionEvaluator(labelCol="tip_amount", predictionCol="prediction", metricName="mae"),
        "R²":   RegressionEvaluator(labelCol="tip_amount", predictionCol="prediction", metricName="r2"),
    }

    print("\n" + "=" * 50)
    print("  Random Forest – Test Set Performance")
    print("=" * 50)
    for name, ev in evaluators.items():
        value = ev.evaluate(predictions)
        print(f"  {name:>5s}: {value:.4f}")
    print("=" * 50)

    # Feature importances
    rf_model = model.stages[-1]
    importances = list(zip(feature_cols, rf_model.featureImportances.toArray()))
    importances.sort(key=lambda x: x[1], reverse=True)

    print("\n  Top 10 Feature Importances:")
    for feat, imp in importances[:10]:
        print(f"    {feat:30s}  {imp:.4f}")

    # Show a few predictions
    print("\nSample predictions vs actuals:")
    predictions.select("tip_amount", "prediction").show(10, truncate=False)


    spark.stop()
    print("Done – Spark session stopped.")


if __name__ == "__main__":
    main()

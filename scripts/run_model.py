"""
Phase 3 — Spark ML Modeling
NYC Taxi & Weather Pipeline

Train Linear Regression (baseline) and Random Forest models to predict
tip_amount, then compare their performance.

Steps:
  1. Load taxi_trips, weather_daily, tip_by_weather, hourly_trip_stats
     from MongoDB Atlas into Spark DataFrames via the Spark Connector
  2. Join taxi_trips with weather_daily on pickup_date
  3. Engineer features from tip_by_weather and hourly_trip_stats aggregates
  4. Train Linear Regression and Random Forest regressors
  5. Evaluate both with RMSE, MAE, R² and compare

Usage:
    python scripts/run_model.py
"""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
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

CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"


def create_spark_session():
    """Create a SparkSession configured with the MongoDB Spark Connector."""
    print("Creating SparkSession ...")
    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-Tip-Prediction")
        .config("spark.jars.packages", CONNECTOR)
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession ready.\n")
    return spark


def load_collection(spark, collection_name):
    """Read a MongoDB collection into a Spark DataFrame via the Spark Connector."""
    print(f"Loading {collection_name} ...")
    t0 = time.time()
    df = (
        spark.read
        .format("mongodb")
        .option("database", MONGO_DB_NAME)
        .option("collection", collection_name)
        .load()
    )
    count = df.count()
    elapsed = time.time() - t0
    print(f"  {collection_name}: {count:,} rows ({elapsed:.1f}s)")
    return df


def main():
    if not MONGO_URI:
        print("ERROR: MONGO_URI is not set. Fill in your .env file.")
        sys.exit(1)

    spark = create_spark_session()

    # ------------------------------------------------------------------
    # 1. Load collections from MongoDB Atlas into Spark DataFrames
    # ------------------------------------------------------------------
    print("=" * 60)
    print("STEP 1: Loading MongoDB collections into Spark DataFrames")
    print("=" * 60)

    taxi_df = load_collection(spark, TAXI_COLLECTION)
    weather_df = load_collection(spark, WEATHER_COLLECTION)
    tip_weather_df = load_collection(spark, TIP_WEATHER_AGG_COLLECTION)
    hourly_stats_df = load_collection(spark, HOURLY_STATS_COLLECTION)

    # ------------------------------------------------------------------
    # 2. Join taxi_trips with weather_daily on pickup_date
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 2: Joining taxi_trips with weather_daily")
    print("=" * 60)

    joined_df = taxi_df.join(
        weather_df,
        taxi_df["pickup_date"] == weather_df["DATE"],
        "inner",
    ).select(
        taxi_df["pickup_date"],
        taxi_df["pickup_hour"],
        taxi_df["passenger_count"],
        taxi_df["trip_distance"],
        taxi_df["fare_amount"],
        taxi_df["tip_amount"],
        taxi_df["total_amount"],
        taxi_df["payment_type"],
        taxi_df["pulocationid"],
        taxi_df["dolocationid"],
        weather_df["PRCP"],
        weather_df["SNOW"],
        weather_df["SNWD"],
        weather_df["TMAX"],
        weather_df["TMIN"],
        weather_df["weather_condition"],
    )

    joined_count = joined_df.count()
    print(f"  Joined: {joined_count:,} rows")

    # ------------------------------------------------------------------
    # 3. Feature engineering from aggregate collections
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 3: Feature engineering from aggregated collections")
    print("=" * 60)

    # 3a - Merge tip_by_weather features on weather_condition
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

    # 3b - Merge hourly_trip_stats features on (pickup_hour, weather_condition)
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

    print("  Aggregate features merged.")

    # ------------------------------------------------------------------
    # 4. Prepare features for ML
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 4: Preparing features")
    print("=" * 60)

    # Deduplicate column names from joins
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

    drop_cols = {
        "_id",
        "tip_amount",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_date",
        "DATE",
        "weather_condition",
    }
    drop_cols.update(c for c in joined_df.columns if "_dup" in c)

    # String-index the weather_condition column
    if "weather_condition" in joined_df.columns:
        indexer = StringIndexer(
            inputCol="weather_condition",
            outputCol="weather_condition_idx",
            handleInvalid="keep",
        )
        joined_df = indexer.fit(joined_df).transform(joined_df)

    feature_cols = []
    for col_name, dtype in joined_df.dtypes:
        if col_name in drop_cols:
            continue
        if dtype in ("double", "float", "int", "bigint", "long"):
            joined_df = joined_df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
            feature_cols.append(col_name)

    model_df = joined_df.select(feature_cols + ["tip_amount"]).dropna()

    print(f"  Features ({len(feature_cols)}):")
    for fc in sorted(feature_cols):
        print(f"    - {fc}")
    print(f"\n  Modelling rows: {model_df.count():,}")

    # ------------------------------------------------------------------
    # 5. Train / test split
    # ------------------------------------------------------------------
    train_df, test_df = model_df.randomSplit([0.8, 0.2], seed=42)
    train_df.cache()
    test_df.cache()
    print(f"  Train: {train_df.count():,}  |  Test: {test_df.count():,}\n")

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    evaluators = {
        "RMSE": RegressionEvaluator(labelCol="tip_amount", predictionCol="prediction", metricName="rmse"),
        "MAE":  RegressionEvaluator(labelCol="tip_amount", predictionCol="prediction", metricName="mae"),
        "R2":   RegressionEvaluator(labelCol="tip_amount", predictionCol="prediction", metricName="r2"),
    }

    results = {}

    # ------------------------------------------------------------------
    # 6a. Linear Regression (baseline)
    # ------------------------------------------------------------------
    print("=" * 60)
    print("STEP 5a: Training Linear Regression (baseline)")
    print("=" * 60)

    lr = LinearRegression(
        featuresCol="features",
        labelCol="tip_amount",
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.0,
    )
    lr_pipeline = Pipeline(stages=[assembler, lr])
    lr_model = lr_pipeline.fit(train_df)
    lr_predictions = lr_model.transform(test_df)

    lr_metrics = {name: ev.evaluate(lr_predictions) for name, ev in evaluators.items()}
    results["Linear Regression"] = lr_metrics

    print("\n  Linear Regression - Test Set Performance")
    print("  " + "-" * 40)
    for name, value in lr_metrics.items():
        print(f"  {name:>5s}: {value:.4f}")
    print(f"\n  Coefficients (non-zero): {lr_model.stages[-1].coefficients.numNonzeros()}")
    print(f"  Intercept:               {lr_model.stages[-1].intercept:.4f}")

    # ------------------------------------------------------------------
    # 6b. Random Forest Regressor
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 5b: Training Random Forest (100 trees, maxDepth=8)")
    print("=" * 60)

    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="tip_amount",
        numTrees=100,
        maxDepth=8,
        seed=42,
    )
    rf_pipeline = Pipeline(stages=[assembler, rf])
    rf_model = rf_pipeline.fit(train_df)
    rf_predictions = rf_model.transform(test_df)

    rf_metrics = {name: ev.evaluate(rf_predictions) for name, ev in evaluators.items()}
    results["Random Forest"] = rf_metrics

    print("\n  Random Forest - Test Set Performance")
    print("  " + "-" * 40)
    for name, value in rf_metrics.items():
        print(f"  {name:>5s}: {value:.4f}")

    rf_fitted = rf_model.stages[-1]
    importances = list(zip(feature_cols, rf_fitted.featureImportances.toArray()))
    importances.sort(key=lambda x: x[1], reverse=True)

    print("\n  Top 10 Feature Importances:")
    for feat, imp in importances[:10]:
        print(f"    {feat:30s}  {imp:.4f}")

    # ------------------------------------------------------------------
    # 7. Model Comparison Summary
    # ------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("  MODEL COMPARISON")
    print("=" * 60)
    header = f"  {'Metric':<8s}  {'Linear Regression':>20s}  {'Random Forest':>16s}  {'Winner':>18s}"
    print(header)
    print(f"  {'-'*8}  {'-'*20}  {'-'*16}  {'-'*18}")

    for metric in ["RMSE", "MAE", "R2"]:
        lr_val = results["Linear Regression"][metric]
        rf_val = results["Random Forest"][metric]
        if metric == "R2":
            winner = "Random Forest" if rf_val > lr_val else "Linear Regression"
        else:
            winner = "Random Forest" if rf_val < lr_val else "Linear Regression"
        label = "R^2" if metric == "R2" else metric
        print(f"  {label:<8s}  {lr_val:>20.4f}  {rf_val:>16.4f}  {winner:>18s}")

    print("=" * 60)

    print("\nSample predictions (Random Forest) vs actuals:")
    rf_predictions.select("tip_amount", "prediction").show(10, truncate=False)

    spark.stop()
    print("Done - Spark session stopped.")


if __name__ == "__main__":
    main()

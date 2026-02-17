"""
Airflow DAG: NYC Taxi & Weather Data Pipeline (Phase 2)

This DAG orchestrates the full Phase 2 pipeline:
  1. Download NYC Yellow Taxi Parquet files from TLC CDN
  2. Fetch NOAA daily weather data for Central Park
  3. Upload raw data to Google Cloud Storage
  4. Sample taxi data and load into MongoDB Atlas
  5. Load weather data into MongoDB Atlas
  6. Run $lookup aggregation to create enriched_trips (data fusion)
  7. Run aggregation to compute tip_by_weather stats
  8. Run aggregation to compute hourly_trip_stats

Schedule: Manual trigger (or change to "@weekly" for production).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "nyc-taxi-weather-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def task_download_taxi(**context):
    """Download 12 monthly Parquet files from TLC CDN."""
    from utils.taxi_downloader import download_taxi_parquets
    from config.settings import TAXI_YEAR

    paths = download_taxi_parquets(year=TAXI_YEAR)
    context["ti"].xcom_push(key="parquet_paths", value=paths)
    return f"Downloaded {len(paths)} Parquet files"


def task_fetch_weather(**context):
    """Fetch weather data from NOAA and push to XCom."""
    from utils.weather_fetcher import fetch_weather_data, add_weather_category
    from config.settings import DATA_START_DATE, DATA_END_DATE

    df = fetch_weather_data(DATA_START_DATE, DATA_END_DATE)
    df = add_weather_category(df)
    tmp_path = "/tmp/weather_raw.csv"
    df.to_csv(tmp_path, index=False)
    context["ti"].xcom_push(key="weather_csv_path", value=tmp_path)
    return f"Fetched {len(df)} weather records"


def task_upload_taxi_to_gcs(**context):
    """Upload all Parquet files to GCS."""
    from utils.gcs_helpers import upload_file_to_gcs
    from config.settings import GCS_RAW_TAXI_PREFIX
    import os

    parquet_paths = context["ti"].xcom_pull(key="parquet_paths")
    uris = []
    for path in parquet_paths:
        fname = os.path.basename(path)
        uri = upload_file_to_gcs(path, f"{GCS_RAW_TAXI_PREFIX}{fname}")
        uris.append(uri)
    return f"Uploaded {len(uris)} Parquet files to GCS"


def task_upload_weather_to_gcs(**context):
    """Upload weather CSV to GCS."""
    import pandas as pd
    from utils.gcs_helpers import upload_df_to_gcs
    from config.settings import GCS_RAW_WEATHER_PREFIX

    csv_path = context["ti"].xcom_pull(key="weather_csv_path")
    df = pd.read_csv(csv_path)
    blob_name = f"{GCS_RAW_WEATHER_PREFIX}weather_daily.csv"
    uri = upload_df_to_gcs(df, blob_name, file_format="csv")
    return f"Uploaded to {uri}"


def task_load_taxi_to_mongo(**context):
    """Sample taxi data from Parquet files and load into MongoDB."""
    from utils.taxi_downloader import sample_for_mongodb
    from utils.mongo_helpers import load_taxi_data
    from config.settings import MONGO_SAMPLE_SIZE

    parquet_paths = context["ti"].xcom_pull(key="parquet_paths")
    df = sample_for_mongodb(parquet_paths, target_rows=MONGO_SAMPLE_SIZE)
    count = load_taxi_data(df)
    return f"Loaded {count} taxi docs into MongoDB"


def task_load_weather_to_mongo(**context):
    """Load weather data from temp CSV into MongoDB."""
    import pandas as pd
    from utils.mongo_helpers import load_weather_data

    csv_path = context["ti"].xcom_pull(key="weather_csv_path")
    df = pd.read_csv(csv_path)
    count = load_weather_data(df)
    return f"Loaded {count} weather docs into MongoDB"


def task_create_enriched_trips(**context):
    """Run the $lookup aggregation to fuse taxi + weather data."""
    from utils.mongo_helpers import create_enriched_trips
    count = create_enriched_trips()
    return f"Created {count} enriched trip documents"


def task_create_tip_aggregates(**context):
    """Run the aggregation for avg tip % by weather condition."""
    from utils.mongo_helpers import create_tip_by_weather_aggregate
    count = create_tip_by_weather_aggregate()
    return f"Created {count} tip-by-weather aggregate documents"


def task_create_hourly_stats(**context):
    """Run the aggregation for hourly trip statistics."""
    from utils.mongo_helpers import create_hourly_trip_stats
    count = create_hourly_trip_stats()
    return f"Created {count} hourly stats documents"


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="nyc_taxi_weather_pipeline",
    default_args=default_args,
    description="Download TLC taxi data + NOAA weather → GCS → MongoDB → Aggregations",
    schedule_interval=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["nyc-taxi", "weather", "phase2"],
) as dag:

    # --- Download / Fetch tasks (parallel) ---
    download_taxi = PythonOperator(
        task_id="download_taxi_parquets",
        python_callable=task_download_taxi,
    )

    fetch_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=task_fetch_weather,
    )

    # --- Upload to GCS (parallel) ---
    upload_taxi_gcs = PythonOperator(
        task_id="upload_taxi_to_gcs",
        python_callable=task_upload_taxi_to_gcs,
    )

    upload_weather_gcs = PythonOperator(
        task_id="upload_weather_to_gcs",
        python_callable=task_upload_weather_to_gcs,
    )

    # --- Load into MongoDB (parallel) ---
    load_taxi_mongo = PythonOperator(
        task_id="load_taxi_to_mongodb",
        python_callable=task_load_taxi_to_mongo,
    )

    load_weather_mongo = PythonOperator(
        task_id="load_weather_to_mongodb",
        python_callable=task_load_weather_to_mongo,
    )

    # --- Aggregation tasks (sequential) ---
    enrich_trips = PythonOperator(
        task_id="create_enriched_trips",
        python_callable=task_create_enriched_trips,
    )

    tip_aggregates = PythonOperator(
        task_id="create_tip_by_weather_aggregates",
        python_callable=task_create_tip_aggregates,
    )

    hourly_stats = PythonOperator(
        task_id="create_hourly_trip_stats",
        python_callable=task_create_hourly_stats,
    )

    # --- Task dependencies ---
    download_taxi >> upload_taxi_gcs >> load_taxi_mongo
    fetch_weather >> upload_weather_gcs >> load_weather_mongo

    [load_taxi_mongo, load_weather_mongo] >> enrich_trips

    enrich_trips >> [tip_aggregates, hourly_stats]

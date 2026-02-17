"""
Airflow DAG: NYC Taxi & Weather Data Pipeline (Phase 2)

This DAG orchestrates the full Phase 2 pipeline:
  1. Fetch NYC Yellow Taxi trip data from Socrata API
  2. Fetch NOAA daily weather data for Central Park
  3. Upload raw data to Google Cloud Storage
  4. Load raw data into MongoDB Atlas collections
  5. Run $lookup aggregation to create enriched_trips (data fusion)
  6. Run aggregation to compute tip_by_weather stats
  7. Run aggregation to compute hourly_trip_stats

Schedule: Weekly (or manual trigger for development).
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

def task_fetch_taxi(**context):
    """Fetch taxi data from Socrata and push to XCom."""
    from utils.taxi_fetcher import fetch_taxi_data
    from config.settings import DATA_START_DATE, DATA_END_DATE, TAXI_FETCH_LIMIT

    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    df = fetch_taxi_data(DATA_START_DATE, DATA_END_DATE, TAXI_FETCH_LIMIT)
    # Save to a temp file and push the path via XCom
    tmp_path = "/tmp/taxi_raw.csv"
    df.to_csv(tmp_path, index=False)
    context["ti"].xcom_push(key="taxi_csv_path", value=tmp_path)
    return f"Fetched {len(df)} taxi records"


def task_fetch_weather(**context):
    """Fetch weather data from NOAA and push to XCom."""
    from utils.weather_fetcher import fetch_weather_data, add_weather_category
    from config.settings import DATA_START_DATE, DATA_END_DATE

    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    df = fetch_weather_data(DATA_START_DATE, DATA_END_DATE)
    df = add_weather_category(df)
    tmp_path = "/tmp/weather_raw.csv"
    df.to_csv(tmp_path, index=False)
    context["ti"].xcom_push(key="weather_csv_path", value=tmp_path)
    return f"Fetched {len(df)} weather records"


def task_upload_taxi_to_gcs(**context):
    """Upload raw taxi CSV to GCS."""
    import pandas as pd
    from utils.gcs_helpers import upload_df_to_gcs
    from config.settings import GCS_RAW_TAXI_PREFIX

    csv_path = context["ti"].xcom_pull(key="taxi_csv_path")
    df = pd.read_csv(csv_path)
    blob_name = f"{GCS_RAW_TAXI_PREFIX}taxi_trips.csv"
    uri = upload_df_to_gcs(df, blob_name, file_format="csv")
    context["ti"].xcom_push(key="taxi_gcs_uri", value=uri)
    return f"Uploaded to {uri}"


def task_upload_weather_to_gcs(**context):
    """Upload raw weather CSV to GCS."""
    import pandas as pd
    from utils.gcs_helpers import upload_df_to_gcs
    from config.settings import GCS_RAW_WEATHER_PREFIX

    csv_path = context["ti"].xcom_pull(key="weather_csv_path")
    df = pd.read_csv(csv_path)
    blob_name = f"{GCS_RAW_WEATHER_PREFIX}weather_daily.csv"
    uri = upload_df_to_gcs(df, blob_name, file_format="csv")
    context["ti"].xcom_push(key="weather_gcs_uri", value=uri)
    return f"Uploaded to {uri}"


def task_load_taxi_to_mongo(**context):
    """Load taxi data from temp CSV into MongoDB."""
    import pandas as pd
    from utils.mongo_helpers import load_taxi_data

    csv_path = context["ti"].xcom_pull(key="taxi_csv_path")
    df = pd.read_csv(csv_path, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
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
    description="Ingest NYC taxi & weather data → GCS → MongoDB → Aggregations",
    schedule_interval=None,  # Manual trigger; change to "@weekly" for auto
    start_date=datetime(2026, 2, 1),
    catchup=False,
    tags=["nyc-taxi", "weather", "phase2"],
) as dag:

    # --- Fetch tasks (run in parallel) ---
    fetch_taxi = PythonOperator(
        task_id="fetch_taxi_data",
        python_callable=task_fetch_taxi,
    )

    fetch_weather = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=task_fetch_weather,
    )

    # --- Upload to GCS (run in parallel) ---
    upload_taxi_gcs = PythonOperator(
        task_id="upload_taxi_to_gcs",
        python_callable=task_upload_taxi_to_gcs,
    )

    upload_weather_gcs = PythonOperator(
        task_id="upload_weather_to_gcs",
        python_callable=task_upload_weather_to_gcs,
    )

    # --- Load into MongoDB (run in parallel) ---
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
    # Fetch → Upload to GCS → Load into MongoDB (parallel paths for taxi & weather)
    fetch_taxi >> upload_taxi_gcs >> load_taxi_mongo
    fetch_weather >> upload_weather_gcs >> load_weather_mongo

    # Both collections must be loaded before we run the $lookup join
    [load_taxi_mongo, load_weather_mongo] >> enrich_trips

    # Aggregations run after enrichment
    enrich_trips >> [tip_aggregates, hourly_stats]

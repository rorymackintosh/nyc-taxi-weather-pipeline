# NYC Taxi Tipping & Weather Impact Pipeline

Distributed data pipeline analyzing the relationship between weather conditions and NYC taxi tipping behavior. We ingest taxi trip data and historical weather data, store them in MongoDB, process them using Apache Spark, and develop machine learning models to predict tip amounts.

Built with **Apache Airflow**, **Google Cloud Storage**, **MongoDB Atlas**, **Apache Spark** (SparkSQL + MLlib).

---

## Getting Started (for teammates)

### 1. Clone the repo

```bash
git clone https://github.com/rorymackintosh/nyc-taxi-weather-pipeline.git
cd nyc-taxi-weather-pipeline
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Set up your `.env` file

```bash
cp .env.example .env
```

Then fill in the credentials. Ask me (Rory) for the shared values:
- `NOAA_API_TOKEN` — free token from NOAA
- `MONGO_URI` — our shared MongoDB Atlas connection string
- `GCP_PROJECT_ID`, `GCS_BUCKET_NAME` — our GCP project info
- `GOOGLE_APPLICATION_CREDENTIALS` — path to the GCP service account key JSON

The `.env` file is gitignored so your secrets stay local.

### 4. Run the full Phase 2 pipeline

```bash
python scripts/run_full_pipeline.py
```

This does everything end-to-end:
1. Downloads 12 monthly Parquet files from the NYC TLC website (~600 MB)
2. Fetches 2023 weather data from the NOAA API
3. Uploads all raw data to our GCS bucket
4. Samples 500K taxi records and loads them into MongoDB
5. Loads 365 days of weather data into MongoDB
6. Runs the MongoDB aggregation pipelines (data fusion + analytics)
7. Verifies everything is in place

First run takes ~5 min (mostly downloading Parquet files). Subsequent runs skip the download since the files are cached locally in `data/`.

### 5. Run Phase 3 Spark analysis (SparkSQL benchmark)

```bash
python scripts/spark_analysis.py
```

Loads taxi and weather data from MongoDB Atlas into Spark DataFrames, runs SparkSQL queries, and benchmarks query performance against MongoDB's aggregation framework.

### 6. Run Phase 3 ML models

```bash
python scripts/run_model.py
```

Trains Linear Regression (baseline) and Random Forest models to predict tip_amount. Outputs RMSE, MAE, R² for both models with a side-by-side comparison.

### 7. Generate visualizations and stats

```bash
python scripts/generate_visualizations.py
```

Produces 8 charts in `outputs/` plus a `key_stats.txt` summary.

### 8. Run MongoDB queries

```bash
python scripts/run_queries.py
```

Demonstrates 10 queries on both original and aggregated collections.

---

## How the pipeline works

### Data Sources

| Dataset | Source | Volume |
|---------|--------|--------|
| **NYC Yellow Taxi Trips (2023)** | [NYC TLC Parquet files](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | ~38 million records (12 monthly files, 606 MB) |
| **NYC Weather (Central Park)** | [NOAA CDO API](https://www.ncei.noaa.gov/cdo-web/) — Station USW00094728 | 365 daily observations |

### Architecture

```
NYC TLC Website                NOAA API
(12 Parquet files)             (weather data)
       |                            |
       v                            v
  [ Download ]               [ Fetch API ]
       |                            |
       v                            v
  Google Cloud Storage         Google Cloud Storage
  raw-taxi/*.parquet           raw-weather/weather_daily.csv
       |                            |
       v                            v
  MongoDB Atlas                MongoDB Atlas
  taxi_trips (500K sample)     weather_daily (365 days)
       |                            |
       +----------+  $lookup  +-----+
                  |  (join)   |
                  v           v
            enriched_trips (temporary)
                  |
         +--------+--------+
         v                  v
   tip_by_weather     hourly_trip_stats
   (7 groups)         (146 combos)
         |                  |
         +--------+---------+
                  |
                  v
        Apache Spark (Phase 3)
        ├── SparkSQL queries + MongoDB benchmark
        └── ML: Linear Regression + Random Forest
```

The full 38M-record dataset lives in GCS as Parquet files. We load a random 500K sample into MongoDB for aggregations and queries (fits the free tier). The `enriched_trips` collection is created temporarily for the aggregation pipelines, then dropped to save storage. In Phase 3, Spark reads directly from MongoDB Atlas via the Spark Connector.

### Airflow DAG

The Airflow DAG (`nyc_taxi_weather_pipeline`) orchestrates 11 tasks across both phases:

```
download_taxi → upload_taxi_gcs → load_taxi_mongo ─┐
                                                    ├→ enrich_trips → [tip_aggregates, hourly_stats] → spark_sql_analysis → spark_ml_modeling
fetch_weather → upload_weather_gcs → load_weather_mongo ─┘
```

### MongoDB Collections

| Collection | Records | Description |
|------------|---------|-------------|
| `taxi_trips` | ~500,000 | Random sample of 2023 yellow taxi trips (from 38M total) |
| `weather_daily` | 365 | Daily weather: precipitation, snow, temp for Central Park |
| `tip_by_weather` | 7 | Avg tip percentage grouped by weather condition |
| `hourly_trip_stats` | 146 | Avg fare, tip, distance by hour and weather condition |

### Weather Categories

Trips are categorized by weather condition based on daily observations:

| Category | Rule |
|----------|------|
| Snow | Snowfall > 0 |
| Heavy Rain | Precipitation > 0.5" |
| Light Rain | Precipitation > 0 |
| Extreme Heat | Max temp >= 90°F |
| Freezing | Max temp <= 32°F |
| Clear/Normal | Everything else |

### ML Model Results

| Metric | Linear Regression | Random Forest | Winner |
|--------|-------------------|---------------|--------|
| RMSE | 3.04 | 2.47 | Random Forest |
| MAE | 1.70 | 1.20 | Random Forest |
| R² | 0.46 | 0.64 | Random Forest |

Top features: fare_amount (38%), payment_type (35%), trip_distance (22%).

---

## Project Structure

```
├── .env.example                  # Template for env vars (safe to commit)
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yaml           # Local Airflow setup (optional)
│
├── config/
│   └── settings.py               # All config reads from .env
│
├── dags/
│   ├── nyc_taxi_weather_dag.py   # Airflow DAG (full pipeline, 11 tasks)
│   └── utils/
│       ├── taxi_downloader.py    # Downloads TLC Parquet files + samples for MongoDB
│       ├── weather_fetcher.py    # Fetches weather from NOAA API
│       ├── gcs_helpers.py        # GCS upload/download
│       └── mongo_helpers.py      # MongoDB loading + aggregation pipelines
│
├── scripts/
│   ├── run_full_pipeline.py      # One command to run Phase 2 pipeline
│   ├── spark_analysis.py         # Phase 3: SparkSQL queries + MongoDB benchmark
│   ├── run_model.py              # Phase 3: Linear Regression + Random Forest ML
│   ├── run_aggregations.py       # Run MongoDB aggregations standalone
│   ├── run_queries.py            # Demo queries on original + aggregate data
│   ├── generate_visualizations.py # Generate charts and stats
│   └── verify_data.py            # Check collection status
│
├── notebook/
│   ├── data_load.ipynb           # Data upload to GCS (exploratory)
│   ├── data_cleaning.ipynb       # Data cleaning notebook (exploratory)
│   └── data_load_dag.py          # Notebook helper script
│
├── outputs/                      # Generated charts and stats
│   ├── 01_temp_vs_tip.png
│   ├── 02_hourly_tip_by_weather.png
│   ├── 03_precipitation_vs_tip.png
│   ├── 04_monthly_trends.png
│   ├── 05_distance_by_weather.png
│   ├── 06_weekend_vs_weekday.png
│   ├── 07_trip_heatmap.png
│   ├── 08_fare_premium.png
│   └── key_stats.txt
│
└── data/                         # Local cache of Parquet files (gitignored)
```

---

## Phase 2 Checklist (Task 2 — Due Feb 21)

- [x] Load data into GCS (12 Parquet files + weather CSV)
- [x] Import data into MongoDB collections (`taxi_trips`, `weather_daily`)
- [x] Create new aggregated datasets (`tip_by_weather`, `hourly_trip_stats`)
- [x] Store aggregates in separate MongoDB collections
- [x] Query data in MongoDB (both original and aggregated)
- [x] Airflow DAG orchestrating the full pipeline

## Phase 3 Checklist (Task 3 — Due Mar 14)

- [x] Create Spark DataFrames from MongoDB Atlas data
- [x] Run SparkSQL queries over the DataFrames
- [x] Train ML models (Linear Regression + Random Forest) to predict tip amount
- [x] Benchmark MongoDB aggregation vs SparkSQL performance
- [x] Airflow DAG updated to orchestrate Spark + ML tasks
- [x] Final report

---

## Team

Distributed Data Systems — Spring 2026

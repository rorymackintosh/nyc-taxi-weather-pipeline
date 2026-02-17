# NYC Taxi Tipping & Weather Impact Pipeline

A distributed data pipeline that analyzes the relationship between weather conditions and NYC taxi tipping behavior. Built with **Apache Airflow**, **Google Cloud Storage**, **MongoDB Atlas**, and **Apache Spark**.

## Project Overview

This pipeline ingests NYC Yellow Taxi trip data and NOAA daily weather data, fuses them by date, and produces aggregated analytics to explore how weather affects tipping patterns.

### Data Sources

| Dataset | Source | Format | Key Fields |
|---------|--------|--------|------------|
| NYC Yellow Taxi Trips | [NYC Open Data (Socrata API)](https://data.cityofnewyork.us/) | JSON | `tpep_pickup_datetime`, `trip_distance`, `fare_amount`, `tip_amount`, `PULocationID` |
| NYC Weather (Central Park) | [NOAA CDO API](https://www.ncei.noaa.gov/cdo-web/) (Station USW00094728) | JSON | `DATE`, `PRCP`, `SNOW`, `SNWD`, `TMAX`, `TMIN` |

### Pipeline Architecture

```
┌──────────────┐    ┌──────────────┐
│  Socrata API │    │   NOAA API   │
│  (Taxi Data) │    │ (Weather)    │
└──────┬───────┘    └──────┬───────┘
       │                   │
       ▼                   ▼
┌──────────────────────────────────┐
│        Apache Airflow DAG        │
│   nyc_taxi_weather_pipeline      │
└──────┬───────────────────┬───────┘
       │                   │
       ▼                   ▼
┌──────────────┐    ┌──────────────┐
│     GCS      │    │     GCS      │
│  raw-taxi/   │    │ raw-weather/ │
└──────┬───────┘    └──────┬───────┘
       │                   │
       ▼                   ▼
┌──────────────────────────────────┐
│         MongoDB Atlas            │
│  ┌────────────┐ ┌──────────────┐ │
│  │ taxi_trips  │ │weather_daily │ │
│  └─────┬──────┘ └──────┬───────┘ │
│        │    $lookup     │         │
│        ▼    (join)      ▼         │
│  ┌─────────────────────────────┐ │
│  │      enriched_trips         │ │
│  └─────────┬───────────────────┘ │
│            │                     │
│   ┌───────┴────────┐            │
│   ▼                ▼            │
│ ┌──────────┐ ┌──────────────┐   │
│ │tip_by_   │ │hourly_trip_  │   │
│ │weather   │ │stats         │   │
│ └──────────┘ └──────────────┘   │
└──────────────────────────────────┘
```

## MongoDB Collections

| Collection | Description |
|------------|-------------|
| `taxi_trips` | Raw taxi trip records from NYC TLC |
| `weather_daily` | Daily weather observations from NOAA |
| `enriched_trips` | Taxi trips joined with weather via `$lookup` on date |
| `tip_by_weather` | Avg tip percentage grouped by weather condition |
| `hourly_trip_stats` | Avg fare, tip, distance by hour and weather |

## Quick Start

### Prerequisites

- Python 3.10+
- Docker & Docker Compose (for local Airflow)
- A GCP project with a GCS bucket
- A MongoDB Atlas cluster
- API tokens for NYC Open Data (Socrata) and NOAA CDO

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/nyc-taxi-weather-pipeline.git
cd nyc-taxi-weather-pipeline
```

### 2. Set up environment variables

```bash
cp .env.example .env
# Edit .env with your credentials (API tokens, MongoDB URI, GCS config)
```

### 3. Install dependencies (for running scripts locally)

```bash
pip install -r requirements.txt
```

### 4. Run with Docker Compose (Airflow)

```bash
docker-compose up -d
```

Open [http://localhost:8080](http://localhost:8080) (user: `airflow` / pass: `airflow`).

Trigger the `nyc_taxi_weather_pipeline` DAG from the Airflow UI.

### 5. Or run scripts manually

```bash
# After data is loaded into MongoDB:
python scripts/run_aggregations.py   # Create aggregated collections
python scripts/run_queries.py        # Run sample queries
python scripts/verify_data.py        # Check collection status
```

## Project Structure

```
├── .env.example              # Template for environment variables
├── .gitignore
├── README.md
├── requirements.txt
├── docker-compose.yaml       # Local Airflow setup
├── config/
│   ├── __init__.py
│   └── settings.py           # Centralized config (reads from env)
├── dags/
│   ├── nyc_taxi_weather_dag.py   # Main Airflow DAG
│   └── utils/
│       ├── __init__.py
│       ├── taxi_fetcher.py       # Socrata API client
│       ├── weather_fetcher.py    # NOAA CDO API client
│       ├── gcs_helpers.py        # GCS upload/download
│       └── mongo_helpers.py      # MongoDB load + aggregations
└── scripts/
    ├── run_aggregations.py       # Manual aggregation runner
    ├── run_queries.py            # MongoDB query demos
    └── verify_data.py            # Data verification utility
```

## Phase 2 Deliverables Checklist

- [x] Load data into GCS (Google Cloud Storage)
- [x] Import data into MongoDB collections (`taxi_trips`, `weather_daily`)
- [x] Create new datasets via aggregation (`enriched_trips`, `tip_by_weather`, `hourly_trip_stats`)
- [x] Store aggregates in separate MongoDB collections
- [x] Query data in MongoDB (both original and aggregated)
- [x] Airflow DAG orchestrating the full pipeline

## Team

NYC Taxi & Weather Pipeline Team - Distributed Data Systems, Spring 2026

## License

For academic use only.

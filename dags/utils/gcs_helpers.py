"""
Google Cloud Storage helper functions.

Upload DataFrames as CSV/JSON to GCS and download them back.
"""

import io
import logging

import pandas as pd
from google.cloud import storage

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from config.settings import GCS_BUCKET_NAME, GCP_PROJECT_ID

logger = logging.getLogger(__name__)


def get_gcs_client() -> storage.Client:
    """Return an authenticated GCS client."""
    return storage.Client(project=GCP_PROJECT_ID)


def upload_df_to_gcs(
    df: pd.DataFrame,
    destination_blob: str,
    file_format: str = "csv",
) -> str:
    """
    Upload a pandas DataFrame to a GCS blob.

    Args:
        df:               DataFrame to upload.
        destination_blob: Full blob path (e.g. 'raw-taxi/taxi_2024-01.csv').
        file_format:      'csv' or 'json'.

    Returns:
        GCS URI string (gs://bucket/blob).
    """
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob)

    if file_format == "csv":
        content = df.to_csv(index=False)
        content_type = "text/csv"
    else:
        content = df.to_json(orient="records", lines=True, date_format="iso")
        content_type = "application/json"

    blob.upload_from_string(content, content_type=content_type)
    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{destination_blob}"
    logger.info("Uploaded %d rows to %s", len(df), gcs_uri)
    return gcs_uri


def download_df_from_gcs(
    source_blob: str,
    file_format: str = "csv",
) -> pd.DataFrame:
    """
    Download a blob from GCS and return it as a pandas DataFrame.

    Args:
        source_blob: Full blob path inside the bucket.
        file_format: 'csv' or 'json'.

    Returns:
        pandas DataFrame.
    """
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(source_blob)

    content = blob.download_as_text()

    if file_format == "csv":
        df = pd.read_csv(io.StringIO(content))
    else:
        df = pd.read_json(io.StringIO(content), lines=True)

    logger.info("Downloaded %d rows from gs://%s/%s", len(df), GCS_BUCKET_NAME, source_blob)
    return df


def upload_file_to_gcs(
    local_path: str,
    destination_blob: str,
) -> str:
    """
    Upload a local file (e.g. Parquet) directly to a GCS blob.

    Args:
        local_path:       Path to the local file.
        destination_blob: Full blob path in the bucket.

    Returns:
        GCS URI string (gs://bucket/blob).
    """
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob)

    blob.upload_from_filename(local_path)
    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{destination_blob}"
    size_mb = os.path.getsize(local_path) / (1024 * 1024)
    logger.info("Uploaded %s (%.1f MB) to %s", local_path, size_mb, gcs_uri)
    return gcs_uri


def list_blobs(prefix: str) -> list[str]:
    """List all blob names under a given prefix."""
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    return [b.name for b in bucket.list_blobs(prefix=prefix)]

import io
import time
import socket
import os
import re
import tempfile
from collections import defaultdict
from typing import Dict, List

from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from http.client import IncompleteRead
from googleapiclient.errors import HttpError

# ==========================================
# CONFIGURATION
# ==========================================
SERVICE_ACCOUNT_FILE = "/Users/joannasanchez/Desktop/gcp_env/path"

GDRIVE_FOLDER_ID = "1o-mDbVPcXfplRIdJwto6P_PYWCaqSlxP"

GCS_BUCKET_NAME = "hmda-post2017"
GCS_TARGET_PREFIX = "bronze/historical_data"

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/cloud-platform",
]

# ==========================================
# SPARK SESSION BUILDER
# ==========================================
def get_local_spark_session(
    app_name: str = "HMDA_Bronze_EL", keyfile: str = SERVICE_ACCOUNT_FILE
) -> SparkSession:
    """Configures Spark with GCS Hadoop connector for direct GCS reads/writes."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[8]")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # --- Network & Execution Safeguards ---
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        # --- GCS Connector Settings ---
        .config(
            "spark.jars.packages",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22",
        )
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.fs.gs.auth.service.account.enable", "true")
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", keyfile)
        # --- GCS Resilient Streaming Buffer Tuning ---
        .config("spark.hadoop.fs.gs.outputstream.upload.chunk.size", "67108864")  # 64MB streaming chunks
        .config("spark.hadoop.fs.gs.outputstream.upload.cache.size", "134217728") # 128MB in-memory buffer
        .config("spark.hadoop.fs.gs.http.max.retry", "5")
        .getOrCreate()
    )

# ==========================================
# EXTRACT: GOOGLE DRIVE UTILITIES
# ==========================================
def get_drive_service(creds):
    return build("drive", "v3", credentials=creds)


def clean_folder_id(folder_id_or_url: str) -> str:
    match = re.search(r"folders/([a-zA-Z0-9-_]+)", folder_id_or_url)
    return match.group(1) if match else folder_id_or_url


def list_csv_files_in_folder(service, folder_id: str) -> List[Dict]:
    """Queries the Google Drive API to list all non-trashed CSV files inside a specified folder."""
    # Normalize the folder ID in case a full Google Drive URL was passed instead of a raw ID
    clean_id = clean_folder_id(folder_id)

    # Build the Google Drive API search query filter:
    # 1. '{clean_id}' in parents: Only search directly inside this specific folder
    # 2. mimeType != '...': Exclude subfolders so only actual files are returned
    # 3. trashed = false: Ignore files currently in the Google Drive trash/bin
    # 4. name contains '.csv': Filter for files with '.csv' in their filename
    query = (
        f"'{clean_id}' in parents and mimeType != 'application/vnd.google-apps.folder' "
        f"and trashed = false and name contains '.csv'"
    )

    # Execute the search request against the Drive v3 files resource:
    # - q: The filter criteria defined above
    # - fields: Projection mask requesting only necessary attributes (id, name, createdTime) to save bandwidth
    # - pageSize=1000: Fetch up to the maximum allowed 1,000 file entries in a single request
    results = (
        service.files()
        .list(q=query, fields="files(id, name, createdTime)", pageSize=1000)
        .execute()
    )

    # Return the list of file metadata dictionaries, defaulting to an empty list if no files match
    return results.get("files", [])


def download_file_to_disk(service, file_id, destination_path, chunk_size_mb=50, max_retries=5):
    """
    Downloads a large file from Google Drive in explicit chunk slices
    with automated retry handling for IncompleteRead, timeouts, and network resets.
    """
    request = service.files().get_media(fileId=file_id)
    chunk_bytes = chunk_size_mb * 1024 * 1024  # 50MB chunks
    
    with open(destination_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=chunk_bytes)
        done = False
        start_time = time.time()
        
        while not done:
            retry_count = 0
            while retry_count < max_retries:
                try:
                    status, done = downloader.next_chunk()
                    if status:
                        pct = int(status.progress() * 100)
                        elapsed = time.time() - start_time
                        print(f"   [Drive Download] {pct}% complete ({elapsed:.1f}s elapsed)...", flush=True)
                    break  # Chunk downloaded successfully, exit inner retry loop
                except (IncompleteRead, socket.timeout, ConnectionResetError, HttpError) as err:
                    retry_count += 1
                    wait_seconds = 2 ** retry_count
                    print(f"\n   [Network Warning] Interrupted ({type(err).__name__}). Retrying chunk in {wait_seconds}s (Attempt {retry_count}/{max_retries})...", flush=True)
                    time.sleep(wait_seconds)
                    if retry_count >= max_retries:
                        raise RuntimeError(f"Failed to download file {file_id} after {max_retries} retries due to persistent network drops.") from err

    print(f"   Download verified -> {destination_path}", flush=True)

def extract_year_from_filename(file_name: str) -> str:
    match = re.search(r"(19\d{2}|20\d{2})", file_name)
    return match.group(0) if match else "unknown_year"

# ==========================================
# PIPELINE EXECUTION
# ==========================================
def run_bronze_elt_pipeline():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    drive_service = get_drive_service(creds)
    spark = get_local_spark_session(keyfile=SERVICE_ACCOUNT_FILE)

    print("Fetching file list from Google Drive...")
    files = list_csv_files_in_folder(drive_service, GDRIVE_FOLDER_ID)
    if not files:
        print("No CSV files located.")
        spark.stop()
        return

    print(f"Found {len(files)} CSV file(s) to process.")

# Iterate over the retrieved Drive files one by one, tracking the loop index starting at 1 for logging
    for idx, file_info in enumerate(files, start=1):
        # Extract the original file name (e.g., 'hmda_2020_data.csv') from Google Drive metadata
        file_name = file_info["name"]

        # Extract the unique Google Drive file ID used by the Drive API to fetch the contents
        file_id = file_info["id"]

        # Parse the 4-digit year from the filename using regex to determine the GCS partition path
        year = extract_year_from_filename(file_name)

        # Strip the file extension (e.g., '.csv') to get the base stem (e.g., 'hmda_2020_data')
        # for naming the downstream Parquet dataset directory cleanly
        file_base_name = os.path.splitext(file_name)[0]

        # Log current batch progress showing index, total count, file name, and parsed year
        print(
            f"\n[{idx}/{len(files)}] Processing: {file_name} (Detected Year: {year})"
        )

        # Create an isolated temporary directory on disk that automatically deletes its contents
        # when this context block exits, preventing the local disk from filling up
        with tempfile.TemporaryDirectory(
            prefix="gdrive_download_"
        ) as temp_dir:
            # Construct the absolute path where the downloaded CSV will temporarily live
            local_csv_path = os.path.join(temp_dir, file_name)

            # ==========================================================
            # STEP 1: Download File from Google Drive to Local Disk
            # ==========================================================
            print("  - Downloading from Drive...")
            # Stream the CSV bytes in chunks from Google Drive directly to the temporary path on disk
            download_file_to_disk(drive_service, file_id, local_csv_path)

            # ==========================================================
            # STEP 2: Read CSV into a PySpark DataFrame
            # ==========================================================
            print("  - Reading CSV into PySpark DataFrame...")
            df = (
                spark.read
                # Treat row 1 of the CSV as column headers
                .option("header", "true")
                # Best practice for Bronze EL: read every column as StringType rather than guessing
                # data types, preventing job failures caused by malformed records or dirty data
                .option("inferSchema", "false")
                # Read the file located at the temporary disk path into a distributed Spark DataFrame
                .csv(local_csv_path)
            )

            # ==========================================================
            # STEP 3: Append Bronze Audit & Partition Columns
            # ==========================================================
            df_transformed = (
                df
                # Add lineage tracking: records the original filename each row came from
                .withColumn("_source_file", F.lit(file_name))
                # Add ingestion timestamp: records exactly when the record was processed into Bronze
                .withColumn("_ingested_at", F.current_timestamp())
                # Add explicit year column matching the partition structure
                .withColumn("hmda_year", F.lit(year))
            )

            # ==========================================================
            # STEP 4: Write Directly to GCS in Snappy-Compressed Parquet
            # ==========================================================
            # Format target URI following Hive partitioning conventions:
            # gs://<bucket>/<prefix>/year=<YYYY>/<file_stem>/
            gcs_destination = (
                f"gs://{GCS_BUCKET_NAME}/{GCS_TARGET_PREFIX}/year={year}/{file_base_name}"
            )
            print(f"  - Writing Parquet to: {gcs_destination}")

            (
                df_transformed
                # Repartition across 8 cores to parallelize compression and network uploads.
                # Creates ~50-80MB part files—optimal for BigQuery and Silver-layer scans.
                .repartition(8)
                # Overwrite any existing files at this destination prefix to allow safe pipeline reruns
                .write.mode("overwrite")
                # Stream the DataFrame out as columnar Parquet directly to the GCS bucket
                .parquet(gcs_destination)
            )

            print(f"  - Completed: {file_name}")
            
            # Clean up the uncompressed CSV from local temp before the next annual file downloads
            if os.path.exists(local_csv_path):
                os.remove(local_csv_path)
                print(f"  - Cleaned up local spool: {local_csv_path}")

    # Shut down the JVM-backed SparkSession and release local CPU/RAM resources back to the OS
    spark.stop()
    print("\nAll files successfully converted to Parquet and uploaded to GCS.")


if __name__ == "__main__":
    run_bronze_elt_pipeline()

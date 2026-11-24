
# ============================================================
# TIMBER - Azure Source to Landing Generator
#
# Pipeline:
# Azure source -> Download -> Validate -> Split -> Upload -> Snowpipe
#
# Configuration:
# Snowflake CONTROL.PIPELINE_CONFIG
#
# Modes:
# INITIAL -> Full historical data, daily files
# DAILY   -> Reprocess last N calendar days
# ============================================================

import os
from pathlib import Path
from datetime import date, timedelta, datetime

import pandas as pd
import snowflake.connector

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 1. GENERAL CONFIGURATION
# ============================================================

STORAGE_ACCOUNT = "sttimber001"

SOURCE_CONTAINER = "source"
LANDING_CONTAINER = "landing"

SOURCE_DIR = Path("data/source")
OUTPUT_DIR = Path("data/landing")

MODE = "INITIAL"  # "INITIAL" or "DAILY"

REPROCESS_DAYS = 7

DOWNLOAD_FROM_AZURE = True
UPLOAD_TO_AZURE = True


# ============================================================
# 2. SNOWFLAKE CONFIGURATION
# ============================================================

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": "WOODWORKS_WH",
    "database": "WOODWORKS_DB",
    "schema": "CONTROL",
}


# ============================================================
# 3. RUN CONFIGURATION
# ============================================================

if MODE not in ("INITIAL", "DAILY"):
    raise ValueError("MODE must be 'INITIAL' or 'DAILY'.")

if REPROCESS_DAYS < 1:
    raise ValueError("REPROCESS_DAYS must be at least 1.")

today = date.today()
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

if MODE == "INITIAL":
    date_from = None
    date_to = None
else:
    date_from = today - timedelta(days=REPROCESS_DAYS - 1)
    date_to = today

RUN_OUTPUT_DIR = OUTPUT_DIR / f"{MODE}_{run_id}"
RUN_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 4. DATE FORMAT CONVERSION
# ============================================================

# Snowflake date formats -> Python/Pandas date formats

DATE_FORMAT_MAPPING = {
    "YYYY-MM-DD": "%Y-%m-%d",
    "YYYY-MM-DD HH24:MI:SS": "%Y-%m-%d %H:%M:%S",
}


def convert_date_format(snowflake_format):
    """
    Convert Snowflake date format into Python/Pandas format.
    """

    if snowflake_format not in DATE_FORMAT_MAPPING:
        raise ValueError(
            f"Unsupported Snowflake date format: {snowflake_format}"
        )

    return DATE_FORMAT_MAPPING[snowflake_format]


# ============================================================
# 5. GET PIPELINE CONFIGURATION FROM SNOWFLAKE
# ============================================================

def get_pipeline_config():
    """
    Fetch active pipeline configurations from Snowflake.
    """

    missing_settings = [
        key
        for key, value in SNOWFLAKE_CONFIG.items()
        if key in ("account", "user", "password") and not value
    ]

    if missing_settings:
        raise ValueError(
            "Missing Snowflake environment variables: "
            + ", ".join(missing_settings)
        )

    connection = None

    query = """
        SELECT
            SOURCE_SYSTEM,
            SOURCE_TABLE,
            SOURCE_FILE,
            LANDING_PATH,
            TARGET_SCHEMA,
            TARGET_TABLE,
            IS_ACTIVE,
            LOAD_TYPE,
            INCREMENTAL_COLUMN,
            PRIMARY_KEY_COLUMNS,
            DATE_COLUMN,
            DATE_FORMAT,
            ADDITIONAL_CONFIG
        FROM WOODWORKS_DB.CONTROL.PIPELINE_CONFIG
        WHERE IS_ACTIVE = TRUE
        ORDER BY CONFIG_ID;
    """

    try:
        connection = snowflake.connector.connect(
            **SNOWFLAKE_CONFIG
        )

        cursor = connection.cursor(
            snowflake.connector.DictCursor
        )

        try:
            cursor.execute(query)
            configurations = cursor.fetchall()
        finally:
            cursor.close()

        if not configurations:
            raise ValueError(
                "No active pipeline configurations found "
                "in WOODWORKS_DB.CONTROL.PIPELINE_CONFIG."
            )

        print(
            f"Loaded {len(configurations)} pipeline "
            "configurations from Snowflake."
        )

        return configurations

    except Exception as error:
        raise RuntimeError(
            f"Failed to fetch pipeline configuration: {error}"
        ) from error

    finally:
        if connection is not None:
            connection.close()


# ============================================================
# 6. AZURE CONNECTION
# ============================================================

credential = DefaultAzureCredential()

account_url = (
    f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
)

blob_service_client = BlobServiceClient(
    account_url=account_url,
    credential=credential,
)


# ============================================================
# 7. DOWNLOAD SOURCE FILES FROM AZURE
# ============================================================

def download_sources_from_azure(configurations):

    print("\n" + "=" * 60)
    print("DOWNLOADING SOURCE FILES FROM AZURE")
    print("=" * 60)

    source_container = blob_service_client.get_container_client(
        SOURCE_CONTAINER
    )

    SOURCE_DIR.mkdir(parents=True, exist_ok=True)

    missing_blobs = []

    for config in configurations:

        file_name = config["SOURCE_FILE"]

        blob_client = source_container.get_blob_client(file_name)

        if not blob_client.exists():
            missing_blobs.append(file_name)
            print(f"MISSING IN AZURE: {file_name}")
            continue

        local_path = SOURCE_DIR / file_name

        with open(local_path, "wb") as file:

            download_stream = blob_client.download_blob()

            file.write(download_stream.readall())

        print(f"Downloaded: {file_name}")

    if missing_blobs:
        raise FileNotFoundError(
            "Missing Azure source blobs: "
            + ", ".join(missing_blobs)
        )


# ============================================================
# 8. VALIDATE LOCAL SOURCE FILES
# ============================================================

def validate_source_files(configurations):

    print("\n" + "=" * 60)
    print("VALIDATING SOURCE FILES")
    print("=" * 60)

    missing_files = []
    missing_columns = []

    for config in configurations:

        file_name = config["SOURCE_FILE"]
        date_column = config["DATE_COLUMN"]

        source_path = SOURCE_DIR / file_name

        if not source_path.is_file():
            missing_files.append(file_name)
            print(f"MISSING FILE: {file_name}")
            continue

        source_columns = pd.read_csv(
            source_path,
            nrows=0
        ).columns

        if date_column not in source_columns:

            missing_columns.append(
                f"{file_name} -> {date_column}"
            )

            print(
                f"MISSING COLUMN: "
                f"{file_name} -> {date_column}"
            )

        else:

            print(
                f"OK: {file_name} -> {date_column}"
            )

        # Validate configured date format.
        convert_date_format(config["DATE_FORMAT"])

    if missing_files or missing_columns:

        problems = []

        if missing_files:
            problems.append(
                "Missing files: "
                + ", ".join(missing_files)
            )

        if missing_columns:
            problems.append(
                "Missing columns: "
                + ", ".join(missing_columns)
            )

        raise ValueError(
            "Input validation failed. "
            + " ".join(problems)
        )


# ============================================================
# 9. LOAD, PARSE AND VALIDATE DATA
# ============================================================

def load_source_data(configurations):

    loaded_data = {}

    for config in configurations:

        file_name = config["SOURCE_FILE"]
        date_column = config["DATE_COLUMN"]

        date_format = convert_date_format(
            config["DATE_FORMAT"]
        )

        source_path = SOURCE_DIR / file_name

        data = pd.read_csv(source_path)

        data[date_column] = pd.to_datetime(
            data[date_column],
            format=date_format,
            errors="coerce",
        )

        invalid_dates = data[date_column].isna().sum()

        if invalid_dates > 0:

            raise ValueError(
                f"{file_name}: found {invalid_dates} invalid dates "
                f"in {date_column}. Export aborted."
            )

        loaded_data[file_name] = data

        print(
            f"Loaded: {file_name} | "
            f"Rows: {len(data):,} | "
            f"Invalid dates: {invalid_dates}"
        )

    return loaded_data


# ============================================================
# 10. GENERATE DAILY CSV FILES
# ============================================================

def generate_daily_files(loaded_data, configurations):

    print("\n" + "=" * 60)
    print("GENERATING DAILY FILES")
    print("=" * 60)

    export_summary = {}

    # Index configurations by source file name.
    config_by_file = {
        config["SOURCE_FILE"]: config
        for config in configurations
    }

    for file_name, data in loaded_data.items():

        config = config_by_file[file_name]

        date_column = config["DATE_COLUMN"]

        date_format = convert_date_format(
            config["DATE_FORMAT"]
        )

        # Remove trailing slash from landing path.
        landing_folder = config["LANDING_PATH"].strip("/")

        # DAILY: filter last N calendar days.
        if MODE == "DAILY":

            data = data.loc[
                (data[date_column].dt.date >= date_from)
                & (data[date_column].dt.date <= date_to)
            ].copy()

        file_output_dir = (
            RUN_OUTPUT_DIR / landing_folder
        )

        file_output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        rows_exported = 0
        files_exported = 0

        date_groups = data.groupby(
            data[date_column].dt.strftime("%Y-%m-%d"),
            sort=True,
        )

        for date_value, daily_data in date_groups:

            if MODE == "INITIAL":

                output_file_name = (
                    f"{landing_folder}_{date_value}"
                    f"_i{run_id}.csv"
                )

            else:

                output_file_name = (
                    f"{landing_folder}_{date_value}"
                    f"_r{run_id}.csv"
                )

            output_path = (
                file_output_dir / output_file_name
            )

            daily_data.to_csv(
                output_path,
                index=False,
                date_format=date_format,
            )

            rows_exported += len(daily_data)
            files_exported += 1

        if rows_exported != len(data):

            raise ValueError(
                f"Row count mismatch in {file_name}: "
                f"source={len(data)}, "
                f"exported={rows_exported}"
            )

        export_summary[file_name] = {
            "landing_folder": landing_folder,
            "rows": rows_exported,
            "files": files_exported,
        }

        print(
            f"Generated: {landing_folder} | "
            f"Rows: {rows_exported:,} | "
            f"Files: {files_exported}"
        )

    return export_summary


# ============================================================
# 11. UPLOAD DAILY FILES TO AZURE LANDING
# ============================================================

def upload_to_azure(configurations):

    print("\n" + "=" * 60)
    print("UPLOADING FILES TO AZURE LANDING")
    print("=" * 60)

    landing_container = (
        blob_service_client.get_container_client(
            LANDING_CONTAINER
        )
    )

    uploaded_files = 0

    for config in configurations:

        landing_folder = config["LANDING_PATH"].strip("/")

        local_folder = (
            RUN_OUTPUT_DIR / landing_folder
        )

        for local_file in local_folder.glob("*.csv"):

            # Azure path MUST match the Snowpipe folder.
            # The local run directory is intentionally omitted.
            blob_name = (
                f"{landing_folder}/{local_file.name}"
            )

            blob_client = (
                landing_container.get_blob_client(
                    blob_name
                )
            )

            with open(local_file, "rb") as data:

                blob_client.upload_blob(
                    data,
                    overwrite=False
                )

            uploaded_files += 1

            print(
                f"Uploaded: {blob_name}"
            )

    print(
        f"\nUpload completed. "
        f"Files uploaded: {uploaded_files}"
    )

    return uploaded_files


# ============================================================
# 12. MAIN PIPELINE
# ============================================================

def main():

    print("=" * 60)
    print("TIMBER SOURCE TO LANDING PIPELINE")
    print("=" * 60)

    print(f"Mode:       {MODE}")
    print(f"Run ID:     {run_id}")
    print(f"Output:     {RUN_OUTPUT_DIR.resolve()}")

    if MODE == "DAILY":

        print(
            f"Date range: {date_from} -> {date_to}"
        )

    # Step 1: Get configuration from Snowflake.
    configurations = get_pipeline_config()

    print(
        f"\nActive source files: {len(configurations)}"
    )

    # Step 2: Download.
    if DOWNLOAD_FROM_AZURE:
        download_sources_from_azure(configurations)

    # Step 3: Validate.
    validate_source_files(configurations)

    # Step 4: Load data.
    loaded_data = load_source_data(configurations)

    # Step 5: Generate daily CSV files.
    export_summary = generate_daily_files(
        loaded_data,
        configurations
    )

    # Step 6: Upload only after successful generation.
    uploaded_files = 0

    if UPLOAD_TO_AZURE:
        uploaded_files = upload_to_azure(configurations)

    # Step 7: Final summary.
    total_rows = sum(
        item["rows"]
        for item in export_summary.values()
    )

    total_files = sum(
        item["files"]
        for item in export_summary.values()
    )

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETED")
    print("=" * 60)

    print(f"Mode:             {MODE}")
    print(f"Run ID:           {run_id}")
    print(f"Total rows:       {total_rows:,}")
    print(f"Files generated:  {total_files:,}")
    print(f"Files uploaded:   {uploaded_files}")
    print(f"Local output:     {RUN_OUTPUT_DIR.resolve()}")

    print("\nSummary per source:")

    for file_name, summary in export_summary.items():

        print(
            f"{file_name} -> "
            f"{summary['landing_folder']} | "
            f"{summary['rows']:,} rows | "
            f"{summary['files']} files"
        )

    print("=" * 60)


# ============================================================
# 13. EXECUTE
# ============================================================

# if __name__ == "__main__":
#    main()

if __name__ == "__main__":
    configurations = get_pipeline_config()

    download_sources_from_azure(configurations)
    validate_source_files(configurations)

    loaded_data = load_source_data(configurations)
    generate_daily_files(loaded_data, configurations)

    upload_to_azure(configurations)

# ============================================================
# TIMBER - Azure Source to Landing Generator
#
# Pipeline:
# Azure source -> Download -> Validate -> Split -> Upload -> Snowpipe
#
# Modes:
#   INITIAL -> Full historical data, daily files
#   DAILY   -> Reprocess last N calendar days
# ============================================================

from pathlib import Path
from datetime import date, timedelta, datetime

import pandas as pd

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


# ============================================================
# 1. GENERAL CONFIGURATION
# ============================================================

STORAGE_ACCOUNT = "sttimber001"

SOURCE_CONTAINER = "source"
LANDING_CONTAINER = "landing"

SOURCE_DIR = Path("data/source")
OUTPUT_DIR = Path("data/prepared")

MODE = "INITIAL"  # "INITIAL" or "DAILY"

REPROCESS_DAYS = 7

DOWNLOAD_FROM_AZURE = True
UPLOAD_TO_AZURE = True


# ============================================================
# 2. SOURCE FILE CONFIGURATION
# ============================================================

FILES_CONFIG = {
    "timber_anaplan_rmcpnl.csv": {
        "split_column": "ETL_TIMESTAMP",
        "date_format": "%Y-%m-%d %H:%M:%S",
        "landing_folder": "ANAPLAN_RMCPNL",
    },
    "timber_delivery_notes_v3_carriers.csv": {
        "split_column": "DELIVERY_DATE",
        "date_format": "%Y-%m-%d",
        "landing_folder": "DELIVERY_NOTES",
    },
    "timber_mixdesign_cost_daily.csv": {
        "split_column": "COST_DATE",
        "date_format": "%Y-%m-%d",
        "landing_folder": "MIXDESIGN_COST_DAILY",
    },
    "timber_sap_cash_contract_v2.csv": {
        "split_column": "ETL_TIMESTAMP",
        "date_format": "%Y-%m-%d %H:%M:%S",
        "landing_folder": "SAP_CASH_CONTRACT",
    },
    "timber_sap_inv_cost_v2_carriers.csv": {
        "split_column": "COST_DATE",
        "date_format": "%Y-%m-%d",
        "landing_folder": "SAP_INV_COST",
    },
    "timber_sap_inv_rev_v2.csv": {
        "split_column": "INVOICE_DATE",
        "date_format": "%Y-%m-%d",
        "landing_folder": "SAP_INV_REV",
    },
    "timber_sf_account.csv": {
        "split_column": "ETL_TIMESTAMP",
        "date_format": "%Y-%m-%d %H:%M:%S",
        "landing_folder": "SF_ACCOUNT",
    },
    "timber_sf_subscription_c_v2.csv": {
        "split_column": "LASTMODIFIEDDATE",
        "date_format": "%Y-%m-%d %H:%M:%S",
        "landing_folder": "SF_SUBSCRIPTION",
    },
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
# 4. AZURE CONNECTION
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
# 5. DOWNLOAD SOURCE FILES FROM AZURE
# ============================================================

def download_sources_from_azure():

    print("\n" + "=" * 60)
    print("DOWNLOADING SOURCE FILES FROM AZURE")
    print("=" * 60)

    source_container = blob_service_client.get_container_client(
        SOURCE_CONTAINER
    )

    SOURCE_DIR.mkdir(parents=True, exist_ok=True)

    missing_blobs = []

    for file_name in FILES_CONFIG:

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
# 6. VALIDATE LOCAL SOURCE FILES
# ============================================================

def validate_source_files():

    print("\n" + "=" * 60)
    print("VALIDATING SOURCE FILES")
    print("=" * 60)

    missing_files = []
    missing_columns = []

    for file_name, settings in FILES_CONFIG.items():

        source_path = SOURCE_DIR / file_name

        if not source_path.is_file():
            missing_files.append(file_name)
            print(f"MISSING FILE: {file_name}")
            continue

        date_column = settings["split_column"]

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
# 7. LOAD, PARSE AND VALIDATE DATA
# ============================================================

def load_source_data():

    loaded_data = {}

    for file_name, settings in FILES_CONFIG.items():

        source_path = SOURCE_DIR / file_name

        date_column = settings["split_column"]
        date_format = settings["date_format"]

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
# 8. GENERATE DAILY CSV FILES
# ============================================================

def generate_daily_files(loaded_data):

    print("\n" + "=" * 60)
    print("GENERATING DAILY FILES")
    print("=" * 60)

    export_summary = {}

    for file_name, data in loaded_data.items():

        settings = FILES_CONFIG[file_name]

        date_column = settings["split_column"]
        date_format = settings["date_format"]
        landing_folder = settings["landing_folder"]

        # DAILY: filter last N calendar days
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
# 9. UPLOAD DAILY FILES TO AZURE LANDING
# ============================================================

def upload_to_azure():

    print("\n" + "=" * 60)
    print("UPLOADING FILES TO AZURE LANDING")
    print("=" * 60)

    landing_container = (
        blob_service_client.get_container_client(
            LANDING_CONTAINER
        )
    )

    uploaded_files = 0

    for file_name, settings in FILES_CONFIG.items():

        landing_folder = settings["landing_folder"]

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
# 10. MAIN PIPELINE
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

    # Step 1: Download
    if DOWNLOAD_FROM_AZURE:
        download_sources_from_azure()

    # Step 2: Validate
    validate_source_files()

    # Step 3: Load data
    loaded_data = load_source_data()

    # Step 4: Generate daily CSV files
    export_summary = generate_daily_files(
        loaded_data
    )

    # Step 5: Upload only after successful generation
    uploaded_files = 0

    if UPLOAD_TO_AZURE:
        uploaded_files = upload_to_azure()

    # Step 6: Final summary
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
# 11. EXECUTE
# ============================================================

if __name__ == "__main__":
    main()

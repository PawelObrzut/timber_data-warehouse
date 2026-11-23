# Python Data Preparation and Azure File Loading

## 1. Overview

The Python component of the TIMBER project is responsible for preparing source CSV files and uploading them to Azure Blob Storage.

Instead of manually splitting and uploading files, the process is automated through a Python script:

```text
Azure Storage (source)
          |
          v
   Download CSV files
          |
          v
   Validate source data
          |
          v
   Split files by date
          |
          v
   Save prepared CSV files locally
          |
          v
   Upload to Azure (landing)
          |
          v
       Snowpipe
          |
          v
    Snowflake Bronze
```

The main script is:

`source-to-landing-generator.py`

It supports two execution modes:

- **INITIAL:** Processes the complete historical source data.
- **DAILY:** Processes only a configurable number of recent days.

---

## 2. Python Environment Setup

A dedicated virtual environment should be created for the project. This keeps project dependencies isolated from the system Python installation.

The project uses Python 3.14.

### Create a Virtual Environment

From the project root directory:

```bash
/opt/homebrew/opt/python@3.14/bin/python3.14 -m venv .venv
```

### Activate the Environment

```bash
source .venv/bin/activate
```

Verify that the correct Python installation is being used:

```bash
python --version
which python
```

Expected Python executable:

```text
/Users/<username>/Desktop/TIMBER/.venv/bin/python
```

Whenever working on the project, activate `.venv` before running Python commands.

To leave the environment:

```bash
deactivate
```

---

## 3. Install Python Dependencies

The pipeline requires the following libraries:

| Package | Purpose |
|---|---|
| `pandas` | Reading, validating, filtering and splitting CSV files |
| `azure-identity` | Authentication with Microsoft Azure |
| `azure-storage-blob` | Downloading and uploading files in Azure Blob Storage |

Install the dependencies:

```bash
python -m pip install pandas azure-identity azure-storage-blob
```

Verify installed packages:

```bash
python -m pip list
```

### Requirements File

To keep track of installed dependencies, generate a `requirements.txt` file:

```bash
python -m pip freeze > requirements.txt
```

This records the installed package versions.

When setting up the project on another machine, create and activate a virtual environment first, then install the recorded dependencies:

```bash
python -m pip install -r requirements.txt
```

Update `requirements.txt` whenever project dependencies change.

---

## 4. Azure Authentication and Access Control

The Python script uses `DefaultAzureCredential` to authenticate with Azure.

This allows the script to use the credentials from Azure CLI without storing account keys or passwords in the source code.

### Install Azure CLI

Install Azure CLI if it is not already available:

https://learn.microsoft.com/en-us/cli/azure/install-azure-cli

### Log in to Azure

```bash
az login
```

If multiple subscriptions are available, select the correct one:

```bash
az account set --subscription "<SUBSCRIPTION_ID>"
```

Verify the active subscription:

```bash
az account show --output table
```

### Configure Storage Permissions

The Azure account used by the script requires the following role:

**Storage Blob Data Contributor**

Navigate to:

**Azure Portal → Storage Account → Access Control (IAM) → Add role assignment**

1. Select `Storage Blob Data Contributor`.
2. Choose `User, group, or service principal`.
3. Select the account used with `az login`.
4. Assign the role at the Storage Account level.

This role provides the necessary Blob Storage data permissions for the project:

- Read source files.
- Upload prepared files to `landing`.
- Delete or move blobs when archive functionality is implemented.

Note that listing containers and reading blobs are different permissions. Successful `az storage container list` does not necessarily mean that blob access is configured.

### Verify Blob Access

List the files in `source`:

```bash
az storage blob list \
  --account-name sttimber001 \
  --container-name source \
  --auth-mode login \
  --output table
```

Count the files:

```bash
az storage blob list \
  --account-name sttimber001 \
  --container-name source \
  --auth-mode login \
  --query "length(@)" \
  --output tsv
```

Expected result:

```text
8
```

---

## 5. Project Directory Structure

The script uses local directories to organize downloaded source files and generated output.

```text
TIMBER/
│
├── .venv/
├── data/
│   ├── source/
│   │   └── Original downloaded CSV files
│   │
│   ├── prepared/
│   │   └── INITIAL_<run_id>/
│   │       ├── ANAPLAN_RMCPNL/
│   │       ├── DELIVERY_NOTES/
│   │       ├── MIXDESIGN_COST_DAILY/
│   │       ├── SAP_CASH_CONTRACT/
│   │       ├── SAP_INV_COST/
│   │       ├── SAP_INV_REV/
│   │       ├── SF_ACCOUNT/
│   │       └── SF_SUBSCRIPTION/
│   │
│   └── archive/
│
├── source-to-landing-generator.py
├── requirements.txt
└── ...
```

Each execution creates a separate output directory identified by its mode and run ID.

This prevents generated files from different executions from being mixed together.

The `data/source/` and `data/prepared/` directories contain working data and should generally be excluded from Git.

---

## 6. Configure the Generator

Open:

`source-to-landing-generator.py`

The script contains configuration parameters controlling how the pipeline operates.

### Execution Mode

```python
MODE = "INITIAL"
```

Available values:

| Mode | Description |
|---|---|
| `INITIAL` | Processes all available historical records |
| `DAILY` | Processes only the most recent configured number of days |

For daily processing, the number of days is controlled by:

```python
REPROCESS_DAYS = 7
```

This example means that the script processes the last seven calendar days, including today.

### Azure Configuration

The script uses the following settings:

```python
STORAGE_ACCOUNT = "sttimber001"

SOURCE_CONTAINER = "source"
LANDING_CONTAINER = "landing"

DOWNLOAD_FROM_AZURE = True
UPLOAD_TO_AZURE = True
```

These settings enable downloading source files and uploading generated files.

### Source File Configuration

Each source file is mapped to its corresponding date column, date format and destination folder.

For example:

```python
"timber_sap_inv_rev_v2.csv": {
    "split_column": "INVOICE_DATE",
    "date_format": "%Y-%m-%d",
    "landing_folder": "SAP_INV_REV"
}
```

The configuration must match the actual CSV column names and date formats.

---

## 7. Run the Generator

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Run the script:

```bash
python source-to-landing-generator.py
```

### INITIAL Mode

Use INITIAL when loading the complete historical dataset.

The script:

1. Downloads all configured source CSV files from Azure.
2. Validates that the required files and date columns exist.
3. Parses and validates the date values.
4. Splits each source file into separate daily CSV files.
5. Saves the generated files locally.
6. Uploads the generated files to Azure `landing`.

The generated filenames contain the processing run ID to distinguish separate executions.

Example:

```text
SAP_INV_REV/
├── SAP_INV_REV_2026-11-01_i20261123_153000.csv
├── SAP_INV_REV_2026-11-02_i20261123_153000.csv
└── SAP_INV_REV_2026-11-03_i20261123_153000.csv
```

### DAILY Mode

Use DAILY for subsequent processing runs.

Change the configuration:

```python
MODE = "DAILY"
```

The script processes only the most recent `REPROCESS_DAYS` calendar days.

For example:

```python
REPROCESS_DAYS = 7
```

The generated files receive a run-specific revision suffix:

```text
SAP_INV_REV_2026-11-23_r20261123_153000.csv
```

This allows the same business date to be processed again without reusing the previous filename.

**Important:** DAILY mode reprocesses a rolling date window. It is not an incremental change-data-capture mechanism. The destination tables and loading strategy must account for repeated dates to prevent duplicate records.

---

## 8. Azure Upload and Snowpipe Ingestion

Generated files are uploaded to the `landing` container.

The Azure blob path follows this structure:

```text
<landing_folder>/<filename>
```

For example:

```text
landing/SAP_INV_REV/SAP_INV_REV_2026-11-23_r20261123_153000.csv
```

The local run directory is not included in the Azure blob path.

This is important because Snowpipe listens for files in specific directories configured in Snowflake.

Once a file is uploaded:

1. Azure Blob Storage generates a `Blob Created` event.
2. Event Grid sends the event to the configured Storage Queue.
3. Snowflake receives the notification.
4. The appropriate Snowpipe loads the file into its Bronze table.

No manual `COPY INTO` command is required for normal automatic ingestion.

---

## 9. Verify the Ingestion

After the script finishes, verify that files were uploaded:

```bash
az storage blob list \
  --account-name sttimber001 \
  --container-name landing \
  --auth-mode login \
  --query "length(@)" \
  --output tsv
```

The result should be greater than zero.

Check Snowpipe status in Snowflake:

```sql
SELECT SYSTEM$PIPE_STATUS(
    'WOODWORKS_DB.BRONZE.PIPE_SAP_INV_REV'
);
```

Check recent ingestion history:

```sql
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'SAP_INV_REV',
        START_TIME => DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
    )
);
```

Once the Bronze tables have been recreated and ingestion has completed, verify the loaded row counts:

```sql
SELECT COUNT(*) FROM WOODWORKS_DB.BRONZE.SAP_INV_REV;
```

Repeat for the other source tables.

---

## 10. Future Pipeline Improvements

The current generator handles source downloading, validation, daily file generation and Azure upload.

The following capabilities are planned for subsequent development:

### Archive Process

Successfully processed files will be moved from `landing` to `archive`.

Files should only be archived after successful Snowflake ingestion and the required downstream processing.

### Audit Tables

The pipeline will record execution details in Snowflake audit tables, including:

- Pipeline run ID.
- Source file name.
- Processing start and completion timestamps.
- Processing mode (`INITIAL` or `DAILY`).
- Number of input and output rows.
- Upload and ingestion status.
- Error details, if applicable.

### Error Handling

Failed operations should be recorded and made available for investigation and retry.

The pipeline should not silently skip files or treat unsuccessful uploads as completed operations.

The long-term objective is to execute the complete process through a single command, with automated processing, monitoring, error handling and audit logging.
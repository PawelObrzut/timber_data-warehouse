# Azure Storage, Event Grid and Snowflake Notifications

## 1. Azure Storage Account

Azure Blob Storage is used as the central storage layer for the TIMBER data pipeline. It stores original source files, prepared files waiting for ingestion, and successfully processed files.

### Create a Storage Account

Navigate to **Azure Portal → Storage accounts → Create**.

Configure the following:

- **Subscription:** Select your Azure subscription.
- **Resource Group:** Select an existing resource group or create a new one.
- **Storage Account Name:** Specify a globally unique name.
- **Region:** Select the appropriate Azure region.
- **Performance:** Standard is sufficient for this project.
- **Redundancy:** Select the required replication option.

Review the configuration and create the Storage Account.

### Create Blob Containers

Navigate to:

**Storage Account → Data Storage → Containers → + Container**

Create the following containers:

| Container | Purpose |
|---|---|
| `source` | Original source CSV files |
| `landing` | Prepared daily CSV files waiting for Snowpipe ingestion |
| `archive` | Successfully processed files retained for historical reference |

The pipeline uses virtual directories inside `landing` to organize files by source table.

For example:

```text
landing/
├── ANAPLAN_RMCPNL/
├── DELIVERY_NOTES/
├── MIXDESIGN_COST_DAILY/
├── SAP_CASH_CONTRACT/
├── SAP_INV_COST/
├── SAP_INV_REV/
├── SF_ACCOUNT/
└── SF_SUBSCRIPTION/
```

Upload the original CSV files to the `source` container.

The `source` container should retain the original files. The pipeline reads from this container and generates prepared files in `landing`.

---

## 2. Azure Storage Queue

Azure Storage Queue is used to deliver storage events to Snowflake.

When a new blob is created in the `landing` container, Azure Event Grid publishes an event to the configured Storage Queue. Snowflake reads these notifications and triggers the corresponding Snowpipe.

### Create a Storage Queue

Navigate to:

**Storage Account → Data Storage → Queues → + Queue**

- Specify the queue name, for example `timber-snowflake-queue`.
- Create the queue.

The queue will have a URL associated with it. This URL is required later when configuring the Snowflake notification integration.

The queue stores notifications about new blobs. It does not store the actual CSV files.

---

## 3. Azure Event Grid

Azure Event Grid monitors storage events and routes notifications to the Storage Queue.

### Register the Event Grid Resource Provider

If Event Grid is not registered in your Azure subscription, registration is required before creating the event subscription.

1. Navigate to **Azure Portal → Subscriptions**.
2. Select the subscription used by the project.
3. Open **Resource providers**.
4. Search for `Microsoft.EventGrid`.
5. Select it and click **Register**.
6. Refresh the page until the registration status becomes `Registered`.

### Create an Event Subscription

Navigate to:

**Storage Account → Events → + Event Subscription**

Configure the subscription:

- **Name:** Specify an event subscription name.
- **Event Schema:** Event Grid Schema.
- **Event Types:** Select only `Blob Created`.
- **Endpoint Type:** Storage Queue.
- **Endpoint:** Select the subscription, storage account and previously created queue.

Create the event subscription.

The resulting flow is:

```text
Azure Blob Storage (landing)
          |
          | Blob Created event
          v
      Event Grid
          |
          v
    Storage Queue
          |
          v
  Snowflake Notification
          |
          v
       Snowpipe
          |
          v
    Bronze Tables
```

This allows Snowflake to react to new files without requiring a manual `COPY INTO` command for every upload.

---

## 4. Configure Snowflake Storage Integration

Snowflake needs permission to access the Azure Storage Account.

The storage integration establishes the connection and provides the Azure consent information required to authorize Snowflake.

See: `azure-storage-integration.sql`

Create the storage integration in Snowflake, specifying:

- Azure Tenant ID — available under **Microsoft Entra ID → Overview**.
- Azure Storage Allowed Locations — the relevant Blob Storage container URLs.

For example:

```sql
CREATE STORAGE INTEGRATION AZURE_BLOB_TIMBER
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = AZURE
ENABLED = TRUE
AZURE_TENANT_ID = '<AZURE_TENANT_ID>'
STORAGE_ALLOWED_LOCATIONS = (
    'azure://sttimber001.blob.core.windows.net/landing/'
);
```

Use the actual values from your Azure environment.

### Grant Azure Consent

After creating the integration, execute:

```sql
DESC STORAGE INTEGRATION AZURE_BLOB_TIMBER;
```

Locate the `AZURE_CONSENT_URL` property.

1. Copy the URL.
2. Open it in a browser where you are logged into the appropriate Azure account.
3. Grant the requested permissions.

Snowflake will also provide an Azure application identity. Grant this identity the appropriate access to the Storage Account according to the Snowflake Azure integration setup instructions.

---

## 5. Configure Snowflake Notification Integration

The notification integration allows Snowflake to consume Azure Storage Queue notifications.

See: `azure-storage-integration.sql`

Create the integration using the Storage Queue URL:

```sql
CREATE NOTIFICATION INTEGRATION TIMBER_AZURE_NOTIFICATION
TYPE = QUEUE
NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
ENABLED = TRUE
AZURE_STORAGE_QUEUE_PRIMARY_URI =
    '<AZURE_STORAGE_QUEUE_URL>';
```

The queue URL can be found under:

**Storage Account → Data Storage → Queues → Your Queue**

Snowflake also provides an Azure consent URL for the notification integration. Follow the authorization instructions returned by Snowflake and grant the required permissions.

---

## 6. External Stage and Snowpipes

After configuring the storage and notification integrations, Snowflake can access files in Azure and automatically ingest newly created blobs.

The project uses:

- **External Stage:** `AZURE_TIMBER_STAGE`
- **Storage Integration:** `AZURE_BLOB_TIMBER`
- **Notification Integration:** `TIMBER_AZURE_NOTIFICATION`
- **Snowpipes:** One pipe per source table.

See:

- `azure-storage-integration.sql`
- `04_data-ingress-update.sql`

Each Snowpipe listens for new files in its corresponding virtual directory.

For example, files uploaded to:

```text
landing/SAP_INV_REV/
```

are processed by the `SAP_INV_REV` Snowpipe.

The blob path must match the directory expected by the pipe. Do not include the local Python output directory or run ID as an additional directory in the Azure blob path.

### Verify the Configuration

Useful Snowflake commands:

```sql
SHOW STORAGE INTEGRATIONS;
SHOW NOTIFICATION INTEGRATIONS;
SHOW STAGES;
SHOW PIPES;
```

To inspect a specific pipe:

```sql
DESC PIPE PIPE_DELIVERY_NOTES;
```

To check pipe status:

```sql
SELECT SYSTEM$PIPE_STATUS(
    'WOODWORKS_DB.BRONZE.PIPE_DELIVERY_NOTES'
);
```

To inspect ingestion history:

```sql
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'SAP_INV_REV',
        START_TIME => DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
    )
);
```

---

## 7. Important Notes

- Azure Event Grid is responsible for delivering Blob Created events to the Storage Queue.
- The Storage Queue contains notifications, not CSV data.
- Snowpipe automatically processes files when it receives the relevant notifications.
- The Azure blob path must match the path configured in the corresponding Snowpipe.
- Keep the original files in `source`.
- Successfully processed files will eventually be moved to `archive` by the Python pipeline.
- Do not delete or recreate integrations and queues during a routine data reset. They are infrastructure components and should remain configured.
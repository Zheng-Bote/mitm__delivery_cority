# MitM Cority Delivery Layer (CLI Batch Job)

This directory contains the Go implementation of the MitM Delivery Layer. It serves as the final stage of the Data Aggregator pipeline, reliably pushing transformed JSON packages from PostgreSQL into external target systems.

## Overview

The Delivery Layer is designed for extreme resilience and high concurrency:

- **Idempotency:** Every delivery attempt is bound to an `Idempotency-Key` (UUID) to prevent duplicated records if network timeouts occur.
- **Retry Engine:** Implements Exponential Backoff for transient errors (e.g., `HTTP 429 Too Many Requests` or `HTTP 503 Service Unavailable`).
- **Dead Letter Queue (DLQ):** Permanently failing deliveries (e.g., `HTTP 400 Bad Request` or exhausting retry limits) are safely moved into the `dead_letter_queue` table to prevent queue blocking.
- **Interchangeable Adapters:** Extensible architecture using Strategy Pattern to support multiple target platforms.

## Supported Adapters

1. **Cority SaaS Adapter (`CORITY_SAAS`)**: Specialized adapter implementing Cority's two-step OAuth flow and dynamic metadata injection. It automatically restricts concurrency to a single worker to comply with Cority's API limits.

## Building

To build the executable, run:

```bash
go build -o bin/mitm-cority ./cmd/cority/main.go
```

## Usage

The `mitm-cority` module is executed as a short-lived batch job by the `mitm_scheduler`. It expects:

1. **IPC Scheduler Connection** (Preferred) to dynamically fetch PostgreSQL credentials and the `MASTER_KEY`, or direct **Environment Variables** (Fallback).
2. **A single JSON Argument (`os.Args[1]`)** specifying the Job constraints and the `Topic`.

### Example Execution

```bash
# 1. IPC Execution (Running under Scheduler - Preferred)
export RUN_ID="123"
export SCHEDULER_SOCKET_PATH="/tmp/scheduler.sock"

# OR via Direct Environment Variables (Fallback for standalone execution)
export MITM_DB_CONFIG_JSON='{"db":{"host":"192.168.7.31","port":5432,"user":"mitm_user","password":"...","database":"mitm"}}'
export MASTER_KEY="<base64_encryption_key>"

# 2. Define the Job parameters
ARGS_JSON='{
  "topic": "Employee",
  "workers": 5,
  "batch_size": 200,
  "max_retries": 5,
  "source_name": "DELIVERY",
  "blocking_jobs": ["mitm_deliver_Org"]
}'

./bin/mitm-cority "$ARGS_JSON"
```

The Delivery job will automatically connect to the database (fetching credentials via IPC if running under the scheduler), query the `delivery_targets` table for the target configuration matching the `Topic` ("Employee"), decrypt the `config_payload` using `MASTER_KEY`, and dynamically instantiate the correct adapter.

### Config Payload Examples (Database / Admin UI)

When configuring the Target via the Admin Frontend, the `Config Payload` JSON differs per adapter.

#### Cority SaaS (Adapter: CORITY_SAAS)

```json
{
  "login_user": "Cority_User",
  "login_pass": "CorityMy$ecret",
  "auth_refresh_path": "/api/refreshtoken",
  "auth_token_path": "/api/token/",
  "import_path": "/api/employeeimport",
  "slowdown": "5",
  "timeout": "300",
  "upload_options": {
    "import_mode": "upsert",
    "batch_size": 500,
    "updateExistingRecords": true,
    "insertBaseTables": true,
    "forceLookupTableUpdate": true,
    "disableSegUpdate": false,
    "autoCreatePortalUser": true,
    "mergeRecordsWithMatchingSsn": false,
    "dateFormat": "dd.mm.yyyy"
  }
}
```

**Note:** 
- The `slowdown` parameter is optional (in seconds). If set and > 0, the adapter will wait for the specified time interval between package uploads.
- The `timeout` parameter is optional (in seconds). If set and > 0, it overrides the default HTTP timeout (300s) for each of the three Cority API requests.

The adapter uses this configuration to assemble the final payload for the target API. Specifically, the data is uploaded in the following format:

```json
{
  "options": {
    "import_mode": "upsert",
    "batch_size": 500
    // ... other options
  },
  "records": [
    // Array of transformed data records
  ]
}
```

## Database Interaction

The job interacts strictly with the tables predefined in `./migrations`:

- `packages`: Pulls pending/retryable records.
- `dead_letter_queue`: Safely unloads fatal records.

Concurrency is handled natively in Postgres using `FOR UPDATE SKIP LOCKED`.

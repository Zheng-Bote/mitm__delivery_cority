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
  "blocking_jobs": ["mitm_deliver_Org"],
  "blocking_max_runtime_minutes": 360,
  "max_consecutive_errors": 5,
  "max_runtime_minutes": 120,
  "max_retry_wait_seconds": 300
}'

./bin/mitm-cority "$ARGS_JSON"
```

**Note:** `SCHEDULER_SOCKET_PATH` is only used together with `RUN_ID` (set by the scheduler). Without `RUN_ID`, the job falls back to the environment variables and sends no IPC events. Credentials fetched via IPC are kept in memory only and are never written to the process environment. All IPC events of a run share one socket connection.

#### Job Parameters

| Parameter | Default | Description |
|---|---|---|
| `topic` | – | Topic to deliver; selects the `delivery_targets` row. |
| `workers` | `5` | Always forced to `1` for `CORITY_SAAS`. |
| `batch_size` | `100` | Maximum number of target fragments per delivery package, and number of packages claimed per dispatcher round. |
| `max_retries` | `5` | Transient failures are retried with a backoff of 2^n seconds. Retries that are due later are picked up by the next run. |
| `source_name` | `DELIVERY` | Prefix for IPC status and audit messages. |
| `blocking_jobs` | `[]` | Scheduled program names; if one of them has an unfinished `program_runs` entry, the job exits with code 0. |
| `blocking_max_runtime_minutes` | `0` | Ignores blocking runs started longer ago than this (protects against stale runs left by crashed jobs). `0` = no limit. |
| `max_consecutive_errors` | `5` | Aborts the job (exit code `1`) after this many failed DB claim attempts in a row; attempts back off 2, 4, 8 … s (max 30 s). |
| `max_runtime_minutes` | `0` | Stops the job gracefully after this time (status `timeout`); remaining packages are left for the next run. `0` = no limit. |
| `max_retry_wait_seconds` | `300` | If only retries are left and the next one is due within this time, the job waits and delivers it in the same run. Negative = off. |

#### Exit Codes & Recovery

| Exit Code | Meaning |
|---|---|
| `0` | Finished (or exited because of a blocking job / another instance of the topic). |
| `0` | Also on `max_runtime_minutes` reached (status `timeout`) or SIGTERM (status `cancelled`). |
| `1` | Fatal error, e.g. invalid DB config, wrong `MASTER_KEY` (must decode to 32 bytes), undecryptable or invalid `config_payload`, failed encrypted-field lookup, Cority authentication failure (`AUTH_FAIL`), or `max_consecutive_errors` reached. No package is moved to the DLQ because of these errors. |
| `2` | Finished, but at least one package or record was moved to the DLQ or a package state could not be persisted. |

The final IPC status event (`finished`, `failed`, `timeout` or `cancelled`) carries the counts of delivered, retry-scheduled, DLQ, withheld-record and interrupted packages.
Packages left in status `sending` (crash, SIGTERM) are reset to `pending` at the start and at the end of every run.
**Note:** Jobs with `restart_on_exit = true` are restarted once by the scheduler after exit code `2`.

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
- The `slowdown` parameter is optional (in seconds, JSON string or number). If set and > 0, the adapter will wait for the specified time interval between package uploads.
- The `timeout` parameter is optional (in seconds, JSON string or number). If set and > 0, it overrides the default HTTP timeout (300s) for each of the three Cority API requests.
- `upload_options.batch_size` (if > 0) splits the records of a package into several import requests of at most that size. Each chunk gets a deterministic `Idempotency-Key` derived from the package key. If a chunk fails, the whole package is retried and earlier chunks are sent again with the same keys (safe with `import_mode: upsert`).
- If Cority rejects a cached access token (HTTP 401), the adapter invalidates it, re-authenticates once and retries the request.
- All data is sent to Cority **decrypted**. Values in envelope format (`{"ciphertext": ..., "nonce": ...}`) are always decrypted; string values are decrypted for the fields marked `encrypted` in the mapping.
- A record with any undecryptable field is **never sent**. The other records of the package are delivered; the withheld record is stored (still encrypted) as its own row in `dead_letter_queue` (`DECRYPT_FAIL`).
- On HTTP 429/5xx with a `Retry-After` header, the package is rescheduled at least that far ahead (max 1 h) and further uploads pause until then.
- Cority authentication failures (`AUTH_FAIL`) abort the job (exit code `1`); the affected package returns to `pending` without consuming a retry.

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

- `target_fragments`: Packages `PENDING` fragments; malformed JSON fragments are marked `FAILED`.
- `packages`: Pulls pending/retryable records.
- `dead_letter_queue`: Safely unloads fatal records.
- `adapter_tokens`: Shared Cority token cache (row-locked with `FOR UPDATE`).

Concurrency is handled natively in Postgres using `FOR UPDATE SKIP LOCKED`.

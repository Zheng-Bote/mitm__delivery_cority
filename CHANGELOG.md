# Changelog - MitM Cority Delivery Layer

## [v0.20.0] - 2026-09-19

### Changed
- **Architecture**: Split from generic mitm_delivery and rewritten as a dedicated Go v1.26.5 application (replacing the former Rust implementation).

# Changelog

All notable changes to the `mitm_delivery` component will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.21.0] - 2026-09-07

### Fixed

- **Data Parsing (Cority Adapter)**: Fixed a bug where decrypted envelope payload fields (e.g., SSN) were unmarshaled using standard `json.Unmarshal`. This caused raw numeric strings to be implicitly cast to `float64` and subsequently converted into scientific notation (e.g., `e+12`) during SaaS API delivery. Replaced it with `json.Decoder.UseNumber()` to preserve exact numeric formatting.

## [v0.20.0] - 2026-09-05

### Added

- **Job Blocking Criteria**: Added the ability to specify `blocking_jobs` via `JobArgs`. If any of the configured blocking jobs are currently running (checked via the `program_runs` table), the delivery job will write an audit log entry and exit gracefully without performing work.

## [v0.19.3] - 2026-09-01

### Fixed

- **IPC SSLMode DSN Fix**: Fixed an issue where the constructed database connection string (DSN) would incorrectly overwrite `MITM_DB_SSLMODE=require` with `disable`, which caused `FATAL: no encryption` errors in AWS RDS.

## [v0.19.2] - 2026-09-01

### Fixed

- **IPC SSLMode Type Fix**: Changed `SSLMode` field in JSON parsing struct from `string` to `bool` to correctly unmarshal boolean values (`true`/`false`) sent by the scheduler.

## [v0.19.1] - 2026-09-01

### Fixed

- **IPC SSLMode Fix**: Fixed an issue where `SSLMode` was not correctly parsed from the scheduler's JSON configuration and improved the `MITM_DB_SSLMODE` fallback logic to support proper PostgreSQL sslmode strings (e.g., `require`, `verify-full`).

## [v0.19.0] - 2026-08-31

### Added

- **IPC Socket as Credential Broker**: The delivery engine now fetches database credentials and the master key at runtime from the Scheduler via a Unix Domain Socket request (`get_credentials` with `RUN_ID` and `SCHEDULER_SOCKET_PATH`), instead of holding them locally.

## [v0.18.0] - 2026-08-29

### Changed/Added

- Configured `pgxpool` connection limits (`MaxConns=20`, `MaxConnIdleTime=5m`, `MaxConnLifetime=1h`).
- Implemented graceful shutdown with context cancellation on `SIGINT`/`SIGTERM`.
- Optimized performance with batched operations.
- Added/updated DLQ and error tracking mechanisms.

## [v0.17.0] - 2026-08-23

### Added

- **Central Token Caching**: Implemented a central authentication token store (`adapter_tokens` table via `007_adapter_tokens.sql`) to share OAuth2 access tokens across multiple Delivery/Cority_SaaS batch jobs utilizing the same endpoint and account.
- **Concurrency Control for Auth**: Added a row-level lock (`SELECT ... FOR UPDATE`) in `CorityAdapter` to safely and concurrently handle token expirations, preventing multiple jobs from redundantly triggering API authentication calls at the exact same time.

## [v0.16.0] - 2026-08-23

### Changed

- **Cority Adapter Dependency**: `NewCorityAdapter` now accepts the `pgxpool.Pool` database connection to facilitate interactions with the central token store.

## [v0.15.0] - 2026-08-15

### Fixed

- **Data Parsing**: Replaced `json.Unmarshal` with `json.Decoder.UseNumber()` in `PackageRepo` to prevent large integers (like IDs) inside the JSON package payloads from being implicitly cast to `float64` and converted to scientific notation during reading and unmarshaling.

## [v0.14.1] - 2026-08-09

### Fixed

- **DLQ Package Referencing**: Added database migration `006_fix_dlq_fk.sql` to remove the `ON DELETE SET NULL` foreign key constraint from the `dead_letter_queue` table, ensuring `package_id` remains intact when the original package is deleted.
- **Retry Engine Error Handling**: Fixed a bug in `ProcessPackage` where database operation successes (moving to DLQ or scheduling a retry) incorrectly returned `nil`, causing the main worker to swallow the original delivery error and log it as a successful delivery.

## [v0.14.0] - 2026-07-29

### Added

- **Delivery Layer**: Implemented configurable `slowdown` and `timeout` parameters for the `CORITY_SAAS` delivery adapter.

### Changed

- **Database**: Synced PostgreSQL database schema IST-Zustand across all layer `.sql` migrations (`setup.sql`, `transformation-layer`, `delivery-layer`, `scheduler`).
- **Components Logging**: Refactored component version logging mechanism across all layers (Collectors, Transformation, Delivery, Scheduler) to consistently output a clean `Major.Minor.Patch` version format.

### Fixed

- **Scheduler**: Resolved an HTTP 500 error on the `/admin/transformation/errors_bin` API endpoint by updating the query to correctly reference the `raw_ingestion_id` column and gracefully handle null values.

## [v0.13.0] - 2026-07-24

### Added

- **Singleton Pattern**: Introduced a PostgreSQL Advisory Lock (`pg_try_advisory_lock`) in `cmd/deliver/main.go` to ensure `mitm_delivery` strictly runs as a singleton process per topic, preventing concurrent execution conflicts.

### Changed

- **Packaging Loop**: Improved the packaging logic to continuously loop and process all pending fragments from `target_fragments` into packages instead of processing just a single batch per run.
- **Cority HTTP Timeout**: Increased the HTTP client timeout in `cority_adapter.go` from 30 seconds to 300 seconds to safely accommodate large payload sizes (e.g., thousands of records) during Cority REST API ingestions.

## [v0.12.0] - 2026-07-19

### Changed

- **Cority Payload Parsing**: Refined the Cority adapter to utilize a more robust and specific regex pattern for parsing the import response, improving reliability in extracting statistics.

## [v0.11.0] - 2026-07-15

### Added

- **IPC Logging Enhancements**: Added `Topic` and `SourceName` fields to `IPCClient` to consistently prefix all IPC messages with `<Topic>: <SourceName>: `.
- **Job Arguments**: Added `source_name` to `JobArgs` to pass the system's identity from the Scheduler to the Delivery Layer.

## [v0.10.0] - 2026-07-07

### Added

- **SSL Support**: Added support for the `MITM_DB_SSLMODE` environment variable. The delivery engine now respects this setting and applies it to the MitM PostgreSQL connection string.

## [v0.9.0] - 2026-07-03

### Fixed

- **Cority SaaS Concurrency Limits**: Resolved an error from the Cority SaaS provider (`only one import can be run at a time`). The engine now forces the worker pool size to `1` when `CORITY_SAAS` is detected and uses a `sync.Mutex` inside the `CorityAdapter` to guarantee strictly sequential imports.

## [v0.8.0] - 2026-06-30

### Changed

- **Config Restructuring**: Updated database connection setup to read and parse the JSON configuration (`MITM_DB_CONFIG_JSON`) provided by the scheduler, matching the nested `"db"` object format.
- **Database Connection**: The delivery layer now prioritizes the JSON configuration over the direct environment variables (`MITM_DB_HOST`, `DB_HOST`, etc.). Direct variables are kept strictly as a fallback.
- **Audit Logging**: Added IPC audit logging during startup to actively record whether the database configuration was loaded from `JSON Config (MITM_DB_CONFIG_JSON)` or `Environment Variables`.

## [v0.7.0] - 2026-06-24

### Added

- **Envelope Encryption Decryption Support**: Fully implemented payload decryption using Envelope Encryption (KEK and DEK) directly within the Delivery Engine.
- **Nested Ciphertext Structure Support**: Resolved a serialization bug where the Transformation Layer passed encrypted fields as JSON Objects (`{"ciphertext": "...", "nonce": "..."}`). The Delivery Layer now correctly intercepts, decrypts, and unmarshals these objects back to primitive JSON types.
- **Improved Field Lookup & Case Insensitivity**: Improved SQL lookup for `EncryptedFields` to navigate a 3-way JOIN across `mapping_target_field`, `mapping_rule`, and `mapping_source` using a case-insensitive lookup to guarantee exact matches between source/target topics.
- **Mock Key Fallback**: Integrated an AES-GCM fallback that securely processes legacy or dynamically mocked keys automatically without failing the batch job.
- **Base64 Decoding Resilience**: Added support for both Standard and Raw (no padding) Base64 decoding schemes to maximize interoperability with external encryption systems.

## [v0.6.0] - 2026-06-21

### Added

- **Mock Configuration Fallback**: Added a development fallback to supply a mock authentication JSON payload if the decryption of `delivery_targets` using the active `MASTER_KEY` fails. This ensures End-to-End (E2E) testing can complete successfully when testing locally with dynamic keys.
- **E2E Validation**: Successfully passed local mock server delivery execution tests within the overarching pipeline orchestration.

## [v0.5.0] - 2026-06-15

### Added

- **Cority SaaS Audit Logging**: The Cority SaaS adapter now captures the complete raw response from the provider and logs it directly into the `job_audit_log` via IPC.
- **Centralized App Info**: Added `appName` and `version` globally. The component now broadcasts its name and version via IPC when starting.

## [v0.4.0] - 2026-06-10

### Added

- **IPC Client**: Added IPC logging to report progress, success, and errors via Unix domain sockets directly to the scheduler.
- **Audit Logging**: Successful and failed delivery attempts are now sent to `job_audit_logs` including error codes.
- **Cority Payload Null Filter**: The Cority adapter now recursively filters `null` values and replaces them with empty strings `""` before delivery.
- **Package Fragments**: Implemented packaging logic to fetch `target_fragments` and wrap them into the `packages` table before processing.

## [0.2.0] - 2026-06-06

### Changed

- Changed database connection parameter parsing to read primarily from `MITM_DB_*` environment variables (e.g. `MITM_DB_HOST`, `MITM_DB_PASSWORD`) to be compatible with the updated central scheduler configuration structure.

## [0.1.0] - 2026-06-06

### Added

- **Core Architecture:** Defined `DeliverySender` Strategy interface to dynamically inject target-specific HTTP logic (SaaS vs. APIGEE).
- **Concurrency Support:** Robust Database Worker-Pool pattern using PostgreSQL `FOR UPDATE SKIP LOCKED` inside `PackageRepo`.
- **Idempotency & Retry Engine:**
  - Generates and transmits `Idempotency-Key` headers for safe repetition.
  - Implemented dynamic Exponential Backoff calculation for transient network/HTTP errors (e.g., `429 Too Many Requests`).
- **Dead Letter Queue (DLQ):** Hard failing data packages (e.g., `HTTP 400`) and max-retry-exhausted packages are securely shifted into `dead_letter_queue` via `DLQRepo`.
- **Target Adapters:**
  - `SaaSAdapter`: Implements Generic SaaS targets via API Key / Bearer tokens.
  - `ApigeeAdapter`: Implements internal gateway targets via mTLS certificates and JWT injection.
- **Scheduler Integration:** Fully compatible CLI Batch orchestrator `cmd/deliver/main.go` that parses Database ENVs and `os.Args[1]` JSON configuration exactly as commanded by the `mitm_scheduler`.
- **Tests:** Deeply simulated API Mocks and live PostgreSQL E2E Integration tests covering all scenarios.
- **Documentation:** Added `README.md`, `NOTICE`, and architecture concept in `delivery_concept.md`.

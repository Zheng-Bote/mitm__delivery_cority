# Feature: Ensure IPC SendEvent and SendAudit on Every Program Exit (Issue #6)

**Source:** [Zheng-Bote/mitm__delivery_cority#6](https://github.com/Zheng-Bote/mitm__delivery_cority/issues/6)
**Status:** Done (pending review)
**Base:** v0.22.0 → **Target Version:** v0.22.1

## 1. Feature Intent

Ensure that `mitm_delivery_cority` reliably emits both a status event (`ipcClient.SendEvent`) and job audit log entries (`ipcClient.SendAudit`) on **every** program termination path (early argument/config/DB/lock failures, graceful early exits, packaging errors, and normal/failed/timed-out/cancelled batch completion), while running all deferred resource cleanups before process exit.

## 2. Scope

- **Component:** `delivery-layer/mitm_delivery_cority` (`cmd/cority/main.go`, `cmd/cority/main_test.go`, `CHANGELOG.md`, `Cargo.toml`).
- **Database:** existing schema (`mitm-2/migrations/setup.sql`). **No schema change.**

### Non-Goals

- No change to `internal/ipc/ipc.go` wire format or connection semantics.
- No change to `internal/delivery`, `internal/engine`, or `internal/db`.

## 3. Requirements (EARS)

| ID | Requirement |
|---|---|
| R1 | **When** `RUN_ID > 0` and `SCHEDULER_SOCKET_PATH` is non-empty, the system **shall** initialize `ipcClient` (`Component: "mitm_delivery"`, initial `SourceName: "DELIVERY"`) at the start of execution before parsing CLI arguments or connecting to the database, and **shall** immediately send `started` via `SendEvent` and `SendAudit`. |
| R2 | **When** `JobArgs` is parsed from `os.Args[1]`, the system **shall** update `ipcClient.Topic` and `ipcClient.SourceName` so all subsequent status and audit messages carry the `<topic>: <source_name>: ` prefix. |
| R3 | **If** any fatal error occurs (missing/invalid `os.Args[1]`, invalid `MITM_DB_CONFIG_JSON`, DB connection failure, advisory lock failure, `ResetSending` failure, `GetDeliveryTarget` failure, unsupported adapter type, invalid `CorityAuthConfig`, or `PackageTargetFragments` failure), **then** the system **shall** send `ipcClient.SendEvent("failed", msg, 0)`, `ipcClient.SendAudit("ERROR: " + msg)`, and `ipcClient.SendAudit(fmt.Sprintf("%s (%s) failed", appName, version))`, and exit with code `1`. |
| R4 | **When** the job exits early with code `0` because the topic advisory lock is already held or a configured blocking job is currently running, the system **shall** send `ipcClient.SendAudit(msg)`, `ipcClient.SendEvent("exited", msg, 0)`, and `ipcClient.SendAudit(fmt.Sprintf("%s (%s) exited", appName, version))`. |
| R5 | **When** the batch run completes with `status` in `{"finished", "failed", "timeout", "cancelled"}`, the system **shall** send the summary via `SendAudit` (prefixed with `"ERROR: "` when `status == "failed"`), `ipcClient.SendEvent(status, summary, 100)`, and `ipcClient.SendAudit(fmt.Sprintf("%s (%s) %s", appName, versionTag(), status))`. |
| R6 | The system **shall** execute the job logic inside `run() int` so all `defer` statements (`ipcClient.Close()`, `lockConn.Release()`, `pool.Close()`) execute before `os.Exit(code)`. |
| R7 | **When** connected to PostgreSQL, the system **shall** write lifecycle entries with the version (`v<version>`) to `system_logs` (`component = 'mitm_delivery'`): `"Starting job <jobName> (v<version>)"` (`INFO`) at start, and `"Job <jobName> (v<version>) <status>..."` at every exit (`INFO` for `finished`/`exited`/`timeout`/`cancelled`, `ERROR` for `failed`), where `<jobName>` is resolved from `scheduled_programs.name` via `RUN_ID` with fallback to `"Delivery " + topic`. |

## 4. Acceptance Criteria

- [x] AC1 (R1, R2): `ipcClient` is initialized before argument/DB setup, emits `started` (Event + Audit), and updates `Topic`/`SourceName` after `JobArgs` parsing.
- [x] AC2 (R3): Every fatal exit path sends `SendEvent("failed", msg, 0)`, `SendAudit("ERROR: " + msg)`, and `SendAudit("<appName> (v<version>) failed")` before returning exit code `1` (unit test).
- [x] AC3 (R4): Early graceful exits (advisory lock held, blocking job running) send `SendAudit(msg)`, `SendEvent("exited", msg, 0)`, and `SendAudit("<appName> (v<version>) exited")` before returning exit code `0` (unit test).
- [x] AC4 (R5): End-of-run completion sends the summary audit (`ERROR: ` prefix on `"failed"`), `SendEvent(status, summary, 100)`, and `SendAudit("<appName> (v<version>) <status>")` for all terminal statuses (`finished`, `failed`, `timeout`, `cancelled`) (unit test).
- [x] AC5 (R6): `main()` delegates to `run() int` and only calls `os.Exit` after `run()` returns.
- [x] AC6 (R7): `system_logs` start and exit messages include the job name and version in the format `"Starting job Delivery Employee (v0.22.1)"` (unit test).
- [x] AC7: `CHANGELOG.md`, `Cargo.toml`, and `main.go` version strings are updated to `0.22.1`.

## 5. SpecDD Architecture Alignment

- **Architecture:** Single-component change inside `delivery-layer/mitm_delivery_cority`.
- **Security:** Envelope Encryption and RAM-only KEK handling remain untouched.
- **Logging & Governance:** Satisfies `mitm-2.sdd` and `delivery-layer.sdd` requirements on strict separation of logging concerns (`job_status_events` via `SendEvent` and `job_audit_logs` via `SendAudit`) and matches `mitm_maintenance_monitor` expectations (`ERROR: ` prefix in `job_audit_logs` on failed runs).

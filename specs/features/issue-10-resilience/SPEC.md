# Feature: Circuit Breaker, Retry-After Deadline, HTTP Transport and Hard Runtime Limit (Issue #10)

**Source:** [Zheng-Bote/mitm__delivery_cority#10](https://github.com/Zheng-Bote/mitm__delivery_cority/issues/10)
**Status:** Done (pending review)
**Base:** v0.22.2 → **Target Version:** v0.23.0
**Related:** [mitm_core-layer#96](https://github.com/Zheng-Bote/mitm_core-layer/issues/96) (scheduler never read job stdout/stderr — main cause of the observed hang)

## 1. Feature Intent

Make the Cority delivery job robust against a degraded Cority SaaS and make `max_runtime_minutes` a hard limit: stop a run early when Cority keeps failing (circuit breaker), never wait for a `Retry-After` pause that ends after the run deadline, drop stale HTTP connections after network errors, bound every IPC write, and terminate the process when a graceful shutdown does not finish shortly after the deadline.

Cority returns HTTP 200 unless it has a 5xx problem; only network errors, timeouts and HTTP 429/5xx are transient.

## 2. Scope

- **Component:** `delivery-layer/mitm_delivery_cority` (`cmd/cority/main.go`, `internal/engine/retry_engine.go`, `internal/delivery/delivery.go`, `internal/delivery/cority_adapter.go`, `internal/ipc/ipc.go`, their tests, `README.md`, `CHANGELOG.md`, `Cargo.toml`).
- **Database:** existing schema. **No schema change.**
- **Out of scope (decided):** persisting the Retry-After pause (schema change of `adapter_tokens`), retry ordering/backoff, poison-package detection.

## 3. Requirements (EARS)

| ID | Requirement |
|---|---|
| R1 | **When** `max_consecutive_delivery_failures` (job arg, default `3`, negative = off) packages in a row fail with a transient error, the system **shall** stop claiming and sending, return the remaining claimed packages to `pending` without consuming retries, and finish with status `degraded`, exit code `0`, an `ERROR:` audit and a `system_logs` entry of level `ERROR`. |
| R2 | **When** a package is delivered or fails with a non-transient error, the system **shall** reset the consecutive failure count to `0`. |
| R3 | The system **shall** cap a `Retry-After` pause at `max_retry_after_seconds` from the Cority `config_payload` (default and maximum `3600`). |
| R4 | **When** a `Retry-After` pause would end after the run deadline, the system **shall not** wait, **shall** return the package to `pending` without consuming a retry and **shall** finish with status `timeout` and exit code `0`. |
| R5 | The adapter **shall** use its own HTTP transport (connect timeout 30 s, TLS handshake timeout 10 s) and **shall** close idle connections after a network error or timeout of an upload. |
| R6 | The IPC client **shall** bound dialing and every write with a 5 s deadline; a failed write is retried once on a new connection and then dropped (best effort), so one event blocks at most 2 × 5 s. |
| R7 | **Where** `max_runtime_minutes > 0`, **when** the job still runs 60 s after the deadline, a watchdog **shall** reset the topic's `sending` packages to `pending` (bounded by 10 s), send the final `timeout` status and audit events, write a `WARN` entry to `system_logs` and exit with code `0`, without using `log.Printf`. |

## 4. Acceptance Criteria

- [x] AC1 (R1, R2): A sender failing transiently trips the breaker after 3 packages (status `degraded`, exit code `0`); a delivered or non-transient package resets the count (unit tests).
- [x] AC2 (R3): `max_retry_after_seconds` caps the pause; values `<= 0` or `> 3600` use `3600` (unit test).
- [x] AC3 (R4): A Retry-After pause ending after the ctx deadline returns a postpone error without waiting; the engine leaves the package untouched (unit tests); the run finishes with `timeout`/`0` (unit test of `resolveTerminalOutcome`).
- [x] AC4 (R5): A network error during upload closes idle connections (unit test).
- [x] AC5 (R6): `SendAudit` to a scheduler that never reads returns within 2 × 5 s (unit test).
- [x] AC6 (R7): `hardStop` sends the `timeout` events and calls exit with `0` (unit test).
- [x] AC7: Version `0.23.0` in `main.go`, `Cargo.toml`, `CHANGELOG.md`; `README.md` documents the new options and statuses.

## 5. SpecDD Architecture Alignment

- **Architecture:** Isolated within `delivery-layer/mitm_delivery_cority`.
- **Security:** Unchanged; KEK and envelope decryption remain untouched.
- **Scheduler Integration:** Exit code `0` for `degraded` and `timeout` prevents immediate `restart_on_exit` loops against a degraded target; the `ERROR` in `system_logs` lets `mitm_maintenance_monitor` alert.

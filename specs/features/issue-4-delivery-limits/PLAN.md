# Implementation Plan: Issue #4

## 1. Spec Chain & Authority

`mitm-2.sdd` → `delivery-layer/delivery-layer.sdd` (Owns `./**`) → this Spec Kit feature. No `.sdd` change.
Schema: `mitm-2/migrations/setup.sql` (read-only).

## 2. Architecture Check

| Constraint | Result |
|---|---|
| Envelope Encryption / PII | Stronger: per-record guard; envelope objects are always decrypted; rejected records stay encrypted in the DLQ. |
| KEK never persisted | Stronger: no `os.Setenv`; the KEK is passed as a variable. |
| IPC nil-safe | Kept (nil receiver checks), now with a mutex-guarded persistent connection. |
| DLQ / backoff / Idempotency-Key | Kept; backoff extended by `Retry-After`. |

## 3. Design

### A. Contracts (`internal/delivery/delivery.go`, `internal/engine`)
- `DeliverySender.Send` returns `(SendResult, error)`; `SendResult.Rejected []RejectedRecord{Index, Payload json.RawMessage, ErrorCode, ErrorMessage}`.
- `DeliveryError` gets `RetryAfter time.Duration` (R7) and `Abort bool` (R11).
- `ProcessPackage` returns `(Result, error)` with `Result{Outcome, RejectedRecords}`. New `OutcomeAbort`.
  - On success: `DLQRepo.InsertRejectedRecords` → `UpdateStatusDelivered` (both with `WithoutCancel`).
  - `Abort` errors: no DB write, `OutcomeAbort`.
  - Backoff: `max(2^n s, RetryAfter)`.

### B. Adapter (`internal/delivery/cority_adapter.go`)
- Payload decoded as `[]json.RawMessage`; each record is decrypted separately; failures are collected as rejected (original raw JSON kept).
- Envelope detection: object value with a `ciphertext` key, at the top level of a record.
- `pauseUntil` is set from `Retry-After`; `Send` waits for it (ctx-aware) like the slowdown.
- `parseRetryAfter(header)`: integer seconds or `http.ParseTime`, capped at 1 h.
- `AUTH_FAIL` errors carry `Abort: true`.

### C. DB (`internal/db`)
- `DLQRepo.InsertRejectedRecords(ctx, packageID, []RejectedRecord)` in one transaction.
- `PackageRepo.NextRetryAt(ctx, topic) (time.Time, bool, error)`: `MIN(next_retry_at)` of `failed` packages.
- `TargetRepo.GetDeliveryTarget(ctx, topic, masterKey)`: the encrypted-field query error and `rows.Err()` become errors.

### D. IPC (`internal/ipc/ipc.go`)
- `IPCClient` gets `mu sync.Mutex`, `conn net.Conn`; `send()` dials lazily, reconnects once on a write error; `Close()`.

### E. Main (`cmd/cority/main.go`)
- Credentials kept in `dbConfigJSON`, `masterKey`, `sslMode` variables.
- Lock: `pg_try_advisory_lock(hashtextextended($1, 0))`.
- JobArgs: `max_consecutive_errors` (5), `max_runtime_minutes` (0), `max_retry_wait_seconds` (300; a negative value disables it, because 0 cannot be told apart from "unset" in JSON → **decision:** `0` = default 300, `-1` = off).
- Context: base cancel (signals and abort) plus an optional `WithTimeout` for the max runtime.
- Dispatcher: per-batch `WaitGroup` (so retries scheduled by the batch are visible), claim error counter with backoff, retry wait via `NextRetryAt`.
- Worker: `OutcomeAbort` → set the `aborted` flag and cancel.
- Final status: aborted → `failed`/1; claim errors exceeded → `failed`/1; DLQ/rejected/state errors → `failed`/2; deadline → `timeout`/0; signal → `cancelled`/0; else `finished`/0.

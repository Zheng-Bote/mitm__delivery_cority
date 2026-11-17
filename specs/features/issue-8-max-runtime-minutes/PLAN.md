# Implementation Plan: Issue #8

## 1. Spec Chain & Authority

`mitm-2.sdd` → `delivery-layer/delivery-layer.sdd` (Owns `./**`) → `specs/features/issue-8-max-runtime-minutes/SPEC.md`.
No `.sdd` or database schema changes.

## 2. Architecture Check

| Constraint | Result |
|---|---|
| Envelope Encryption / KEK in RAM only | Unchanged. |
| Nil-safe IPC & logging separation | Preserved; `timeout` status and audit log emitted consistently. |
| Exponential backoff & DLQ | Preserved; interrupted packages are returned to `pending` without consuming retries. |

## 3. Design

### A. `internal/delivery/cority_adapter.go`
- Add `MaxRuntimeMinutes flexInt` (`json:"max_runtime_minutes"`) to `CorityAuthConfig` and helper `EffectiveMaxRuntimeMinutes() int` that returns `int(cfg.MaxRuntimeMinutes)` if `> 0`, else checks `cfg.UploadOptions["max_runtime_minutes"]` (number or string).
- In `CorityAdapter.Send` chunk loop (`for i, chunk := range chunks`), apply `cfg.Slowdown` and `pauseUntil` before every chunk (not only before chunk 0) and check `ctx.Err()` before starting a chunk.
- In `CorityAdapter.upload`, if `err != nil && ctx.Err() != nil`, do not audit-log `"Upload Error: Network failure..."` (since the request was cancelled by shutdown or max-runtime deadline).

### B. `cmd/cority/main.go`
- Add `flexInt` support (or unmarshal helper) for `JobArgs` numeric fields so both JSON numbers and numeric strings work in `os.Args[1]`.
- After `corityCfg, err := delivery.ParseCorityAuthConfig(targetConfig.AuthConfig)`, if `jobArgs.MaxRuntimeMinutes <= 0` and `corityCfg.EffectiveMaxRuntimeMinutes() > 0`, set `jobArgs.MaxRuntimeMinutes = corityCfg.EffectiveMaxRuntimeMinutes()`.
- Log the effective runtime limit via `logAudit`:
  - `Max Runtime: <N> minutes` when `jobArgs.MaxRuntimeMinutes > 0`
  - `Max Runtime: unlimited` when `jobArgs.MaxRuntimeMinutes <= 0`
- Extract `shouldWaitForRetry(ctx context.Context, next time.Time, maxRetryWait time.Duration) (time.Duration, bool)`:
  - Returns `(0, false)` if `maxRetryWait <= 0` or `time.Until(next) > maxRetryWait`.
  - If `deadline, ok := ctx.Deadline(); ok && next.After(deadline)`, returns `(0, false)` so the dispatcher does not sleep into a guaranteed timeout.
- In `processPackage`:
  - When `res.Outcome == engine.OutcomeInterrupted`, do not log `"Package <id> failed: ..."`.
- Extract `resolveTerminalOutcome(ctxErr error, aborted, claimFailed, packagingFailed bool, stats *deliveryStats)`:
  - `aborted || claimFailed || packagingFailed` → `("failed", 1)`
  - `errors.Is(ctxErr, context.DeadlineExceeded)` → `("timeout", 0)` (logs `"Max runtime of <N> minutes reached; remaining packages are left for the next run"`, taking precedence over `stats.dlq/rejected/errors` so `restart_on_exit` does not restart a timed-out run)
  - ` errors.Is(ctxErr, context.Canceled)` → `("cancelled", 0)`
  - `stats.dlq > 0 || stats.rejected > 0 || stats.errors > 0` → `("failed", 2)`
  - default → `("finished", 0)`

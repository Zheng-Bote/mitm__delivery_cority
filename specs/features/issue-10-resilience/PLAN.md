# Implementation Plan: Issue #10

## 1. Spec Chain & Authority

`mitm-2.sdd` → `delivery-layer/delivery-layer.sdd` (Owns `./**`) → `specs/features/issue-10-resilience/SPEC.md`.
No `.sdd` or database schema changes.

## 2. Architecture Check

| Constraint | Result |
|---|---|
| Envelope Encryption / KEK in RAM only | Unchanged. |
| Nil-safe IPC & logging separation | Preserved; IPC stays best effort, now with deadlines. |
| Exponential backoff & DLQ | Preserved; postponed and breaker-interrupted packages return to `pending` without consuming retries. |

## 3. Design

### A. `internal/delivery/delivery.go`
- `DeliveryError.Postpone bool`: stops the run without consuming a retry; the package returns to `pending`.

### B. `internal/engine/retry_engine.go`
- New `OutcomePostponed`: when `delErr.Postpone`, leave the package in `sending` (like `OutcomeAbort`) and return.

### C. `internal/delivery/cority_adapter.go`
- `CorityAuthConfig.MaxRetryAfterSeconds flexInt` (`max_retry_after_seconds`) and `retryAfterCap()` (`<= 0` or `> 3600` → 1 h).
- `waitForPause(ctx)` replaces both Retry-After blocks in `Send`: no pause → nil; pause ends after `ctx.Deadline()` → audit + `DeliveryError{IsTransient: true, Postpone: true, ErrorCode: "RETRY_AFTER_EXCEEDS_DEADLINE"}`; otherwise audit + `waitCtx` as before.
- `pauseUntil` is set from `min(delErr.RetryAfter, cfg.retryAfterCap())`; the returned error carries the capped value.
- `NewCorityAdapter(nil, …)` builds the client with `newTransport()` (clone of `http.DefaultTransport`, dialer timeout 30 s, keep-alive 30 s, TLS handshake 10 s).
- `upload`: on a non-cancellation network error call `client.CloseIdleConnections()`.

### D. `internal/ipc/ipc.go`
- `ipcTimeout = 5 s`; `net.DialTimeout` and `conn.SetWriteDeadline(now + ipcTimeout)` before each write.

### E. `cmd/cority/main.go`
- `JobArgs.MaxConsecutiveDeliveryFailures` (`max_consecutive_delivery_failures`, `0` → 3, negative = off).
- `stopReason` (`stopNone`, `stopAbort`, `stopDegraded`, `stopPostponed`) and `runControl` (cancel func, first stop reason, breaker counter/limit) replace `aborted`/`abortJob`.
- `processPackage(…, rc *runControl)`: `OutcomeAbort` → `rc.stop(stopAbort)`; `OutcomePostponed` → interrupted + `rc.stop(stopPostponed)`; `rc.recordResult(outcome, err)` counts transient failures (`OutcomeRetry`, or `OutcomeDLQ` caused by a transient error), resets on delivered / non-transient; trips → `ERROR:` audit + `rc.stop(stopDegraded)`.
- `resolveTerminalOutcome(ctxErr, reason, claimFailed, packagingFailed, stats)`: abort/claim/packaging → `failed`/1; degraded → `degraded`/0; postponed or `DeadlineExceeded` → `timeout`/0; other cancel → `cancelled`/0; DLQ/rejected/errors → `failed`/2; else `finished`/0.
- `finishRun`: `degraded` is logged like `failed` (`ERROR:` audit, `ERROR` in `system_logs`).
- Watchdog: `time.AfterFunc(maxRuntime + hardStopGrace(60 s), hardStop)`, stopped when `run()` returns. `hardStop` resets `sending` packages (10 s timeout), sends `WARNING:` audit, `timeout` status and the final audit via IPC, writes `WARN` to `system_logs` and calls `exitProcess(0)` (`os.Exit`, replaceable in tests). No `log.Printf`.

### F. Docs
- README: `max_consecutive_delivery_failures`, `max_retry_after_seconds`, status `degraded`, hard runtime limit.
- CHANGELOG, `Cargo.toml`, `main.go` version `0.23.0`.

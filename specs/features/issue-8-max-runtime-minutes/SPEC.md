# Feature: Ensure `max_runtime_minutes` Reliably Takes Effect (Issue #8)

**Source:** [Zheng-Bote/mitm__delivery_cority#8](https://github.com/Zheng-Bote/mitm__delivery_cority/issues/8)
**Status:** Done (pending review)
**Base:** v0.22.1 → **Target Version:** v0.22.2

## 1. Feature Intent

Ensure that `max_runtime_minutes` reliably stops the Cority delivery batch job after the configured duration without being overridden by prior DLQ/rejected counts (which previously caused `restart_on_exit` loops), accepts both JSON numbers and strings as well as target-config fallback, logs the effective runtime limit at startup, avoids false error audit messages on context deadline/shutdown interruption, and skips retry sleeps that would exceed the runtime deadline.

## 2. Scope

- **Component:** `delivery-layer/mitm_delivery_cority` (`cmd/cority/main.go`, `cmd/cority/main_test.go`, `internal/delivery/cority_adapter.go`, `internal/delivery/cority_adapter_test.go`, `CHANGELOG.md`, `Cargo.toml`, `README.md`).
- **Database:** existing schema (`mitm-2/migrations/setup.sql`). **No schema change.**

## 3. Requirements (EARS)

| ID | Requirement |
|---|---|
| R1 | **When** `errors.Is(ctx.Err(), context.DeadlineExceeded)` holds at the end of a run (and neither `aborted`, `claimFailed`, nor `packagingFailed` is set), the system **shall** audit-log `"Max runtime of <N> minutes reached; remaining packages are left for the next run"` and finish with status `"timeout"` and exit code `0`, even if earlier packages in the run had DLQ/rejected records (`stats.dlq > 0 || stats.rejected > 0 || stats.errors > 0`). |
| R2 | The system **shall** accept numeric fields in `JobArgs` (`workers`, `batch_size`, `max_retries`, `blocking_max_runtime_minutes`, `max_consecutive_errors`, `max_runtime_minutes`, `max_retry_wait_seconds`) as JSON numbers or numeric JSON strings. |
| R3 | **Where** `JobArgs.MaxRuntimeMinutes <= 0`, the system **shall** fall back to `CorityAuthConfig.MaxRuntimeMinutes` (or `upload_options.max_runtime_minutes`) if > 0, and **shall** audit-log the effective setting at startup (`"Max Runtime: <N> minutes"` or `"Max Runtime: unlimited"`). |
| R4 | **When** an in-flight upload or package processing is interrupted by context cancellation or deadline (`ctx.Err() != nil` / `OutcomeInterrupted`), the system **shall not** log `"Upload Error: Network failure..."` or `"Package <id> failed..."`, and instead **shall** leave the package in `sending` to be reset to `pending`. |
| R5 | **When** no packages are immediately claimable and `NextRetryAt` returns a retry timestamp that lies after the active `ctx` deadline, the dispatcher **shall** stop immediately instead of sleeping until the deadline. |

## 4. Acceptance Criteria

- [x] AC1 (R1): When `ctx` expires with `context.DeadlineExceeded` after earlier DLQ/rejected records, the run logs the max-runtime audit message and returns status `"timeout"` with exit code `0` (unit test).
- [x] AC2 (R2, R3): `"max_runtime_minutes": "45"` in `JobArgs` and `"max_runtime_minutes": 45` (or `"45"`) in `CorityAuthConfig` / `upload_options` are parsed and applied when `JobArgs.MaxRuntimeMinutes == 0` (unit test).
- [x] AC3 (R4): Cancelling `ctx` during `CorityAdapter.Send` or `processPackage` does not emit `"Upload Error: Network failure"` or `"Package <id> failed"` audit messages (unit test).
- [x] AC4 (R5): `shouldWaitForRetry(ctx, next, maxRetryWait)` returns `false` when `next` is after `ctx.Deadline()` (unit test).
- [x] AC5: Version bumped to `0.22.2` in `main.go`, `Cargo.toml`, and `CHANGELOG.md`; `README.md` updated.

## 5. SpecDD Architecture Alignment

- **Architecture:** Isolated within `delivery-layer/mitm_delivery_cority`.
- **Security:** Unchanged; KEK and envelope decryption remain untouched.
- **Scheduler Integration:** Returning exit code `0` with status `"timeout"` on `max_runtime_minutes` prevents `mitm_scheduler-server` (`restart_on_exit = true`) from immediately restarting a timed-out job.

# Work Breakdown: Issue #10

All tasks affect **Component** `delivery-layer/mitm_delivery_cority`.

- [x] **T1 – Contracts & Engine (`delivery.go`, `retry_engine.go` + test)**: `DeliveryError.Postpone`, `OutcomePostponed`. (R4)
- [x] **T2 – Adapter (`cority_adapter.go` + test)**: `max_retry_after_seconds`, `waitForPause` with deadline check, own HTTP transport, close idle connections after network errors. (R3, R4, R5)
- [x] **T3 – IPC (`ipc.go` + test)**: dial and write deadline of 5 s. (R6)
- [x] **T4 – Main (`main.go` + test)**: `max_consecutive_delivery_failures`, `runControl`/`stopReason`, circuit breaker, `degraded` status, `resolveTerminalOutcome`, watchdog/`hardStop`. (R1, R2, R4, R7)
- [x] **T5 – Docs & Version**: README, CHANGELOG, `Cargo.toml`, `main.go` → `0.23.0`. (AC7)
- [x] **T6 – Converge & Verification**: `gofmt -l`, `go vet ./...`, `go test -race ./...`, `./build.sh` / `go build`.

## Converge Report

- R1–R7 and AC1–AC7 are satisfied; `gofmt -l`, `go vet ./...` and `go test -race ./...` pass.
- **Verified by unit tests:**
  - `TestCircuitBreakerStopsRunAfterConsecutiveTransientFailures`, `TestCircuitBreakerDisabledWithNegativeLimit`, `TestResolveTerminalOutcomeStopReasons` (AC1/R1/R2)
  - `TestMaxRetryAfterSecondsCapsPause` (AC2/R3)
  - `TestRetryAfterBeyondDeadlinePostponesWithoutWaiting`, `TestRetryEngine_PostponeLeavesPackageUntouched`, `TestPostponedPackageStopsRunAsTimeout` (AC3/R4)
  - `TestNetworkErrorClosesIdleConnections`, `TestDefaultClientUsesBoundedTransport` (AC4/R5)
  - `TestSendDoesNotBlockWhenSchedulerDoesNotRead` (AC5/R6)
  - `TestHardStopSendsTimeoutAndExits` (AC6/R7)
- `TestRetryAfterPausesNextUpload` now cancels without a deadline: with a deadline before the pause end, R4 intentionally stops instead of waiting.

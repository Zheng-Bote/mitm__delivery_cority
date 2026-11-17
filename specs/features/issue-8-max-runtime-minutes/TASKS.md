# Work Breakdown: Issue #8

All tasks affect **Component** `delivery-layer/mitm_delivery_cority`.

- [x] **T1 – Adapter (`internal/delivery/cority_adapter.go` + test)**: Add `MaxRuntimeMinutes` + `EffectiveMaxRuntimeMinutes()` to `CorityAuthConfig`, suppress false `"Upload Error: Network failure"` audit log when `ctx.Err() != nil`, and apply slowdown/ctx check per chunk. (R3, R4)
- [x] **T2 – Main (`cmd/cority/main.go` + test)**: Use `flexInt` in `JobArgs`, fall back to `corityCfg.EffectiveMaxRuntimeMinutes()`, audit-log `Max Runtime`, add `shouldWaitForRetry` deadline check, suppress `"Package failed"` log on `OutcomeInterrupted`, and give `DeadlineExceeded` (`timeout`, exit code `0`) precedence over `stats.dlq/rejected/errors` in `resolveTerminalOutcome`. (R1–R5)
- [x] **T3 – Docs & Version (`CHANGELOG.md`, `Cargo.toml`, `README.md`, `cmd/cority/main.go`)**: Bump version to `0.22.2` and document `max_runtime_minutes` behavior. (AC5)
- [x] **T4 – Converge & Verification**: Run `gofmt -l`, `go vet ./...`, `go test -v -race ./...`, and rebuild `./bin/mitm-deliver-cority` + `app/bin/mitm-deliver-cority`.

## Converge Report

- R1–R5 and AC1–AC5 are satisfied.
- `gofmt -l`, `go vet ./...`, and `go test -v -race ./...` pass without issues.
- **Verified by unit tests:**
  - `TestResolveTerminalOutcomeTimeoutPrecedesDLQ` (AC1/R1)
  - `TestJobArgsAcceptNumericStrings` and `TestEffectiveMaxRuntimeMinutesFromAuthConfigAndUploadOptions` (AC2/R2/R3)
  - `TestCancelledUploadDoesNotAuditNetworkError` and `TestProcessPackageInterruptedDoesNotLogPackageFailed` (AC3/R4)
  - `TestShouldWaitForRetryRespectsDeadline` (AC4/R5)

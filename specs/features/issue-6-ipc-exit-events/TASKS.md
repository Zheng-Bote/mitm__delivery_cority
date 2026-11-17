# Work Breakdown: Issue #6

All tasks affect **Component** `delivery-layer/mitm_delivery_cority`.

- [x] **T1 – Refactor `main.go` exit, IPC & `system_logs` lifecycle** (`delivery-layer/mitm_delivery_cority/cmd/cority/main.go`): Extract `run() int`, initialize `ipcClient` at startup, log versioned start/exit messages (`"Starting job <jobName> (v0.22.1)"`) to `system_logs`, implement `failf`, `exitGracefully`, and `finishRun`, and route all exit paths through them. (R1–R7)
- [x] **T2 – Unit tests for IPC exit helpers, `system_logs` formatting & CLI failure path** (`delivery-layer/mitm_delivery_cority/cmd/cority/main_test.go`): Verify that `failf`, `exitGracefully`, `finishRun`, `startSystemLogMessage`, and `run()` always emit the expected `SendEvent`, `SendAudit`, and versioned `system_logs` strings. (AC1–AC6)
- [x] **T3 – Version & Changelog update** (`delivery-layer/mitm_delivery_cority/CHANGELOG.md`, `Cargo.toml`, `cmd/cority/main.go`): Bump version to `0.22.1` and document the changes. (AC7)
- [x] **T4 – Converge & Verification**: Verify against `SPEC.md` and `.sdd` rules; run `go vet ./...`, `gofmt -l .`, `go test -race ./...`, and build `./bin/mitm-deliver-cority`.

## Converge Report

- R1–R6 and AC1–AC6 are satisfied.
- `gofmt -l`, `go vet ./...`, and `go test -v -race ./...` pass without issues.
- **Verified by unit tests (`cmd/cority/main_test.go`):**
  - `TestFailfSendsEventAndAudit`: verifies `failed` status event, `ERROR: ` audit event, and `<appName> (<version>) failed` audit event with exit code `1`.
  - `TestExitGracefullySendsEventAndAudit`: verifies reason audit event, `exited` status event, and `<appName> (<version>) exited` audit event with exit code `0`.
  - `TestFinishRunStatusesSendEventAndAudit`: verifies summary audit (`ERROR: ` prefix on `failed`), terminal status event (`progress = 100`), and `<appName> (<version>) <status>` audit event across `finished`, `failed`, `timeout`, and `cancelled`.
  - `TestRunEarlyExitEmitsStartAndFailOverIPC`: verifies end-to-end `run()` startup and early failure over a local Unix Domain Socket, including `<topic>: <source_name>: ` prefix propagation.

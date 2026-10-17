# Work Breakdown: Harden Cority Delivery Job (Issue #2)

All tasks: **Component** `delivery-layer/mitm_delivery_cority`. Ordered as interfaces and contracts first.

- [x] **T1 – Engine contract** (`internal/engine/retry_engine.go`, `retry_engine_test.go`): `Outcome` type, `ProcessPackage` returns `(Outcome, error)`, `WithoutCancel` for DB writes, `OutcomeInterrupted`. (R2, R11)
- [x] **T2 – Token store contract** (`internal/delivery/token_store.go`): `TokenStore` interface + `PGTokenStore` on `adapter_tokens`. (R5)
- [x] **T3 – Package repo** (`internal/db/package_repo.go`): `ResetSending`; `PackageTargetFragments` marks malformed fragments `FAILED` and returns their IDs. (R1, R2, R8)
- [x] **T4 – Target repo** (`internal/db/target_repo.go`): KEK length check, no mock config fallback. (R3, R14)
- [x] **T5 – Cority adapter** (`internal/delivery/cority_adapter.go`): `flexInt`, `ParseCorityAuthConfig`, JSON login body, ctx-aware slowdown, `DECRYPT_FAIL`, chunking + per-chunk keys, 401 re-auth, body logging on failure only. (R4, R5, R7, R9, R10, R13, R16)
- [x] **T6 – Main** (`cmd/cority/main.go`): `buildConnString`, `fatalf`, startup/shutdown `ResetSending`, config validation, blocking max runtime, packager limit, outcome counters, final event, exit code 2. (R1–R3, R6, R8, R9, R11, R12)
- [x] **T7 – Tests** (`internal/delivery/cority_adapter_test.go`, `internal/crypto/crypto_test.go`, `cmd/cority/main_test.go`; skip logic in `internal/db/db_test.go`, `test/e2e_test.go`). (AC4–AC9, AC12)
- [x] **T8 – Versions & modules** (`cmd/cority/main.go`, `Cargo.toml`, `go.mod`/`go.sum` via `go mod tidy`). (R15)
- [x] **T9 – Docs** (`README.md`, `CHANGELOG.md`): new parameters, chunking semantics, exit codes, `blocking_max_runtime_minutes`. (AC14)
- [x] **T10 – Converge**: verify every R/AC against the code; `go vet`, `go test ./...`, `gofmt -l`.

## Converge Report

- All R1–R16 are implemented; all ACs are covered by unit tests or code review (see below).
- **Verified by unit tests:** R2 (engine), R4, R5, R6, R7, R9, R10, R13, crypto round trip.
- **Verified by review only (no PostgreSQL server available locally):** R1/R2 `ResetSending`, R8 `FAILED` fragments, R12 blocking query, `PGTokenStore`. Converge found and fixed two NULL edge cases: `program_runs.started_at` and `adapter_tokens.refresh_token` are nullable, so both are now wrapped in `COALESCE`.
- **Open for the Operator:** `delivery-layer.sdd` names `delivery_outbox`, the code uses `packages` (finding 18); the e2e test scenario is outdated (generic `SAAS` adapter) and now only skips.

# Work Breakdown: Issue #4

All tasks: **Component** `delivery-layer/mitm_delivery_cority`. Contracts first.

- [x] **T1 – Contracts** (`internal/delivery/delivery.go`): `SendResult`, `RejectedRecord`, `DeliveryError.RetryAfter`, `DeliveryError.Abort`. (R2, R7, R11)
- [x] **T2 – Engine** (`internal/engine/retry_engine.go` + test): `Result`, `OutcomeAbort`, rejected records → DLQ, `Retry-After` backoff. (R2, R7, R11)
- [x] **T3 – DB** (`internal/db/dlq_repo.go`, `package_repo.go`, `target_repo.go`): `InsertRejectedRecords`, `NextRetryAt`, `masterKey` parameter, fatal field query. (R2, R3, R4, R8)
- [x] **T4 – Adapter** (`internal/delivery/cority_adapter.go` + test): per-record decryption, envelope detection, `Retry-After` parse + pause, `AUTH_FAIL` abort. (R1, R2, R7, R11)
- [x] **T5 – IPC** (`internal/ipc/ipc.go` + test): persistent connection with reconnect. (R9)
- [x] **T6 – Main** (`cmd/cority/main.go`): secrets in variables, 64-bit lock, job limits, batch-wise dispatcher with retry wait, abort handling, final status. (R3–R6, R8, R10, R11)
- [x] **T7 – Docs & version** (`README.md`, `CHANGELOG.md`, `Cargo.toml`, `main.go`): v0.22.0. (AC11)
- [x] **T8 – Converge**: verify R/AC; `go vet`, `gofmt -l`, `go test ./...`.

## Converge Report

- R1–R11 are implemented. `go vet`, `gofmt -l` and `go test -race ./...` pass.
- **Verified by unit tests:** R1, R2 (adapter + engine), R7 (parse, backoff, pause), R9 (connection reuse, reconnect, nil safety), R11 (adapter `Abort`, engine `OutcomeAbort`).
- **Verified by review only (DB-dependent, no local PostgreSQL server):** R3 (fatal field query), R5/R8 (dispatcher with `NextRetryAt`), R6 (deadline → `timeout`), R10 (`hashtextextended`), `InsertRejectedRecords`.
- **Converge finding (fixed):** a field marked as encrypted that arrives as an envelope object was decrypted in step 1 and then treated as legacy base64 in step 2, so the record was wrongly withheld. Fixed with `decryptedKeys`; regression test `TestListedEnvelopeObjectIsDecryptedOnce`.
- **Deployment note:** the lock key changed (R10); avoid overlapping v0.21/v0.22 runs of the same topic.

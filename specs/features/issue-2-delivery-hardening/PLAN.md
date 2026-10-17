# Implementation Plan: Harden Cority Delivery Job (Issue #2)

## 1. Spec Chain & Authority

```text
mitm-2/mitm-2.sdd                         (global Must: Envelope Encryption, KEK in RAM, IPC, logging, SPDX, English)
└── delivery-layer/delivery-layer.sdd     (Owns ./**: batching, Idempotency-Key, backoff, DLQ, runtime targets)
    └── mitm_delivery_cority/             (no local .sdd; this Spec Kit feature)
```

- **Target path:** `delivery-layer/mitm_delivery_cority/**` (owned by `delivery-layer.sdd`).
- **Schema source:** `mitm-2/migrations/setup.sql` (read-only for this feature).
- **No `.sdd` change.** The `delivery_outbox` naming drift is reported to the Operator, not changed.

## 2. Architecture Check

| Constraint | Result |
|---|---|
| Envelope Encryption for PII | Strengthened: the hardcoded mock-key fallback is removed; undecryptable fields fail the package. |
| KEK never persisted | Unchanged: the KEK comes from IPC/ENV and stays in RAM; it is now length-checked. |
| IPC nil-safe status/audit | Kept: new final events go through the existing nil-safe `IPCClient`. |
| Logging separation | Audit (`job_audit_logs` via IPC) gets the delivery metrics; lifecycle stays in `log`/IPC events. |
| Batching, Idempotency-Key, backoff, DLQ | Kept. Per-chunk keys are derived deterministically (UUIDv5) from the package key. |
| Independent `go.mod` | Kept; `go mod tidy` fixes `// indirect` markers. |
| Core schema intact | Yes; no DDL. `target_fragments.delivery_status` has no CHECK constraint, so `FAILED` is allowed. |

## 3. Design

### A. `cmd/cority/main.go`
- `JobArgs` gets `BlockingMaxRuntimeMinutes int` (`blocking_max_runtime_minutes`, default `0` = no limit).
- `buildConnString(user, pass, host, port, db, sslmode)` uses `net/url` (R6).
- After the advisory lock: `pkgRepo.ResetSending(topic)` (R1).
- After creating the IPC client, a `fatalf` helper sends a `failed` event and exits 1 (R3).
- Target config: `GetDeliveryTarget` errors (decrypt, KEK length) and `delivery.ParseCorityAuthConfig` errors are fatal (R3, R14).
- Blocking query adds `AND ($2 = 0 OR pr.started_at > NOW() - make_interval(mins => $2))` and returns the run age (R12).
- Packager limit is `BatchSize` (no ×10); malformed fragments are audit-logged (R8, R9).
- Workers skip packages once the context is cancelled; after `wg.Wait()` the job calls `ResetSending` again (R2).
- Workers count the `engine.Outcome` per package (atomic counters). Final audit message and IPC event `finished` / `failed`; exit code 2 if any DLQ move (R11).

### B. `internal/engine/retry_engine.go`
- `ProcessPackage` returns `(Outcome, error)` with `OutcomeDelivered | OutcomeRetry | OutcomeDLQ | OutcomeInterrupted`.
- DB status writes use `context.WithoutCancel(ctx)` (R2).
- If `Send` fails while the context is cancelled → `OutcomeInterrupted`, no DB write, no retry increment (R2).

### C. `internal/db`
- `PackageRepo.ResetSending(ctx, topic) (int64, error)`: `UPDATE packages SET status='pending' WHERE topic=$1 AND status='sending'`.
- `PackageTargetFragments` returns `(packaged int, failedIDs []string, err error)`. Malformed fragments are set to `FAILED`; no package is created if nothing valid remains (R8).
- `TargetRepo.GetDeliveryTarget`: returns an error if the KEK is not 32 bytes or the payload cannot be decrypted; the mock config is removed (R3, R14).
- `buildConnString` moves to `main` (unit-tested in `cmd/cority/main_test.go`).

### D. `internal/delivery`
- `token_store.go`: `TokenStore` interface (`WithLock`, `Delete`) plus `PGTokenStore` (existing `adapter_tokens` `FOR UPDATE` logic). This makes the adapter unit-testable without a DB.
- `cority_adapter.go`:
  - `flexInt` accepts JSON number or numeric string for `slowdown` / `timeout` (R10); `ParseCorityAuthConfig` is exported for the startup check.
  - Login body via `json.Marshal` (R7).
  - The slowdown wait is a `select` on `ctx.Done()` (R13), and the slowdown state is logged once (R16).
  - Field decryption errors return `DECRYPT_FAIL` (fatal); the mock key and `crypto/aes` / `crypto/cipher` imports are removed (R4).
  - Records are split by `upload_options.batch_size` (if > 0). Chunk *i* of *n > 1* uses `uuid.NewSHA1(pkgKey, "chunk-i")` as its Idempotency-Key, so retries reuse the same keys (R9).
  - On HTTP 401: `Delete` the token, authenticate, retry the chunk once; a second 401 is transient `HTTP_401` (R5).
  - The response body is audit-logged only on failure, truncated to 1 KiB (R16).
- Partial-failure semantics: chunks are sent in order; if chunk *k* fails, the whole package is retried later and chunks 1..k-1 are resent with the same keys. This is safe with Cority's `upsert` import mode. Documented in the README.

### E. Tests
- `internal/delivery/cority_adapter_test.go`: `httptest` server + in-memory `TokenStore`: 401 re-auth (AC5), chunking (AC8), `flexInt` (AC9), login body escaping (AC6), decrypt failure → `DECRYPT_FAIL` (AC4).
- `internal/crypto/crypto_test.go`: `GenerateWrappedDEK` → `EnvelopeEncrypt` → `EnvelopeDecrypt` round trip; wrong KEK fails (AC12).
- `cmd/cority/main_test.go`: `buildConnString` with special characters (AC6).
- `internal/engine/retry_engine_test.go`: adapted to `Outcome`, plus an interrupted case (AC2).
- `internal/db/db_test.go`, `test/e2e_test.go`: `t.Skip` when `data/config.json` is missing (AC12).

## 4. Edge Cases
- Empty payload array: one empty chunk is sent (unchanged behaviour); the packager no longer creates empty packages.
- `upload_options.batch_size` given as number or string: both are supported; invalid or ≤ 0 means no chunking.
- The token row is locked across processes (`FOR UPDATE`) during re-authentication; this is unchanged.
- Restart: with `restart_on_exit = true`, exit code 2 triggers one scheduler restart (see SPEC §5).

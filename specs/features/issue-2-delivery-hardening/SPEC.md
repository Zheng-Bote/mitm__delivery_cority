# Feature: Harden Cority Delivery Job (Issue #2)

**Source:** [Zheng-Bote/mitm__delivery_cority#2](https://github.com/Zheng-Bote/mitm__delivery_cority/issues/2)
**Status:** Done (pending review)
**Base Version:** v0.20.0 → **Target Version:** v0.21.0

## 1. Feature Intent

Harden the Cority delivery job against data loss, silent misconfiguration and silent DLQ flooding.
The job must fail loudly on configuration errors, recover orphaned work automatically, and report
truthful results to the scheduler, without changing the layered architecture or the envelope-encryption model.

## 2. Scope

- **Component:** `delivery-layer/mitm_delivery_cority` only (isolated feature, spec lives in the component repo).
- **Touched modules:** `cmd/cority`, `internal/db`, `internal/delivery`, `internal/crypto`, tests, `README.md`, `CHANGELOG.md`, `Cargo.toml`, `go.mod`.
- **Database:** existing schema from `mitm-2/migrations/setup.sql` (`packages`, `target_fragments`, `adapter_tokens`,
  `program_runs`, `scheduled_programs`, `dead_letter_queue`). **No schema change.**

### Non-Goals

- No change to Collectors, Transformation or the Scheduler.
- Using the Cority refresh token instead of a full login (issue item 16): deferred, as the Cority refresh-token semantics are unverified.
- Resolving the `delivery_outbox` vs. `packages` naming drift in `delivery-layer.sdd` (issue finding 18): needs an Operator decision on the `.sdd`; reported only.
- Fixing the outdated e2e test scenario (generic `SAAS` adapter): only made skippable.

## 3. Requirements (EARS)

| ID | Requirement |
|---|---|
| R1 | **When** the job holds the topic advisory lock, the system **shall** reset all packages of its topic in status `sending` to `pending`, and audit-log the count. (The lock guarantees that no other instance of the topic runs, so every `sending` package is orphaned. `packages` has no claim timestamp, so the issue's "stale threshold" is not needed.) |
| R2 | **When** the job receives SIGINT/SIGTERM, the system **shall** stop claiming and processing, **shall** persist the result of completed deliveries with a non-cancelled context, and **shall** return all remaining `sending` packages of its topic to `pending` without incrementing `retry_count`. |
| R3 | **If** the `config_payload` cannot be decrypted or parsed, **then** the system **shall** send a `failed` IPC event and exit with code 1, without any mock fallback. |
| R4 | **If** decryption of an encrypted payload field fails, **then** the system **shall** fail the package fatally (DLQ, `DECRYPT_FAIL`) and **shall not** try any hardcoded key. |
| R5 | **When** the Cority import returns HTTP 401, the system **shall** delete the cached token, re-authenticate once and retry once; **if** the retry returns 401 again, **then** the package **shall** fail transiently. |
| R6 | The system **shall** build the PostgreSQL connection string with URL-escaped user and password. |
| R7 | The system **shall** build the Cority login body with `json.Marshal`. |
| R8 | **If** a target fragment contains malformed JSON, **then** the system **shall** set its `delivery_status` to `FAILED` (not `PACKAGED`) and audit-log the fragment ID. |
| R9 | The packager **shall** put at most `batch_size` fragments into one package (no hidden ×10 factor). **Where** `upload_options.batch_size` > 0, the adapter **shall** send the records in chunks of at most that size, each with a deterministic per-chunk Idempotency-Key. |
| R10 | The system **shall** accept `slowdown` and `timeout` as JSON string or JSON number. |
| R11 | **When** the job finishes, the system **shall** send a final IPC event (`finished`, `failed` or `cancelled`) with the counts of delivered, retry-scheduled, DLQ and interrupted packages, and **shall** exit with code 2 if at least one package was moved to the DLQ or its state could not be persisted. |
| R12 | **Where** `blocking_max_runtime_minutes` > 0, the blocking-job check **shall** ignore `program_runs` entries started longer ago than that; the audit message **shall** include the age of the blocking run. |
| R13 | **While** the slowdown delay is active, the system **shall** abort the wait on context cancellation. |
| R14 | **If** the decoded `MASTER_KEY` is not exactly 32 bytes, **then** the system **shall** exit with code 1. |
| R15 | Version strings **shall** be consistent (`main.go` default, `Cargo.toml`, `CHANGELOG.md`); `go.mod` **shall** pass `go mod tidy` without diff. |
| R16 | The audit log **shall** contain the Cority response body only on failure (truncated to 1 KiB); the slowdown state **shall** be logged once per run. |

## 4. Acceptance Criteria

- [x] AC1 (R1): Packages left in `sending` after a crash are redelivered by the next run.
- [x] AC2 (R2): After SIGTERM no package of the topic remains in `sending`.
- [x] AC3 (R3, R14): A wrong or wrong-length `MASTER_KEY` exits with code 1; no package is moved to the DLQ.
- [x] AC4 (R3, R4): No mock key and no mock config remain in non-test code.
- [x] AC5 (R5): An import 401 triggers exactly one re-authentication and one retry (unit test).
- [x] AC6 (R6, R7): A DB password with `@:/?#%` and a Cority password with `"\` work (unit tests).
- [x] AC7 (R8): A malformed fragment is marked `FAILED`; valid fragments of the same batch are packaged.
- [x] AC8 (R9): No import request contains more records than `upload_options.batch_size` (unit test).
- [x] AC9 (R10): `"slowdown": 5` and `"slowdown": "5"` are both accepted (unit test).
- [x] AC10 (R11): The exit code is 2 if any package went to the DLQ; the final IPC event carries the counts.
- [x] AC11 (R12): A stale blocking run older than `blocking_max_runtime_minutes` no longer blocks the job.
- [x] AC12: Unit tests exist for `CorityAdapter` and `crypto.EnvelopeDecrypt`; DB/e2e tests skip when `data/config.json` is missing.
- [x] AC13 (R15): Version strings are consistent; `go mod tidy` produces no diff.
- [x] AC14: CHANGELOG.md and README.md are up to date.

## 5. SpecDD Architecture Alignment

- **Architecture:** Layering is maintained; the job keeps reading only delivery-owned tables plus the read-only `program_runs` check.
- **Security:** Envelope Encryption is strengthened (the hardcoded-key bypass is removed). The KEK stays in RAM only.
- **Data Model:** No schema change. `FAILED` is a new value in the unconstrained `target_fragments.delivery_status` column.
- **IPC / Scheduler:** New final status events (`finished` / `failed`). Exit codes 1 (fatal) and 2 (DLQ moves) mark the run as
  `success = false` in `program_runs`. **Note:** jobs with `restart_on_exit = true` are restarted once by the scheduler after
  exit code 2; the restarted run finds no DLQ'd packages and exits with 0.

# Feature: Per-Record Decryption Guard, Job Limits, Retry-After, AUTH_FAIL Abort (Issue #4)

**Source:** [Zheng-Bote/mitm__delivery_cority#4](https://github.com/Zheng-Bote/mitm__delivery_cority/issues/4)
**Status:** Done (pending review)
**Base:** v0.21.0 (PR #3, issue #2) → **Target Version:** v0.22.0

## 1. Feature Intent

Follow-up hardening decided by the Operator after the review of #2:
all data must reach Cority decrypted (undecryptable records are never sent), secrets stay out of the process environment,
the job gets error and runtime limits, honours `Retry-After`, finishes due retries in the same run, aborts on authentication
failures instead of flooding the DLQ, and uses a persistent IPC connection and a 64-bit lock key.

## 2. Scope

- **Component:** `delivery-layer/mitm_delivery_cority` (`cmd/cority`, `internal/delivery`, `internal/engine`, `internal/db`, `internal/ipc`).
- **Database:** existing schema (`mitm-2/migrations/setup.sql`). **No schema change.** Rejected records are stored as single rows in `dead_letter_queue` (its `package_id` FK was dropped in `006_fix_dlq_fk.sql`).

### Non-Goals

- DB TLS default, field-mapping strictness, PII masking in error messages, CI, integration tests (not selected by the Operator).

## 3. Requirements (EARS)

| ID | Requirement |
|---|---|
| R1 | **When** a top-level record value is an object containing `ciphertext`, the system **shall** treat it as envelope-encrypted and decrypt it, whether or not the field is listed in the mapping. **Where** a field is listed as encrypted, non-empty string values **shall** be decrypted (legacy format). |
| R2 | **If** decrypting any field of a record fails, **then** the system **shall not** transmit that record, **shall** deliver the remaining records, and, once the package is delivered, **shall** store the original (still encrypted) record in `dead_letter_queue` (`DECRYPT_FAIL`, `package_id` = package). The audit log **shall** name the record index and field, never values. |
| R3 | **If** loading the encrypted-field list fails, **then** the system **shall** exit with code 1. |
| R4 | The system **shall** hold `MASTER_KEY` and the DB config in variables only (no `os.Setenv`); ENV `MASTER_KEY` / `MITM_DB_CONFIG_JSON` remain the fallback. |
| R5 | **If** claiming packages fails `max_consecutive_errors` times in a row (default `5`), **then** the system **shall** exit with code 1; waits between attempts back off exponentially (2^n s, max 30 s). |
| R6 | **Where** `max_runtime_minutes` > 0, the system **shall** stop after that time like a graceful shutdown, send status `timeout` and exit with code 0 (unless R11 or DLQ moves apply). |
| R7 | **When** a transient upload error carries `Retry-After` (seconds or HTTP date), the system **shall** set `next_retry_at = NOW() + max(backoff, Retry-After)` (capped at 1 h) and **shall** pause further uploads until then, abortable by shutdown or max runtime. |
| R8 | **While** no package is claimable but a failed package of the topic becomes due within `max_retry_wait_seconds` (`0` = default `300` s, negative = off), the system **shall** wait and continue in the same run. |
| R9 | The IPC client **shall** reuse one socket connection for status and audit events and **shall** reconnect once on a write error. |
| R10 | The singleton lock **shall** use `pg_try_advisory_lock(hashtextextended('delivery_' || topic, 0))`. |
| R11 | **If** authentication fails (`AUTH_FAIL`), **then** the system **shall** leave the package without consuming a retry (reset to `pending`), stop processing, send status `failed` and exit with code 1; no package **shall** be moved to the DLQ because of `AUTH_FAIL`. |

## 4. Acceptance Criteria

- [x] AC1 (R2): A package with one undecryptable record delivers the other records; the failed record is returned as rejected with its original payload and is never sent (unit test).
- [x] AC2 (R1): An envelope object in an unlisted field is decrypted (unit test).
- [x] AC3 (R3): A failing encrypted-field query is returned as an error by `GetDeliveryTarget` and is fatal in `main`.
- [x] AC4 (R4): No `os.Setenv` remains for `MASTER_KEY`, DB config or SSL mode.
- [x] AC5 (R5, R6): Claim errors are limited; max runtime ends with status `timeout` and no package left in `sending`.
- [x] AC6 (R7): `Retry-After: 120` on 429 yields `RetryAfter = 120s` and a backoff of at least 120 s (unit tests).
- [x] AC7 (R8): The dispatcher waits for retries due within the limit (code review; DB-dependent).
- [x] AC8 (R9): Several events use one socket connection; a broken connection is re-dialled (unit test).
- [x] AC9 (R10): The advisory lock uses `hashtextextended`.
- [x] AC10 (R11): `AUTH_FAIL` yields `OutcomeAbort` without DB writes (unit test); `main` exits 1.
- [x] AC11: CHANGELOG.md and README.md are up to date.

## 5. SpecDD Architecture Alignment

- **Architecture:** unchanged layering; Delivery only.
- **Security:** strengthened. No ciphertext or partially decrypted record leaves the system; the KEK no longer leaks into `/proc/<pid>/environ` or child processes.
- **Data Model:** no DDL; `dead_letter_queue` reused per record.
- **IPC / Scheduler:** same message format; one connection per run (the scheduler reads multiple lines per connection). New status `timeout`.
- **Lock key change:** a v0.21 and a v0.22 instance of the same topic would not exclude each other during a rolling deploy. Deploy without overlapping runs.

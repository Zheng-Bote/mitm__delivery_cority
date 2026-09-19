# Feature: Job Blocking Criteria (Issue #2)

**Source:** [Zheng-Bote/mitm_delivery#2](https://github.com/Zheng-Bote/mitm_delivery/issues/2)

## 1. Feature Intent

The delivery layer should optionally provide a way to specify—via a parameter—one or more jobs as "blocking criteria" (KO-Jobs) for the current job run.

If any of these specified blocking jobs are currently running:

1. The current job must not proceed with its actual work.
2. The current job must generate a corresponding job audit log entry indicating an early exit due to a blocking job.
3. The current job must terminate gracefully (normal exit, no error/panic).

_Example:_
The `mitm_deliver` job with the "Org" topic is currently running. It has been specified as a blocking criterion for the `mitm_deliver` job with the "Employee" topic. When the "Employee" job starts, it detects the "Org" job is running, sends an audit log message, and terminates normally.

_Note:_ A control mechanism already exists to prevent a job from running multiple instances of itself in parallel (via the PostgreSQL table `program_runs`). This feature extends the logic to check _other_ specified jobs.

## 2. Scope

- **Component:** `delivery-layer/mitm_delivery`
- **Dependency:** PostgreSQL state table `program_runs` (to check running status) and the audit logging mechanism.

## 3. Acceptance Criteria

- [ ] A job terminates regularly (exit code 0 / no crash) without performing its main delivery work if a defined blocking job is currently running.
- [ ] The job successfully writes an audit-log message documenting the early exit before terminating.
- [ ] The mechanism is configurable via parameter (e.g., CLI flag, environment variable, or configuration file) to specify the list of blocking jobs.
- [ ] the configration option is documented

## 4. SpecDD Architecture Alignment

- **Architecture:** The layered architecture is maintained.
- **Security:** Envelope Encryption is not impacted.
- **Data Model:** Reuses existing `program_runs` table schema. No schema drift.

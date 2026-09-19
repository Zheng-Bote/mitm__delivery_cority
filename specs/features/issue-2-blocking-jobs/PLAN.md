# Implementation Plan: Job Blocking Criteria (Issue #2)

## 1. Architectural Approach
The feature will be implemented in `delivery-layer/mitm_delivery/cmd/deliver/main.go`. We will extend the `JobArgs` struct to accept a list of blocking job names. During startup, before initializing the worker pool and claiming packages, the application will check the `program_runs` PostgreSQL table to see if any of the specified blocking jobs are currently active.

If a blocking job is active, the process will emit an audit log and exit gracefully (exit code 0).

## 2. Component Modifications

### A. `cmd/deliver/main.go`
1. **Extend `JobArgs`**:
   Add a `BlockingJobs []string` field to the JSON parameter struct.
   ```go
   type JobArgs struct {
       // ... existing fields ...
       BlockingJobs []string `json:"blocking_jobs"`
   }
   ```

2. **Implement Check Logic**:
   Right after the Singleton Advisory Lock (which prevents multiple instances of the *same* job), we will insert a query to check for blocking jobs.
   
   **Query**:
   ```sql
   SELECT sp.name 
   FROM program_runs pr
   JOIN scheduled_programs sp ON pr.program_id = sp.id
   WHERE sp.name = ANY($1) 
     AND pr.finished_at IS NULL
   LIMIT 1;
   ```
   *Note:* The query checks if there is any active run (`finished_at IS NULL`) for the job names provided in `BlockingJobs`.

3. **Handle Early Exit**:
   If the query returns a blocking job name (e.g., "mitm_deliver_Org"):
   - Log the event via `log.Printf`.
   - Send an audit log message via `logAudit(...)` stating which blocking job caused the exit.
   - Send an IPC event if `ipcClient` is active (e.g., `ipcClient.SendEvent("exited", "Blocked by running job", 0)`).
   - Exit with `os.Exit(0)` (graceful termination).

## 3. SpecDD Constraints Validation
- **Architecture**: Valid. The delivery layer uses its existing PostgreSQL pool to query the central state tables (`program_runs`, `scheduled_programs`) in read-only mode.
- **Security**: Valid. No impact on PII or Envelope Encryption.
- **Data Model**: Valid. We are reusing the existing `program_runs` schema.

## 4. Edge Cases Addressed
- **Multiple Blocking Jobs**: The `ANY($1)` SQL clause handles an array of blocking jobs efficiently in a single query.
- **No Blocking Jobs Configured**: If `len(jobArgs.BlockingJobs) == 0`, the query is bypassed to save a database roundtrip.
- **Job Crashes**: We rely on the Scheduler's existing mechanisms for cleaning up stale `finished_at IS NULL` states (as noted in the SpecDD architecture for the scheduler).

---
*Status: Ready for review and conversion into tasks.*

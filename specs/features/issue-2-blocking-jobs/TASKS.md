# Work Breakdown: Job Blocking Criteria (Issue #2)

## Task 1: Extend JobArgs and implement Blocking Check
**Component:** `delivery-layer/mitm_delivery`
**Target File:** `cmd/deliver/main.go`

**Instructions:**
1. Extend the `JobArgs` struct to include a new field: `BlockingJobs []string json:"blocking_jobs"`.
2. Locate the database initialization and Advisory Lock section (around line 175, after the `pg_try_advisory_lock` check).
3. Add a check: if `len(jobArgs.BlockingJobs) > 0`, query the `program_runs` and `scheduled_programs` tables.
   - Use `pool.QueryRow(ctx, "SELECT sp.name FROM program_runs pr JOIN scheduled_programs sp ON pr.program_id = sp.id WHERE sp.name = ANY($1) AND pr.finished_at IS NULL LIMIT 1", jobArgs.BlockingJobs).Scan(&blockingJobName)`
   - Be sure to handle `pgx.ErrNoRows` gracefully (it means no blocking job is running, which is the good path).
4. If a blocking job *is* found (no error):
   - Use `log.Printf` to log that the job is blocked.
   - If `ipcClient != nil`, send an audit message (`ipcClient.SendAudit(...)`).
   - If `ipcClient != nil`, send an IPC event (e.g., `ipcClient.SendEvent("exited", fmt.Sprintf("Blocked by running job: %s", blockingJobName), 0)`).
   - Exit the application with `os.Exit(0)`.

*Note for Implementation Phase:* Ensure that `context` and `pgx` imports are properly utilized and that the check happens *before* the repositories and worker pools are initialized.

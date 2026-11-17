# Implementation Plan: Issue #6

## 1. Spec Chain & Authority

`mitm-2.sdd` → `delivery-layer/delivery-layer.sdd` (Owns `./**`) → `specs/features/issue-6-ipc-exit-events/SPEC.md`.
No `.sdd` file or database schema changes required.

## 2. Architecture Check

| Constraint | Result |
|---|---|
| Envelope Encryption / KEK in RAM only | Preserved (`dbConfigJSON` and `masterKey` remain local variables in `run()`). |
| Nil-safe IPC communication (`mitm-2.sdd`) | Preserved and extended to cover all startup, early-exit, fatal, and completion paths. |
| Logging separation (`system_logs` vs `job_audit_logs`) | Strengthened: every job termination path emits both status event and job audit log. |
| Independent Go binary (`go.mod`) | Preserved. |

## 3. Design (`cmd/cority/main.go`)

1. **Extract `run() int` from `main()`:**
   - `main()` calls `if code := run(); code != 0 { os.Exit(code) }`.
   - All `defer` statements (`ipcClient.Close()`, `pool.Close()`, `lockConn.Release()`, context cancel funcs) execute deterministically on every return path from `run()`.
2. **Early IPC Initialization:**
   - Immediately after fetching credentials from the scheduler and normalizing `version`, parse `RUN_ID` and `SCHEDULER_SOCKET_PATH`.
   - If `runID > 0 && socketPath != ""`, initialize `ipcClient` with `Component: "mitm_delivery"`, `SourceName: "DELIVERY"`, `defer ipcClient.Close()`, and emit:
     - `ipcClient.SendEvent("started", fmt.Sprintf("%s (%s) started", appName, version), 0)`
     - `ipcClient.SendAudit(fmt.Sprintf("%s (%s) started", appName, version))`
   - Once `jobArgs` is parsed and defaults applied, update `ipcClient.Topic = jobArgs.Topic` and `ipcClient.SourceName = jobArgs.SourceName`.
3. **Central Exit Helpers:**
   - `failf(ipcClient, format, args...) int`:
     - Formats `msg`, logs via `log.Print(msg)`.
     - Calls `ipcClient.SendEvent("failed", msg, 0)`.
     - Calls `ipcClient.SendAudit("ERROR: " + msg)`.
     - Calls `ipcClient.SendAudit(fmt.Sprintf("%s (%s) failed", appName, version))`.
     - Returns `1`.
   - `exitGracefully(ipcClient, msg string) int`:
     - Logs `AUDIT: <msg>`, calls `ipcClient.SendAudit(msg)`.
     - Calls `ipcClient.SendEvent("exited", msg, 0)`.
     - Calls `ipcClient.SendAudit(fmt.Sprintf("%s (%s) exited", appName, version))`.
     - Returns `0`.
   - `finishRun(ipcClient, status, summary string, exitCode int) int`:
     - If `status == "failed"`, calls `logAudit("ERROR: " + summary)`, else `logAudit(summary)`.
     - Logs `Delivery Batch Job <status>. <summary>`.
     - Calls `ipcClient.SendEvent(status, summary, 100)`.
     - Calls `ipcClient.SendAudit(fmt.Sprintf("%s (%s) %s", appName, version, status))`.
     - Returns `exitCode`.
4. **System Log Lifecycle Entries (`system_logs`):**
   - `versionTag()` normalizes `version` to `"v<semver>"` (e.g., `"v0.22.1"`).
   - `resolveJobName(ctx, pool, runID, topic)` queries `scheduled_programs.name` for `runID` and falls back to `"Delivery " + topic` (or `appName` if topic is empty).
   - `startSystemLogMessage(jobName)` formats `"Starting job <jobName> (v<version>)"` and `logSystem(pool, "INFO", ...)` writes it to `system_logs` (`component = 'mitm_delivery'`) once `pool` is connected.
   - `failf`, `exitGracefully`, and `finishRun` also call `logSystem(pool, ...)` (nil-safe when `pool == nil`) with the versioned job title.
5. **Packager Error Handling:**
   - If `pkgRepo.PackageTargetFragments` returns a non-context error (`ctx.Err() == nil`), record the failure via `logAudit("ERROR: " + ...)` and mark `packagingFailed = true` so the run finishes with status `"failed"` and exit code `1` after draining/cleaning up workers, instead of silently reporting `"finished"`.

/**
 * SPDX-FileComment: Cority Delivery Batch Job
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file main.go
 * @brief Entry point: packages target fragments and delivers them to Cority SaaS
 * @version 0.23.0
 * @date 2026-11-18
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"mitm_cority/internal/db"
	"mitm_cority/internal/delivery"
	"mitm_cority/internal/engine"
	"mitm_cority/internal/ipc"
)

var (
	appName        = "Delivery Engine"
	appDescription = "Delivers packaged data to target systems"
	version        = "0.23.0"
)

// flexInt accepts a JSON number or a numeric JSON string ("" and null count as 0).
type flexInt int

func (f *flexInt) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(strings.Trim(string(b), `"`))
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("invalid integer value %s", string(b))
	}
	*f = flexInt(n)
	return nil
}

type JobArgs struct {
	Topic        string   `json:"topic"`
	Workers      flexInt  `json:"workers"`
	BatchSize    flexInt  `json:"batch_size"`
	MaxRetries   flexInt  `json:"max_retries"`
	SourceName   string   `json:"source_name"`
	BlockingJobs []string `json:"blocking_jobs"`
	// BlockingMaxRuntimeMinutes ignores blocking runs started longer ago (0 = no limit).
	BlockingMaxRuntimeMinutes flexInt `json:"blocking_max_runtime_minutes"`
	// MaxConsecutiveErrors aborts the job after this many failed claim attempts in a row (default 5).
	MaxConsecutiveErrors flexInt `json:"max_consecutive_errors"`
	// MaxRuntimeMinutes stops the job gracefully after this time (0 = no limit).
	MaxRuntimeMinutes flexInt `json:"max_runtime_minutes"`
	// MaxRetryWaitSeconds waits in-run for retries due within this time (0 = default 300, negative = off).
	MaxRetryWaitSeconds flexInt `json:"max_retry_wait_seconds"`
	// MaxConsecutiveDeliveryFailures stops the run after this many transient package failures in a row (0 = default 3, negative = off).
	MaxConsecutiveDeliveryFailures flexInt `json:"max_consecutive_delivery_failures"`
}

// hardStopGrace is how long a run may take to shut down gracefully after max_runtime_minutes.
const hardStopGrace = 60 * time.Second

// exitProcess terminates the process; replaced in tests.
var exitProcess = os.Exit

// stopReason records why a run was stopped before all packages were processed.
type stopReason int32

const (
	stopNone      stopReason = iota
	stopAbort                // job-level failure (e.g. authentication)
	stopDegraded             // circuit breaker: the target keeps failing transiently
	stopPostponed            // the target asked for a pause that ends after the run deadline
)

// runControl stops a run and implements the circuit breaker over consecutive transient failures.
type runControl struct {
	cancel      context.CancelFunc
	maxFailures int // <= 0 disables the circuit breaker
	reason      atomic.Int32
	failures    atomic.Int64
}

// stop cancels the run; the first reason wins.
func (rc *runControl) stop(r stopReason) {
	rc.reason.CompareAndSwap(int32(stopNone), int32(r))
	rc.cancel()
}

func (rc *runControl) stopReason() stopReason {
	return stopReason(rc.reason.Load())
}

// recordResult updates the consecutive transient failure count and reports whether the breaker tripped.
func (rc *runControl) recordResult(outcome engine.Outcome, err error) bool {
	var delErr *delivery.DeliveryError
	transient := errors.As(err, &delErr) && delErr.IsTransient
	switch {
	case outcome == engine.OutcomeRetry || (outcome == engine.OutcomeDLQ && transient):
		n := rc.failures.Add(1)
		return rc.maxFailures > 0 && n == int64(rc.maxFailures)
	case outcome == engine.OutcomeDelivered || outcome == engine.OutcomeDLQ:
		rc.failures.Store(0)
	}
	return false
}

// deliveryStats counts package outcomes across workers.
type deliveryStats struct {
	delivered, retry, dlq, interrupted, errors, rejected atomic.Int64
}

// shouldWaitForRetry decides whether the dispatcher should sleep until next before claiming again.
func shouldWaitForRetry(ctx context.Context, next time.Time, maxRetryWait time.Duration) (time.Duration, bool) {
	if maxRetryWait <= 0 {
		return 0, false
	}
	wait := time.Until(next)
	if wait > maxRetryWait {
		return 0, false
	}
	if deadline, ok := ctx.Deadline(); ok && next.After(deadline) {
		return 0, false
	}
	return max(wait, time.Second), true
}

// resolveTerminalOutcome maps run state to the final IPC status and process exit code.
// A graceful max-runtime timeout, a degraded target or signal cancellation returns exit code 0 even if earlier
// packages had DLQ/rejected records so that scheduler restart_on_exit does not restart a timed-out or stopped run.
func resolveTerminalOutcome(ctxErr error, reason stopReason, claimFailed, packagingFailed bool, stats *deliveryStats) (string, int) {
	switch {
	case reason == stopAbort || claimFailed || packagingFailed:
		return "failed", 1
	case reason == stopDegraded:
		return "degraded", 0
	case reason == stopPostponed || errors.Is(ctxErr, context.DeadlineExceeded):
		return "timeout", 0
	case ctxErr != nil:
		return "cancelled", 0
	case stats.dlq.Load() > 0 || stats.rejected.Load() > 0 || stats.errors.Load() > 0:
		return "failed", 2
	default:
		return "finished", 0
	}
}

// sleepCtx sleeps for d and returns false if ctx was cancelled meanwhile.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// buildConnString builds a PostgreSQL URL with URL-escaped credentials.
func buildConnString(user, pass, host, port, dbName, sslMode string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, pass),
		Host:     net.JoinHostPort(host, port),
		Path:     "/" + dbName,
		RawQuery: url.Values{"sslmode": {sslMode}}.Encode(),
	}
	return u.String()
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func versionTag() string {
	v := strings.TrimPrefix(strings.Split(version, "-")[0], "v")
	return "v" + v
}

func startSystemLogMessage(jobName string) string {
	if jobName == "" {
		jobName = appName
	}
	return fmt.Sprintf("Starting job %s (%s)", jobName, versionTag())
}

func resolveJobName(ctx context.Context, pool *pgxpool.Pool, runID int, topic string) string {
	if pool != nil && runID > 0 {
		var name string
		err := pool.QueryRow(ctx, `
			SELECT sp.name
			FROM program_runs pr
			JOIN scheduled_programs sp ON pr.program_id = sp.id
			WHERE pr.id = $1`, runID).Scan(&name)
		if err == nil && name != "" {
			return name
		}
	}
	if topic != "" {
		return "Delivery " + topic
	}
	return appName
}

func logSystem(pool *pgxpool.Pool, level, message string) {
	if pool == nil {
		return
	}
	_, _ = pool.Exec(context.Background(),
		"INSERT INTO system_logs (level, component, message) VALUES ($1, 'mitm_delivery', $2)",
		level, message)
}

func logAudit(ipcClient *ipc.IPCClient, msg string) {
	log.Printf("AUDIT: %s", msg)
	ipcClient.SendAudit(msg)
}

func failf(ipcClient *ipc.IPCClient, pool *pgxpool.Pool, jobName, format string, args ...interface{}) int {
	if jobName == "" {
		jobName = appName
	}
	msg := fmt.Sprintf(format, args...)
	log.Print(msg)
	ipcClient.SendEvent("failed", msg, 0)
	ipcClient.SendAudit("ERROR: " + msg)
	ipcClient.SendAudit(fmt.Sprintf("%s (%s) failed", appName, versionTag()))
	logSystem(pool, "ERROR", fmt.Sprintf("Job %s (%s) failed: %s", jobName, versionTag(), msg))
	return 1
}

func exitGracefully(ipcClient *ipc.IPCClient, pool *pgxpool.Pool, jobName, msg string) int {
	if jobName == "" {
		jobName = appName
	}
	logAudit(ipcClient, msg)
	ipcClient.SendEvent("exited", msg, 0)
	ipcClient.SendAudit(fmt.Sprintf("%s (%s) exited", appName, versionTag()))
	logSystem(pool, "INFO", fmt.Sprintf("Job %s (%s) exited: %s", jobName, versionTag(), msg))
	return 0
}

func finishRun(ipcClient *ipc.IPCClient, pool *pgxpool.Pool, jobName, status, summary string, exitCode int) int {
	if jobName == "" {
		jobName = appName
	}
	level := "INFO"
	if status == "failed" || status == "degraded" {
		level = "ERROR"
		logAudit(ipcClient, "ERROR: "+summary)
	} else {
		logAudit(ipcClient, summary)
	}
	log.Printf("Delivery Batch Job %s. %s", status, summary)
	ipcClient.SendEvent(status, summary, 100)
	ipcClient.SendAudit(fmt.Sprintf("%s (%s) %s", appName, versionTag(), status))
	logSystem(pool, level, fmt.Sprintf("Job %s (%s) %s. %s", jobName, versionTag(), status, summary))
	return exitCode
}

func main() {
	if exitCode := run(); exitCode != 0 {
		os.Exit(exitCode)
	}
}

func run() int {
	// Secrets are kept in variables only (never written to the process environment).
	// ENV values are the fallback for standalone execution.
	dbConfigJSON := os.Getenv("MITM_DB_CONFIG_JSON")
	masterKey := os.Getenv("MASTER_KEY")

	// Fetch credentials via IPC if running under scheduler
	if ipcDBCfg, ipcMasterKey, err := fetchCredentialsFromScheduler(); err == nil {
		if ipcDBCfg != "" {
			dbConfigJSON = ipcDBCfg
		}
		if ipcMasterKey != "" {
			masterKey = ipcMasterKey
		}
	} else if os.Getenv("RUN_ID") != "" && os.Getenv("SCHEDULER_SOCKET_PATH") != "" {
		log.Printf("[IPC Warning] Failed to get credentials from scheduler: %v", err)
	}

	version = strings.TrimPrefix(strings.Split(version, "-")[0], "v")

	runID, _ := strconv.Atoi(getEnv("RUN_ID", "0"))
	socketPath := getEnv("SCHEDULER_SOCKET_PATH", "")

	var ipcClient *ipc.IPCClient
	if runID > 0 && socketPath != "" {
		ipcClient = &ipc.IPCClient{
			SocketPath: socketPath,
			RunID:      runID,
			Component:  "mitm_delivery",
			SourceName: "DELIVERY",
		}
		ipcClient.SendEvent("started", fmt.Sprintf("%s (%s) started", appName, versionTag()), 0)
		ipcClient.SendAudit(fmt.Sprintf("%s (%s) started", appName, versionTag()))
	}
	defer ipcClient.Close()

	jobName := appName
	if len(os.Args) < 2 {
		return failf(ipcClient, nil, jobName, "Usage: %s <job_args_json>", os.Args[0])
	}

	// 1. Parse Job Arguments (os.Args[1])
	var jobArgs JobArgs
	if err := json.Unmarshal([]byte(os.Args[1]), &jobArgs); err != nil {
		return failf(ipcClient, nil, jobName, "Failed to parse JobArgs JSON from os.Args[1]: %v", err)
	}

	if jobArgs.Workers <= 0 {
		jobArgs.Workers = 5
	}
	if jobArgs.BatchSize <= 0 {
		jobArgs.BatchSize = 100
	}
	if jobArgs.MaxRetries <= 0 {
		jobArgs.MaxRetries = 5
	}
	if jobArgs.SourceName == "" {
		jobArgs.SourceName = "DELIVERY"
	}
	if jobArgs.MaxConsecutiveErrors <= 0 {
		jobArgs.MaxConsecutiveErrors = 5
	}
	if jobArgs.MaxRetryWaitSeconds == 0 {
		jobArgs.MaxRetryWaitSeconds = 300
	}
	if jobArgs.MaxConsecutiveDeliveryFailures == 0 {
		jobArgs.MaxConsecutiveDeliveryFailures = 3
	}
	if jobArgs.Topic != "" {
		jobName = "Delivery " + jobArgs.Topic
	}

	if ipcClient != nil {
		ipcClient.Topic = jobArgs.Topic
		ipcClient.SourceName = jobArgs.SourceName
	}

	// 2. Database Connection Setup
	configSource := "Environment Variables"
	dbHost := ""
	dbPort := ""
	dbUser := ""
	dbPass := ""
	dbName := ""

	sslModeSetting := os.Getenv("MITM_DB_SSLMODE")
	jsonConfig := dbConfigJSON
	if jsonConfig != "" {
		var fullCfg struct {
			DB struct {
				Host     string `json:"host"`
				Port     int    `json:"port"`
				User     string `json:"user"`
				Password string `json:"password"`
				Database string `json:"database"`
				SSLMode  bool   `json:"sslmode"`
			} `json:"db"`
		}
		if err := json.Unmarshal([]byte(jsonConfig), &fullCfg); err != nil {
			return failf(ipcClient, nil, jobName, "Failed to parse MitM JSON configuration: %v", err)
		}
		dbHost = fullCfg.DB.Host
		dbPort = fmt.Sprintf("%d", fullCfg.DB.Port)
		dbUser = fullCfg.DB.User
		dbPass = fullCfg.DB.Password
		dbName = fullCfg.DB.Database
		if fullCfg.DB.SSLMode {
			sslModeSetting = "require"
		} else {
			sslModeSetting = "disable"
		}
		configSource = "JSON Config (MITM_DB_CONFIG_JSON)"
	} else {
		dbHost = getEnv("MITM_DB_HOST", getEnv("DB_HOST", getEnv("PGHOST", "localhost")))
		dbPort = getEnv("MITM_DB_PORT", getEnv("DB_PORT", getEnv("PGPORT", "5432")))
		dbUser = getEnv("MITM_DB_USER", getEnv("DB_USER", getEnv("PGUSER", "postgres")))
		dbPass = getEnv("MITM_DB_PASSWORD", getEnv("DB_PASS", getEnv("PGPASSWORD", "")))
		dbName = getEnv("MITM_DB_NAME", getEnv("DB_NAME", getEnv("PGDATABASE", "postgres")))
	}

	ipcClient.SendAudit(fmt.Sprintf("Loaded database configuration from %s", configSource))

	sslMode := "disable"
	if sslModeSetting != "" {
		if sslModeSetting == "true" {
			sslMode = "require"
		} else if sslModeSetting == "false" {
			sslMode = "disable"
		} else {
			sslMode = sslModeSetting
		}
	}
	connString := buildConnString(dbUser, dbPass, dbHost, dbPort, dbName, sslMode)
	config_pool, err := pgxpool.ParseConfig(connString)
	if err == nil {
		config_pool.MaxConns = 20
		config_pool.MaxConnIdleTime = 5 * time.Minute
		config_pool.MaxConnLifetime = 1 * time.Hour
	}
	var pool *pgxpool.Pool
	if err == nil {
		pool, err = pgxpool.NewWithConfig(context.Background(), config_pool)
	}
	if err != nil {
		return failf(ipcClient, nil, jobName, "Unable to connect to database: %v", err)
	}
	defer pool.Close()

	jobName = resolveJobName(context.Background(), pool, runID, jobArgs.Topic)
	logSystem(pool, "INFO", startSystemLogMessage(jobName))

	// 2b. Singleton Lock via Postgres Advisory Lock
	lockConn, err := pool.Acquire(context.Background())
	if err != nil {
		return failf(ipcClient, pool, jobName, "Failed to acquire connection for advisory lock: %v", err)
	}
	defer lockConn.Release()

	// 64-bit lock key derived from the topic (avoids 32-bit hash collisions between topics)
	lockName := "delivery_" + jobArgs.Topic

	var locked bool
	err = lockConn.QueryRow(context.Background(), "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lockName).Scan(&locked)
	if err != nil {
		return failf(ipcClient, pool, jobName, "Failed to execute pg_try_advisory_lock: %v", err)
	}
	if !locked {
		return exitGracefully(ipcClient, pool, jobName, fmt.Sprintf("Another delivery instance for topic '%s' is already running (Advisory Lock '%s'). Exiting gracefully.", jobArgs.Topic, lockName))
	}

	// 3. Initialize Repositories and Engine
	pkgRepo := db.NewPackageRepo(pool)
	dlqRepo := db.NewDLQRepo(pool)
	retryEngine := engine.NewRetryEngine(pkgRepo, dlqRepo, int(jobArgs.MaxRetries))

	targetRepo := db.NewTargetRepo(pool)

	auditFn := func(msg string) {
		logAudit(ipcClient, msg)
	}

	// 3b. Check for Blocking Jobs
	if len(jobArgs.BlockingJobs) > 0 {
		var blockingJobName string
		var runningMinutes int
		err := pool.QueryRow(context.Background(), `
			SELECT sp.name, COALESCE((EXTRACT(EPOCH FROM (NOW() - pr.started_at)) / 60)::int, 0)
			FROM program_runs pr 
			JOIN scheduled_programs sp ON pr.program_id = sp.id 
			WHERE sp.name = ANY($1) 
			  AND pr.finished_at IS NULL 
			  AND ($2::int = 0 OR pr.started_at > NOW() - make_interval(mins => $2::int))
			ORDER BY pr.started_at DESC
			LIMIT 1`, jobArgs.BlockingJobs, int(jobArgs.BlockingMaxRuntimeMinutes)).Scan(&blockingJobName, &runningMinutes)

		if err == nil {
			return exitGracefully(ipcClient, pool, jobName, fmt.Sprintf("Exiting gracefully: blocking job '%s' is currently running (for %d minutes).", blockingJobName, runningMinutes))
		} else if err != pgx.ErrNoRows {
			log.Printf("Warning: Failed to check blocking jobs: %v", err)
		}
	}

	// 3c. Recover packages orphaned by a previous crashed run (we hold the topic lock)
	if n, err := pkgRepo.ResetSending(context.Background(), jobArgs.Topic); err != nil {
		return failf(ipcClient, pool, jobName, "Failed to reset orphaned packages: %v", err)
	} else if n > 0 {
		logAudit(ipcClient, fmt.Sprintf("Reset %d orphaned packages from 'sending' to 'pending'", n))
	}

	// Fetch target config dynamically
	targetConfig, err := targetRepo.GetDeliveryTarget(context.Background(), jobArgs.Topic, masterKey)
	if err != nil {
		return failf(ipcClient, pool, jobName, "Failed to fetch delivery target config for topic '%s': %v", jobArgs.Topic, err)
	}

	// 4. Instantiate Delivery Sender
	var sender delivery.DeliverySender
	if targetConfig.AdapterType != "CORITY_SAAS" {
		return failf(ipcClient, pool, jobName, "Unsupported adapter_type for Cority: %s", targetConfig.AdapterType)
	}
	corityCfg, err := delivery.ParseCorityAuthConfig(targetConfig.AuthConfig)
	if err != nil {
		return failf(ipcClient, pool, jobName, "Invalid delivery target config for topic '%s': %v", jobArgs.Topic, err)
	}
	if jobArgs.MaxRuntimeMinutes <= 0 && corityCfg.EffectiveMaxRuntimeMinutes() > 0 {
		jobArgs.MaxRuntimeMinutes = flexInt(corityCfg.EffectiveMaxRuntimeMinutes())
	}
	if jobArgs.MaxRuntimeMinutes > 0 {
		logAudit(ipcClient, fmt.Sprintf("Max Runtime: %d minutes", jobArgs.MaxRuntimeMinutes))
	} else {
		logAudit(ipcClient, "Max Runtime: unlimited")
	}
	sender = delivery.NewCorityAdapter(nil, auditFn, delivery.NewPGTokenStore(pool))
	if jobArgs.Workers > 1 {
		log.Printf("Forcing workers to 1 for CORITY_SAAS to prevent concurrent import errors.")
		jobArgs.Workers = 1
	}

	// Context for graceful shutdown (signals, job abort) and the optional max runtime
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := baseCtx
	if jobArgs.MaxRuntimeMinutes > 0 {
		var cancelRuntime context.CancelFunc
		ctx, cancelRuntime = context.WithTimeout(baseCtx, time.Duration(jobArgs.MaxRuntimeMinutes)*time.Minute)
		defer cancelRuntime()
	}

	// rc stops all processing after a job-level failure, a degraded target or a postponed run
	rc := &runControl{cancel: cancel, maxFailures: int(jobArgs.MaxConsecutiveDeliveryFailures)}

	// Hard limit: terminate the process if the graceful shutdown does not finish after the deadline.
	if jobArgs.MaxRuntimeMinutes > 0 {
		watchdog := time.AfterFunc(time.Duration(jobArgs.MaxRuntimeMinutes)*time.Minute+hardStopGrace, func() {
			hardStop(ipcClient, pool, pkgRepo, jobName, jobArgs.Topic, int(jobArgs.MaxRuntimeMinutes), exitProcess)
		})
		defer watchdog.Stop()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	go func() {
		select {
		case <-sigs:
			log.Println("Shutting down gracefully...")
			cancel()
		case <-baseCtx.Done():
		}
	}()

	log.Printf("Starting Delivery Batch Job (Workers: %d, Batch Size: %d)...", jobArgs.Workers, jobArgs.BatchSize)
	ipcClient.SendEvent("processing", fmt.Sprintf("Starting Delivery Batch Job (Workers: %d, Batch Size: %d)...", jobArgs.Workers, jobArgs.BatchSize), 0)

	// 5. Worker Pool Setup
	jobs := make(chan db.Package, int(jobArgs.BatchSize))
	var wg sync.WaitGroup
	var batch sync.WaitGroup // tracks the packages of the current dispatcher batch
	var stats deliveryStats

	// Start workers
	for i := 0; i < int(jobArgs.Workers); i++ {
		wg.Add(1)
		go worker(ctx, &wg, &batch, jobs, retryEngine, sender, *targetConfig, auditFn, &stats, rc)
	}

	// 6. Packager: Create delivery packages from target_fragments
	packagingFailed := false
	for {
		packagedCount, failedIDs, err := pkgRepo.PackageTargetFragments(ctx, jobArgs.Topic, int(jobArgs.BatchSize))
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("Error packaging target fragments: %v", err)
				logAudit(ipcClient, fmt.Sprintf("ERROR: Error packaging target fragments: %v", err))
				packagingFailed = true
			}
			break
		}
		if len(failedIDs) > 0 {
			logAudit(ipcClient, fmt.Sprintf("Marked %d malformed target fragments as FAILED: %s", len(failedIDs), strings.Join(failedIDs, ", ")))
		}
		if packagedCount == 0 && len(failedIDs) == 0 {
			break
		}
		if packagedCount > 0 {
			log.Printf("Packaged %d target fragments into a new delivery package.", packagedCount)
		}
	}

	// 7. Dispatcher Loop: claim a batch, wait until it is processed, then claim the next one
	maxRetryWait := time.Duration(jobArgs.MaxRetryWaitSeconds) * time.Second
	consecutiveErrors := 0
	claimFailed := false
	// handleDBError counts a failed DB attempt and backs off; it returns false when the job must stop.
	handleDBError := func(what string, err error) bool {
		if ctx.Err() != nil {
			return false
		}
		consecutiveErrors++
		log.Printf("Error %s (%d/%d): %v", what, consecutiveErrors, jobArgs.MaxConsecutiveErrors, err)
		if consecutiveErrors >= int(jobArgs.MaxConsecutiveErrors) {
			logAudit(ipcClient, fmt.Sprintf("Aborting: %s failed %d times in a row: %v", what, consecutiveErrors, err))
			claimFailed = true
			return false
		}
		backoff := min(time.Duration(1<<consecutiveErrors)*time.Second, 30*time.Second)
		return sleepCtx(ctx, backoff)
	}

dispatcherLoop:
	for {
		if ctx.Err() != nil {
			break dispatcherLoop
		}

		packages, err := pkgRepo.ClaimPendingPackages(ctx, jobArgs.Topic, int(jobArgs.BatchSize))
		if err != nil {
			if !handleDBError("claiming packages", err) {
				break dispatcherLoop
			}
			continue
		}
		consecutiveErrors = 0

		if len(packages) == 0 {
			// Wait in-run for retries that become due soon
			if maxRetryWait > 0 {
				next, ok, err := pkgRepo.NextRetryAt(ctx, jobArgs.Topic)
				if err != nil {
					if !handleDBError("checking pending retries", err) {
						break dispatcherLoop
					}
					continue
				}
				if ok {
					if wait, okWait := shouldWaitForRetry(ctx, next, maxRetryWait); okWait {
						log.Printf("Waiting %v for packages scheduled for retry...", wait.Round(time.Second))
						if !sleepCtx(ctx, wait) {
							break dispatcherLoop
						}
						continue
					}
				}
			}
			log.Println("No more packages to deliver. Batch job complete.")
			break dispatcherLoop
		}

		log.Printf("Claimed batch of %d packages", len(packages))
		for _, p := range packages {
			batch.Add(1)
			select {
			case jobs <- p:
			case <-ctx.Done():
				batch.Done()
				break dispatcherLoop
			}
		}
		batch.Wait()
	}

	close(jobs)
	wg.Wait()

	// Return claimed but unprocessed packages (shutdown, persistence errors) to 'pending'
	if n, err := pkgRepo.ResetSending(context.WithoutCancel(ctx), jobArgs.Topic); err != nil {
		log.Printf("Failed to reset unprocessed packages: %v", err)
	} else if n > 0 {
		logAudit(ipcClient, fmt.Sprintf("Reset %d unprocessed packages from 'sending' to 'pending'", n))
	}

	summary := fmt.Sprintf("Topic %s: %d delivered, %d scheduled for retry, %d moved to DLQ, %d records withheld (DLQ), %d interrupted, %d state errors",
		jobArgs.Topic, stats.delivered.Load(), stats.retry.Load(), stats.dlq.Load(), stats.rejected.Load(), stats.interrupted.Load(), stats.errors.Load())

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		logAudit(ipcClient, fmt.Sprintf("Max runtime of %d minutes reached; remaining packages are left for the next run", jobArgs.MaxRuntimeMinutes))
	}
	status, exitCode := resolveTerminalOutcome(ctx.Err(), rc.stopReason(), claimFailed, packagingFailed, &stats)
	return finishRun(ipcClient, pool, jobName, status, summary, exitCode)
}

func worker(ctx context.Context, wg *sync.WaitGroup, batch *sync.WaitGroup, jobs <-chan db.Package, retryEngine *engine.RetryEngine, sender delivery.DeliverySender, config delivery.TargetConfig, logAudit func(string), stats *deliveryStats, rc *runControl) {
	defer wg.Done()
	for pkg := range jobs {
		processPackage(ctx, pkg, retryEngine, sender, config, logAudit, stats, rc)
		batch.Done()
	}
}

func processPackage(ctx context.Context, pkg db.Package, retryEngine *engine.RetryEngine, sender delivery.DeliverySender, config delivery.TargetConfig, logAudit func(string), stats *deliveryStats, rc *runControl) {
	if ctx.Err() != nil {
		// Shutdown: leave the package in 'sending'; it is reset to 'pending' after all workers stopped.
		stats.interrupted.Add(1)
		return
	}
	res, err := retryEngine.ProcessPackage(ctx, pkg, sender, config)
	stats.rejected.Add(int64(res.RejectedRecords))
	switch res.Outcome {
	case engine.OutcomeAbort:
		logAudit(fmt.Sprintf("Aborting job: %v", err))
		rc.stop(stopAbort)
	case engine.OutcomePostponed:
		// Left in 'sending'; reset to 'pending' after all workers stopped.
		stats.interrupted.Add(1)
		logAudit(fmt.Sprintf("Package %s postponed: %v; remaining packages are left for the next run", pkg.ID, err))
		rc.stop(stopPostponed)
		return
	case engine.OutcomeDelivered:
		stats.delivered.Add(1)
	case engine.OutcomeRetry:
		stats.retry.Add(1)
	case engine.OutcomeDLQ:
		stats.dlq.Add(1)
	case engine.OutcomeInterrupted:
		stats.interrupted.Add(1)
		return
	default:
		stats.errors.Add(1)
	}
	if err != nil {
		log.Printf("Failed to process package %s: %v", pkg.ID, err)
		logAudit(fmt.Sprintf("Package %s failed: %v", pkg.ID, err))
	} else {
		logAudit(fmt.Sprintf("Package %s delivered successfully", pkg.ID))
	}
	if rc.recordResult(res.Outcome, err) {
		logAudit(fmt.Sprintf("ERROR: Target seems degraded: %d packages in a row failed transiently; stopping the run, remaining packages are left for the next run", rc.maxFailures))
		rc.stop(stopDegraded)
	}
}

// hardStop ends a run that did not shut down within hardStopGrace after max_runtime_minutes.
// It must not use log.Printf, which blocks when the job's stderr is not read.
func hardStop(ipcClient *ipc.IPCClient, pool *pgxpool.Pool, pkgRepo *db.PackageRepo, jobName, topic string, maxRuntimeMinutes int, exit func(int)) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg := fmt.Sprintf("Max runtime of %d minutes reached and the graceful shutdown did not finish within %v; terminating the job", maxRuntimeMinutes, hardStopGrace)
	if pkgRepo != nil {
		if n, err := pkgRepo.ResetSending(ctx, topic); err == nil && n > 0 {
			msg += fmt.Sprintf("; reset %d packages from 'sending' to 'pending'", n)
		}
	}
	ipcClient.SendAudit("WARNING: " + msg)
	ipcClient.SendEvent("timeout", msg, 100)
	ipcClient.SendAudit(fmt.Sprintf("%s (%s) timeout", appName, versionTag()))
	if pool != nil {
		_, _ = pool.Exec(ctx, "INSERT INTO system_logs (level, component, message) VALUES ('WARN', 'mitm_delivery', $1)",
			fmt.Sprintf("Job %s (%s) timeout: %s", jobName, versionTag(), msg))
	}
	exit(0)
}

func fetchCredentialsFromScheduler() (string, string, error) {
	runIDStr := os.Getenv("RUN_ID")
	socketPath := os.Getenv("SCHEDULER_SOCKET_PATH")
	if runIDStr == "" || socketPath == "" {
		return "", "", fmt.Errorf("not running under scheduler")
	}

	runID, err := strconv.Atoi(runIDStr)
	if err != nil {
		return "", "", err
	}

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return "", "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	req := map[string]interface{}{
		"type":   "get_credentials",
		"run_id": runID,
	}
	data, _ := json.Marshal(req)
	if _, err := conn.Write(append(data, '\n')); err != nil {
		return "", "", err
	}

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		var resp struct {
			MasterKey    string `json:"master_key"`
			DBConfigJSON string `json:"db_config_json"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &resp); err == nil {
			return resp.DBConfigJSON, resp.MasterKey, nil
		}
	}
	return "", "", fmt.Errorf("no response or invalid JSON from scheduler")
}

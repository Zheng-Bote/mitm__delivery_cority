/**
 * SPDX-FileComment: Cority Delivery Batch Job
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file main.go
 * @brief Entry point: packages target fragments and delivers them to Cority SaaS
 * @version 0.22.0
 * @date 2026-10-17
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
	version        = "0.22.0"
)

type JobArgs struct {
	Topic        string   `json:"topic"`
	Workers      int      `json:"workers"`
	BatchSize    int      `json:"batch_size"`
	MaxRetries   int      `json:"max_retries"`
	SourceName   string   `json:"source_name"`
	BlockingJobs []string `json:"blocking_jobs"`
	// BlockingMaxRuntimeMinutes ignores blocking runs started longer ago (0 = no limit).
	BlockingMaxRuntimeMinutes int `json:"blocking_max_runtime_minutes"`
	// MaxConsecutiveErrors aborts the job after this many failed claim attempts in a row (default 5).
	MaxConsecutiveErrors int `json:"max_consecutive_errors"`
	// MaxRuntimeMinutes stops the job gracefully after this time (0 = no limit).
	MaxRuntimeMinutes int `json:"max_runtime_minutes"`
	// MaxRetryWaitSeconds waits in-run for retries due within this time (0 = default 300, negative = off).
	MaxRetryWaitSeconds int `json:"max_retry_wait_seconds"`
}

// deliveryStats counts package outcomes across workers.
type deliveryStats struct {
	delivered, retry, dlq, interrupted, errors, rejected atomic.Int64
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

func main() {
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

	version = strings.Split(version, "-")[0]

	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <job_args_json>", os.Args[0])
	}

	// 1. Parse Job Arguments (os.Args[1])
	var jobArgs JobArgs
	if err := json.Unmarshal([]byte(os.Args[1]), &jobArgs); err != nil {
		log.Fatalf("Failed to parse JobArgs JSON from os.Args[1]: %v", err)
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
			log.Fatalf("Failed to parse MitM JSON configuration: %v", err)
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
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer pool.Close()

	// 2b. Singleton Lock via Postgres Advisory Lock
	lockConn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Fatalf("Failed to acquire connection for advisory lock: %v", err)
	}
	defer lockConn.Release()

	// 64-bit lock key derived from the topic (avoids 32-bit hash collisions between topics)
	lockName := "delivery_" + jobArgs.Topic

	var locked bool
	err = lockConn.QueryRow(context.Background(), "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lockName).Scan(&locked)
	if err != nil {
		log.Fatalf("Failed to execute pg_try_advisory_lock: %v", err)
	}
	if !locked {
		log.Printf("Another delivery instance for topic '%s' is already running (Advisory Lock '%s'). Exiting gracefully.", jobArgs.Topic, lockName)
		os.Exit(0)
	}

	// 3. Initialize Repositories and Engine
	pkgRepo := db.NewPackageRepo(pool)
	dlqRepo := db.NewDLQRepo(pool)
	retryEngine := engine.NewRetryEngine(pkgRepo, dlqRepo, jobArgs.MaxRetries)

	targetRepo := db.NewTargetRepo(pool)

	runIDStr := getEnv("RUN_ID", "0")
	var runID int
	fmt.Sscanf(runIDStr, "%d", &runID)
	socketPath := getEnv("SCHEDULER_SOCKET_PATH", "")

	var ipcClient *ipc.IPCClient
	if runID > 0 && socketPath != "" {
		ipcClient = &ipc.IPCClient{
			SocketPath: socketPath,
			RunID:      runID,
			Component:  "mitm_delivery",
			Topic:      jobArgs.Topic,
			SourceName: jobArgs.SourceName,
		}
		ipcClient.SendEvent("started", fmt.Sprintf("%s (%s) started", appName, version), 0)
		ipcClient.SendAudit(fmt.Sprintf("%s (%s) started", appName, version))
		ipcClient.SendAudit(fmt.Sprintf("Loaded database configuration from %s", configSource))
	}
	defer ipcClient.Close()

	// Helper for audit logging
	logAudit := func(msg string) {
		log.Printf("AUDIT: %s", msg)
		if ipcClient != nil {
			ipcClient.SendAudit(msg)
		}
	}

	// Helper for fatal errors after the scheduler has been notified about the start
	fatalf := func(format string, args ...interface{}) {
		msg := fmt.Sprintf(format, args...)
		log.Print(msg)
		if ipcClient != nil {
			ipcClient.SendEvent("failed", msg, 0)
		}
		os.Exit(1)
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
			LIMIT 1`, jobArgs.BlockingJobs, jobArgs.BlockingMaxRuntimeMinutes).Scan(&blockingJobName, &runningMinutes)

		if err == nil {
			msg := fmt.Sprintf("Exiting gracefully: blocking job '%s' is currently running (for %d minutes).", blockingJobName, runningMinutes)
			logAudit(msg)
			if ipcClient != nil {
				ipcClient.SendEvent("exited", msg, 0)
			}
			os.Exit(0)
		} else if err != pgx.ErrNoRows {
			log.Printf("Warning: Failed to check blocking jobs: %v", err)
		}
	}

	// 3c. Recover packages orphaned by a previous crashed run (we hold the topic lock)
	if n, err := pkgRepo.ResetSending(context.Background(), jobArgs.Topic); err != nil {
		fatalf("Failed to reset orphaned packages: %v", err)
	} else if n > 0 {
		logAudit(fmt.Sprintf("Reset %d orphaned packages from 'sending' to 'pending'", n))
	}

	// Fetch target config dynamically
	targetConfig, err := targetRepo.GetDeliveryTarget(context.Background(), jobArgs.Topic, masterKey)
	if err != nil {
		fatalf("Failed to fetch delivery target config for topic '%s': %v", jobArgs.Topic, err)
	}

	// 4. Instantiate Delivery Sender
	var sender delivery.DeliverySender
	if targetConfig.AdapterType != "CORITY_SAAS" {
		fatalf("Unsupported adapter_type for Cority: %s", targetConfig.AdapterType)
	}
	if _, err := delivery.ParseCorityAuthConfig(targetConfig.AuthConfig); err != nil {
		fatalf("Invalid delivery target config for topic '%s': %v", jobArgs.Topic, err)
	}
	sender = delivery.NewCorityAdapter(nil, logAudit, delivery.NewPGTokenStore(pool))
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

	// abortJob stops all processing after a job-level failure (e.g. authentication)
	var aborted atomic.Bool
	abortJob := func() {
		aborted.Store(true)
		cancel()
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("Shutting down gracefully...")
		cancel()
	}()

	log.Printf("Starting Delivery Batch Job (Workers: %d, Batch Size: %d)...", jobArgs.Workers, jobArgs.BatchSize)
	if ipcClient != nil {
		ipcClient.SendEvent("processing", fmt.Sprintf("Starting Delivery Batch Job (Workers: %d, Batch Size: %d)...", jobArgs.Workers, jobArgs.BatchSize), 0)
	}

	// 5. Worker Pool Setup
	jobs := make(chan db.Package, jobArgs.BatchSize)
	var wg sync.WaitGroup
	var batch sync.WaitGroup // tracks the packages of the current dispatcher batch
	var stats deliveryStats

	// Start workers
	for i := 0; i < jobArgs.Workers; i++ {
		wg.Add(1)
		go worker(ctx, &wg, &batch, jobs, retryEngine, sender, *targetConfig, logAudit, &stats, abortJob)
	}

	// 6. Packager: Create delivery packages from target_fragments
	for {
		packagedCount, failedIDs, err := pkgRepo.PackageTargetFragments(ctx, jobArgs.Topic, jobArgs.BatchSize)
		if err != nil {
			log.Printf("Error packaging target fragments: %v", err)
			break
		}
		if len(failedIDs) > 0 {
			logAudit(fmt.Sprintf("Marked %d malformed target fragments as FAILED: %s", len(failedIDs), strings.Join(failedIDs, ", ")))
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
		if consecutiveErrors >= jobArgs.MaxConsecutiveErrors {
			logAudit(fmt.Sprintf("Aborting: %s failed %d times in a row: %v", what, consecutiveErrors, err))
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

		packages, err := pkgRepo.ClaimPendingPackages(ctx, jobArgs.Topic, jobArgs.BatchSize)
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
				if wait := time.Until(next); ok && wait <= maxRetryWait {
					// At least 1s, so that clock skew to the DB cannot cause a busy loop
					wait = max(wait, time.Second)
					log.Printf("Waiting %v for packages scheduled for retry...", wait.Round(time.Second))
					if !sleepCtx(ctx, wait) {
						break dispatcherLoop
					}
					continue
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
		logAudit(fmt.Sprintf("Reset %d unprocessed packages from 'sending' to 'pending'", n))
	}

	summary := fmt.Sprintf("Topic %s: %d delivered, %d scheduled for retry, %d moved to DLQ, %d records withheld (DLQ), %d interrupted, %d state errors",
		jobArgs.Topic, stats.delivered.Load(), stats.retry.Load(), stats.dlq.Load(), stats.rejected.Load(), stats.interrupted.Load(), stats.errors.Load())
	logAudit(summary)

	status, exitCode := "finished", 0
	switch {
	case aborted.Load() || claimFailed:
		status, exitCode = "failed", 1
	case stats.dlq.Load() > 0 || stats.rejected.Load() > 0 || stats.errors.Load() > 0:
		status, exitCode = "failed", 2
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		status = "timeout"
		logAudit(fmt.Sprintf("Max runtime of %d minutes reached; remaining packages are left for the next run", jobArgs.MaxRuntimeMinutes))
	case ctx.Err() != nil:
		status = "cancelled"
	}
	log.Printf("Delivery Batch Job %s. %s", status, summary)
	if ipcClient != nil {
		ipcClient.SendEvent(status, summary, 100)
		ipcClient.SendAudit(fmt.Sprintf("%s (%s) finished", appName, version))
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func worker(ctx context.Context, wg *sync.WaitGroup, batch *sync.WaitGroup, jobs <-chan db.Package, retryEngine *engine.RetryEngine, sender delivery.DeliverySender, config delivery.TargetConfig, logAudit func(string), stats *deliveryStats, abortJob func()) {
	defer wg.Done()
	for pkg := range jobs {
		processPackage(ctx, pkg, retryEngine, sender, config, logAudit, stats, abortJob)
		batch.Done()
	}
}

func processPackage(ctx context.Context, pkg db.Package, retryEngine *engine.RetryEngine, sender delivery.DeliverySender, config delivery.TargetConfig, logAudit func(string), stats *deliveryStats, abortJob func()) {
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
		abortJob()
	case engine.OutcomeDelivered:
		stats.delivered.Add(1)
	case engine.OutcomeRetry:
		stats.retry.Add(1)
	case engine.OutcomeDLQ:
		stats.dlq.Add(1)
	case engine.OutcomeInterrupted:
		stats.interrupted.Add(1)
	default:
		stats.errors.Add(1)
	}
	if err != nil {
		log.Printf("Failed to process package %s: %v", pkg.ID, err)
		logAudit(fmt.Sprintf("Package %s failed: %v", pkg.ID, err))
	} else {
		logAudit(fmt.Sprintf("Package %s delivered successfully", pkg.ID))
	}
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

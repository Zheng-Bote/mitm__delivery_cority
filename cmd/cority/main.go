package main

import (
	"bufio"
	"net"
	"strconv"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
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
	version        = "0.19.3"
)

type JobArgs struct {
	Topic        string   `json:"topic"`
	Workers      int      `json:"workers"`
	BatchSize    int      `json:"batch_size"`
	MaxRetries   int      `json:"max_retries"`
	SourceName   string   `json:"source_name"`
	BlockingJobs []string `json:"blocking_jobs"`
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func main() {
	// Fetch credentials via IPC if running under scheduler
	if dbCfg, masterKey, err := fetchCredentialsFromScheduler(); err == nil {
		if dbCfg != "" {
			os.Setenv("MITM_DB_CONFIG_JSON", dbCfg)
		}
		if masterKey != "" {
			os.Setenv("MASTER_KEY", masterKey)
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

	// 2. Database Connection Setup
	configSource := "Environment Variables"
	dbHost := ""
	dbPort := ""
	dbUser := ""
	dbPass := ""
	dbName := ""

	jsonConfig := os.Getenv("MITM_DB_CONFIG_JSON")
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
			os.Setenv("MITM_DB_SSLMODE", "require")
		} else {
			os.Setenv("MITM_DB_SSLMODE", "disable")
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
	envSSLMode := os.Getenv("MITM_DB_SSLMODE")
	if envSSLMode != "" {
		if envSSLMode == "true" {
			sslMode = "require"
		} else if envSSLMode == "false" {
			sslMode = "disable"
		} else {
			sslMode = envSSLMode
		}
	}
	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s", dbUser, dbPass, dbHost, dbPort, dbName, sslMode)
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

	// Hash topic to 32-bit integer for lock ID
	importHash := crc32.ChecksumIEEE([]byte("delivery_" + jobArgs.Topic))
	lockID := int32(importHash)

	var locked bool
	err = lockConn.QueryRow(context.Background(), "SELECT pg_try_advisory_lock($1)", lockID).Scan(&locked)
	if err != nil {
		log.Fatalf("Failed to execute pg_try_advisory_lock: %v", err)
	}
	if !locked {
		log.Printf("Another delivery instance for topic '%s' is already running (Advisory Lock %d). Exiting gracefully.", jobArgs.Topic, lockID)
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

	// Helper for audit logging
	logAudit := func(msg string) {
		log.Printf("AUDIT: %s", msg)
		if ipcClient != nil {
			ipcClient.SendAudit(msg)
		}
	}

	// 3b. Check for Blocking Jobs
	if len(jobArgs.BlockingJobs) > 0 {
		var blockingJobName string
		err := pool.QueryRow(context.Background(), `
			SELECT sp.name 
			FROM program_runs pr 
			JOIN scheduled_programs sp ON pr.program_id = sp.id 
			WHERE sp.name = ANY($1) 
			  AND pr.finished_at IS NULL 
			LIMIT 1`, jobArgs.BlockingJobs).Scan(&blockingJobName)

		if err == nil {
			msg := fmt.Sprintf("Exiting gracefully: blocking job '%s' is currently running.", blockingJobName)
			logAudit(msg)
			if ipcClient != nil {
				ipcClient.SendEvent("exited", msg, 0)
			}
			os.Exit(0)
		} else if err != pgx.ErrNoRows {
			log.Printf("Warning: Failed to check blocking jobs: %v", err)
		}
	}

	// Fetch target config dynamically
	targetConfig, err := targetRepo.GetDeliveryTarget(context.Background(), jobArgs.Topic)
	if err != nil {
		log.Fatalf("Failed to fetch delivery target config for topic '%s': %v", jobArgs.Topic, err)
	}

	// 4. Instantiate Delivery Sender
	var sender delivery.DeliverySender
	if targetConfig.AdapterType != "CORITY_SAAS" {
		log.Fatalf("Unsupported adapter_type for Cority: %s", targetConfig.AdapterType)
	}
	sender = delivery.NewCorityAdapter(nil, logAudit, pool)
	if jobArgs.Workers > 1 {
		log.Printf("Forcing workers to 1 for CORITY_SAAS to prevent concurrent import errors.")
		jobArgs.Workers = 1
	}

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	// Start workers
	for i := 0; i < jobArgs.Workers; i++ {
		wg.Add(1)
		go worker(ctx, &wg, jobs, retryEngine, sender, *targetConfig, logAudit)
	}

	totalProcessed := 0

	// 6. Packager: Create delivery packages from target_fragments
	for {
		packagedCount, err := pkgRepo.PackageTargetFragments(ctx, jobArgs.Topic, jobArgs.BatchSize*10)
		if err != nil {
			log.Printf("Error packaging target fragments: %v", err)
			break
		}
		if packagedCount == 0 {
			break
		}
		log.Printf("Packaged %d target fragments into new delivery packages.", packagedCount)
	}

	// 7. Dispatcher Loop
dispatcherLoop:
	for {
		if ctx.Err() != nil {
			break dispatcherLoop
		}

		packages, err := pkgRepo.ClaimPendingPackages(ctx, jobArgs.Topic, jobArgs.BatchSize)
		if err != nil {
			log.Printf("Error claiming packages: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if len(packages) == 0 {
			log.Println("No more packages to deliver. Batch job complete.")
			break dispatcherLoop
		}

		log.Printf("Claimed batch of %d packages", len(packages))
		for _, p := range packages {
			select {
			case jobs <- p:
			case <-ctx.Done():
				break dispatcherLoop
			}
		}
		totalProcessed += len(packages)
	}

	close(jobs)
	wg.Wait()
	log.Printf("Delivery Batch Job finished successfully. Processed %d records.", totalProcessed)
	if ipcClient != nil {
		ipcClient.SendAudit(fmt.Sprintf("Successfully delivered %d Delivery packages for topic %s", totalProcessed, jobArgs.Topic))
		ipcClient.SendAudit(fmt.Sprintf("%s (%s) finished", appName, version))
	}
}

func worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan db.Package, engine *engine.RetryEngine, sender delivery.DeliverySender, config delivery.TargetConfig, logAudit func(string)) {
	defer wg.Done()
	for pkg := range jobs {
		err := engine.ProcessPackage(ctx, pkg, sender, config)
		if err != nil {
			log.Printf("Failed to process package %s: %v", pkg.ID, err)
			logAudit(fmt.Sprintf("Package %s failed: %v", pkg.ID, err))
		} else {
			logAudit(fmt.Sprintf("Package %s delivered successfully (Code: 200/OK)", pkg.ID))
		}
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

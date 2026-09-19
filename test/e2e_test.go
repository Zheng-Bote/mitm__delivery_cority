package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"time"
)

type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

func loadConfig(t *testing.T) Config {
	configPath := "../../../data/config.json"
	b, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read config: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}
	return cfg
}

func setupDatabase(t *testing.T, pool *pgxpool.Pool) {
	// Execute delivery migrations
	migrationsDir := "../../migrations"
	files, err := os.ReadDir(migrationsDir)
	if err != nil {
		t.Fatalf("failed to read migrations dir: %v", err)
	}
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sql" {
			content, err := os.ReadFile(filepath.Join(migrationsDir, file.Name()))
			if err != nil {
				t.Fatalf("failed to read migration %s: %v", file.Name(), err)
			}
			_, err = pool.Exec(context.Background(), string(content))
			if err != nil {
				t.Fatalf("failed to execute migration %s: %v", file.Name(), err)
			}
		}
	}
}

func TestE2EDeliveryBatchJob(t *testing.T) {
	cfg := loadConfig(t)
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	config_pool, err := pgxpool.ParseConfig(connStr)
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
		t.Fatalf("failed to connect to db: %v", err)
	}
	defer pool.Close()

	setupDatabase(t, pool)

	// Clean up only our test data
	pool.Exec(context.Background(), `DELETE FROM packages WHERE idempotency_key IN ('22222222-2222-2222-2222-222222222221', '22222222-2222-2222-2222-222222222222')`)
	pool.Exec(context.Background(), `DELETE FROM dead_letter_queue`)

	// Insert test data
	var pkg1ID, pkg2ID string
	err = pool.QueryRow(context.Background(), `
		INSERT INTO packages (payload, idempotency_key) VALUES ('{"user": "Alice"}', '22222222-2222-2222-2222-222222222221') RETURNING id::text
	`).Scan(&pkg1ID)
	err = pool.QueryRow(context.Background(), `
		INSERT INTO packages (payload, idempotency_key) VALUES ('{"user": "Bob"}', '22222222-2222-2222-2222-222222222222') RETURNING id::text
	`).Scan(&pkg2ID)

	if err != nil {
		t.Fatalf("failed to insert mock packages: %v", err)
	}

	// Create Mock SaaS Server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idem := r.Header.Get("Idempotency-Key")
		t.Logf("Mock Server received request with Idempotency-Key: '%s'", idem)
		if idem == "22222222-2222-2222-2222-222222222221" {
			// Success
			w.WriteHeader(http.StatusOK)
		} else if idem == "22222222-2222-2222-2222-222222222222" {
			// Fatal error to force DLQ immediately via MaxRetries 0, OR we just return 400
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	// Prepare Job Config
	jobArgs := map[string]interface{}{
		"workers":     2,
		"batch_size":  10,
		"max_retries": 1,
		"target_config": map[string]interface{}{
			"adapter_type": "SAAS",
			"endpoint_url": ts.URL,
			"auth_config":  map[string]string{"api_key": "test-key"},
		},
	}
	jobArgsBytes, _ := json.Marshal(jobArgs)

	// Build and Run CLI
	buildCmd := exec.Command("go", "build", "-o", "../bin/mitm-deliver", "../cmd/deliver/main.go")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build CLI: %v\nOutput: %s", err, string(out))
	}

	cmd := exec.Command("../bin/mitm-deliver", string(jobArgsBytes))
	// Inject ENV variables
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("DB_HOST=%s", cfg.Host),
		fmt.Sprintf("DB_PORT=%d", cfg.Port),
		fmt.Sprintf("DB_USER=%s", cfg.User),
		fmt.Sprintf("DB_PASS=%s", cfg.Password),
		fmt.Sprintf("DB_NAME=%s", cfg.Database),
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("CLI run failed: %v\nOutput:\n%s", err, string(out))
	}
	t.Logf("CLI Output: %s", string(out))

	// Verify DB state
	// Package 1 should be 'delivered'
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM packages WHERE id = $1`, pkg1ID).Scan(&status); err != nil {
		t.Logf("Failed to query pkg1 status: %v", err)
	}
	if status != "delivered" {
		t.Errorf("Expected pkg1 status 'delivered', got '%s'", status)
	}

	// Package 2 should be in DLQ
	var dlqCount int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM dead_letter_queue WHERE error_code = 'HTTP_400'`).Scan(&dlqCount); err != nil {
		t.Logf("Failed to query dlqCount: %v", err)
	}
	if dlqCount != 1 {
		t.Errorf("Expected 1 DLQ entry for pkg2, got %d", dlqCount)
	}
}

package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"mitm_cority/internal/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

func loadConfig(t *testing.T) Config {
	configPath := "../../../../data/config.json"
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
	migrationsDir := "../../../migrations"
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

func TestRepositories(t *testing.T) {
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
	pool.Exec(context.Background(), `DELETE FROM packages WHERE idempotency_key IN ('11111111-1111-1111-1111-111111111111', '11111111-1111-1111-1111-111111111112')`)
	pool.Exec(context.Background(), `DELETE FROM dead_letter_queue`)

	pkgRepo := db.NewPackageRepo(pool)
	dlqRepo := db.NewDLQRepo(pool)

	// 1. Insert a pending package
	var pkgID string
	err = pool.QueryRow(context.Background(), `
		INSERT INTO packages (payload, idempotency_key) 
		VALUES ('{"test": 1}', '11111111-1111-1111-1111-111111111111') 
		RETURNING id::text
	`).Scan(&pkgID)
	if err != nil {
		t.Fatalf("failed to insert package: %v", err)
	}

	// 2. Claim pending
	claimed, err := pkgRepo.ClaimPendingPackages(context.Background(), "test-topic", 10)
	if err != nil {
		t.Fatalf("ClaimPendingPackages failed: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("Expected 1 claimed package, got %d", len(claimed))
	}
	if claimed[0].ID != pkgID {
		t.Errorf("Expected ID %s, got %s", pkgID, claimed[0].ID)
	}
	if claimed[0].Status != "sending" {
		t.Errorf("Expected status 'sending', got '%s'", claimed[0].Status)
	}

	// 3. Update status to delivered
	err = pkgRepo.UpdateStatusDelivered(context.Background(), pkgID)
	if err != nil {
		t.Fatalf("UpdateStatusDelivered failed: %v", err)
	}

	var status string
	pool.QueryRow(context.Background(), `SELECT status FROM packages WHERE id = $1`, pkgID).Scan(&status)
	if status != "delivered" {
		t.Errorf("Expected status 'delivered', got '%s'", status)
	}

	// 4. Insert another package, fail it, claim it again
	var pkg2ID string
	pool.QueryRow(context.Background(), `
		INSERT INTO packages (payload, idempotency_key) 
		VALUES ('{"test": 2}', '11111111-1111-1111-1111-111111111112') 
		RETURNING id::text
	`).Scan(&pkg2ID)

	nextRetry := time.Now().Add(-1 * time.Minute) // set to past so it gets picked up immediately
	err = pkgRepo.UpdateStatusFailed(context.Background(), pkg2ID, "transient error", nextRetry, 1)
	if err != nil {
		t.Fatalf("UpdateStatusFailed failed: %v", err)
	}

	claimedRetry, err := pkgRepo.ClaimPendingPackages(context.Background(), "test-topic", 10)
	if err != nil {
		t.Fatalf("ClaimPendingPackages failed on retry: %v", err)
	}
	if len(claimedRetry) != 1 || claimedRetry[0].ID != pkg2ID {
		t.Fatalf("Expected to claim retryable package %s, got %v", pkg2ID, claimedRetry)
	}

	// 5. Move to DLQ
	err = dlqRepo.MoveToDLQ(context.Background(), claimedRetry[0], "HTTP_400", "fatal bad request")
	if err != nil {
		t.Fatalf("MoveToDLQ failed: %v", err)
	}

	var dlqCount int
	pool.QueryRow(context.Background(), `SELECT count(*) FROM dead_letter_queue WHERE error_code = 'HTTP_400'`).Scan(&dlqCount)
	if dlqCount != 1 {
		t.Errorf("Expected 1 DLQ entry, got %d", dlqCount)
	}

	var pkgCount int
	pool.QueryRow(context.Background(), `SELECT count(*) FROM packages WHERE id = $1`, pkg2ID).Scan(&pkgCount)
	if pkgCount != 0 {
		t.Errorf("Expected package to be deleted from packages table, but got %d", pkgCount)
	}
}

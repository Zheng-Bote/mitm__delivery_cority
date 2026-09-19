package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DLQRepo struct {
	pool *pgxpool.Pool
}

func NewDLQRepo(pool *pgxpool.Pool) *DLQRepo {
	return &DLQRepo{pool: pool}
}

// MoveToDLQ moves a package that has permanently failed into the dead_letter_queue table,
// and deletes it from the packages table using a single transaction.
func (r *DLQRepo) MoveToDLQ(ctx context.Context, pkg Package, errorCode string, errMsg string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Insert into DLQ
	insertQuery := `
		INSERT INTO dead_letter_queue (package_id, payload, error_code, error_message)
		VALUES ($1, $2, $3, $4)
	`
	_, err = tx.Exec(ctx, insertQuery, pkg.ID, pkg.Payload, errorCode, errMsg)
	if err != nil {
		return fmt.Errorf("failed to insert into DLQ: %w", err)
	}

	// 2. Delete from packages table
	_, err = tx.Exec(ctx, `DELETE FROM packages WHERE id = $1`, pkg.ID)
	if err != nil {
		return fmt.Errorf("failed to delete package from packages table: %w", err)
	}

	return tx.Commit(ctx)
}

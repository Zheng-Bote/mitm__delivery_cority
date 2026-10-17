/**
 * SPDX-FileComment: Dead Letter Queue Repository
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file dlq_repo.go
 * @brief Moves failed packages and rejected records into the dead_letter_queue
 * @version 0.22.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package db

import (
	"context"
	"fmt"

	"mitm_cority/internal/delivery"

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

// InsertRejectedRecords stores single records withheld from a delivered package in the DLQ.
// The record payload is stored as received (still encrypted).
func (r *DLQRepo) InsertRejectedRecords(ctx context.Context, packageID string, records []delivery.RejectedRecord) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, rec := range records {
		_, err = tx.Exec(ctx, `
			INSERT INTO dead_letter_queue (package_id, payload, error_code, error_message)
			VALUES ($1, $2, $3, $4)
		`, packageID, []byte(rec.Payload), rec.ErrorCode, fmt.Sprintf("record #%d: %s", rec.Index, rec.ErrorMessage))
		if err != nil {
			return fmt.Errorf("failed to insert rejected record into DLQ: %w", err)
		}
	}

	return tx.Commit(ctx)
}

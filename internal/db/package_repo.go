/**
 * SPDX-FileComment: Package Repository
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file package_repo.go
 * @brief Packaging of target fragments and package state transitions
 * @version 0.22.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Package struct {
	ID             string
	Topic          string
	Payload        []byte
	Status         string
	RetryCount     int
	IdempotencyKey string
	ErrorMessage   *string
	CreatedAt      time.Time
	DeliveredAt    *time.Time
	NextRetryAt    *time.Time
}

type PackageRepo struct {
	pool *pgxpool.Pool
}

func NewPackageRepo(pool *pgxpool.Pool) *PackageRepo {
	return &PackageRepo{pool: pool}
}

// PackageTargetFragments reads pending fragments and batches them into a single Package.
// It returns the number of packaged fragments and the IDs of malformed fragments, which are marked 'FAILED'.
func (r *PackageRepo) PackageTargetFragments(ctx context.Context, topic string, batchSize int) (int, []string, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer tx.Rollback(ctx)

	// Fetch fragments
	query := `
		SELECT id, payload_jsonb 
		FROM target_fragments 
		WHERE topic = $1 AND delivery_status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`
	rows, err := tx.Query(ctx, query, topic, batchSize)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to fetch pending fragments: %w", err)
	}

	var fragmentIDs []string
	var failedIDs []string
	var payloads []interface{}

	for rows.Next() {
		var id string
		var p []byte
		if err := rows.Scan(&id, &p); err != nil {
			rows.Close()
			return 0, nil, fmt.Errorf("failed to scan fragment: %w", err)
		}

		var parsed interface{}
		decoder := json.NewDecoder(bytes.NewReader(p))
		decoder.UseNumber()
		if err := decoder.Decode(&parsed); err != nil {
			// Malformed JSON: keep it out of the package and mark it as failed
			failedIDs = append(failedIDs, id)
			continue
		}
		fragmentIDs = append(fragmentIDs, id)

		if slice, isSlice := parsed.([]interface{}); isSlice {
			payloads = append(payloads, slice...)
		} else {
			payloads = append(payloads, parsed)
		}
	}
	rows.Close()

	if len(failedIDs) > 0 {
		failQuery := `
			UPDATE target_fragments
			SET delivery_status = 'FAILED', updated_at = NOW()
			WHERE id = ANY($1)
		`
		if _, err := tx.Exec(ctx, failQuery, failedIDs); err != nil {
			return 0, nil, fmt.Errorf("failed to mark malformed fragments: %w", err)
		}
	}

	if len(fragmentIDs) == 0 {
		return 0, failedIDs, tx.Commit(ctx)
	}

	// Create JSON array for package
	packagePayload, err := json.Marshal(payloads)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to marshal package payload: %w", err)
	}

	idempotencyKey := uuid.New().String()

	// Insert into packages
	insertQuery := `
		INSERT INTO packages (payload, status, idempotency_key, topic)
		VALUES ($1, 'pending', $2, $3)
	`
	if _, err := tx.Exec(ctx, insertQuery, packagePayload, idempotencyKey, topic); err != nil {
		return 0, nil, fmt.Errorf("failed to create package: %w", err)
	}

	// Update fragments
	updateQuery := `
		UPDATE target_fragments
		SET delivery_status = 'PACKAGED', updated_at = NOW()
		WHERE id = ANY($1)
	`
	if _, err := tx.Exec(ctx, updateQuery, fragmentIDs); err != nil {
		return 0, nil, fmt.Errorf("failed to update fragments: %w", err)
	}

	return len(fragmentIDs), failedIDs, tx.Commit(ctx)
}

// NextRetryAt returns the earliest next_retry_at of the topic's failed packages (ok = false if none).
func (r *PackageRepo) NextRetryAt(ctx context.Context, topic string) (time.Time, bool, error) {
	var next *time.Time
	err := r.pool.QueryRow(ctx, `SELECT MIN(next_retry_at) FROM packages WHERE topic = $1 AND status = 'failed'`, topic).Scan(&next)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("failed to query next retry: %w", err)
	}
	if next == nil {
		return time.Time{}, false, nil
	}
	return *next, true, nil
}

// ResetSending returns all packages of a topic that are stuck in 'sending' to 'pending'.
// Callers must hold the topic advisory lock, so no other instance can own these packages.
func (r *PackageRepo) ResetSending(ctx context.Context, topic string) (int64, error) {
	tag, err := r.pool.Exec(ctx, `UPDATE packages SET status = 'pending' WHERE topic = $1 AND status = 'sending'`, topic)
	if err != nil {
		return 0, fmt.Errorf("failed to reset sending packages: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ClaimPendingPackages claims packages that are 'pending' or 'failed' and ready for retry for a specific topic.
func (r *PackageRepo) ClaimPendingPackages(ctx context.Context, topic string, limit int) ([]Package, error) {
	query := `
		UPDATE packages
		SET status = 'sending'
		WHERE id IN (
			SELECT id FROM packages
			WHERE topic = $1 AND (status = 'pending' 
			   OR (status = 'failed' AND next_retry_at <= NOW()))
			ORDER BY created_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id::text, topic, payload, status, retry_count, idempotency_key::text, error_message, created_at, delivered_at, next_retry_at
	`
	rows, err := r.pool.Query(ctx, query, topic, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to claim packages: %w", err)
	}
	defer rows.Close()

	var packages []Package
	for rows.Next() {
		var p Package
		if err := rows.Scan(
			&p.ID, &p.Topic, &p.Payload, &p.Status, &p.RetryCount,
			&p.IdempotencyKey, &p.ErrorMessage, &p.CreatedAt, &p.DeliveredAt, &p.NextRetryAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan package row: %w", err)
		}
		packages = append(packages, p)
	}
	return packages, rows.Err()
}

// UpdateStatusDelivered marks a package as successfully delivered.
func (r *PackageRepo) UpdateStatusDelivered(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `UPDATE packages SET status = 'delivered', delivered_at = NOW() WHERE id = $1`, id)
	return err
}

// UpdateStatusFailed updates a package to failed state and schedules its next retry.
func (r *PackageRepo) UpdateStatusFailed(ctx context.Context, id string, errMsg string, nextRetryAt time.Time, retryCount int) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE packages 
		SET status = 'failed', error_message = $1, next_retry_at = $2, retry_count = $3 
		WHERE id = $4
	`, errMsg, nextRetryAt, retryCount, id)
	return err
}

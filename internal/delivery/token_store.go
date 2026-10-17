/**
 * SPDX-FileComment: Adapter Token Store
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file token_store.go
 * @brief Shared, row-locked cache for adapter authentication tokens (adapter_tokens)
 * @version 0.21.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package delivery

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CachedToken is an authentication token pair shared across delivery processes.
type CachedToken struct {
	AccessToken   string
	AccessExpiry  time.Time
	RefreshToken  string
	RefreshExpiry time.Time
}

// TokenStore persists adapter tokens per connection hash.
type TokenStore interface {
	// WithLock loads the cached token (nil if none) under an exclusive lock and calls fn.
	// If fn returns a non-nil token, it is persisted before the lock is released.
	WithLock(ctx context.Context, connectionHash string, fn func(current *CachedToken) (*CachedToken, error)) error
	// Delete removes the cached token, e.g. after the target rejected it.
	Delete(ctx context.Context, connectionHash string) error
}

// PGTokenStore implements TokenStore on the adapter_tokens table.
type PGTokenStore struct {
	pool *pgxpool.Pool
}

func NewPGTokenStore(pool *pgxpool.Pool) *PGTokenStore {
	return &PGTokenStore{pool: pool}
}

func (s *PGTokenStore) WithLock(ctx context.Context, connectionHash string, fn func(current *CachedToken) (*CachedToken, error)) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var current *CachedToken
	var t CachedToken
	var accessExpiry, refreshExpiry *time.Time
	// FOR UPDATE ensures only one worker attempts to refresh at a time
	err = tx.QueryRow(ctx, "SELECT access_token, access_expiry, COALESCE(refresh_token, ''), refresh_expiry FROM adapter_tokens WHERE connection_hash = $1 FOR UPDATE", connectionHash).Scan(&t.AccessToken, &accessExpiry, &t.RefreshToken, &refreshExpiry)
	if err == nil {
		if accessExpiry != nil {
			t.AccessExpiry = *accessExpiry
		}
		if refreshExpiry != nil {
			t.RefreshExpiry = *refreshExpiry
		}
		current = &t
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}

	fresh, err := fn(current)
	if err != nil {
		return err
	}

	if fresh != nil {
		_, err = tx.Exec(ctx, `
			INSERT INTO adapter_tokens (connection_hash, access_token, access_expiry, refresh_token, refresh_expiry, updated_at)
			VALUES ($1, $2, $3, $4, $5, NOW())
			ON CONFLICT (connection_hash)
			DO UPDATE SET access_token = EXCLUDED.access_token, access_expiry = EXCLUDED.access_expiry, refresh_token = EXCLUDED.refresh_token, refresh_expiry = EXCLUDED.refresh_expiry, updated_at = NOW()
		`, connectionHash, fresh.AccessToken, fresh.AccessExpiry, fresh.RefreshToken, fresh.RefreshExpiry)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *PGTokenStore) Delete(ctx context.Context, connectionHash string) error {
	_, err := s.pool.Exec(ctx, "DELETE FROM adapter_tokens WHERE connection_hash = $1", connectionHash)
	return err
}

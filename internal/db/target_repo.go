/**
 * SPDX-FileComment: Delivery Target Repository
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file target_repo.go
 * @brief Loads and decrypts the delivery target configuration of a topic
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
	"encoding/base64"
	"fmt"

	"mitm_cority/internal/crypto"
	"mitm_cority/internal/delivery"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TargetRepo struct {
	Pool *pgxpool.Pool
}

func NewTargetRepo(pool *pgxpool.Pool) *TargetRepo {
	return &TargetRepo{Pool: pool}
}

// GetDeliveryTarget fetches and decrypts the target config for a given topic.
// masterKey is the KEK (base64 or raw), obtained via IPC or ENV by the caller.
func (r *TargetRepo) GetDeliveryTarget(ctx context.Context, topic string, masterKey string) (*delivery.TargetConfig, error) {
	query := `
		SELECT 
			dt.adapter_type, dt.endpoint_url, dt.config_payload, dt.nonce, sk.wrapped_key
		FROM delivery_targets dt
		JOIN storage_keys sk ON dt.dek_id = sk.id
		WHERE dt.topic = $1 AND dt.is_active = true
	`

	var adapterType, endpointURL string
	var payload, nonce, wrappedKey []byte

	err := r.Pool.QueryRow(ctx, query, topic).Scan(&adapterType, &endpointURL, &payload, &nonce, &wrappedKey)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch target for topic %s: %w", topic, err)
	}

	var kek []byte
	if decoded, err := base64.StdEncoding.DecodeString(masterKey); err == nil {
		kek = decoded
	} else {
		kek = []byte(masterKey)
	}
	if len(kek) != 32 {
		return nil, fmt.Errorf("MASTER_KEY must be 32 bytes (AES-256), got %d bytes", len(kek))
	}

	decrypted, err := crypto.EnvelopeDecrypt(kek, wrappedKey, nonce, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt config_payload for topic %s (wrong MASTER_KEY?): %w", topic, err)
	}

	// Fetch encrypted fields for the topic (via source -> rule -> target_field relationship)
	fieldsQuery := `
		SELECT DISTINCT mtf.field_name 
		FROM mapping_target_field mtf
		JOIN mapping_rule mr ON mtf.id = mr.target_field_id
		JOIN mapping_source ms ON mr.source_id = ms.id
		WHERE LOWER(ms.topic) = LOWER($1) AND mtf.encrypted = true
	`
	// Without the list, encrypted values could be sent unnoticed, so any failure is fatal.
	rows, err := r.Pool.Query(ctx, fieldsQuery, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to load encrypted fields for topic %s: %w", topic, err)
	}
	var encryptedFields []string
	for rows.Next() {
		var fieldName string
		if err := rows.Scan(&fieldName); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan encrypted field for topic %s: %w", topic, err)
		}
		encryptedFields = append(encryptedFields, fieldName)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to load encrypted fields for topic %s: %w", topic, err)
	}

	cfg := &delivery.TargetConfig{
		AdapterType:     adapterType,
		EndpointURL:     endpointURL,
		AuthConfig:      decrypted,
		WrappedKey:      wrappedKey,
		KEK:             kek,
		EncryptedFields: encryptedFields,
	}

	return cfg, nil
}

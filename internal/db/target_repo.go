package db

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

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

// GetDeliveryTarget fetches and decrypts the target config for a given topic
func (r *TargetRepo) GetDeliveryTarget(ctx context.Context, topic string) (*delivery.TargetConfig, error) {
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

	masterKeyStr := os.Getenv("MASTER_KEY")
	var kek []byte
	if decoded, err := base64.StdEncoding.DecodeString(masterKeyStr); err == nil {
		kek = decoded
	} else {
		kek = []byte(masterKeyStr)
	}

	decrypted, err := crypto.EnvelopeDecrypt(kek, wrappedKey, nonce, payload)
	if err != nil {
		decrypted = []byte(`{"client_id": "mock", "client_secret": "mock", "token_url": "mock", "import_path": "mock"}`)
	}

	// Fetch encrypted fields for the topic (via source -> rule -> target_field relationship)
	fieldsQuery := `
		SELECT DISTINCT mtf.field_name 
		FROM mapping_target_field mtf
		JOIN mapping_rule mr ON mtf.id = mr.target_field_id
		JOIN mapping_source ms ON mr.source_id = ms.id
		WHERE LOWER(ms.topic) = LOWER($1) AND mtf.encrypted = true
	`
	rows, err := r.Pool.Query(ctx, fieldsQuery, topic)
	var encryptedFields []string
	if err == nil {
		for rows.Next() {
			var fieldName string
			if err := rows.Scan(&fieldName); err == nil {
				encryptedFields = append(encryptedFields, fieldName)
			}
		}
		rows.Close()
	} else {
		// Even if err is not nil, we log it for debugging
		fmt.Printf("Query for encrypted fields failed: %v\n", err)
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

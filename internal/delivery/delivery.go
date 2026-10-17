/**
 * SPDX-FileComment: Delivery Contracts
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file delivery.go
 * @brief Interfaces and types shared by delivery adapters and the retry engine
 * @version 0.22.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// DeliveryError represents an error during delivery and indicates if it's transient or fatal.
type DeliveryError struct {
	IsTransient  bool
	HTTPCode     int
	ErrorMessage string
	ErrorCode    string
	// RetryAfter is the delay requested by the target (Retry-After header), 0 if none.
	RetryAfter time.Duration
	// Abort stops the whole job without consuming a retry (e.g. authentication failure).
	Abort bool
}

func (e *DeliveryError) Error() string {
	return e.ErrorMessage
}

// TargetConfig defines the dynamic configuration passed to the orchestrator.
type TargetConfig struct {
	AdapterType     string          `json:"adapter_type"` // e.g., "SAAS", "APIGEE"
	EndpointURL     string          `json:"endpoint_url"`
	AuthConfig      json.RawMessage `json:"auth_config"` // Specific to the adapter
	WrappedKey      []byte          `json:"-"`
	KEK             []byte          `json:"-"`
	EncryptedFields []string        `json:"-"`
}

// RejectedRecord is a single record that was withheld from the target (e.g. undecryptable).
type RejectedRecord struct {
	Index        int             // Position of the record in the package payload
	Payload      json.RawMessage // Original record as stored (still encrypted)
	ErrorCode    string
	ErrorMessage string
}

// SendResult reports the records of a successfully sent package that were not transmitted.
type SendResult struct {
	Rejected []RejectedRecord
}

// DeliverySender is the interface that all target adapters must implement.
type DeliverySender interface {
	// Send transmits the payload to the configured target endpoint.
	// Returns a DeliveryError if transmission fails. On success, SendResult lists withheld records.
	Send(ctx context.Context, config TargetConfig, idempotencyKey string, payload []byte) (SendResult, error)
}

var ErrInvalidConfig = errors.New("invalid target configuration")

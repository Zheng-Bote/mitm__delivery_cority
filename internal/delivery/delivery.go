package delivery

import (
	"context"
	"encoding/json"
	"errors"
)

// DeliveryError represents an error during delivery and indicates if it's transient or fatal.
type DeliveryError struct {
	IsTransient  bool
	HTTPCode     int
	ErrorMessage string
	ErrorCode    string
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

// DeliverySender is the interface that all target adapters must implement.
type DeliverySender interface {
	// Send transmits the payload to the configured target endpoint.
	// Returns a DeliveryError if transmission fails, nil on success.
	Send(ctx context.Context, config TargetConfig, idempotencyKey string, payload []byte) error
}

var ErrInvalidConfig = errors.New("invalid target configuration")

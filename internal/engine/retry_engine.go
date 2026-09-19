package engine

import (
	"context"
	"fmt"
	"math"
	"time"

	"mitm_cority/internal/db"
	"mitm_cority/internal/delivery"
)

// PackageRepo defines the database interactions for packages needed by the engine.
type PackageRepo interface {
	UpdateStatusDelivered(ctx context.Context, id string) error
	UpdateStatusFailed(ctx context.Context, id string, errMsg string, nextRetryAt time.Time, retryCount int) error
}

// DLQRepo defines the database interactions for the DLQ needed by the engine.
type DLQRepo interface {
	MoveToDLQ(ctx context.Context, pkg db.Package, errorCode string, errMsg string) error
}

// RetryEngine handles the delivery lifecycle of packages, including exponential backoff.
type RetryEngine struct {
	pkgRepo    PackageRepo
	dlqRepo    DLQRepo
	maxRetries int
}

func NewRetryEngine(pkgRepo PackageRepo, dlqRepo DLQRepo, maxRetries int) *RetryEngine {
	if maxRetries <= 0 {
		maxRetries = 5 // Default max retries
	}
	return &RetryEngine{
		pkgRepo:    pkgRepo,
		dlqRepo:    dlqRepo,
		maxRetries: maxRetries,
	}
}

// ProcessPackage attempts to send a package and updates its state in the database.
func (e *RetryEngine) ProcessPackage(ctx context.Context, pkg db.Package, sender delivery.DeliverySender, targetCfg delivery.TargetConfig) error {
	err := sender.Send(ctx, targetCfg, pkg.IdempotencyKey, pkg.Payload)

	if err == nil {
		// Delivery successful
		return e.pkgRepo.UpdateStatusDelivered(ctx, pkg.ID)
	}

	// Delivery failed, inspect error
	delErr, ok := err.(*delivery.DeliveryError)
	if !ok {
		// Unrecognized error format, treat as fatal
		delErr = &delivery.DeliveryError{
			IsTransient:  false,
			ErrorCode:    "UNKNOWN_ERROR",
			ErrorMessage: err.Error(),
		}
	}

	if !delErr.IsTransient || pkg.RetryCount >= e.maxRetries {
		// Fatal error or max retries exceeded -> Move to DLQ
		if dbErr := e.dlqRepo.MoveToDLQ(ctx, pkg, delErr.ErrorCode, delErr.ErrorMessage); dbErr != nil {
			return fmt.Errorf("failed to move to DLQ: %w", dbErr)
		}
		return fmt.Errorf("delivery failed permanently (moved to DLQ): %w", delErr)
	}

	// Transient error with remaining retries -> Exponential backoff
	// Backoff formula: 2 ^ retryCount seconds
	pkg.RetryCount++
	backoffSeconds := math.Pow(2, float64(pkg.RetryCount))
	nextRetryAt := time.Now().Add(time.Duration(backoffSeconds) * time.Second)

	if dbErr := e.pkgRepo.UpdateStatusFailed(ctx, pkg.ID, delErr.ErrorMessage, nextRetryAt, pkg.RetryCount); dbErr != nil {
		return fmt.Errorf("failed to schedule retry: %w", dbErr)
	}
	return fmt.Errorf("delivery failed transiently (scheduled for retry): %w", delErr)
}

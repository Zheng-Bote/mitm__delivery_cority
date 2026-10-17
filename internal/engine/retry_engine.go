/**
 * SPDX-FileComment: Delivery Retry Engine
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file retry_engine.go
 * @brief Delivery lifecycle of packages with exponential backoff and DLQ handling
 * @version 0.22.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

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
	InsertRejectedRecords(ctx context.Context, packageID string, records []delivery.RejectedRecord) error
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

// Outcome describes what happened to a package after a delivery attempt.
type Outcome int

const (
	OutcomeDelivered   Outcome = iota // Delivered and marked as delivered.
	OutcomeRetry                      // Failed transiently and scheduled for retry.
	OutcomeDLQ                        // Failed permanently and moved to the DLQ.
	OutcomeInterrupted                // Aborted by shutdown; left in 'sending' to be reset to 'pending'.
	OutcomeError                      // The package state could not be persisted.
	OutcomeAbort                      // The job must stop (e.g. authentication failure); left in 'sending'.
)

// maxRetryAfter caps a target-requested retry delay.
const maxRetryAfter = time.Hour

// Result is the outcome of processing one package.
type Result struct {
	Outcome Outcome
	// RejectedRecords is the number of records withheld from the target and stored in the DLQ.
	RejectedRecords int
}

// ProcessPackage attempts to send a package and updates its state in the database.
func (e *RetryEngine) ProcessPackage(ctx context.Context, pkg db.Package, sender delivery.DeliverySender, targetCfg delivery.TargetConfig) (Result, error) {
	sendResult, err := sender.Send(ctx, targetCfg, pkg.IdempotencyKey, pkg.Payload)

	// Persist the result even if a shutdown cancels ctx meanwhile.
	dbCtx := context.WithoutCancel(ctx)

	if err == nil {
		// Delivery successful; withheld records go to the DLQ individually
		if len(sendResult.Rejected) > 0 {
			if dbErr := e.dlqRepo.InsertRejectedRecords(dbCtx, pkg.ID, sendResult.Rejected); dbErr != nil {
				return Result{Outcome: OutcomeError}, fmt.Errorf("failed to store rejected records in DLQ: %w", dbErr)
			}
		}
		if dbErr := e.pkgRepo.UpdateStatusDelivered(dbCtx, pkg.ID); dbErr != nil {
			return Result{Outcome: OutcomeError}, fmt.Errorf("failed to mark package as delivered: %w", dbErr)
		}
		return Result{Outcome: OutcomeDelivered, RejectedRecords: len(sendResult.Rejected)}, nil
	}

	if ctx.Err() != nil {
		// Shutdown interrupted the attempt; do not count it as a retry.
		return Result{Outcome: OutcomeInterrupted}, fmt.Errorf("delivery interrupted by shutdown: %w", err)
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

	if delErr.Abort {
		// Job-level failure: leave the package untouched (no retry consumed, no DLQ).
		return Result{Outcome: OutcomeAbort}, fmt.Errorf("delivery aborted (%s): %w", delErr.ErrorCode, delErr)
	}

	if !delErr.IsTransient || pkg.RetryCount >= e.maxRetries {
		// Fatal error or max retries exceeded -> Move to DLQ
		if dbErr := e.dlqRepo.MoveToDLQ(dbCtx, pkg, delErr.ErrorCode, delErr.ErrorMessage); dbErr != nil {
			return Result{Outcome: OutcomeError}, fmt.Errorf("failed to move to DLQ: %w", dbErr)
		}
		return Result{Outcome: OutcomeDLQ}, fmt.Errorf("delivery failed permanently (moved to DLQ): %w", delErr)
	}

	// Transient error with remaining retries -> Exponential backoff
	// Backoff formula: 2 ^ retryCount seconds, at least the target's Retry-After
	pkg.RetryCount++
	backoff := time.Duration(math.Pow(2, float64(pkg.RetryCount))) * time.Second
	if retryAfter := min(delErr.RetryAfter, maxRetryAfter); retryAfter > backoff {
		backoff = retryAfter
	}
	nextRetryAt := time.Now().Add(backoff)

	if dbErr := e.pkgRepo.UpdateStatusFailed(dbCtx, pkg.ID, delErr.ErrorMessage, nextRetryAt, pkg.RetryCount); dbErr != nil {
		return Result{Outcome: OutcomeError}, fmt.Errorf("failed to schedule retry: %w", dbErr)
	}
	return Result{Outcome: OutcomeRetry}, fmt.Errorf("delivery failed transiently (scheduled for retry): %w", delErr)
}

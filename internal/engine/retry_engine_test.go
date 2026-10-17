package engine_test

import (
	"context"
	"testing"
	"time"

	"mitm_cority/internal/db"
	"mitm_cority/internal/delivery"
	"mitm_cority/internal/engine"
)

type mockPackageRepo struct {
	deliveredID string
	failedID    string
	failedMsg   string
	nextRetryAt time.Time
	retryCount  int
}

func (m *mockPackageRepo) UpdateStatusDelivered(ctx context.Context, id string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	m.deliveredID = id
	return nil
}

func (m *mockPackageRepo) UpdateStatusFailed(ctx context.Context, id string, errMsg string, nextRetryAt time.Time, retryCount int) error {
	m.failedID = id
	m.failedMsg = errMsg
	m.nextRetryAt = nextRetryAt
	m.retryCount = retryCount
	return nil
}

type mockDLQRepo struct {
	pkg       db.Package
	errorCode string
	errMsg    string
	rejected  []delivery.RejectedRecord
}

func (m *mockDLQRepo) InsertRejectedRecords(ctx context.Context, packageID string, records []delivery.RejectedRecord) error {
	m.rejected = append(m.rejected, records...)
	return nil
}

func (m *mockDLQRepo) MoveToDLQ(ctx context.Context, pkg db.Package, errorCode string, errMsg string) error {
	m.pkg = pkg
	m.errorCode = errorCode
	m.errMsg = errMsg
	return nil
}

type mockSender struct {
	err    error
	result delivery.SendResult
}

func (m *mockSender) Send(ctx context.Context, config delivery.TargetConfig, idempotencyKey string, payload []byte) (delivery.SendResult, error) {
	return m.result, m.err
}

func TestRetryEngine_Success(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	dlqRepo := &mockDLQRepo{}
	eng := engine.NewRetryEngine(pkgRepo, dlqRepo, 3)

	sender := &mockSender{err: nil}
	pkg := db.Package{ID: "pkg-1", Payload: []byte(`{}`), IdempotencyKey: "idem-1"}

	res, err := eng.ProcessPackage(context.Background(), pkg, sender, delivery.TargetConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Outcome != engine.OutcomeDelivered {
		t.Errorf("expected OutcomeDelivered, got %d", res.Outcome)
	}
	if pkgRepo.deliveredID != "pkg-1" {
		t.Errorf("expected deliveredID 'pkg-1', got '%s'", pkgRepo.deliveredID)
	}
}

func TestRetryEngine_TransientError(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	dlqRepo := &mockDLQRepo{}
	eng := engine.NewRetryEngine(pkgRepo, dlqRepo, 3)

	// Simulate HTTP 429
	sender := &mockSender{
		err: &delivery.DeliveryError{IsTransient: true, HTTPCode: 429, ErrorMessage: "Rate limited", ErrorCode: "HTTP_429"},
	}
	pkg := db.Package{ID: "pkg-2", RetryCount: 1} // 1 retry already

	res, err := eng.ProcessPackage(context.Background(), pkg, sender, delivery.TargetConfig{})
	if err == nil {
		t.Fatalf("expected error due to transient failure, got nil")
	}
	if res.Outcome != engine.OutcomeRetry {
		t.Errorf("expected OutcomeRetry, got %d", res.Outcome)
	}

	if pkgRepo.failedID != "pkg-2" {
		t.Fatalf("expected package to be marked failed for retry")
	}
	if pkgRepo.retryCount != 2 {
		t.Errorf("expected retryCount 2, got %d", pkgRepo.retryCount)
	}
	// Backoff for retry count 2 is 2^2 = 4 seconds
	diff := time.Until(pkgRepo.nextRetryAt)
	if diff < 3*time.Second || diff > 5*time.Second {
		t.Errorf("expected next retry in about 4 seconds, got %v", diff)
	}
}

func TestRetryEngine_FatalError(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	dlqRepo := &mockDLQRepo{}
	eng := engine.NewRetryEngine(pkgRepo, dlqRepo, 3)

	// Simulate HTTP 400
	sender := &mockSender{
		err: &delivery.DeliveryError{IsTransient: false, HTTPCode: 400, ErrorMessage: "Bad Request", ErrorCode: "HTTP_400"},
	}
	pkg := db.Package{ID: "pkg-3", Payload: []byte(`bad payload`)}

	res, err := eng.ProcessPackage(context.Background(), pkg, sender, delivery.TargetConfig{})
	if err == nil {
		t.Fatalf("expected error due to fatal failure, got nil")
	}
	if res.Outcome != engine.OutcomeDLQ {
		t.Errorf("expected OutcomeDLQ, got %d", res.Outcome)
	}

	if dlqRepo.pkg.ID != "pkg-3" {
		t.Fatalf("expected package to be moved to DLQ")
	}
	if dlqRepo.errorCode != "HTTP_400" {
		t.Errorf("expected error code HTTP_400, got %s", dlqRepo.errorCode)
	}
}

func TestRetryEngine_MaxRetriesExceeded(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	dlqRepo := &mockDLQRepo{}
	eng := engine.NewRetryEngine(pkgRepo, dlqRepo, 3)

	// Simulate transient error, but max retries reached
	sender := &mockSender{
		err: &delivery.DeliveryError{IsTransient: true, HTTPCode: 503, ErrorMessage: "Unavailable", ErrorCode: "HTTP_503"},
	}
	pkg := db.Package{ID: "pkg-4", RetryCount: 3} // max is 3

	_, err := eng.ProcessPackage(context.Background(), pkg, sender, delivery.TargetConfig{})
	if err == nil {
		t.Fatalf("expected error due to max retries exceeded, got nil")
	}

	if dlqRepo.pkg.ID != "pkg-4" {
		t.Fatalf("expected package to be moved to DLQ due to max retries")
	}
	if dlqRepo.errorCode != "HTTP_503" {
		t.Errorf("expected error code HTTP_503, got %s", dlqRepo.errorCode)
	}
}

func TestRetryEngine_InterruptedByShutdown(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	dlqRepo := &mockDLQRepo{}
	eng := engine.NewRetryEngine(pkgRepo, dlqRepo, 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sender := &mockSender{
		err: &delivery.DeliveryError{IsTransient: true, ErrorMessage: "context canceled", ErrorCode: "NETWORK_ERROR"},
	}
	pkg := db.Package{ID: "pkg-5"}

	res, err := eng.ProcessPackage(ctx, pkg, sender, delivery.TargetConfig{})
	if err == nil {
		t.Fatalf("expected error due to interruption, got nil")
	}
	if res.Outcome != engine.OutcomeInterrupted {
		t.Errorf("expected OutcomeInterrupted, got %d", res.Outcome)
	}
	if pkgRepo.failedID != "" || dlqRepo.pkg.ID != "" {
		t.Errorf("interrupted package must not be marked failed or moved to DLQ")
	}
}

func TestRetryEngine_DeliveredPersistedAfterCancel(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	dlqRepo := &mockDLQRepo{}
	eng := engine.NewRetryEngine(pkgRepo, dlqRepo, 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := eng.ProcessPackage(ctx, db.Package{ID: "pkg-6"}, &mockSender{}, delivery.TargetConfig{})
	if err != nil || res.Outcome != engine.OutcomeDelivered {
		t.Fatalf("expected OutcomeDelivered without error, got %d / %v", res.Outcome, err)
	}
	if pkgRepo.deliveredID != "pkg-6" {
		t.Errorf("expected delivered status to be persisted despite cancelled context")
	}
}

func TestRetryEngine_RetryAfterExtendsBackoff(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	eng := engine.NewRetryEngine(pkgRepo, &mockDLQRepo{}, 3)

	sender := &mockSender{
		err: &delivery.DeliveryError{IsTransient: true, HTTPCode: 429, ErrorCode: "HTTP_429", RetryAfter: 120 * time.Second},
	}
	if _, err := eng.ProcessPackage(context.Background(), db.Package{ID: "pkg-7"}, sender, delivery.TargetConfig{}); err == nil {
		t.Fatalf("expected transient error")
	}
	if diff := time.Until(pkgRepo.nextRetryAt); diff < 119*time.Second || diff > 121*time.Second {
		t.Errorf("expected next retry in about 120 seconds, got %v", diff)
	}
}

func TestRetryEngine_AbortLeavesPackageUntouched(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	dlqRepo := &mockDLQRepo{}
	eng := engine.NewRetryEngine(pkgRepo, dlqRepo, 3)

	sender := &mockSender{
		err: &delivery.DeliveryError{ErrorCode: "AUTH_FAIL", ErrorMessage: "refresh token failed", Abort: true},
	}
	res, err := eng.ProcessPackage(context.Background(), db.Package{ID: "pkg-8"}, sender, delivery.TargetConfig{})
	if err == nil || res.Outcome != engine.OutcomeAbort {
		t.Fatalf("expected OutcomeAbort with error, got %d / %v", res.Outcome, err)
	}
	if pkgRepo.failedID != "" || pkgRepo.deliveredID != "" || dlqRepo.pkg.ID != "" {
		t.Errorf("aborted package must not be updated or moved to DLQ")
	}
}

func TestRetryEngine_RejectedRecordsGoToDLQ(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	dlqRepo := &mockDLQRepo{}
	eng := engine.NewRetryEngine(pkgRepo, dlqRepo, 3)

	sender := &mockSender{result: delivery.SendResult{Rejected: []delivery.RejectedRecord{
		{Index: 1, Payload: []byte(`{"ssn":"x"}`), ErrorCode: "DECRYPT_FAIL"},
	}}}
	res, err := eng.ProcessPackage(context.Background(), db.Package{ID: "pkg-9"}, sender, delivery.TargetConfig{})
	if err != nil || res.Outcome != engine.OutcomeDelivered || res.RejectedRecords != 1 {
		t.Fatalf("expected delivered with 1 rejected record, got %+v / %v", res, err)
	}
	if len(dlqRepo.rejected) != 1 || pkgRepo.deliveredID != "pkg-9" {
		t.Errorf("expected rejected record in DLQ and package delivered")
	}
}

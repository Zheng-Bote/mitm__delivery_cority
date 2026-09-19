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
}

func (m *mockDLQRepo) MoveToDLQ(ctx context.Context, pkg db.Package, errorCode string, errMsg string) error {
	m.pkg = pkg
	m.errorCode = errorCode
	m.errMsg = errMsg
	return nil
}

type mockSender struct {
	err error
}

func (m *mockSender) Send(ctx context.Context, config delivery.TargetConfig, idempotencyKey string, payload []byte) error {
	return m.err
}

func TestRetryEngine_Success(t *testing.T) {
	pkgRepo := &mockPackageRepo{}
	dlqRepo := &mockDLQRepo{}
	eng := engine.NewRetryEngine(pkgRepo, dlqRepo, 3)

	sender := &mockSender{err: nil}
	pkg := db.Package{ID: "pkg-1", Payload: []byte(`{}`), IdempotencyKey: "idem-1"}

	err := eng.ProcessPackage(context.Background(), pkg, sender, delivery.TargetConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
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

	err := eng.ProcessPackage(context.Background(), pkg, sender, delivery.TargetConfig{})
	if err == nil {
		t.Fatalf("expected error due to transient failure, got nil")
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

	err := eng.ProcessPackage(context.Background(), pkg, sender, delivery.TargetConfig{})
	if err == nil {
		t.Fatalf("expected error due to fatal failure, got nil")
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

	err := eng.ProcessPackage(context.Background(), pkg, sender, delivery.TargetConfig{})
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

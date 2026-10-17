/**
 * SPDX-FileComment: Cority Adapter Tests
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file cority_adapter_test.go
 * @brief Unit tests for the Cority adapter using an httptest server and an in-memory token store
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
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"mitm_cority/internal/crypto"
)

// memTokenStore is an in-memory TokenStore for tests.
type memTokenStore struct {
	mu     sync.Mutex
	tokens map[string]*CachedToken
}

func newMemTokenStore() *memTokenStore {
	return &memTokenStore{tokens: map[string]*CachedToken{}}
}

func (s *memTokenStore) WithLock(ctx context.Context, hash string, fn func(*CachedToken) (*CachedToken, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fresh, err := fn(s.tokens[hash])
	if err != nil {
		return err
	}
	if fresh != nil {
		s.tokens[hash] = fresh
	}
	return nil
}

func (s *memTokenStore) Delete(ctx context.Context, hash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tokens, hash)
	return nil
}

// fakeCority simulates the Cority login and import endpoints.
type fakeCority struct {
	mu          sync.Mutex
	logins      int
	loginBodies [][]byte
	imports     []map[string]interface{}
	importKeys  []string
	// rejectTokens lists access tokens answered with HTTP 401 on import.
	rejectTokens map[string]bool
}

func (f *fakeCority) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/refreshtoken", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var body json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		f.logins++
		f.loginBodies = append(f.loginBodies, body)
		json.NewEncoder(w).Encode(map[string]string{"Token": "refresh"})
	})
	mux.HandleFunc("/api/token/", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]string{"AccessToken": "access-" + string(rune('0'+f.logins))})
	})
	mux.HandleFunc("/api/employeeimport", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		token := r.Header.Get("Authorization")[len("Bearer "):]
		if f.rejectTokens[token] {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		f.imports = append(f.imports, body)
		f.importKeys = append(f.importKeys, r.Header.Get("Idempotency-Key"))
		w.Write([]byte(`{"recordsTotal": 1}`))
	})
	return mux
}

func corityTarget(t *testing.T, url string, cfg map[string]interface{}) TargetConfig {
	t.Helper()
	base := map[string]interface{}{
		"login_user":        "Cority_User",
		"login_pass":        "pass",
		"auth_refresh_path": "/api/refreshtoken",
		"auth_token_path":   "/api/token/",
		"import_path":       "/api/employeeimport",
	}
	for k, v := range cfg {
		base[k] = v
	}
	raw, _ := json.Marshal(base)
	return TargetConfig{AdapterType: "CORITY_SAAS", EndpointURL: url, AuthConfig: raw}
}

func TestFlexIntAcceptsStringAndNumber(t *testing.T) {
	for _, in := range []string{`{"slowdown": 5, "timeout": "300"}`, `{"slowdown": "5", "timeout": 300}`} {
		cfg, err := ParseCorityAuthConfig([]byte(in))
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", in, err)
		}
		if cfg.Slowdown != 5 || cfg.Timeout != 300 {
			t.Errorf("%s: got slowdown=%d timeout=%d", in, cfg.Slowdown, cfg.Timeout)
		}
	}
	cfg, err := ParseCorityAuthConfig([]byte(`{"slowdown": ""}`))
	if err != nil || cfg.Slowdown != 0 {
		t.Errorf("empty string must mean 0, got %d / %v", cfg.Slowdown, err)
	}
	if _, err := ParseCorityAuthConfig([]byte(`{"slowdown": "abc"}`)); err == nil {
		t.Errorf("expected error for non-numeric slowdown")
	}
}

func TestLoginBodyEscapesCredentials(t *testing.T) {
	fake := &fakeCority{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	pass := `My"$ecret\pa:ss`
	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	target := corityTarget(t, srv.URL, map[string]interface{}{"login_pass": pass})

	if _, err := adapter.Send(context.Background(), target, "11111111-1111-1111-1111-111111111111", []byte(`[{"a":"1"}]`)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var body struct {
		User struct {
			LoginName     string `json:"LoginName"`
			Loginpassword string `json:"Loginpassword"`
		} `json:"user"`
	}
	if err := json.Unmarshal(fake.loginBodies[0], &body); err != nil {
		t.Fatalf("login body is not valid JSON: %v", err)
	}
	if body.User.Loginpassword != pass || body.User.LoginName != "Cority_User" {
		t.Errorf("credentials not preserved: %+v", body.User)
	}
}

func TestUnauthorizedTriggersSingleReauthentication(t *testing.T) {
	fake := &fakeCority{rejectTokens: map[string]bool{"stale": true}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	store := newMemTokenStore()
	adapter := NewCorityAdapter(nil, nil, store)
	target := corityTarget(t, srv.URL, nil)

	// Seed a cached token that Cority rejects although it has not expired yet.
	cfg, _ := ParseCorityAuthConfig(target.AuthConfig)
	hash := connectionHashFor(srv.URL, cfg.LoginUser)
	store.tokens[hash] = &CachedToken{AccessToken: "stale", AccessExpiry: time.Now().Add(time.Hour)}

	if _, err := adapter.Send(context.Background(), target, "11111111-1111-1111-1111-111111111111", []byte(`[{"a":"1"}]`)); err != nil {
		t.Fatalf("expected success after re-authentication, got %v", err)
	}
	if fake.logins != 1 {
		t.Errorf("expected exactly 1 re-authentication, got %d", fake.logins)
	}
	if len(fake.imports) != 1 {
		t.Errorf("expected 1 successful import, got %d", len(fake.imports))
	}
	if store.tokens[hash].AccessToken == "stale" {
		t.Errorf("stale token must be replaced in the store")
	}
}

func TestRepeatedUnauthorizedIsTransient(t *testing.T) {
	fake := &fakeCority{rejectTokens: map[string]bool{"access-1": true}}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	target := corityTarget(t, srv.URL, nil)

	// access-1 is issued by the first login and rejected; the retry login issues access-2.
	fake.rejectTokens["access-2"] = true
	_, err := adapter.Send(context.Background(), target, "11111111-1111-1111-1111-111111111111", []byte(`[{"a":"1"}]`))

	var delErr *DeliveryError
	if !errors.As(err, &delErr) || !delErr.IsTransient || delErr.ErrorCode != "HTTP_401" {
		t.Fatalf("expected transient HTTP_401, got %v", err)
	}
	if fake.logins != 2 {
		t.Errorf("expected initial login plus exactly 1 re-authentication, got %d logins", fake.logins)
	}
}

func TestRecordsAreChunkedByUploadBatchSize(t *testing.T) {
	fake := &fakeCority{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	target := corityTarget(t, srv.URL, map[string]interface{}{
		"upload_options": map[string]interface{}{"import_mode": "upsert", "batch_size": 2},
	})

	payload := []byte(`[{"n":1},{"n":2},{"n":3},{"n":4},{"n":5}]`)
	key := "11111111-1111-1111-1111-111111111111"
	if _, err := adapter.Send(context.Background(), target, key, payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(fake.imports) != 3 {
		t.Fatalf("expected 3 import requests, got %d", len(fake.imports))
	}
	total := 0
	for _, body := range fake.imports {
		n := len(body["records"].([]interface{}))
		if n > 2 {
			t.Errorf("chunk exceeds upload batch size: %d records", n)
		}
		total += n
	}
	if total != 5 {
		t.Errorf("expected 5 records in total, got %d", total)
	}

	seen := map[string]bool{}
	for i, k := range fake.importKeys {
		if seen[k] || k == key {
			t.Errorf("chunk keys must be unique and differ from the package key: %v", fake.importKeys)
		}
		seen[k] = true
		if k != chunkIdempotencyKey(key, i, 3) {
			t.Errorf("chunk key %d is not deterministic", i)
		}
	}
}

func TestSingleChunkKeepsPackageKey(t *testing.T) {
	if got := chunkIdempotencyKey("abc", 0, 1); got != "abc" {
		t.Errorf("expected package key, got %s", got)
	}
	if got := uploadBatchSize(map[string]interface{}{"batch_size": "500"}); got != 500 {
		t.Errorf("expected 500 from string option, got %d", got)
	}
	if got := len(chunkRecords(nil, 500)); got != 1 {
		t.Errorf("an empty payload must still produce one chunk, got %d", got)
	}
}

func TestEncryptedFieldIsDecrypted(t *testing.T) {
	kek := []byte("0123456789abcdef0123456789abcdef")
	wrapped, err := crypto.GenerateWrappedDEK(kek)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, nonce, err := crypto.EnvelopeEncrypt(kek, wrapped, []byte(`"537732"`))
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.StdEncoding.EncodeToString(append(nonce, ciphertext...))

	fake := &fakeCority{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	target := corityTarget(t, srv.URL, nil)
	target.KEK, target.WrappedKey, target.EncryptedFields = kek, wrapped, []string{"ssn"}

	payload, _ := json.Marshal([]map[string]string{{"ssn": encoded}})
	if _, err := adapter.Send(context.Background(), target, "11111111-1111-1111-1111-111111111111", payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	record := fake.imports[0]["records"].([]interface{})[0].(map[string]interface{})
	if record["ssn"] != "537732" {
		t.Errorf("expected decrypted ssn 537732, got %v", record["ssn"])
	}
}

// envelopeValue encrypts a JSON value with the given keys in legacy string format.
func envelopeValue(t *testing.T, kek, wrapped []byte, plaintext string) string {
	t.Helper()
	ciphertext, nonce, err := crypto.EnvelopeEncrypt(kek, wrapped, []byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(append(nonce, ciphertext...))
}

func TestUndecryptableRecordIsWithheld(t *testing.T) {
	kek := []byte("0123456789abcdef0123456789abcdef")
	otherKEK := []byte("fedcba9876543210fedcba9876543210")
	wrapped, _ := crypto.GenerateWrappedDEK(kek)
	otherWrapped, _ := crypto.GenerateWrappedDEK(otherKEK)

	fake := &fakeCority{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	target := corityTarget(t, srv.URL, nil)
	target.KEK, target.WrappedKey, target.EncryptedFields = kek, wrapped, []string{"ssn"}

	payload, _ := json.Marshal([]map[string]string{
		{"name": "ok", "ssn": envelopeValue(t, kek, wrapped, `"111"`)},
		{"name": "wrong-key", "ssn": envelopeValue(t, otherKEK, otherWrapped, `"222"`)},
		{"name": "not-base64", "ssn": "not base64 !!"},
	})
	res, err := adapter.Send(context.Background(), target, "11111111-1111-1111-1111-111111111111", payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(res.Rejected) != 2 || res.Rejected[0].Index != 1 || res.Rejected[1].Index != 2 {
		t.Fatalf("expected records #1 and #2 rejected, got %+v", res.Rejected)
	}
	for _, r := range res.Rejected {
		if r.ErrorCode != "DECRYPT_FAIL" {
			t.Errorf("expected DECRYPT_FAIL, got %s", r.ErrorCode)
		}
		var orig map[string]string
		json.Unmarshal(r.Payload, &orig)
		if orig["ssn"] == "222" {
			t.Errorf("rejected payload must keep the original encrypted value")
		}
	}

	records := fake.imports[0]["records"].([]interface{})
	if len(records) != 1 || records[0].(map[string]interface{})["ssn"] != "111" {
		t.Errorf("expected only the decryptable record to be sent, got %v", records)
	}
}

func TestAllRecordsWithheldSendsNothing(t *testing.T) {
	fake := &fakeCority{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	target := corityTarget(t, srv.URL, nil)
	target.KEK, target.EncryptedFields = []byte("0123456789abcdef0123456789abcdef"), []string{"ssn"}

	res, err := adapter.Send(context.Background(), target, "11111111-1111-1111-1111-111111111111", []byte(`[{"ssn":"not base64 !!"}]`))
	if err != nil || len(res.Rejected) != 1 {
		t.Fatalf("expected 1 rejected record without error, got %+v / %v", res, err)
	}
	if len(fake.imports) != 0 {
		t.Errorf("no import request expected when all records are withheld")
	}
}

func TestEnvelopeObjectDecryptedWithoutMapping(t *testing.T) {
	kek := []byte("0123456789abcdef0123456789abcdef")
	wrapped, _ := crypto.GenerateWrappedDEK(kek)
	ciphertext, nonce, _ := crypto.EnvelopeEncrypt(kek, wrapped, []byte(`"secret"`))

	fake := &fakeCority{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	target := corityTarget(t, srv.URL, nil)
	target.KEK, target.WrappedKey = kek, wrapped // no EncryptedFields configured

	payload, _ := json.Marshal([]map[string]interface{}{{"pin": map[string]string{
		"ciphertext": base64.StdEncoding.EncodeToString(ciphertext),
		"nonce":      base64.StdEncoding.EncodeToString(nonce),
	}}})
	res, err := adapter.Send(context.Background(), target, "11111111-1111-1111-1111-111111111111", payload)
	if err != nil || len(res.Rejected) != 0 {
		t.Fatalf("unexpected result: %+v / %v", res, err)
	}
	record := fake.imports[0]["records"].([]interface{})[0].(map[string]interface{})
	if record["pin"] != "secret" {
		t.Errorf("expected decrypted envelope object, got %v", record["pin"])
	}
}

func TestListedEnvelopeObjectIsDecryptedOnce(t *testing.T) {
	kek := []byte("0123456789abcdef0123456789abcdef")
	wrapped, _ := crypto.GenerateWrappedDEK(kek)
	ciphertext, nonce, _ := crypto.EnvelopeEncrypt(kek, wrapped, []byte(`"537732"`))

	fake := &fakeCority{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	target := corityTarget(t, srv.URL, nil)
	target.KEK, target.WrappedKey, target.EncryptedFields = kek, wrapped, []string{"ssn"}

	payload, _ := json.Marshal([]map[string]interface{}{{"ssn": map[string]string{
		"ciphertext": base64.StdEncoding.EncodeToString(ciphertext),
		"nonce":      base64.StdEncoding.EncodeToString(nonce),
	}}})
	res, err := adapter.Send(context.Background(), target, "11111111-1111-1111-1111-111111111111", payload)
	if err != nil || len(res.Rejected) != 0 {
		t.Fatalf("decrypted value must not be decrypted a second time: %+v / %v", res, err)
	}
	record := fake.imports[0]["records"].([]interface{})[0].(map[string]interface{})
	if record["ssn"] != "537732" {
		t.Errorf("expected 537732, got %v", record["ssn"])
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	cases := map[string]time.Duration{
		"":                              0,
		"120":                           120 * time.Second,
		"-5":                            0,
		"99999":                         time.Hour,
		"Sat, 17 Oct 2026 12:02:00 GMT": 2 * time.Minute,
		"garbage":                       0,
	}
	for in, want := range cases {
		if got := parseRetryAfter(in, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestRetryAfterPausesNextUpload(t *testing.T) {
	fake := &fakeCority{}
	mux := http.NewServeMux()
	mux.Handle("/api/", fake.handler())
	mux.HandleFunc("/api/employeeimport", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	target := corityTarget(t, srv.URL, nil)

	_, err := adapter.Send(context.Background(), target, "11111111-1111-1111-1111-111111111111", []byte(`[{"a":"1"}]`))
	var delErr *DeliveryError
	if !errors.As(err, &delErr) || !delErr.IsTransient || delErr.RetryAfter != 120*time.Second {
		t.Fatalf("expected transient error with RetryAfter 120s, got %v", err)
	}

	// The next upload must wait for the pause; a short context proves it does not send immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = adapter.Send(ctx, target, "11111111-1111-1111-1111-111111111112", []byte(`[{"a":"1"}]`))
	if !errors.As(err, &delErr) || delErr.ErrorCode != "CANCELLED" {
		t.Errorf("expected the paused upload to be cancelled, got %v", err)
	}
}

func TestAuthFailAbortsJob(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
	}))
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	_, err := adapter.Send(context.Background(), corityTarget(t, srv.URL, nil), "11111111-1111-1111-1111-111111111111", []byte(`[]`))

	var delErr *DeliveryError
	if !errors.As(err, &delErr) || delErr.ErrorCode != "AUTH_FAIL" || !delErr.Abort {
		t.Fatalf("expected AUTH_FAIL with Abort, got %v", err)
	}
}

func TestSlowdownAbortsOnCancel(t *testing.T) {
	fake := &fakeCority{}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	adapter := NewCorityAdapter(nil, nil, newMemTokenStore())
	adapter.lastUpload = time.Now()
	target := corityTarget(t, srv.URL, map[string]interface{}{"slowdown": 60})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := adapter.Send(ctx, target, "11111111-1111-1111-1111-111111111111", []byte(`[]`))
	if err == nil {
		t.Fatalf("expected error after cancellation")
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("slowdown did not abort on cancellation")
	}
}

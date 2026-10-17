/**
 * SPDX-FileComment: Cority SaaS Delivery Adapter
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file cority_adapter.go
 * @brief Cority two-step authentication, field decryption, chunked import upload
 * @version 0.21.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"mitm_cority/internal/crypto"

	"github.com/google/uuid"
)

// maxLoggedBodyBytes limits how much of a failed Cority response is written to the audit log.
const maxLoggedBodyBytes = 1024

// flexInt accepts a JSON number or a numeric JSON string ("" and null count as 0).
type flexInt int

func (f *flexInt) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("invalid integer value %s", string(b))
	}
	*f = flexInt(n)
	return nil
}

type CorityAuthConfig struct {
	LoginUser       string                 `json:"login_user"`
	LoginPass       string                 `json:"login_pass"`
	AuthRefreshPath string                 `json:"auth_refresh_path"`
	AuthTokenPath   string                 `json:"auth_token_path"`
	ImportPath      string                 `json:"import_path"`
	UploadOptions   map[string]interface{} `json:"upload_options"`
	Slowdown        flexInt                `json:"slowdown"`
	Timeout         flexInt                `json:"timeout"`
}

// ParseCorityAuthConfig parses the decrypted config_payload of a CORITY_SAAS target.
func ParseCorityAuthConfig(raw []byte) (CorityAuthConfig, error) {
	var cfg CorityAuthConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("invalid auth_config for Cority adapter: %w", err)
	}
	return cfg, nil
}

type CorityAdapter struct {
	client         *http.Client
	logAudit       func(string)
	mu             sync.Mutex
	lastUpload     time.Time
	slowdownLogged bool
	tokens         TokenStore
}

func NewCorityAdapter(client *http.Client, logAudit func(string), tokens TokenStore) *CorityAdapter {
	if client == nil {
		client = &http.Client{Timeout: 300 * time.Second}
	}
	return &CorityAdapter{client: client, logAudit: logAudit, tokens: tokens}
}

func (a *CorityAdapter) audit(msg string) {
	if a.logAudit != nil {
		a.logAudit(msg)
	}
}

func parseCorityDate(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	// Try RFC3339
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	// Try fallback
	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t
	}
	return time.Time{}
}

func (a *CorityAdapter) Send(ctx context.Context, config TargetConfig, idempotencyKey string, payload []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	cfg, err := ParseCorityAuthConfig(config.AuthConfig)
	if err != nil {
		return &DeliveryError{
			IsTransient:  false,
			ErrorMessage: err.Error(),
			ErrorCode:    "INVALID_CONFIG",
		}
	}

	activeClient := a.client
	configuredTimeout := 300
	if cfg.Timeout > 0 {
		clientCopy := *a.client
		clientCopy.Timeout = time.Duration(cfg.Timeout) * time.Second
		activeClient = &clientCopy
		configuredTimeout = int(cfg.Timeout)
	}

	if !a.slowdownLogged {
		if cfg.Slowdown > 0 {
			a.audit(fmt.Sprintf("Upload Slowdown: Active (%d seconds)", cfg.Slowdown))
		} else {
			a.audit("Upload Slowdown: Inactive")
		}
		a.slowdownLogged = true
	}
	if cfg.Slowdown > 0 {
		if delay := time.Duration(cfg.Slowdown)*time.Second - time.Since(a.lastUpload); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("upload slowdown interrupted: %v", ctx.Err()), ErrorCode: "CANCELLED"}
			}
		}
	}

	defer func() { a.lastUpload = time.Now() }()

	baseURL := strings.TrimSuffix(config.EndpointURL, "/")

	connectionHash := connectionHashFor(baseURL, cfg.LoginUser)

	accessToken, err := a.getAccessToken(ctx, activeClient, cfg, baseURL, connectionHash)
	if err != nil {
		return err
	}

	// Send Payload Data
	var rawData []interface{}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&rawData); err != nil {
		return &DeliveryError{IsTransient: false, ErrorMessage: "payload is not valid JSON array", ErrorCode: "INVALID_PAYLOAD"}
	}

	if err := a.decryptFields(rawData, config); err != nil {
		return err
	}

	// Convert all values to strings and nulls to empty strings recursively
	var convertToStrings func(interface{}) interface{}
	convertToStrings = func(data interface{}) interface{} {
		if data == nil {
			return ""
		}
		switch v := data.(type) {
		case map[string]interface{}:
			for key, val := range v {
				v[key] = convertToStrings(val)
			}
			return v
		case []interface{}:
			for i, val := range v {
				v[i] = convertToStrings(val)
			}
			return v
		case string:
			return v
		default:
			return fmt.Sprintf("%v", v)
		}
	}

	for i, item := range rawData {
		rawData[i] = convertToStrings(item)
	}

	chunks := chunkRecords(rawData, uploadBatchSize(cfg.UploadOptions))
	for i, chunk := range chunks {
		chunkKey := chunkIdempotencyKey(idempotencyKey, i, len(chunks))
		if len(chunks) > 1 {
			a.audit(fmt.Sprintf("Uploading chunk %d/%d (%d records, Idempotency-Key: %s)", i+1, len(chunks), len(chunk), chunkKey))
		}

		status, body, err := a.upload(ctx, activeClient, cfg, baseURL, accessToken, chunkKey, chunk, configuredTimeout)
		if err != nil {
			return err
		}

		if status == http.StatusUnauthorized {
			// The cached token was rejected before its expiry: re-authenticate once and retry.
			a.audit("Cority rejected the access token (HTTP 401). Invalidating cached token and re-authenticating...")
			if err := a.tokens.Delete(ctx, connectionHash); err != nil {
				return &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("failed to invalidate token: %v", err), ErrorCode: "DB_ERROR"}
			}
			accessToken, err = a.getAccessToken(ctx, activeClient, cfg, baseURL, connectionHash)
			if err != nil {
				return err
			}
			status, body, err = a.upload(ctx, activeClient, cfg, baseURL, accessToken, chunkKey, chunk, configuredTimeout)
			if err != nil {
				return err
			}
			if status == http.StatusUnauthorized {
				a.audit(fmt.Sprintf("CORITY_SAAS Upload | Idempotency-Key: %s | Return Code: 401 after re-authentication | Response: %s", chunkKey, truncate(body)))
				return &DeliveryError{IsTransient: true, HTTPCode: status, ErrorMessage: fmt.Sprintf("HTTP 401 after re-authentication: %s", string(body)), ErrorCode: "HTTP_401"}
			}
		}

		if err := a.checkUploadResponse(baseURL+cfg.ImportPath, chunkKey, status, body); err != nil {
			return err
		}
	}
	return nil
}

// getAccessToken returns a valid cached access token or authenticates with Cority and caches the result.
func (a *CorityAdapter) getAccessToken(ctx context.Context, client *http.Client, cfg CorityAuthConfig, baseURL, connectionHash string) (string, error) {
	var accessToken string
	err := a.tokens.WithLock(ctx, connectionHash, func(current *CachedToken) (*CachedToken, error) {
		// Margin of 60 seconds
		if current != nil && time.Now().Add(60*time.Second).Before(current.AccessExpiry) {
			accessToken = current.AccessToken
			return nil, nil
		}
		a.audit("Token expired or missing. Authenticating with Cority API...")
		fresh, err := a.authenticate(ctx, client, cfg, baseURL)
		if err != nil {
			return nil, err
		}
		accessToken = fresh.AccessToken
		return fresh, nil
	})
	if err != nil {
		var delErr *DeliveryError
		if errors.As(err, &delErr) {
			return "", delErr
		}
		return "", &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("token cache error: %v", err), ErrorCode: "DB_ERROR"}
	}
	return accessToken, nil
}

// authenticate performs Cority's two-step login (refresh token, then access token).
func (a *CorityAdapter) authenticate(ctx context.Context, client *http.Client, cfg CorityAuthConfig, baseURL string) (*CachedToken, error) {
	var token CachedToken

	// 1. Get Refresh Token
	refreshReqURL := baseURL + cfg.AuthRefreshPath
	type loginUser struct {
		LoginName     string `json:"LoginName"`
		Loginpassword string `json:"Loginpassword"`
	}
	refreshBody, _ := json.Marshal(struct {
		User loginUser `json:"user"`
	}{User: loginUser{LoginName: cfg.LoginUser, Loginpassword: cfg.LoginPass}})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, refreshReqURL, bytes.NewReader(refreshBody))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("failed refresh request: %v", err), ErrorCode: "NETWORK_ERROR"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, &DeliveryError{IsTransient: false, ErrorMessage: fmt.Sprintf("refresh token failed: %s", string(bodyBytes)), ErrorCode: "AUTH_FAIL"}
	}

	var refreshResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&refreshResp); err != nil {
		return nil, &DeliveryError{IsTransient: false, ErrorMessage: "failed to parse refresh token response", ErrorCode: "AUTH_FAIL"}
	}

	if val, ok := refreshResp["Token"].(string); ok && val != "" {
		token.RefreshToken = val
	} else if val, ok := refreshResp["token"].(string); ok && val != "" {
		token.RefreshToken = val
	}

	if token.RefreshToken == "" {
		return nil, &DeliveryError{IsTransient: false, ErrorMessage: "refresh token not found in response", ErrorCode: "AUTH_FAIL"}
	}

	if val, ok := refreshResp["ExpiryDateTime"].(string); ok && val != "" {
		token.RefreshExpiry = parseCorityDate(val)
	} else if val, ok := refreshResp["expiry"].(string); ok && val != "" {
		token.RefreshExpiry = parseCorityDate(val)
	}

	// 2. Get Access Token
	tokenReqURL := baseURL + cfg.AuthTokenPath
	req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, tokenReqURL, nil)
	req2.Header.Set("Authorization", "Bearer "+token.RefreshToken)

	resp2, err := client.Do(req2)
	if err != nil {
		return nil, &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("failed token request: %v", err), ErrorCode: "NETWORK_ERROR"}
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp2.Body)
		return nil, &DeliveryError{IsTransient: false, ErrorMessage: fmt.Sprintf("access token failed: %s", string(bodyBytes)), ErrorCode: "AUTH_FAIL"}
	}

	var tokenResp map[string]interface{}
	if err := json.NewDecoder(resp2.Body).Decode(&tokenResp); err != nil {
		return nil, &DeliveryError{IsTransient: false, ErrorMessage: "failed to parse access token response", ErrorCode: "AUTH_FAIL"}
	}

	if val, ok := tokenResp["AccessToken"].(string); ok && val != "" {
		token.AccessToken = val
	} else if val, ok := tokenResp["access_token"].(string); ok && val != "" {
		token.AccessToken = val
	} else if val, ok := tokenResp["token"].(string); ok && val != "" {
		token.AccessToken = val
	}

	if token.AccessToken == "" {
		return nil, &DeliveryError{IsTransient: false, ErrorMessage: "access token not found in response", ErrorCode: "AUTH_FAIL"}
	}

	if val, ok := tokenResp["AccessTokenExpiryDateTime"].(string); ok && val != "" {
		token.AccessExpiry = parseCorityDate(val)
	} else if val, ok := tokenResp["ExpiryDateTime"].(string); ok && val != "" {
		token.AccessExpiry = parseCorityDate(val)
	} else if val, ok := tokenResp["access_expiry"].(string); ok && val != "" {
		token.AccessExpiry = parseCorityDate(val)
	}

	// Use a minimal fallback expiry if not provided by API
	if token.AccessExpiry.IsZero() {
		token.AccessExpiry = time.Now().Add(59 * time.Minute)
	}
	if token.RefreshExpiry.IsZero() {
		token.RefreshExpiry = time.Now().Add(24 * time.Hour)
	}

	a.audit("Successfully retrieved and cached new Cority tokens")
	return &token, nil
}

// decryptFields decrypts the configured encrypted fields of every record in place.
// Any field that cannot be decrypted fails the whole package (no plaintext fallback, no fallback key).
func (a *CorityAdapter) decryptFields(rawData []interface{}, config TargetConfig) error {
	if len(config.EncryptedFields) == 0 {
		a.audit("No encrypted fields configured for this target.")
		return nil
	}

	decryptFail := func(msg string) error {
		a.audit(msg)
		return &DeliveryError{IsTransient: false, ErrorMessage: msg, ErrorCode: "DECRYPT_FAIL"}
	}

	for _, item := range rawData {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for _, field := range config.EncryptedFields {
			var targetKey string
			var val interface{}
			var exists bool

			if v, ok := m[field]; ok {
				val = v
				targetKey = field
				exists = true
			} else {
				// Case-insensitive fallback ignoring underscores
				for k, v := range m {
					if strings.EqualFold(strings.ReplaceAll(k, "_", ""), strings.ReplaceAll(field, "_", "")) {
						val = v
						targetKey = k
						exists = true
						break
					}
				}
			}

			if !exists {
				continue
			}

			var nonceBytes, ciphertextBytes []byte

			if mapVal, isMap := val.(map[string]interface{}); isMap {
				// Handle map format: {"ciphertext": "...", "nonce": "..."}
				cipherStr, _ := mapVal["ciphertext"].(string)
				nonceStr, _ := mapVal["nonce"].(string)
				if cipherStr == "" || nonceStr == "" {
					continue
				}
				nBytes, err1 := base64.StdEncoding.DecodeString(nonceStr)
				cBytes, err2 := base64.StdEncoding.DecodeString(cipherStr)
				if err1 != nil || err2 != nil {
					return decryptFail(fmt.Sprintf("Base64 decode failed for map field %s", targetKey))
				}
				nonceBytes = nBytes
				ciphertextBytes = cBytes
			} else if strVal, isStr := val.(string); isStr && strVal != "" {
				// Handle legacy string format
				decoded, err := base64.StdEncoding.DecodeString(strVal)
				if err != nil {
					decoded, err = base64.RawStdEncoding.DecodeString(strVal)
				}
				if err != nil {
					return decryptFail(fmt.Sprintf("Base64 decode failed for string field %s: %v", targetKey, err))
				}
				if len(decoded) <= 12 {
					return decryptFail(fmt.Sprintf("Decoded string data too short for field %s (len: %d)", targetKey, len(decoded)))
				}
				nonceBytes = decoded[:12]
				ciphertextBytes = decoded[12:]
			} else {
				continue
			}

			decrypted, err := crypto.EnvelopeDecrypt(config.KEK, config.WrappedKey, nonceBytes, ciphertextBytes)
			if err != nil {
				return decryptFail(fmt.Sprintf("Decryption failed for field %s: %v", targetKey, err))
			}

			// The decrypted data might be a JSON-marshaled string (e.g., `"537732"` with quotes).
			// We try to unmarshal it back to a primitive. If it fails, fallback to raw string.
			var parsedVal interface{}
			dec := json.NewDecoder(bytes.NewReader(decrypted))
			dec.UseNumber()
			errUnmarshal := dec.Decode(&parsedVal)
			if errUnmarshal == nil {
				if _, errTrailing := dec.Token(); errTrailing != io.EOF {
					errUnmarshal = fmt.Errorf("trailing data found")
				}
			}
			if errUnmarshal == nil {
				m[targetKey] = parsedVal
			} else {
				m[targetKey] = string(decrypted)
			}
		}
	}
	return nil
}

// upload posts one chunk of records to the Cority import endpoint and returns status code and body.
func (a *CorityAdapter) upload(ctx context.Context, client *http.Client, cfg CorityAuthConfig, baseURL, accessToken, idempotencyKey string, records []interface{}, configuredTimeout int) (int, []byte, error) {
	finalBody := map[string]interface{}{
		"options": cfg.UploadOptions,
		"records": records,
	}

	finalBytes, _ := json.Marshal(finalBody)

	importReqURL := baseURL + cfg.ImportPath
	req3, _ := http.NewRequestWithContext(ctx, http.MethodPost, importReqURL, bytes.NewReader(finalBytes))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("Authorization", "Bearer "+accessToken)
	req3.Header.Set("Idempotency-Key", idempotencyKey)

	done := make(chan struct{})
	if configuredTimeout > 300 {
		go func() {
			timer300 := time.NewTimer(300 * time.Second)
			defer timer300.Stop()
			select {
			case <-timer300.C:
				a.audit(fmt.Sprintf("Upload Warning: Delivery is taking longer than 300 seconds. Configured timeout is %d seconds.", configuredTimeout))
			case <-done:
			}
		}()
	}

	startTime := time.Now()
	resp3, err := client.Do(req3)
	close(done)
	elapsed := time.Since(startTime)

	if err != nil {
		if elapsed >= time.Duration(configuredTimeout)*time.Second-2*time.Second {
			a.audit(fmt.Sprintf("Upload Error: Configured timeout of %d seconds reached. Request failed: %v", configuredTimeout, err))
		} else {
			a.audit(fmt.Sprintf("Upload Error: Network failure after %v: %v", elapsed, err))
		}
		return 0, nil, &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("network error during upload: %v", err), ErrorCode: "NETWORK_ERROR"}
	}
	defer resp3.Body.Close()

	bodyBytes, _ := io.ReadAll(resp3.Body)
	return resp3.StatusCode, bodyBytes, nil
}

// checkUploadResponse audit-logs the import result and maps non-2xx codes to a DeliveryError.
func (a *CorityAdapter) checkUploadResponse(importReqURL, idempotencyKey string, statusCode int, bodyBytes []byte) error {
	if statusCode >= 200 && statusCode < 300 {
		a.audit(fmt.Sprintf("CORITY_SAAS Upload | Target: %s | Idempotency-Key: %s | Return Code: %d", importReqURL, idempotencyKey, statusCode))

		bodyStr := string(bodyBytes)
		var stats []string

		// More reliable regex to handle JSON quotes, camelCase variations, and case-insensitivity
		patterns := []struct {
			label string
			re    *regexp.Regexp
		}{
			{"Records Total", regexp.MustCompile(`(?i)"?(?:Records Total|recordsTotal|records_total)"?\s*:\s*"?(\d+)"?`)},
			{"Records Added", regexp.MustCompile(`(?i)"?(?:Records Added|recordsAdded|records_added)"?\s*:\s*"?(\d+)"?`)},
			{"Records Updated", regexp.MustCompile(`(?i)"?(?:Records Updated|recordsUpdated|records_updated)"?\s*:\s*"?(\d+)"?`)},
			{"Records Skipped", regexp.MustCompile(`(?i)"?(?:Records Skipped|recordsSkipped|records_skipped)"?\s*:\s*"?(\d+)"?`)},
			{"Records Rejected", regexp.MustCompile(`(?i)"?(?:Records Rejected|recordsRejected|records_rejected)"?\s*:\s*"?(\d+)"?`)},
			{"Errors", regexp.MustCompile(`(?i)"?Errors"?\s*:\s*"?(\d+)"?`)},
		}

		for _, p := range patterns {
			if matches := p.re.FindStringSubmatch(bodyStr); len(matches) > 1 {
				stats = append(stats, fmt.Sprintf("%s: %s", p.label, matches[1]))
			}
		}

		if len(stats) > 0 {
			a.audit(fmt.Sprintf("Cority Import Statistics: %s", strings.Join(stats, " | ")))
		}
		return nil
	}

	a.audit(fmt.Sprintf("CORITY_SAAS Upload | Target: %s | Idempotency-Key: %s | Return Code: %d | Response: %s", importReqURL, idempotencyKey, statusCode, truncate(bodyBytes)))

	isTransient := false
	if statusCode == http.StatusTooManyRequests || (statusCode >= 500 && statusCode <= 599) {
		isTransient = true
	}

	return &DeliveryError{
		IsTransient:  isTransient,
		HTTPCode:     statusCode,
		ErrorMessage: fmt.Sprintf("HTTP %d: %s", statusCode, string(bodyBytes)),
		ErrorCode:    fmt.Sprintf("HTTP_%d", statusCode),
	}
}

// uploadBatchSize reads upload_options.batch_size (number or numeric string); 0 disables chunking.
func uploadBatchSize(options map[string]interface{}) int {
	switch v := options["batch_size"].(type) {
	case float64:
		return int(v)
	case string:
		n, _ := strconv.Atoi(v)
		return n
	}
	return 0
}

// chunkRecords splits records into chunks of at most size records (size <= 0: one chunk).
// It always returns at least one chunk so that an empty package is still sent.
func chunkRecords(records []interface{}, size int) [][]interface{} {
	if size <= 0 || len(records) <= size {
		return [][]interface{}{records}
	}
	var chunks [][]interface{}
	for start := 0; start < len(records); start += size {
		end := min(start+size, len(records))
		chunks = append(chunks, records[start:end])
	}
	return chunks
}

// chunkIdempotencyKey derives a deterministic key per chunk so retries reuse the same keys.
// A package sent as a single chunk keeps its original key.
func chunkIdempotencyKey(packageKey string, index, total int) string {
	if total <= 1 {
		return packageKey
	}
	ns, err := uuid.Parse(packageKey)
	if err != nil {
		return fmt.Sprintf("%s-%d", packageKey, index+1)
	}
	return uuid.NewSHA1(ns, []byte(fmt.Sprintf("chunk-%d", index+1))).String()
}

// connectionHashFor identifies a shared token in adapter_tokens by endpoint and login.
func connectionHashFor(baseURL, loginUser string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(baseURL+"|"+loginUser)))
}

func truncate(body []byte) string {
	if len(body) > maxLoggedBodyBytes {
		return string(body[:maxLoggedBodyBytes]) + "...(truncated)"
	}
	return string(body)
}

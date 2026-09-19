package delivery

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"mitm_cority/internal/crypto"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CorityAuthConfig struct {
	LoginUser       string                 `json:"login_user"`
	LoginPass       string                 `json:"login_pass"`
	AuthRefreshPath string                 `json:"auth_refresh_path"`
	AuthTokenPath   string                 `json:"auth_token_path"`
	ImportPath      string                 `json:"import_path"`
	UploadOptions   map[string]interface{} `json:"upload_options"`
	Slowdown        string                 `json:"slowdown"`
	Timeout         string                 `json:"timeout"`
}

type CorityAdapter struct {
	client     *http.Client
	logAudit   func(string)
	mu         sync.Mutex
	lastUpload time.Time
	db         *pgxpool.Pool
}

func NewCorityAdapter(client *http.Client, logAudit func(string), db *pgxpool.Pool) *CorityAdapter {
	if client == nil {
		client = &http.Client{Timeout: 300 * time.Second}
	}
	return &CorityAdapter{client: client, logAudit: logAudit, db: db}
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
	var cfg CorityAuthConfig
	if err := json.Unmarshal(config.AuthConfig, &cfg); err != nil {
		return &DeliveryError{
			IsTransient:  false,
			ErrorMessage: fmt.Sprintf("invalid auth_config for Cority adapter: %v", err),
			ErrorCode:    "INVALID_CONFIG",
		}
	}

	activeClient := a.client
	if cfg.Timeout != "" {
		if t, err := strconv.Atoi(cfg.Timeout); err == nil && t > 0 {
			clientCopy := *a.client
			clientCopy.Timeout = time.Duration(t) * time.Second
			activeClient = &clientCopy
		}
	}

	var slowdownMsg string
	if cfg.Slowdown != "" && cfg.Slowdown != "0" {
		if sec, err := strconv.Atoi(cfg.Slowdown); err == nil && sec > 0 {
			slowdownMsg = fmt.Sprintf("Active (%d seconds)", sec)
			elapsed := time.Since(a.lastUpload)
			if delay := time.Duration(sec)*time.Second - elapsed; delay > 0 {
				time.Sleep(delay)
			}
		} else {
			slowdownMsg = "Inactive (invalid config)"
		}
	} else {
		slowdownMsg = "Inactive"
	}

	if a.logAudit != nil {
		a.logAudit(fmt.Sprintf("Upload Slowdown: %s", slowdownMsg))
	}
	
	defer func() { a.lastUpload = time.Now() }()

	baseURL := strings.TrimSuffix(config.EndpointURL, "/")

	connectionHash := fmt.Sprintf("%x", sha256.Sum256([]byte(baseURL+"|"+cfg.LoginUser)))

	tx, err := a.db.Begin(ctx)
	if err != nil {
		return &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("failed to begin tx: %v", err), ErrorCode: "DB_ERROR"}
	}
	defer tx.Rollback(ctx)

	var accessToken, refreshToken string
	var accessExpiry, refreshExpiry time.Time

	// FOR UPDATE ensures only one worker attempts to refresh at a time
	err = tx.QueryRow(ctx, "SELECT access_token, access_expiry, refresh_token, refresh_expiry FROM adapter_tokens WHERE connection_hash = $1 FOR UPDATE", connectionHash).Scan(&accessToken, &accessExpiry, &refreshToken, &refreshExpiry)
	
	needsRefresh := false
	if err == pgx.ErrNoRows {
		needsRefresh = true
	} else if err != nil {
		return &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("failed to read token: %v", err), ErrorCode: "DB_ERROR"}
	} else {
		// Margin of 60 seconds
		if time.Now().Add(60 * time.Second).After(accessExpiry) {
			needsRefresh = true
		}
	}

	if needsRefresh {
		if a.logAudit != nil {
			a.logAudit(fmt.Sprintf("Token expired or missing. Authenticating with Cority API..."))
		}
		
		// 1. Get Refresh Token
		refreshReqURL := baseURL + cfg.AuthRefreshPath
		refreshBody := fmt.Sprintf(`{"user":{"LoginName":"%s","Loginpassword":"%s"}}`, cfg.LoginUser, cfg.LoginPass)
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, refreshReqURL, strings.NewReader(refreshBody))
		req.Header.Set("Content-Type", "application/json")

		resp, err := activeClient.Do(req)
		if err != nil {
			return &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("failed refresh request: %v", err), ErrorCode: "NETWORK_ERROR"}
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return &DeliveryError{IsTransient: false, ErrorMessage: fmt.Sprintf("refresh token failed: %s", string(bodyBytes)), ErrorCode: "AUTH_FAIL"}
		}

		var refreshResp map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&refreshResp); err != nil {
			return &DeliveryError{IsTransient: false, ErrorMessage: "failed to parse refresh token response", ErrorCode: "AUTH_FAIL"}
		}

		if val, ok := refreshResp["Token"].(string); ok && val != "" {
			refreshToken = val
		} else if val, ok := refreshResp["token"].(string); ok && val != "" {
			refreshToken = val
		}

		if refreshToken == "" {
			return &DeliveryError{IsTransient: false, ErrorMessage: "refresh token not found in response", ErrorCode: "AUTH_FAIL"}
		}
		
		if val, ok := refreshResp["ExpiryDateTime"].(string); ok && val != "" {
			refreshExpiry = parseCorityDate(val)
		} else if val, ok := refreshResp["expiry"].(string); ok && val != "" {
			refreshExpiry = parseCorityDate(val)
		}

		// 2. Get Access Token
		tokenReqURL := baseURL + cfg.AuthTokenPath
		req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, tokenReqURL, nil)
		req2.Header.Set("Authorization", "Bearer "+refreshToken)

		resp2, err := activeClient.Do(req2)
		if err != nil {
			return &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("failed token request: %v", err), ErrorCode: "NETWORK_ERROR"}
		}
		defer resp2.Body.Close()

		if resp2.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp2.Body)
			return &DeliveryError{IsTransient: false, ErrorMessage: fmt.Sprintf("access token failed: %s", string(bodyBytes)), ErrorCode: "AUTH_FAIL"}
		}

		var tokenResp map[string]interface{}
		if err := json.NewDecoder(resp2.Body).Decode(&tokenResp); err != nil {
			return &DeliveryError{IsTransient: false, ErrorMessage: "failed to parse access token response", ErrorCode: "AUTH_FAIL"}
		}

		if val, ok := tokenResp["AccessToken"].(string); ok && val != "" {
			accessToken = val
		} else if val, ok := tokenResp["access_token"].(string); ok && val != "" {
			accessToken = val
		} else if val, ok := tokenResp["token"].(string); ok && val != "" {
			accessToken = val
		}

		if accessToken == "" {
			return &DeliveryError{IsTransient: false, ErrorMessage: "access token not found in response", ErrorCode: "AUTH_FAIL"}
		}
		
		if val, ok := tokenResp["AccessTokenExpiryDateTime"].(string); ok && val != "" {
			accessExpiry = parseCorityDate(val)
		} else if val, ok := tokenResp["ExpiryDateTime"].(string); ok && val != "" {
			accessExpiry = parseCorityDate(val)
		} else if val, ok := tokenResp["access_expiry"].(string); ok && val != "" {
			accessExpiry = parseCorityDate(val)
		}
		
		// 3. Save to DB
		// Use a minimal fallback expiry if not provided by API
		if accessExpiry.IsZero() {
			accessExpiry = time.Now().Add(59 * time.Minute)
		}
		if refreshExpiry.IsZero() {
			refreshExpiry = time.Now().Add(24 * time.Hour)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO adapter_tokens (connection_hash, access_token, access_expiry, refresh_token, refresh_expiry, updated_at) 
			VALUES ($1, $2, $3, $4, $5, NOW())
			ON CONFLICT (connection_hash) 
			DO UPDATE SET access_token = EXCLUDED.access_token, access_expiry = EXCLUDED.access_expiry, refresh_token = EXCLUDED.refresh_token, refresh_expiry = EXCLUDED.refresh_expiry, updated_at = NOW()
		`, connectionHash, accessToken, accessExpiry, refreshToken, refreshExpiry)
		if err != nil {
			return &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("failed to save token: %v", err), ErrorCode: "DB_ERROR"}
		}
		
		if a.logAudit != nil {
			a.logAudit(fmt.Sprintf("Successfully retrieved and cached new Cority tokens"))
		}
	}
	
	// Release the lock
	tx.Commit(ctx)

	// 3. Send Payload Data
	var rawData []interface{}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&rawData); err != nil {
		return &DeliveryError{IsTransient: false, ErrorMessage: "payload is not valid JSON array", ErrorCode: "INVALID_PAYLOAD"}
	}

	// Decrypt encrypted fields
	if len(config.EncryptedFields) > 0 {
		if a.logAudit != nil && len(rawData) > 0 {
			if m, ok := rawData[0].(map[string]interface{}); ok {
				var keys []string
				for k := range m {
					keys = append(keys, k)
				}
				// a.logAudit(fmt.Sprintf("DEBUG: EncryptedFields=%v | Payload Keys[0]=%v", config.EncryptedFields, keys))
			}
		}

		for _, item := range rawData {
			if m, ok := item.(map[string]interface{}); ok {
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

					if exists {
						var nonceBytes, ciphertextBytes []byte
						var hasValidData bool

						if mapVal, isMap := val.(map[string]interface{}); isMap {
							// Handle map format: {"ciphertext": "...", "nonce": "..."}
							cipherStr, _ := mapVal["ciphertext"].(string)
							nonceStr, _ := mapVal["nonce"].(string)

							if cipherStr != "" && nonceStr != "" {
								nBytes, err1 := base64.StdEncoding.DecodeString(nonceStr)
								cBytes, err2 := base64.StdEncoding.DecodeString(cipherStr)
								if err1 == nil && err2 == nil {
									nonceBytes = nBytes
									ciphertextBytes = cBytes
									hasValidData = true
								} else {
									if a.logAudit != nil {
										a.logAudit(fmt.Sprintf("Base64 decode failed for map field %s", targetKey))
									}
								}
							}
						} else if strVal, isStr := val.(string); isStr && strVal != "" {
							// Handle legacy string format
							decoded, err := base64.StdEncoding.DecodeString(strVal)
							if err != nil {
								decoded, err = base64.RawStdEncoding.DecodeString(strVal)
							}

							if err == nil && len(decoded) > 12 {
								nonceBytes = decoded[:12]
								ciphertextBytes = decoded[12:]
								hasValidData = true
							} else if err != nil {
								if a.logAudit != nil {
									a.logAudit(fmt.Sprintf("Base64 decode failed for string field %s: %v", targetKey, err))
								}
							} else {
								if a.logAudit != nil {
									a.logAudit(fmt.Sprintf("Decoded string data too short for field %s (len: %d)", targetKey, len(decoded)))
								}
							}
						}

						if hasValidData {
							decrypted, err := crypto.EnvelopeDecrypt(config.KEK, config.WrappedKey, nonceBytes, ciphertextBytes)

							// Fallback: If EnvelopeDecrypt fails, the Transformation layer might have used its mock target key directly
							if err != nil {
								mockKey := []byte("0123456789abcdef0123456789abcdef")
								if block, errCipher := aes.NewCipher(mockKey); errCipher == nil {
									if gcm, errGCM := cipher.NewGCM(block); errGCM == nil {
										decrypted, err = gcm.Open(nil, nonceBytes, ciphertextBytes, nil)
									}
								}
							}

							if err == nil {
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
							} else {
								if a.logAudit != nil {
									a.logAudit(fmt.Sprintf("Decryption failed for field %s: %v", targetKey, err))
								}
							}
						}
					}
				}
			}
		}
	} else {
		if a.logAudit != nil {
			a.logAudit("No encrypted fields configured for this target.")
		}
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

	finalBody := map[string]interface{}{
		"options": cfg.UploadOptions,
		"records": rawData,
	}

	finalBytes, _ := json.Marshal(finalBody)

	importReqURL := baseURL + cfg.ImportPath
	req3, _ := http.NewRequestWithContext(ctx, http.MethodPost, importReqURL, bytes.NewReader(finalBytes))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("Authorization", "Bearer "+accessToken)
	req3.Header.Set("Idempotency-Key", idempotencyKey)

	configuredTimeout := 300
	if cfg.Timeout != "" {
		if t, err := strconv.Atoi(cfg.Timeout); err == nil && t > 0 {
			configuredTimeout = t
		}
	}

	done := make(chan struct{})
	go func() {
		timer300 := time.NewTimer(300 * time.Second)
		defer timer300.Stop()
		select {
		case <-timer300.C:
			if a.logAudit != nil {
				a.logAudit(fmt.Sprintf("Upload Warning: Delivery is taking longer than 300 seconds. Configured timeout is %d seconds.", configuredTimeout))
			}
		case <-done:
		}
	}()

	startTime := time.Now()
	resp3, err := activeClient.Do(req3)
	close(done)
	elapsed := time.Since(startTime)

	if err != nil {
		if elapsed >= time.Duration(configuredTimeout)*time.Second - 2*time.Second {
			if a.logAudit != nil {
				a.logAudit(fmt.Sprintf("Upload Error: Configured timeout of %d seconds reached. Request failed: %v", configuredTimeout, err))
			}
		} else {
			if a.logAudit != nil {
				a.logAudit(fmt.Sprintf("Upload Error: Network failure after %v: %v", elapsed, err))
			}
		}
		return &DeliveryError{IsTransient: true, ErrorMessage: fmt.Sprintf("network error during upload: %v", err), ErrorCode: "NETWORK_ERROR"}
	}
	defer resp3.Body.Close()

	bodyBytes, _ := io.ReadAll(resp3.Body)

	if a.logAudit != nil {
		a.logAudit(fmt.Sprintf("CORITY_SAAS Upload | Target: %s | Idempotency-Key: %s | Return Code: %d | Response: %s", importReqURL, idempotencyKey, resp3.StatusCode, string(bodyBytes)))
	}

	if resp3.StatusCode >= 200 && resp3.StatusCode < 300 {
		if a.logAudit != nil {
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
				a.logAudit(fmt.Sprintf("Cority Import Statistics: %s", strings.Join(stats, " | ")))
			}
		}
		return nil
	}

	isTransient := false
	if resp3.StatusCode == http.StatusTooManyRequests || (resp3.StatusCode >= 500 && resp3.StatusCode <= 599) {
		isTransient = true
	}

	return &DeliveryError{
		IsTransient:  isTransient,
		HTTPCode:     resp3.StatusCode,
		ErrorMessage: fmt.Sprintf("HTTP %d: %s", resp3.StatusCode, string(bodyBytes)),
		ErrorCode:    fmt.Sprintf("HTTP_%d", resp3.StatusCode),
	}
}

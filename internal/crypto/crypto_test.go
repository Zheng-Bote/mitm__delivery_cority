/**
 * SPDX-FileComment: Crypto Tests
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file crypto_test.go
 * @brief Unit tests for the envelope encryption primitives
 * @version 0.21.0
 * @date 2026-10-17
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

package crypto

import (
	"bytes"
	"testing"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	kek := bytes.Repeat([]byte{0x42}, 32)
	wrapped, err := GenerateWrappedDEK(kek)
	if err != nil {
		t.Fatal(err)
	}
	plaintext := []byte(`{"login_user":"Cority_User"}`)
	ciphertext, nonce, err := EnvelopeEncrypt(kek, wrapped, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	got, err := EnvelopeDecrypt(kek, wrapped, nonce, ciphertext)
	if err != nil {
		t.Fatalf("unexpected decrypt error: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Errorf("round trip mismatch: %s", got)
	}
}

func TestEnvelopeDecryptWrongKEK(t *testing.T) {
	kek := bytes.Repeat([]byte{0x42}, 32)
	wrapped, _ := GenerateWrappedDEK(kek)
	ciphertext, nonce, _ := EnvelopeEncrypt(kek, wrapped, []byte("secret"))

	if _, err := EnvelopeDecrypt(bytes.Repeat([]byte{0x24}, 32), wrapped, nonce, ciphertext); err == nil {
		t.Errorf("expected error with wrong KEK")
	}
}

func TestEnvelopeDecryptTamperedPayload(t *testing.T) {
	kek := bytes.Repeat([]byte{0x42}, 32)
	wrapped, _ := GenerateWrappedDEK(kek)
	ciphertext, nonce, _ := EnvelopeEncrypt(kek, wrapped, []byte("secret"))
	ciphertext[0] ^= 0xff

	if _, err := EnvelopeDecrypt(kek, wrapped, nonce, ciphertext); err == nil {
		t.Errorf("expected authentication error for tampered ciphertext")
	}
}

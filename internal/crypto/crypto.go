/**
 * SPDX-FileComment: Crypto
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file crypto.go
 * @brief AES-256-GCM encryption/decryption with Argon2id key derivation
 * @version 1.0.0
 * @date 2026-06-02
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @LICENSE Apache-2.0
 */

// Package crypto provides AES-256-GCM encryption and decryption primitives
// protected by Argon2id key derivation. It is used by the config package to
// securely store database credentials and admin tokens on disk, and by the
// encrypt-config CLI tool.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters as per requirements
const (
	ArgonTime      = 3
	ArgonMemory    = 64 * 1024 // 64 MB
	ArgonThreads   = 1
	ArgonKeyLength = 32
)

// DeriveKey derives a 32-byte key from a password and salt using Argon2id
func DeriveKey(password []byte, salt []byte) []byte {
	return argon2.IDKey(password, salt, ArgonTime, ArgonMemory, ArgonThreads, ArgonKeyLength)
}

// Encrypt encrypts data using AES-256-GCM
func Encrypt(plaintext []byte, password []byte) ([]byte, error) {
	salt := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, err
	}

	key := DeriveKey(password, salt)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Result format: salt (16) + nonce (12) + ciphertext
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)

	result := make([]byte, len(salt)+len(nonce)+len(ciphertext))
	copy(result[0:16], salt)
	copy(result[16:16+len(nonce)], nonce)
	copy(result[16+len(nonce):], ciphertext)

	return result, nil
}

// Decrypt decrypts data using AES-256-GCM
func Decrypt(data []byte, password []byte) ([]byte, error) {
	if len(data) < 16+12 {
		return nil, errors.New("invalid encrypted data size")
	}

	salt := data[:16]
	nonce := data[16 : 16+12]
	ciphertext := data[16+12:]

	key := DeriveKey(password, salt)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return gcm.Open(nil, nonce, ciphertext, nil)
}

// EnvelopeDecrypt decrypts a payload using a wrapped DEK and a KEK
func EnvelopeDecrypt(kek []byte, wrappedKey []byte, payloadNonce []byte, payload []byte) ([]byte, error) {
	if len(kek) != 32 {
		adjusted := make([]byte, 32)
		copy(adjusted, kek)
		kek = adjusted
	}

	if len(wrappedKey) < 12 {
		return nil, errors.New("wrapped DEK too short")
	}
	dekNonce := wrappedKey[:12]
	wrappedCipher := wrappedKey[12:]

	kekBlock, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	kekGCM, err := cipher.NewGCM(kekBlock)
	if err != nil {
		return nil, err
	}
	dek, err := kekGCM.Open(nil, dekNonce, wrappedCipher, nil)
	if err != nil {
		return nil, errors.New("failed to decrypt DEK")
	}

	dekBlock, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	dekGCM, err := cipher.NewGCM(dekBlock)
	if err != nil {
		return nil, err
	}
	return dekGCM.Open(nil, payloadNonce, payload, nil)
}

// EnvelopeEncrypt encrypts a payload using a wrapped DEK and a KEK. Returns encrypted payload and new nonce.
func EnvelopeEncrypt(kek []byte, wrappedKey []byte, plaintext []byte) ([]byte, []byte, error) {
	if len(kek) != 32 {
		adjusted := make([]byte, 32)
		copy(adjusted, kek)
		kek = adjusted
	}

	if len(wrappedKey) < 12 {
		return nil, nil, errors.New("wrapped DEK too short")
	}
	dekNonce := wrappedKey[:12]
	wrappedCipher := wrappedKey[12:]

	kekBlock, err := aes.NewCipher(kek)
	if err != nil {
		return nil, nil, err
	}
	kekGCM, err := cipher.NewGCM(kekBlock)
	if err != nil {
		return nil, nil, err
	}
	dek, err := kekGCM.Open(nil, dekNonce, wrappedCipher, nil)
	if err != nil {
		return nil, nil, errors.New("failed to decrypt DEK")
	}

	dekBlock, err := aes.NewCipher(dek)
	if err != nil {
		return nil, nil, err
	}
	dekGCM, err := cipher.NewGCM(dekBlock)
	if err != nil {
		return nil, nil, err
	}

	payloadNonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, payloadNonce); err != nil {
		return nil, nil, err
	}

	ciphertext := dekGCM.Seal(nil, payloadNonce, plaintext, nil)
	return ciphertext, payloadNonce, nil
}

// GenerateWrappedDEK generates a new random 32-byte DEK and wraps it with the provided KEK
func GenerateWrappedDEK(kek []byte) ([]byte, error) {
	if len(kek) != 32 {
		adjusted := make([]byte, 32)
		copy(adjusted, kek)
		kek = adjusted
	}

	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, err
	}

	dekNonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, dekNonce); err != nil {
		return nil, err
	}

	kekBlock, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	kekGCM, err := cipher.NewGCM(kekBlock)
	if err != nil {
		return nil, err
	}
	wrappedCipher := kekGCM.Seal(nil, dekNonce, dek, nil)

	wrappedKey := make([]byte, len(dekNonce)+len(wrappedCipher))
	copy(wrappedKey, dekNonce)
	copy(wrappedKey[len(dekNonce):], wrappedCipher)

	return wrappedKey, nil
}

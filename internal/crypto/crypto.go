/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package crypto provides encryption utilities for FlyMQ.
// It implements AES-256-GCM for data-at-rest encryption with
// authenticated encryption to ensure both confidentiality and integrity.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

const (
	// KeySize is the required key size for AES-256 (32 bytes).
	KeySize = 32

	// NonceSize is the size of the GCM nonce (12 bytes).
	NonceSize = 12

	// TagSize is the size of the GCM authentication tag (16 bytes).
	TagSize = 16
)

var (
	// ErrInvalidKeySize is returned when the key is not 32 bytes.
	ErrInvalidKeySize = errors.New("crypto: key must be 32 bytes (256 bits)")

	// ErrInvalidKeyFormat is returned when the hex key cannot be decoded.
	ErrInvalidKeyFormat = errors.New("crypto: key must be valid hex-encoded string")

	// ErrCiphertextTooShort is returned when ciphertext is shorter than nonce + tag.
	ErrCiphertextTooShort = errors.New("crypto: ciphertext too short")

	// ErrDecryptionFailed is returned when decryption or authentication fails.
	ErrDecryptionFailed = errors.New("crypto: decryption failed - data may be corrupted or tampered")
)

// Encryptor provides AES-256-GCM encryption and decryption.
type Encryptor struct {
	gcm cipher.AEAD
	key []byte
}

// NewEncryptor creates a new Encryptor with the given hex-encoded key.
// The key must be 64 hex characters (32 bytes / 256 bits).
func NewEncryptor(hexKey string) (*Encryptor, error) {
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, ErrInvalidKeyFormat
	}
	return NewEncryptorFromBytes(key)
}

// NewEncryptorFromBytes creates a new Encryptor with the given raw key bytes.
func NewEncryptorFromBytes(key []byte) (*Encryptor, error) {
	if len(key) != KeySize {
		return nil, ErrInvalidKeySize
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: failed to create GCM: %w", err)
	}

	return &Encryptor{
		gcm: gcm,
		key: key,
	}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM.
// Returns: nonce (12 bytes) || ciphertext || tag (16 bytes)
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("crypto: failed to generate nonce: %w", err)
	}

	// Seal appends the ciphertext and tag to nonce
	ciphertext := e.gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts ciphertext that was encrypted with Encrypt.
// Expects: nonce (12 bytes) || ciphertext || tag (16 bytes)
func (e *Encryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < NonceSize+TagSize {
		return nil, ErrCiphertextTooShort
	}

	nonce := ciphertext[:NonceSize]
	encryptedData := ciphertext[NonceSize:]

	plaintext, err := e.gcm.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return nil, ErrDecryptionFailed
	}

	return plaintext, nil
}

// GenerateKey generates a cryptographically secure random 256-bit key.
// Returns the key as a hex-encoded string.
func GenerateKey() (string, error) {
	key := make([]byte, KeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", fmt.Errorf("crypto: failed to generate key: %w", err)
	}
	return hex.EncodeToString(key), nil
}

// ValidateKey checks if a hex-encoded key is valid.
func ValidateKey(hexKey string) error {
	key, err := hex.DecodeString(hexKey)
	if err != nil {
		return ErrInvalidKeyFormat
	}
	if len(key) != KeySize {
		return ErrInvalidKeySize
	}
	return nil
}

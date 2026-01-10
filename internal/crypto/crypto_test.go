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

package crypto

import (
	"bytes"
	"testing"
)

func TestGenerateKey(t *testing.T) {
	key1, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	if len(key1) != 64 { // 32 bytes = 64 hex chars
		t.Errorf("Expected key length 64, got %d", len(key1))
	}

	// Keys should be unique
	key2, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	if key1 == key2 {
		t.Error("Generated keys should be unique")
	}
}

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr error
	}{
		{
			name:    "valid key",
			key:     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			wantErr: nil,
		},
		{
			name:    "invalid hex",
			key:     "not-valid-hex-string-at-all-!!!",
			wantErr: ErrInvalidKeyFormat,
		},
		{
			name:    "too short",
			key:     "0123456789abcdef",
			wantErr: ErrInvalidKeySize,
		},
		{
			name:    "empty",
			key:     "",
			wantErr: ErrInvalidKeySize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKey(tt.key)
			if err != tt.wantErr {
				t.Errorf("ValidateKey() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestEncryptorRoundTrip(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey failed: %v", err)
	}

	enc, err := NewEncryptor(key)
	if err != nil {
		t.Fatalf("NewEncryptor failed: %v", err)
	}

	testCases := [][]byte{
		[]byte("Hello, World!"),
		[]byte(""),
		[]byte("A"),
		make([]byte, 1024*1024), // 1MB of zeros
		[]byte(`{"topic":"test","data":"some json content"}`),
	}

	for i, plaintext := range testCases {
		ciphertext, err := enc.Encrypt(plaintext)
		if err != nil {
			t.Errorf("Case %d: Encrypt failed: %v", i, err)
			continue
		}

		// Ciphertext should be larger than plaintext (nonce + tag)
		if len(ciphertext) < len(plaintext)+NonceSize+TagSize {
			t.Errorf("Case %d: Ciphertext too short", i)
		}

		decrypted, err := enc.Decrypt(ciphertext)
		if err != nil {
			t.Errorf("Case %d: Decrypt failed: %v", i, err)
			continue
		}

		if !bytes.Equal(plaintext, decrypted) {
			t.Errorf("Case %d: Decrypted data doesn't match original", i)
		}
	}
}

func TestEncryptorTamperedData(t *testing.T) {
	key, _ := GenerateKey()
	enc, _ := NewEncryptor(key)

	plaintext := []byte("Secret message")
	ciphertext, _ := enc.Encrypt(plaintext)

	// Tamper with the ciphertext
	ciphertext[len(ciphertext)-1] ^= 0xFF

	_, err := enc.Decrypt(ciphertext)
	if err != ErrDecryptionFailed {
		t.Errorf("Expected ErrDecryptionFailed, got %v", err)
	}
}

func TestEncryptorDifferentKeys(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()

	enc1, _ := NewEncryptor(key1)
	enc2, _ := NewEncryptor(key2)

	plaintext := []byte("Secret message")
	ciphertext, _ := enc1.Encrypt(plaintext)

	// Try to decrypt with different key
	_, err := enc2.Decrypt(ciphertext)
	if err != ErrDecryptionFailed {
		t.Errorf("Expected ErrDecryptionFailed with wrong key, got %v", err)
	}
}

func TestDecryptTooShort(t *testing.T) {
	key, _ := GenerateKey()
	enc, _ := NewEncryptor(key)

	_, err := enc.Decrypt([]byte("short"))
	if err != ErrCiphertextTooShort {
		t.Errorf("Expected ErrCiphertextTooShort, got %v", err)
	}
}

func TestValidateTLSFilesNotFound(t *testing.T) {
	err := ValidateTLSFiles("/nonexistent/cert.pem", "/nonexistent/key.pem")
	if err == nil {
		t.Error("Expected error for non-existent files")
	}
}

func TestNewServerTLSConfigCertNotFound(t *testing.T) {
	cfg := TLSConfig{
		CertFile: "/nonexistent/cert.pem",
		KeyFile:  "/nonexistent/key.pem",
	}

	_, err := NewServerTLSConfig(cfg)
	if err == nil {
		t.Error("Expected error for non-existent certificate")
	}
}

func TestNewClientTLSConfigBasic(t *testing.T) {
	cfg := TLSConfig{
		InsecureSkipVerify: true,
	}

	tlsConfig, err := NewClientTLSConfig(cfg)
	if err != nil {
		t.Fatalf("NewClientTLSConfig failed: %v", err)
	}

	if !tlsConfig.InsecureSkipVerify {
		t.Error("Expected InsecureSkipVerify to be true")
	}
}

func TestNewClientTLSConfigCANotFound(t *testing.T) {
	cfg := TLSConfig{
		CAFile: "/nonexistent/ca.pem",
	}

	_, err := NewClientTLSConfig(cfg)
	if err == nil {
		t.Error("Expected error for non-existent CA file")
	}
}

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

package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"flymq/internal/crypto"
)

func TestNewEncryptedStore(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "encrypted.store"))
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer f.Close()

	// 64 hex chars = 32 bytes for AES-256
	key := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	encryptor, err := crypto.NewEncryptor(key)
	if err != nil {
		t.Fatalf("Failed to create encryptor: %v", err)
	}

	store, err := NewEncryptedStore(f, encryptor)
	if err != nil {
		t.Fatalf("NewEncryptedStore failed: %v", err)
	}
	if store == nil {
		t.Fatal("Expected non-nil store")
	}
}

func TestEncryptedStoreAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "encrypted.store"))
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer f.Close()

	key := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	encryptor, _ := crypto.NewEncryptor(key)
	store, _ := NewEncryptedStore(f, encryptor)

	// Write data
	original := []byte("sensitive data that should be encrypted")
	_, pos, err := store.Append(original)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Read data back
	decrypted, err := store.Read(pos)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !bytes.Equal(original, decrypted) {
		t.Error("Decrypted data does not match original")
	}
}

func TestEncryptedStoreMultipleMessages(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "encrypted.store"))
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer f.Close()

	key := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	encryptor, _ := crypto.NewEncryptor(key)
	store, _ := NewEncryptedStore(f, encryptor)

	messages := [][]byte{
		[]byte("message 1"),
		[]byte("message 2"),
		[]byte("message 3"),
	}

	positions := make([]uint64, len(messages))
	for i, msg := range messages {
		_, pos, err := store.Append(msg)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
		positions[i] = pos
	}

	// Read all messages back
	for i, pos := range positions {
		decrypted, err := store.Read(pos)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if !bytes.Equal(messages[i], decrypted) {
			t.Errorf("Message %d mismatch", i)
		}
	}
}

func TestStoreFactoryNoEncryption(t *testing.T) {
	factory, err := NewStoreFactory("")
	if err != nil {
		t.Fatalf("NewStoreFactory failed: %v", err)
	}

	if factory.IsEncrypted() {
		t.Error("Expected IsEncrypted to be false")
	}

	dir := t.TempDir()
	f, _ := os.Create(filepath.Join(dir, "plain.store"))
	defer f.Close()

	store, err := factory.CreateStore(f)
	if err != nil {
		t.Fatalf("CreateStore failed: %v", err)
	}

	// Should be a regular Store, not EncryptedStore
	_, isEncrypted := store.(*EncryptedStore)
	if isEncrypted {
		t.Error("Expected regular Store, got EncryptedStore")
	}
}

func TestStoreFactoryWithEncryption(t *testing.T) {
	key := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	factory, err := NewStoreFactory(key)
	if err != nil {
		t.Fatalf("NewStoreFactory failed: %v", err)
	}

	if !factory.IsEncrypted() {
		t.Error("Expected IsEncrypted to be true")
	}

	dir := t.TempDir()
	f, _ := os.Create(filepath.Join(dir, "encrypted.store"))
	defer f.Close()

	store, err := factory.CreateStore(f)
	if err != nil {
		t.Fatalf("CreateStore failed: %v", err)
	}

	_, isEncrypted := store.(*EncryptedStore)
	if !isEncrypted {
		t.Error("Expected EncryptedStore")
	}
}

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

/*
Encrypted Storage for FlyMQ.

OVERVIEW:
=========
Wraps the standard Store with transparent encryption/decryption.
Data is encrypted before writing to disk and decrypted when read.

ENCRYPTION:
===========
- Algorithm: AES-256-GCM (authenticated encryption)
- Each message gets a unique nonce
- Key rotation supported via key versioning

DATA FORMAT:
============

	+--------+--------+--------+--------+
	| Key Version (1 byte)              |
	+--------+--------+--------+--------+
	| Nonce (12 bytes)                  |
	+--------+--------+--------+--------+
	| Ciphertext (variable)             |
	+--------+--------+--------+--------+
	| Auth Tag (16 bytes)               |
	+--------+--------+--------+--------+

USE CASES:
==========
- Compliance requirements (HIPAA, PCI-DSS)
- Multi-tenant data isolation
- Sensitive data protection
*/
package storage

import (
	"os"

	"flymq/internal/crypto"
)

// EncryptedStore wraps a Store and provides transparent encryption/decryption.
// Data is encrypted before being written to disk and decrypted when read.
type EncryptedStore struct {
	*Store
	encryptor *crypto.Encryptor
}

// NewEncryptedStore creates a new encrypted store wrapper.
func NewEncryptedStore(f *os.File, encryptor *crypto.Encryptor) (*EncryptedStore, error) {
	store, err := NewStore(f)
	if err != nil {
		return nil, err
	}
	return &EncryptedStore{
		Store:     store,
		encryptor: encryptor,
	}, nil
}

// Append encrypts the data and appends it to the store.
func (s *EncryptedStore) Append(p []byte) (n uint64, pos uint64, err error) {
	encrypted, err := s.encryptor.Encrypt(p)
	if err != nil {
		return 0, 0, err
	}
	return s.Store.Append(encrypted)
}

// Read reads and decrypts data from the store.
func (s *EncryptedStore) Read(pos uint64) ([]byte, error) {
	encrypted, err := s.Store.Read(pos)
	if err != nil {
		return nil, err
	}
	return s.encryptor.Decrypt(encrypted)
}

// StoreFactory creates stores with optional encryption.
type StoreFactory struct {
	encryptor *crypto.Encryptor
}

// NewStoreFactory creates a new store factory.
// If encryptionKey is empty, stores will not be encrypted.
func NewStoreFactory(encryptionKey string) (*StoreFactory, error) {
	if encryptionKey == "" {
		return &StoreFactory{}, nil
	}

	encryptor, err := crypto.NewEncryptor(encryptionKey)
	if err != nil {
		return nil, err
	}

	return &StoreFactory{
		encryptor: encryptor,
	}, nil
}

// IsEncrypted returns true if the factory creates encrypted stores.
func (f *StoreFactory) IsEncrypted() bool {
	return f.encryptor != nil
}

// CreateStore creates a new store, optionally encrypted.
func (f *StoreFactory) CreateStore(file *os.File) (StoreInterface, error) {
	if f.encryptor != nil {
		return NewEncryptedStore(file, f.encryptor)
	}
	return NewStore(file)
}

// StoreInterface defines the interface for store operations.
// Both Store and EncryptedStore implement this interface.
type StoreInterface interface {
	Append(p []byte) (n uint64, pos uint64, err error)
	Read(pos uint64) ([]byte, error)
	ReadAt(p []byte, off int64) (int, error)
	Close() error
	Name() string
	Size() uint64
}

// Size returns the current size of the store.
func (s *Store) Size() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Size returns the current size of the underlying store.
func (s *EncryptedStore) Size() uint64 {
	return s.Store.Size()
}

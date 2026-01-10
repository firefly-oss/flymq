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
	"sync"
	"testing"
)

func TestNewStore(t *testing.T) {
	dir := t.TempDir()
	f, err := os.OpenFile(filepath.Join(dir, "test.store"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer f.Close()

	store, err := NewStore(f)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}

	if store.File != f {
		t.Error("Store file mismatch")
	}
	if store.size != 0 {
		t.Errorf("Expected size 0, got %d", store.size)
	}
}

func TestStoreAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.OpenFile(filepath.Join(dir, "test.store"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	store, _ := NewStore(f)

	testCases := [][]byte{
		[]byte("Hello, World!"),
		[]byte(""),
		[]byte("A"),
		[]byte("This is a longer message with more content"),
		bytes.Repeat([]byte("x"), 1024),
	}

	positions := make([]uint64, len(testCases))
	for i, data := range testCases {
		n, pos, err := store.Append(data)
		if err != nil {
			t.Fatalf("Append failed for case %d: %v", i, err)
		}
		positions[i] = pos
		expectedN := uint64(len(data) + lenWidth)
		if n != expectedN {
			t.Errorf("Case %d: Expected n=%d, got %d", i, expectedN, n)
		}
	}

	// Read back and verify
	for i, data := range testCases {
		read, err := store.Read(positions[i])
		if err != nil {
			t.Fatalf("Read failed for case %d: %v", i, err)
		}
		if !bytes.Equal(data, read) {
			t.Errorf("Case %d: Data mismatch", i)
		}
	}
}

func TestStoreReadAt(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.OpenFile(filepath.Join(dir, "test.store"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	store, _ := NewStore(f)

	data := []byte("Test data for ReadAt")
	_, pos, _ := store.Append(data)

	// Read the length prefix
	lenBuf := make([]byte, lenWidth)
	n, err := store.ReadAt(lenBuf, int64(pos))
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if n != lenWidth {
		t.Errorf("Expected to read %d bytes, got %d", lenWidth, n)
	}

	length := enc.Uint64(lenBuf)
	if length != uint64(len(data)) {
		t.Errorf("Expected length %d, got %d", len(data), length)
	}
}

func TestStoreConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.OpenFile(filepath.Join(dir, "test.store"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	store, _ := NewStore(f)

	var wg sync.WaitGroup
	numGoroutines := 10
	numAppends := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numAppends; j++ {
				data := []byte("message from goroutine")
				_, _, err := store.Append(data)
				if err != nil {
					t.Errorf("Goroutine %d: Append failed: %v", id, err)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestStoreClose(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.OpenFile(filepath.Join(dir, "test.store"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)

	store, _ := NewStore(f)
	store.Append([]byte("test data"))

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestStoreName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.store")
	f, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	defer f.Close()

	store, _ := NewStore(f)
	if store.Name() != path {
		t.Errorf("Expected name %s, got %s", path, store.Name())
	}
}

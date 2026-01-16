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
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestNewIndex(t *testing.T) {
	dir := t.TempDir()
	f, err := os.OpenFile(filepath.Join(dir, "test.index"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer f.Close()

	cfg := Config{
		Segment: SegmentConfig{
			MaxIndexBytes: 1024,
		},
	}

	idx, err := NewIndex(f, cfg)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	defer idx.Close()

	if idx.size != 0 {
		t.Errorf("Expected size 0, got %d", idx.size)
	}
}

func TestIndexWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.OpenFile(filepath.Join(dir, "test.index"), os.O_RDWR|os.O_CREATE, 0644)
	defer f.Close()

	cfg := Config{
		Segment: SegmentConfig{
			MaxIndexBytes: 1024,
		},
	}

	idx, _ := NewIndex(f, cfg)
	defer idx.Close()

	testCases := []struct {
		off uint32
		pos uint64
	}{
		{0, 0},
		{1, 100},
		{2, 250},
		{10, 1000},
		{100, 50000},
	}

	for i, tc := range testCases {
		if err := idx.Write(tc.off, tc.pos); err != nil {
			t.Fatalf("Write failed for case %d: %v", i, err)
		}
	}

	// Read back and verify
	for i, tc := range testCases {
		off, pos, err := idx.Read(int64(i))
		if err != nil {
			t.Fatalf("Read failed for case %d: %v", i, err)
		}
		if off != tc.off {
			t.Errorf("Case %d: Expected offset %d, got %d", i, tc.off, off)
		}
		if pos != tc.pos {
			t.Errorf("Case %d: Expected position %d, got %d", i, tc.pos, pos)
		}
	}
}

func TestIndexReadLast(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.OpenFile(filepath.Join(dir, "test.index"), os.O_RDWR|os.O_CREATE, 0644)
	defer f.Close()

	cfg := Config{
		Segment: SegmentConfig{
			MaxIndexBytes: 1024,
		},
	}

	idx, _ := NewIndex(f, cfg)
	defer idx.Close()

	// Write some entries
	idx.Write(0, 0)
	idx.Write(1, 100)
	idx.Write(2, 200)

	// Read last entry using -1
	off, pos, err := idx.Read(-1)
	if err != nil {
		t.Fatalf("Read(-1) failed: %v", err)
	}
	if off != 2 {
		t.Errorf("Expected offset 2, got %d", off)
	}
	if pos != 200 {
		t.Errorf("Expected position 200, got %d", pos)
	}
}

func TestIndexReadEmpty(t *testing.T) {
	dir := t.TempDir()
	f, _ := os.OpenFile(filepath.Join(dir, "test.index"), os.O_RDWR|os.O_CREATE, 0644)
	defer f.Close()

	cfg := Config{
		Segment: SegmentConfig{
			MaxIndexBytes: 1024,
		},
	}

	idx, _ := NewIndex(f, cfg)
	defer idx.Close()

	_, _, err := idx.Read(0)
	if err != io.EOF {
		t.Errorf("Expected io.EOF, got %v", err)
	}

	_, _, err = idx.Read(-1)
	if err != io.EOF {
		t.Errorf("Expected io.EOF for Read(-1), got %v", err)
	}
}

func TestIndexName(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")
	f, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	defer f.Close()

	cfg := Config{Segment: SegmentConfig{MaxIndexBytes: 1024}}
	idx, _ := NewIndex(f, cfg)
	defer idx.Close()

	if idx.Name() != path {
		t.Errorf("Expected name %s, got %s", path, idx.Name())
	}
}

// TestIndexCrashRecovery tests that the index correctly recovers after a crash
// where the file was left with pre-allocated size (MaxIndexBytes) instead of
// being truncated to the actual data size.
func TestIndexCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")
	cfg := Config{Segment: SegmentConfig{MaxIndexBytes: 1024}}

	// Step 1: Create index and write some entries
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	idx, err := NewIndex(f, cfg)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	// Write 5 entries
	for i := uint32(0); i < 5; i++ {
		if err := idx.Write(i, uint64(i*100)); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	expectedSize := idx.size // Should be 5 * 12 = 60 bytes

	// Step 2: Simulate crash - close without proper cleanup
	// The file will remain at MaxIndexBytes (1024) instead of being truncated
	idx.mmap.Sync(0) // Sync data but don't truncate
	f.Close()

	// Verify file is at MaxIndexBytes (simulating crash state)
	fi, _ := os.Stat(path)
	if fi.Size() != int64(cfg.Segment.MaxIndexBytes) {
		t.Fatalf("Expected file size %d, got %d", cfg.Segment.MaxIndexBytes, fi.Size())
	}

	// Step 3: Reopen index - should recover actual data size
	f2, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to reopen file: %v", err)
	}
	defer f2.Close()

	idx2, err := NewIndex(f2, cfg)
	if err != nil {
		t.Fatalf("NewIndex on recovery failed: %v", err)
	}
	defer idx2.Close()

	// Verify recovered size matches original
	if idx2.size != expectedSize {
		t.Errorf("Expected recovered size %d, got %d", expectedSize, idx2.size)
	}

	// Verify we can read all entries
	for i := int64(0); i < 5; i++ {
		off, pos, err := idx2.Read(i)
		if err != nil {
			t.Fatalf("Read failed after recovery: %v", err)
		}
		if off != uint32(i) {
			t.Errorf("Expected offset %d, got %d", i, off)
		}
		if pos != uint64(i*100) {
			t.Errorf("Expected position %d, got %d", i*100, pos)
		}
	}

	// Verify we can write new entries after recovery
	if err := idx2.Write(5, 500); err != nil {
		t.Fatalf("Write after recovery failed: %v", err)
	}

	off, pos, err := idx2.Read(5)
	if err != nil {
		t.Fatalf("Read new entry failed: %v", err)
	}
	if off != 5 || pos != 500 {
		t.Errorf("New entry mismatch: off=%d, pos=%d", off, pos)
	}
}

// TestIndexCrashRecoveryEmpty tests recovery when the index was empty before crash
func TestIndexCrashRecoveryEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.index")
	cfg := Config{Segment: SegmentConfig{MaxIndexBytes: 1024}}

	// Create empty index
	f, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	idx, _ := NewIndex(f, cfg)
	idx.mmap.Sync(0)
	f.Close()

	// Reopen - should recover as empty
	f2, _ := os.OpenFile(path, os.O_RDWR, 0644)
	defer f2.Close()

	idx2, err := NewIndex(f2, cfg)
	if err != nil {
		t.Fatalf("NewIndex on recovery failed: %v", err)
	}
	defer idx2.Close()

	if idx2.size != 0 {
		t.Errorf("Expected size 0 after recovery, got %d", idx2.size)
	}

	// Should be able to write
	if err := idx2.Write(0, 0); err != nil {
		t.Fatalf("Write after recovery failed: %v", err)
	}
}

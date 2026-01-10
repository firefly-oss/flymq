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

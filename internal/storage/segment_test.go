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
)

func TestNewSegment(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 1024,
			MaxIndexBytes: 1024,
			InitialOffset: 0,
		},
	}

	seg, err := NewSegment(dir, 0, cfg)
	if err != nil {
		t.Fatalf("NewSegment failed: %v", err)
	}
	defer seg.Close()

	if seg.baseOffset != 0 {
		t.Errorf("Expected baseOffset 0, got %d", seg.baseOffset)
	}
	if seg.nextOffset != 0 {
		t.Errorf("Expected nextOffset 0, got %d", seg.nextOffset)
	}

	// Check files were created
	if _, err := os.Stat(filepath.Join(dir, "0.store")); os.IsNotExist(err) {
		t.Error("Store file not created")
	}
	if _, err := os.Stat(filepath.Join(dir, "0.index")); os.IsNotExist(err) {
		t.Error("Index file not created")
	}
}

func TestSegmentAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 4096,
			MaxIndexBytes: 1024,
			InitialOffset: 0,
		},
	}

	seg, _ := NewSegment(dir, 0, cfg)
	defer seg.Close()

	testData := [][]byte{
		[]byte("First message"),
		[]byte("Second message"),
		[]byte("Third message"),
	}

	offsets := make([]uint64, len(testData))
	for i, data := range testData {
		off, err := seg.Append(data)
		if err != nil {
			t.Fatalf("Append failed for message %d: %v", i, err)
		}
		offsets[i] = off
		if off != uint64(i) {
			t.Errorf("Expected offset %d, got %d", i, off)
		}
	}

	// Read back and verify
	for i, data := range testData {
		read, err := seg.Read(offsets[i])
		if err != nil {
			t.Fatalf("Read failed for offset %d: %v", offsets[i], err)
		}
		if !bytes.Equal(data, read) {
			t.Errorf("Data mismatch at offset %d", offsets[i])
		}
	}
}

func TestSegmentWithBaseOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 4096,
			MaxIndexBytes: 1024,
		},
	}

	baseOffset := uint64(1000)
	seg, _ := NewSegment(dir, baseOffset, cfg)
	defer seg.Close()

	data := []byte("Test message")
	off, err := seg.Append(data)
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if off != baseOffset {
		t.Errorf("Expected offset %d, got %d", baseOffset, off)
	}

	read, err := seg.Read(baseOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(data, read) {
		t.Error("Data mismatch")
	}
}

func TestSegmentIsMaxed(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 100, // Very small for testing
			MaxIndexBytes: 1024,
		},
	}

	seg, _ := NewSegment(dir, 0, cfg)
	defer seg.Close()

	if seg.IsMaxed() {
		t.Error("New segment should not be maxed")
	}

	// Fill up the segment
	for !seg.IsMaxed() {
		_, err := seg.Append([]byte("test data to fill segment"))
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if !seg.IsMaxed() {
		t.Error("Segment should be maxed after filling")
	}
}

func TestSegmentRemove(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 1024,
			MaxIndexBytes: 1024,
		},
	}

	seg, _ := NewSegment(dir, 0, cfg)
	seg.Append([]byte("test"))

	if err := seg.Remove(); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Check files were deleted
	if _, err := os.Stat(filepath.Join(dir, "0.store")); !os.IsNotExist(err) {
		t.Error("Store file should be deleted")
	}
	if _, err := os.Stat(filepath.Join(dir, "0.index")); !os.IsNotExist(err) {
		t.Error("Index file should be deleted")
	}
}

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
	"io"
	"sync"
	"testing"
)

func TestNewLog(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 1024,
			MaxIndexBytes: 1024,
			InitialOffset: 0,
		},
	}

	log, err := NewLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	defer log.Close()

	if log.Dir != dir {
		t.Errorf("Expected dir %s, got %s", dir, log.Dir)
	}
	if len(log.segments) != 1 {
		t.Errorf("Expected 1 segment, got %d", len(log.segments))
	}
}

func TestLogAppendAndRead(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 4096,
			MaxIndexBytes: 1024,
		},
	}

	log, _ := NewLog(dir, cfg)
	defer log.Close()

	testData := [][]byte{
		[]byte("Message 1"),
		[]byte("Message 2"),
		[]byte("Message 3"),
		[]byte("Message 4"),
		[]byte("Message 5"),
	}

	for i, data := range testData {
		off, err := log.Append(data)
		if err != nil {
			t.Fatalf("Append failed for message %d: %v", i, err)
		}
		if off != uint64(i) {
			t.Errorf("Expected offset %d, got %d", i, off)
		}
	}

	// Read back and verify
	for i, data := range testData {
		read, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("Read failed for offset %d: %v", i, err)
		}
		if !bytes.Equal(data, read) {
			t.Errorf("Data mismatch at offset %d", i)
		}
	}
}

func TestLogOffsets(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 4096,
			MaxIndexBytes: 1024,
		},
	}

	log, _ := NewLog(dir, cfg)
	defer log.Close()

	// Append some messages
	for i := 0; i < 5; i++ {
		log.Append([]byte("test message"))
	}

	low, err := log.LowestOffset()
	if err != nil {
		t.Fatalf("LowestOffset failed: %v", err)
	}
	if low != 0 {
		t.Errorf("Expected lowest offset 0, got %d", low)
	}

	high, err := log.HighestOffset()
	if err != nil {
		t.Fatalf("HighestOffset failed: %v", err)
	}
	if high != 4 {
		t.Errorf("Expected highest offset 4, got %d", high)
	}
}

func TestLogReadNonExistent(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 4096,
			MaxIndexBytes: 1024,
		},
	}

	log, _ := NewLog(dir, cfg)
	defer log.Close()

	log.Append([]byte("test"))

	_, err := log.Read(100) // Non-existent offset
	if err != io.EOF {
		t.Errorf("Expected io.EOF, got %v", err)
	}
}

func TestLogConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 1024 * 1024,
			MaxIndexBytes: 1024 * 1024,
		},
	}

	log, _ := NewLog(dir, cfg)
	defer log.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numAppends := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numAppends; j++ {
				_, err := log.Append([]byte("concurrent message"))
				if err != nil {
					t.Errorf("Append failed: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	high, _ := log.HighestOffset()
	expected := uint64(numGoroutines*numAppends - 1)
	if high != expected {
		t.Errorf("Expected highest offset %d, got %d", expected, high)
	}
}

func TestLogSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 100, // Very small to force rotation
			MaxIndexBytes: 1024,
		},
	}

	log, _ := NewLog(dir, cfg)
	defer log.Close()

	// Append enough messages to trigger segment rotation
	for i := 0; i < 20; i++ {
		_, err := log.Append([]byte("message to trigger rotation"))
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if len(log.segments) <= 1 {
		t.Errorf("Expected multiple segments, got %d", len(log.segments))
	}
}

func TestLogTruncate(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 100,
			MaxIndexBytes: 1024,
		},
	}

	log, _ := NewLog(dir, cfg)
	defer log.Close()

	// Append messages to create multiple segments
	for i := 0; i < 20; i++ {
		log.Append([]byte("message for truncation test"))
	}

	initialSegments := len(log.segments)
	if initialSegments <= 1 {
		t.Skip("Need multiple segments for truncation test")
	}

	// Truncate to a higher offset
	high, _ := log.HighestOffset()
	err := log.Truncate(high - 5)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}
}

func TestLogReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Segment: SegmentConfig{
			MaxStoreBytes: 4096,
			MaxIndexBytes: 1024,
		},
	}

	// Create log and append data
	log1, _ := NewLog(dir, cfg)
	for i := 0; i < 5; i++ {
		log1.Append([]byte("persistent message"))
	}
	log1.Close()

	// Reopen log
	log2, err := NewLog(dir, cfg)
	if err != nil {
		t.Fatalf("Failed to reopen log: %v", err)
	}
	defer log2.Close()

	high, _ := log2.HighestOffset()
	if high != 4 {
		t.Errorf("Expected highest offset 4 after reopen, got %d", high)
	}

	// Verify data is still readable
	data, err := log2.Read(0)
	if err != nil {
		t.Fatalf("Read failed after reopen: %v", err)
	}
	if !bytes.Equal(data, []byte("persistent message")) {
		t.Error("Data mismatch after reopen")
	}
}

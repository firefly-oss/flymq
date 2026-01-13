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

package performance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestIOOpTypeConstants(t *testing.T) {
	// Verify constants are distinct
	if IORead == IOWrite || IOWrite == IOSync || IORead == IOSync {
		t.Error("IOOpType constants should be distinct")
	}
}

func TestDefaultAsyncIOConfig(t *testing.T) {
	cfg := DefaultAsyncIOConfig()

	if cfg.NumWorkers != 4 {
		t.Errorf("NumWorkers = %d, want 4", cfg.NumWorkers)
	}
	if cfg.QueueSize != 1000 {
		t.Errorf("QueueSize = %d, want 1000", cfg.QueueSize)
	}
	if cfg.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", cfg.BatchSize)
	}
	if cfg.FlushInterval != 10*time.Millisecond {
		t.Errorf("FlushInterval = %v, want 10ms", cfg.FlushInterval)
	}
}

func TestNewAsyncIOManager(t *testing.T) {
	cfg := AsyncIOConfig{
		NumWorkers:    2,
		QueueSize:     100,
		BatchSize:     10,
		FlushInterval: 5 * time.Millisecond,
	}

	manager := NewAsyncIOManager(cfg)
	if manager == nil {
		t.Fatal("NewAsyncIOManager returned nil")
	}

	if len(manager.workers) != 2 {
		t.Errorf("Expected 2 workers, got %d", len(manager.workers))
	}
}

func TestAsyncIOManagerStartStop(t *testing.T) {
	cfg := AsyncIOConfig{
		NumWorkers:    2,
		QueueSize:     100,
		BatchSize:     10,
		FlushInterval: 5 * time.Millisecond,
	}

	manager := NewAsyncIOManager(cfg)
	manager.Start()

	// Let it run briefly
	time.Sleep(20 * time.Millisecond)

	manager.Stop()
}

func TestAsyncIOManagerWrite(t *testing.T) {
	cfg := AsyncIOConfig{
		NumWorkers:    2,
		QueueSize:     100,
		BatchSize:     10,
		FlushInterval: 5 * time.Millisecond,
	}

	manager := NewAsyncIOManager(cfg)
	manager.Start()
	defer manager.Stop()

	// Create a temp file
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.dat")
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	// Write data
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := []byte("Hello, Async I/O!")
	n, err := manager.Write(ctx, file, 0, data)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if n != len(data) {
		t.Errorf("Wrote %d bytes, expected %d", n, len(data))
	}
}

func TestAsyncIOManagerRead(t *testing.T) {
	cfg := AsyncIOConfig{
		NumWorkers:    2,
		QueueSize:     100,
		BatchSize:     10,
		FlushInterval: 5 * time.Millisecond,
	}

	manager := NewAsyncIOManager(cfg)
	manager.Start()
	defer manager.Stop()

	// Create a temp file with data
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.dat")
	testData := []byte("Test data for reading")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Read data
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := manager.Read(ctx, file, 0, len(testData))
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if string(data) != string(testData) {
		t.Errorf("Read data mismatch: got %s, want %s", data, testData)
	}
}

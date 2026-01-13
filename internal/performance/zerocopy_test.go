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
	"bytes"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewZeroCopyReader(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.dat")
	testData := []byte("Hello, Zero Copy!")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	reader := NewZeroCopyReader(file, 0, int64(len(testData)))
	if reader == nil {
		t.Fatal("NewZeroCopyReader returned nil")
	}

	if reader.file != file {
		t.Error("File not set correctly")
	}
	if reader.offset != 0 {
		t.Errorf("Offset = %d, want 0", reader.offset)
	}
	if reader.length != int64(len(testData)) {
		t.Errorf("Length = %d, want %d", reader.length, len(testData))
	}
}

func TestZeroCopyReaderSendToTCP(t *testing.T) {
	// Create test file
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.dat")
	testData := []byte("Test data for zero-copy transfer over TCP connection")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Create TCP listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	// Channel to receive data
	received := make(chan []byte, 1)
	errCh := make(chan error, 1)

	// Accept connection and read data
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()

		data, err := io.ReadAll(conn)
		if err != nil {
			errCh <- err
			return
		}
		received <- data
	}()

	// Connect and send data
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	reader := NewZeroCopyReader(file, 0, int64(len(testData)))
	n, err := reader.SendTo(conn)
	conn.Close()

	if err != nil {
		t.Fatalf("SendTo failed: %v", err)
	}

	if n != int64(len(testData)) {
		t.Errorf("Sent %d bytes, expected %d", n, len(testData))
	}

	// Wait for received data
	select {
	case data := <-received:
		if !bytes.Equal(data, testData) {
			t.Errorf("Received data mismatch")
		}
	case err := <-errCh:
		t.Fatalf("Receiver error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for data")
	}
}

func TestZeroCopyReaderSendToNonTCP(t *testing.T) {
	// Create test file
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.dat")
	testData := []byte("Test data for non-TCP connection")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Use a pipe (not TCP) - should fall back to regular copy
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	reader := NewZeroCopyReader(file, 0, int64(len(testData)))

	// Read in background
	received := make(chan []byte, 1)
	go func() {
		data := make([]byte, len(testData))
		io.ReadFull(server, data)
		received <- data
	}()

	n, err := reader.SendTo(client)
	if err != nil {
		t.Fatalf("SendTo failed: %v", err)
	}

	if n != int64(len(testData)) {
		t.Errorf("Sent %d bytes, expected %d", n, len(testData))
	}

	select {
	case data := <-received:
		if !bytes.Equal(data, testData) {
			t.Errorf("Received data mismatch")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for data")
	}
}

func TestZeroCopyReaderWithOffset(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.dat")
	testData := []byte("0123456789ABCDEFGHIJ")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Read from offset 10, length 10 (should get "ABCDEFGHIJ")
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	reader := NewZeroCopyReader(file, 10, 10)

	received := make(chan []byte, 1)
	go func() {
		data := make([]byte, 10)
		io.ReadFull(server, data)
		received <- data
	}()

	n, err := reader.SendTo(client)
	if err != nil {
		t.Fatalf("SendTo failed: %v", err)
	}

	if n != 10 {
		t.Errorf("Sent %d bytes, expected 10", n)
	}

	select {
	case data := <-received:
		expected := []byte("ABCDEFGHIJ")
		if !bytes.Equal(data, expected) {
			t.Errorf("Received %s, expected %s", data, expected)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for data")
	}
}

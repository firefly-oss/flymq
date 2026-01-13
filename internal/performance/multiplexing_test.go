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
	"net"
	"testing"
	"time"
)

func TestFrameTypeConstants(t *testing.T) {
	// Verify frame types are distinct
	types := []FrameType{FrameData, FrameWindowUpdate, FrameReset, FramePing, FramePong, FrameGoAway}
	seen := make(map[FrameType]bool)
	for _, ft := range types {
		if seen[ft] {
			t.Errorf("Duplicate frame type: %d", ft)
		}
		seen[ft] = true
	}
}

func TestDefaultMultiplexConfig(t *testing.T) {
	cfg := DefaultMultiplexConfig()

	if cfg.MaxStreams != 1000 {
		t.Errorf("MaxStreams = %d, want 1000", cfg.MaxStreams)
	}
	if cfg.WindowSize != 65535 {
		t.Errorf("WindowSize = %d, want 65535", cfg.WindowSize)
	}
	if cfg.MaxFrameSize != 16384 {
		t.Errorf("MaxFrameSize = %d, want 16384", cfg.MaxFrameSize)
	}
	if cfg.KeepAlive != 30*time.Second {
		t.Errorf("KeepAlive = %v, want 30s", cfg.KeepAlive)
	}
}

func TestFrameHeaderSize(t *testing.T) {
	// Frame header: 1 (type) + 4 (stream ID) + 1 (flags) + 4 (length) = 10
	if FrameHeaderSize != 10 {
		t.Errorf("FrameHeaderSize = %d, want 10", FrameHeaderSize)
	}
}

func TestNewMultiplexedConn(t *testing.T) {
	// Create a pipe for testing
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cfg := DefaultMultiplexConfig()
	mc := NewMultiplexedConn(client, cfg)
	if mc == nil {
		t.Fatal("NewMultiplexedConn returned nil")
	}

	// Clean up
	mc.Close()
}

func TestMultiplexedConnOpenStream(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cfg := DefaultMultiplexConfig()
	mc := NewMultiplexedConn(client, cfg)
	defer mc.Close()

	// Open a stream
	stream, err := mc.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	if stream == nil {
		t.Fatal("OpenStream returned nil stream")
	}

	if stream.id == 0 {
		t.Error("Stream ID should not be 0")
	}
}

func TestMultiplexedConnMultipleStreams(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cfg := DefaultMultiplexConfig()
	mc := NewMultiplexedConn(client, cfg)
	defer mc.Close()

	// Open multiple streams
	streams := make([]*Stream, 5)
	for i := 0; i < 5; i++ {
		stream, err := mc.OpenStream()
		if err != nil {
			t.Fatalf("OpenStream %d failed: %v", i, err)
		}
		streams[i] = stream
	}

	// Verify all streams have unique IDs
	ids := make(map[StreamID]bool)
	for _, s := range streams {
		if ids[s.id] {
			t.Errorf("Duplicate stream ID: %d", s.id)
		}
		ids[s.id] = true
	}
}

func TestMultiplexedConnClose(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cfg := DefaultMultiplexConfig()
	mc := NewMultiplexedConn(client, cfg)

	// Close should not error
	if err := mc.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Second close should be safe
	if err := mc.Close(); err != nil {
		t.Errorf("Second Close failed: %v", err)
	}

	// OpenStream after close should fail
	_, err := mc.OpenStream()
	if err == nil {
		t.Error("Expected error when opening stream on closed connection")
	}
}

func TestStreamID(t *testing.T) {
	var id StreamID = 12345
	if id != 12345 {
		t.Errorf("StreamID value mismatch")
	}
}

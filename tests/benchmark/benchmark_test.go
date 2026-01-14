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

package benchmark

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/protocol"
	"flymq/internal/server"
	"flymq/internal/storage"
	"flymq/pkg/client"
)

// testServer wraps a server instance for benchmarking
type testServer struct {
	server *server.Server
	broker *broker.Broker
	addr   string
}

// newTestServer creates a new test server on a random available port
func newTestServer(b *testing.B) *testServer {
	b.Helper()

	// Find an available port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Failed to find available port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := &config.Config{
		BindAddr:     addr,
		DataDir:      b.TempDir(),
		SegmentBytes: 1024 * 1024,
	}

	br, err := broker.NewBroker(cfg)
	if err != nil {
		b.Fatalf("Failed to create broker: %v", err)
	}

	srv := server.NewServer(cfg, br)
	if err := srv.Start(); err != nil {
		br.Close()
		b.Fatalf("Failed to start server: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	ts := &testServer{
		server: srv,
		broker: br,
		addr:   addr,
	}

	b.Cleanup(func() {
		srv.Stop()
		br.Close()
	})

	return ts
}

// BenchmarkStorageAppend benchmarks the storage append operation
func BenchmarkStorageAppend(b *testing.B) {
	dir := b.TempDir()
	cfg := storage.Config{
		Segment: storage.SegmentConfig{
			MaxStoreBytes: 1024 * 1024 * 100, // 100MB
			MaxIndexBytes: 1024 * 1024,
			InitialOffset: 0,
		},
	}

	log, err := storage.NewLog(dir, cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	data := []byte("benchmark message payload for testing performance")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := log.Append(data)
		if err != nil {
			b.Fatalf("Append failed: %v", err)
		}
	}
}

// BenchmarkStorageRead benchmarks the storage read operation
func BenchmarkStorageRead(b *testing.B) {
	dir := b.TempDir()
	cfg := storage.Config{
		Segment: storage.SegmentConfig{
			MaxStoreBytes: 1024 * 1024 * 100,
			MaxIndexBytes: 1024 * 1024,
			InitialOffset: 0,
		},
	}

	log, err := storage.NewLog(dir, cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	// Pre-populate with messages
	data := []byte("benchmark message payload for testing performance")
	numMessages := 10000
	for i := 0; i < numMessages; i++ {
		log.Append(data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := uint64(i % numMessages)
		_, err := log.Read(offset)
		if err != nil {
			b.Fatalf("Read failed at offset %d: %v", offset, err)
		}
	}
}

// BenchmarkProtocolWrite benchmarks protocol message writing
func BenchmarkProtocolWrite(b *testing.B) {
	payload := []byte(`{"topic":"benchmark-topic","data":"YmVuY2htYXJrIG1lc3NhZ2UgcGF5bG9hZCBmb3IgdGVzdGluZyBwZXJmb3JtYW5jZQ=="}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		err := protocol.WriteMessage(&buf, protocol.OpProduce, payload)
		if err != nil {
			b.Fatalf("WriteMessage failed: %v", err)
		}
	}
}

// BenchmarkProtocolRead benchmarks protocol message reading
func BenchmarkProtocolRead(b *testing.B) {
	payload := []byte(`{"topic":"benchmark-topic","data":"YmVuY2htYXJrIG1lc3NhZ2UgcGF5bG9hZCBmb3IgdGVzdGluZyBwZXJmb3JtYW5jZQ=="}`)
	var buf bytes.Buffer
	protocol.WriteMessage(&buf, protocol.OpProduce, payload)
	encoded := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(encoded)
		_, err := protocol.ReadMessage(reader)
		if err != nil {
			b.Fatalf("ReadMessage failed: %v", err)
		}
	}
}

// BenchmarkClientProduce benchmarks client produce operations
func BenchmarkClientProduce(b *testing.B) {
	ts := newTestServer(b)

	c, err := client.NewClient(ts.addr)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	topic := "benchmark-produce"
	if err := c.CreateTopic(topic, 1); err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	data := []byte("benchmark message payload for testing performance")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Produce(topic, data); err != nil {
			b.Fatalf("Produce failed: %v", err)
		}
	}
}

// BenchmarkClientConsume benchmarks client consume operations
func BenchmarkClientConsume(b *testing.B) {
	ts := newTestServer(b)

	c, err := client.NewClient(ts.addr)
	if err != nil {
		b.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	topic := "benchmark-consume"
	if err := c.CreateTopic(topic, 1); err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	// Pre-populate with messages
	data := []byte("benchmark message payload for testing performance")
	numMessages := b.N
	if numMessages < 1000 {
		numMessages = 1000
	}
	for i := 0; i < numMessages; i++ {
		c.Produce(topic, data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := uint64(i % numMessages)
		_, err := c.Consume(topic, offset)
		if err != nil {
			b.Fatalf("Consume failed at offset %d: %v", offset, err)
		}
	}
}

// BenchmarkBrokerProduce benchmarks broker produce operations directly
func BenchmarkBrokerProduce(b *testing.B) {
	cfg := &config.Config{
		DataDir:      b.TempDir(),
		SegmentBytes: 1024 * 1024 * 100,
	}

	br, err := broker.NewBroker(cfg)
	if err != nil {
		b.Fatalf("Failed to create broker: %v", err)
	}
	defer br.Close()

	topic := "benchmark-broker-produce"
	if err := br.CreateTopic(topic, 1); err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	data := []byte("benchmark message payload for testing performance")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := br.Produce(topic, data)
		if err != nil {
			b.Fatalf("Produce failed: %v", err)
		}
	}
}

// BenchmarkBrokerConsume benchmarks broker consume operations directly
func BenchmarkBrokerConsume(b *testing.B) {
	cfg := &config.Config{
		DataDir:      b.TempDir(),
		SegmentBytes: 1024 * 1024 * 100,
	}

	br, err := broker.NewBroker(cfg)
	if err != nil {
		b.Fatalf("Failed to create broker: %v", err)
	}
	defer br.Close()

	topic := "benchmark-broker-consume"
	if err := br.CreateTopic(topic, 1); err != nil {
		b.Fatalf("Failed to create topic: %v", err)
	}

	// Pre-populate with messages
	data := []byte("benchmark message payload for testing performance")
	numMessages := b.N
	if numMessages < 1000 {
		numMessages = 1000
	}
	for i := 0; i < numMessages; i++ {
		br.Produce(topic, data)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := uint64(i % numMessages)
		_, err := br.Consume(topic, offset)
		if err != nil {
			b.Fatalf("Consume failed at offset %d: %v", offset, err)
		}
	}
}

// BenchmarkStorageAppendParallel benchmarks parallel storage append operations
func BenchmarkStorageAppendParallel(b *testing.B) {
	dir := b.TempDir()
	cfg := storage.Config{
		Segment: storage.SegmentConfig{
			MaxStoreBytes: 1024 * 1024 * 100,
			MaxIndexBytes: 1024 * 1024,
			InitialOffset: 0,
		},
	}

	log, err := storage.NewLog(dir, cfg)
	if err != nil {
		b.Fatalf("Failed to create log: %v", err)
	}
	defer log.Close()

	data := []byte("benchmark message payload for testing performance")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := log.Append(data)
			if err != nil {
				b.Errorf("Append failed: %v", err)
			}
		}
	})
}

// BenchmarkMessageSizes benchmarks produce with different message sizes
func BenchmarkMessageSizes(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096, 16384}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			cfg := &config.Config{
				DataDir:      b.TempDir(),
				SegmentBytes: 1024 * 1024 * 100,
			}

			br, err := broker.NewBroker(cfg)
			if err != nil {
				b.Fatalf("Failed to create broker: %v", err)
			}
			defer br.Close()

			topic := fmt.Sprintf("benchmark-size-%d", size)
			if err := br.CreateTopic(topic, 1); err != nil {
				b.Fatalf("Failed to create topic: %v", err)
			}

			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := br.Produce(topic, data)
				if err != nil {
					b.Fatalf("Produce failed: %v", err)
				}
			}
		})
	}
}

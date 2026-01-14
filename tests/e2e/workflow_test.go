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

package e2e

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/server"
	"flymq/pkg/client"
)

// testServer wraps a server instance for testing
type testServer struct {
	server *server.Server
	broker *broker.Broker
	addr   string
}

// newTestServer creates a new test server on a random available port
func newTestServer(t *testing.T) *testServer {
	t.Helper()

	// Find an available port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cfg := &config.Config{
		BindAddr:     addr,
		DataDir:      t.TempDir(),
		SegmentBytes: 1024 * 1024,
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	srv := server.NewServer(cfg, b)
	if err := srv.Start(); err != nil {
		b.Close()
		t.Fatalf("Failed to start server: %v", err)
	}

	// Give server time to start accepting connections
	time.Sleep(50 * time.Millisecond)

	ts := &testServer{
		server: srv,
		broker: b,
		addr:   addr,
	}

	t.Cleanup(func() {
		srv.Stop()
		b.Close()
	})

	return ts
}

// TestProducerConsumerWorkflow tests a complete producer-consumer workflow
func TestProducerConsumerWorkflow(t *testing.T) {
	ts := newTestServer(t)

	// Create producer client
	producer, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create producer client: %v", err)
	}
	defer producer.Close()

	// Create consumer client
	consumer, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create consumer client: %v", err)
	}
	defer consumer.Close()

	topic := "workflow-topic"
	numMessages := 100

	// Producer sends messages
	for i := 0; i < numMessages; i++ {
		msg := fmt.Sprintf(`{"id": %d, "data": "message-%d"}`, i, i)
		_, err := producer.Produce(topic, []byte(msg))
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
	}

	// Consumer fetches all messages
	messages, nextOffset, err := consumer.Fetch(topic, 0, 0, numMessages)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	if len(messages) != numMessages {
		t.Errorf("Expected %d messages, got %d", numMessages, len(messages))
	}

	if nextOffset != uint64(numMessages) {
		t.Errorf("Expected next offset %d, got %d", numMessages, nextOffset)
	}
}

// TestConcurrentProducersConsumers tests multiple producers and consumers
func TestConcurrentProducersConsumers(t *testing.T) {
	ts := newTestServer(t)

	topic := "concurrent-topic"
	numProducers := 5
	numMessagesPerProducer := 20
	totalMessages := numProducers * numMessagesPerProducer

	// Pre-create the topic to avoid race conditions
	setupClient, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create setup client: %v", err)
	}
	if err := setupClient.CreateTopic(topic, 1); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	setupClient.Close()

	var wg sync.WaitGroup

	// Start producers
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()

			c, err := client.NewClient(ts.addr)
			if err != nil {
				t.Errorf("Producer %d failed to connect: %v", producerID, err)
				return
			}
			defer c.Close()

			for i := 0; i < numMessagesPerProducer; i++ {
				msg := fmt.Sprintf("producer-%d-msg-%d", producerID, i)
				_, err := c.Produce(topic, []byte(msg))
				if err != nil {
					t.Errorf("Producer %d failed to produce message %d: %v", producerID, i, err)
					return
				}
			}
		}(p)
	}

	wg.Wait()

	// Verify all messages were produced
	consumer, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	messages, _, err := consumer.Fetch(topic, 0, 0, totalMessages+10)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	if len(messages) != totalMessages {
		t.Errorf("Expected %d messages, got %d", totalMessages, len(messages))
	}
}

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

package integration

import (
	"fmt"
	"net"
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

func TestClientServerProduceConsume(t *testing.T) {
	ts := newTestServer(t)

	c, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Produce a message
	topic := "test-topic"
	message := []byte("Hello, FlyMQ!")

	meta, err := c.Produce(topic, message)
	if err != nil {
		t.Fatalf("Failed to produce message: %v", err)
	}

	if meta.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", meta.Offset)
	}
	if meta.Topic != topic {
		t.Errorf("Expected topic %s, got %s", topic, meta.Topic)
	}

	// Consume the message
	messages, nextOffset, err := c.Fetch(topic, 0, 0, 10)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	if string(messages[0]) != string(message) {
		t.Errorf("Expected message '%s', got '%s'", message, messages[0])
	}

	if nextOffset != 1 {
		t.Errorf("Expected next offset 1, got %d", nextOffset)
	}
}

func TestClientServerMultipleMessages(t *testing.T) {
	ts := newTestServer(t)

	c, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	topic := "multi-msg-topic"
	numMessages := 10

	// Produce multiple messages
	for i := 0; i < numMessages; i++ {
		msg := fmt.Sprintf("Message %d", i)
		meta, err := c.Produce(topic, []byte(msg))
		if err != nil {
			t.Fatalf("Failed to produce message %d: %v", i, err)
		}
		if meta.Offset != uint64(i) {
			t.Errorf("Expected offset %d, got %d", i, meta.Offset)
		}
	}

	// Fetch all messages
	messages, nextOffset, err := c.Fetch(topic, 0, 0, numMessages)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	if len(messages) != numMessages {
		t.Fatalf("Expected %d messages, got %d", numMessages, len(messages))
	}

	if nextOffset != uint64(numMessages) {
		t.Errorf("Expected next offset %d, got %d", numMessages, nextOffset)
	}
}

func TestClientServerTopicManagement(t *testing.T) {
	ts := newTestServer(t)

	c, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Create a topic
	topicName := "managed-topic"
	err = c.CreateTopic(topicName, 3)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// List topics
	topics, err := c.ListTopics()
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}

	found := false
	for _, topic := range topics {
		if topic == topicName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find topic '%s' in list %v", topicName, topics)
	}

	// Delete the topic
	err = c.DeleteTopic(topicName)
	if err != nil {
		t.Fatalf("Failed to delete topic: %v", err)
	}

	// Verify topic is deleted
	topics, err = c.ListTopics()
	if err != nil {
		t.Fatalf("Failed to list topics after delete: %v", err)
	}

	for _, topic := range topics {
		if topic == topicName {
			t.Errorf("Topic '%s' should have been deleted", topicName)
		}
	}
}

func TestClientServerMultipleClients(t *testing.T) {
	ts := newTestServer(t)

	// Create multiple clients
	numClients := 3
	clients := make([]*client.Client, numClients)
	for i := 0; i < numClients; i++ {
		c, err := client.NewClient(ts.addr)
		if err != nil {
			t.Fatalf("Failed to create client %d: %v", i, err)
		}
		clients[i] = c
		defer c.Close()
	}

	topic := "multi-client-topic"

	// Each client produces a message
	for i, c := range clients {
		msg := fmt.Sprintf("Client %d message", i)
		_, err := c.Produce(topic, []byte(msg))
		if err != nil {
			t.Fatalf("Client %d failed to produce: %v", i, err)
		}
	}

	// Any client can consume all messages
	messages, _, err := clients[0].Fetch(topic, 0, 0, numClients)
	if err != nil {
		t.Fatalf("Failed to fetch messages: %v", err)
	}

	if len(messages) != numClients {
		t.Errorf("Expected %d messages, got %d", numClients, len(messages))
	}
}

func TestClientServerLargeMessage(t *testing.T) {
	ts := newTestServer(t)

	c, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	topic := "large-msg-topic"

	// Create a large message (100KB)
	largeMsg := make([]byte, 100*1024)
	for i := range largeMsg {
		largeMsg[i] = byte(i % 256)
	}

	meta, err := c.Produce(topic, largeMsg)
	if err != nil {
		t.Fatalf("Failed to produce large message: %v", err)
	}

	if meta.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", meta.Offset)
	}

	// Fetch the large message
	messages, _, err := c.Fetch(topic, 0, 0, 1)
	if err != nil {
		t.Fatalf("Failed to fetch large message: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	if len(messages[0]) != len(largeMsg) {
		t.Errorf("Expected message size %d, got %d", len(largeMsg), len(messages[0]))
	}
}

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

package client

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"flymq/internal/protocol"
)

// mockServer creates a mock server for testing
func mockServer(t *testing.T, handler func(net.Conn)) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn)
	}()

	return listener
}

func TestNewClient(t *testing.T) {
	listener := mockServer(t, func(conn net.Conn) {
		// Just accept the connection
	})
	defer listener.Close()

	client, err := NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	if client.IsTLS() {
		t.Error("Expected non-TLS client")
	}
}

func TestClientProduce(t *testing.T) {
	listener := mockServer(t, func(conn net.Conn) {
		// Read the produce request
		msg, err := protocol.ReadMessage(conn)
		if err != nil {
			t.Errorf("Failed to read message: %v", err)
			return
		}

		if msg.Header.Op != protocol.OpProduce {
			t.Errorf("Expected OpProduce, got %d", msg.Header.Op)
		}

		// Send binary response
		resp := protocol.EncodeBinaryProduceResponse(&protocol.BinaryProduceResponse{
			Offset:    42,
			Partition: 0,
		})
		protocol.WriteBinaryMessage(conn, protocol.OpProduce, resp)
	})
	defer listener.Close()

	client, err := NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	offset, err := client.Produce("test-topic", []byte("hello"))
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	if offset != 42 {
		t.Errorf("Expected offset 42, got %d", offset)
	}
}

func TestClientConsume(t *testing.T) {
	listener := mockServer(t, func(conn net.Conn) {
		msg, err := protocol.ReadMessage(conn)
		if err != nil {
			t.Errorf("Failed to read message: %v", err)
			return
		}

		if msg.Header.Op != protocol.OpConsume {
			t.Errorf("Expected OpConsume, got %d", msg.Header.Op)
		}

		// Send binary response
		resp := protocol.EncodeBinaryConsumeResponse(&protocol.BinaryConsumeResponse{
			Key:   nil,
			Value: []byte("world"),
		})
		protocol.WriteBinaryMessage(conn, protocol.OpConsume, resp)
	})
	defer listener.Close()

	client, err := NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	data, err := client.Consume("test-topic", 0)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}

	if string(data) != "world" {
		t.Errorf("Expected 'world', got '%s'", string(data))
	}
}

func TestClientCreateTopic(t *testing.T) {
	listener := mockServer(t, func(conn net.Conn) {
		msg, err := protocol.ReadMessage(conn)
		if err != nil {
			t.Errorf("Failed to read message: %v", err)
			return
		}

		if msg.Header.Op != protocol.OpCreateTopic {
			t.Errorf("Expected OpCreateTopic, got %d", msg.Header.Op)
		}

		protocol.WriteMessage(conn, protocol.OpCreateTopic, nil)
	})
	defer listener.Close()

	client, err := NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	err = client.CreateTopic("new-topic", 3)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
}

func TestClientListTopics(t *testing.T) {
	listener := mockServer(t, func(conn net.Conn) {
		msg, err := protocol.ReadMessage(conn)
		if err != nil {
			t.Errorf("Failed to read message: %v", err)
			return
		}

		if msg.Header.Op != protocol.OpListTopics {
			t.Errorf("Expected OpListTopics, got %d", msg.Header.Op)
		}

		// Send binary response
		resp := protocol.EncodeBinaryListTopicsResponse(&protocol.BinaryListTopicsResponse{
			Topics: []string{"topic1", "topic2"},
		})
		protocol.WriteBinaryMessage(conn, protocol.OpListTopics, resp)
	})
	defer listener.Close()

	client, err := NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	topics, err := client.ListTopics()
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}

	if len(topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(topics))
	}
}

func TestClientDeleteTopic(t *testing.T) {
	listener := mockServer(t, func(conn net.Conn) {
		msg, err := protocol.ReadMessage(conn)
		if err != nil {
			t.Errorf("Failed to read message: %v", err)
			return
		}

		if msg.Header.Op != protocol.OpDeleteTopic {
			t.Errorf("Expected OpDeleteTopic, got %d", msg.Header.Op)
		}

		protocol.WriteMessage(conn, protocol.OpDeleteTopic, nil)
	})
	defer listener.Close()

	client, err := NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	err = client.DeleteTopic("old-topic")
	if err != nil {
		t.Fatalf("DeleteTopic failed: %v", err)
	}
}

func TestClientServerError(t *testing.T) {
	listener := mockServer(t, func(conn net.Conn) {
		protocol.ReadMessage(conn)
		protocol.WriteMessage(conn, protocol.OpError, []byte("topic not found"))
	})
	defer listener.Close()

	client, err := NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	_, err = client.Produce("nonexistent", []byte("data"))
	if err == nil {
		t.Error("Expected error for server error response")
	}
}

func TestClientReconnect(t *testing.T) {
	var connCount int32
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(&connCount, 1)
			conn.Close()
		}
	}()

	client, err := NewClient(listener.Addr().String())
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Wait a bit for the first connection to be counted
	time.Sleep(10 * time.Millisecond)

	err = client.Reconnect()
	if err != nil {
		t.Fatalf("Reconnect failed: %v", err)
	}

	// Wait a bit for the second connection to be counted
	time.Sleep(10 * time.Millisecond)

	if atomic.LoadInt32(&connCount) < 2 {
		t.Errorf("Expected at least 2 connections, got %d", atomic.LoadInt32(&connCount))
	}
}

func TestTransactionNotActive(t *testing.T) {
	txn := &Transaction{
		active: false,
	}

	_, err := txn.Produce("topic", []byte("data"))
	if err == nil {
		t.Error("Expected error for inactive transaction")
	}

	err = txn.Commit()
	if err == nil {
		t.Error("Expected error for inactive transaction commit")
	}

	err = txn.Rollback()
	if err == nil {
		t.Error("Expected error for inactive transaction rollback")
	}
}

func TestClientOptions(t *testing.T) {
	opts := ClientOptions{
		TLSEnabled:            true,
		TLSInsecureSkipVerify: true,
	}

	// This will fail to connect but we're testing the options parsing
	_, err := NewClientWithOptions("localhost:99999", opts)
	if err == nil {
		t.Error("Expected connection error")
	}
}

func TestNewTLSClient(t *testing.T) {
	// This will fail to connect but we're testing the function
	_, err := NewTLSClient("localhost:99999", true)
	if err == nil {
		t.Error("Expected connection error")
	}
}

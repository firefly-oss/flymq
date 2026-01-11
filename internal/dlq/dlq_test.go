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

package dlq

import (
	"errors"
	"testing"

	"flymq/internal/config"
)

// mockStore implements MessageStore for testing
type mockStore struct {
	messages map[string][][]byte
}

func newMockStore() *mockStore {
	return &mockStore{
		messages: make(map[string][][]byte),
	}
}

func (m *mockStore) Produce(topic string, data []byte) (uint64, error) {
	m.messages[topic] = append(m.messages[topic], data)
	return uint64(len(m.messages[topic]) - 1), nil
}

func (m *mockStore) Fetch(topic string, partition int, offset uint64, maxMessages int) ([][]byte, uint64, error) {
	msgs := m.messages[topic]
	if offset >= uint64(len(msgs)) {
		return nil, offset, nil
	}
	end := int(offset) + maxMessages
	if end > len(msgs) {
		end = len(msgs)
	}
	return msgs[offset:end], uint64(end), nil
}

func TestNewManager(t *testing.T) {
	cfg := &config.DLQConfig{
		Enabled:     true,
		MaxRetries:  3,
		TopicSuffix: ".dlq",
	}
	store := newMockStore()
	mgr := NewManager(cfg, store)

	if mgr == nil {
		t.Fatal("Expected non-nil manager")
	}
}

func TestGetDLQTopic(t *testing.T) {
	cfg := &config.DLQConfig{TopicSuffix: ".dlq"}
	mgr := NewManager(cfg, newMockStore())

	dlqTopic := mgr.GetDLQTopic("orders")
	if dlqTopic != "orders.dlq" {
		t.Errorf("Expected 'orders.dlq', got %s", dlqTopic)
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	policy := DefaultRetryPolicy()

	if policy.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries 3, got %d", policy.MaxRetries)
	}
	if policy.BackoffFactor != 2.0 {
		t.Errorf("Expected BackoffFactor 2.0, got %f", policy.BackoffFactor)
	}
}

func TestRecordFailure(t *testing.T) {
	cfg := &config.DLQConfig{
		Enabled:     true,
		MaxRetries:  3,
		TopicSuffix: ".dlq",
	}
	store := newMockStore()
	mgr := NewManager(cfg, store)

	testErr := errors.New("processing failed")

	// First failure - should not go to DLQ
	sentToDLQ, err := mgr.RecordFailure("msg-1", "orders", 0, []byte("payload"), testErr)
	if err != nil {
		t.Fatalf("RecordFailure failed: %v", err)
	}
	if sentToDLQ {
		t.Error("Should not be sent to DLQ on first failure")
	}

	// Second failure
	sentToDLQ, _ = mgr.RecordFailure("msg-1", "orders", 0, []byte("payload"), testErr)
	if sentToDLQ {
		t.Error("Should not be sent to DLQ on second failure")
	}

	// Third failure - should go to DLQ
	sentToDLQ, _ = mgr.RecordFailure("msg-1", "orders", 0, []byte("payload"), testErr)
	if !sentToDLQ {
		t.Error("Should be sent to DLQ after max retries")
	}

	// Verify message is in DLQ
	if len(store.messages["orders.dlq"]) != 1 {
		t.Errorf("Expected 1 message in DLQ, got %d", len(store.messages["orders.dlq"]))
	}
}

func TestSendToDLQDirect(t *testing.T) {
	cfg := &config.DLQConfig{
		Enabled:     true,
		TopicSuffix: ".dlq",
	}
	store := newMockStore()
	mgr := NewManager(cfg, store)

	err := mgr.SendToDLQDirect("orders", 5, []byte("failed message"), ReasonValidationFailed, "invalid format")
	if err != nil {
		t.Fatalf("SendToDLQDirect failed: %v", err)
	}

	if len(store.messages["orders.dlq"]) != 1 {
		t.Errorf("Expected 1 message in DLQ, got %d", len(store.messages["orders.dlq"]))
	}
}

func TestGetRetryState(t *testing.T) {
	cfg := &config.DLQConfig{Enabled: true, MaxRetries: 5, TopicSuffix: ".dlq"}
	mgr := NewManager(cfg, newMockStore())

	// No state initially
	_, exists := mgr.GetRetryState("msg-1")
	if exists {
		t.Error("Expected no retry state initially")
	}

	// Record a failure
	mgr.RecordFailure("msg-1", "orders", 0, []byte("payload"), errors.New("error"))

	state, exists := mgr.GetRetryState("msg-1")
	if !exists {
		t.Error("Expected retry state to exist")
	}
	if state.RetryCount != 1 {
		t.Errorf("Expected RetryCount 1, got %d", state.RetryCount)
	}
}

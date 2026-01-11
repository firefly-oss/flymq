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

package transaction

import (
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

func (m *mockStore) ProduceToPartition(topic string, partition int, data []byte) (uint64, error) {
	return m.Produce(topic, data)
}

func (m *mockStore) CommitOffset(topic string, partition int, consumerGroup string, offset uint64) error {
	return nil
}

func TestNewCoordinator(t *testing.T) {
	cfg := &config.TransactionConfig{
		Enabled: true,
		Timeout: 60,
	}
	coord := NewCoordinator(cfg, newMockStore())
	if coord == nil {
		t.Fatal("Expected non-nil coordinator")
	}
}

func TestBegin(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: true, Timeout: 60}
	coord := NewCoordinator(cfg, newMockStore())

	txn, err := coord.Begin("producer-1")
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if txn == nil {
		t.Fatal("Expected non-nil transaction")
	}
	if txn.State != StateActive {
		t.Errorf("Expected state Active, got %s", txn.State)
	}
	if txn.ProducerID != "producer-1" {
		t.Errorf("Expected producer ID 'producer-1', got %s", txn.ProducerID)
	}
}

func TestBeginDisabled(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: false}
	coord := NewCoordinator(cfg, newMockStore())

	_, err := coord.Begin("producer-1")
	if err == nil {
		t.Error("Expected error when transactions are disabled")
	}
}

func TestAddOperation(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: true, Timeout: 60}
	coord := NewCoordinator(cfg, newMockStore())

	txn, _ := coord.Begin("producer-1")

	op := Operation{
		Type:    OpProduce,
		Topic:   "orders",
		Payload: []byte("test message"),
	}

	err := coord.AddOperation(txn.ID, op)
	if err != nil {
		t.Fatalf("AddOperation failed: %v", err)
	}

	txn, _ = coord.Get(txn.ID)
	if len(txn.Operations) != 1 {
		t.Errorf("Expected 1 operation, got %d", len(txn.Operations))
	}
}

func TestPrepare(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: true, Timeout: 60}
	coord := NewCoordinator(cfg, newMockStore())

	txn, _ := coord.Begin("producer-1")
	coord.AddOperation(txn.ID, Operation{
		Type:    OpProduce,
		Topic:   "orders",
		Payload: []byte("test"),
	})

	err := coord.Prepare(txn.ID)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	txn, _ = coord.Get(txn.ID)
	if txn.State != StatePrepared {
		t.Errorf("Expected state Prepared, got %s", txn.State)
	}
}

func TestCommit(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: true, Timeout: 60}
	store := newMockStore()
	coord := NewCoordinator(cfg, store)

	txn, _ := coord.Begin("producer-1")
	coord.AddOperation(txn.ID, Operation{
		Type:    OpProduce,
		Topic:   "orders",
		Payload: []byte("test message"),
	})
	coord.Prepare(txn.ID)

	err := coord.Commit(txn.ID)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	txn, _ = coord.Get(txn.ID)
	if txn.State != StateCommitted {
		t.Errorf("Expected state Committed, got %s", txn.State)
	}

	// Verify message was produced
	if len(store.messages["orders"]) != 1 {
		t.Errorf("Expected 1 message in store, got %d", len(store.messages["orders"]))
	}
}

func TestAbort(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: true, Timeout: 60}
	coord := NewCoordinator(cfg, newMockStore())

	txn, _ := coord.Begin("producer-1")
	coord.AddOperation(txn.ID, Operation{
		Type:    OpProduce,
		Topic:   "orders",
		Payload: []byte("test"),
	})

	err := coord.Abort(txn.ID)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	txn, _ = coord.Get(txn.ID)
	if txn.State != StateAborted {
		t.Errorf("Expected state Aborted, got %s", txn.State)
	}
}

func TestAbortCommittedTransaction(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: true, Timeout: 60}
	coord := NewCoordinator(cfg, newMockStore())

	txn, _ := coord.Begin("producer-1")
	coord.AddOperation(txn.ID, Operation{
		Type:    OpProduce,
		Topic:   "orders",
		Payload: []byte("test"),
	})
	coord.Commit(txn.ID)

	err := coord.Abort(txn.ID)
	if err == nil {
		t.Error("Expected error when aborting committed transaction")
	}
}

func TestGetNonExistent(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: true, Timeout: 60}
	coord := NewCoordinator(cfg, newMockStore())

	_, err := coord.Get("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent transaction")
	}
}

func TestGetByProducer(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: true, Timeout: 60}
	coord := NewCoordinator(cfg, newMockStore())

	coord.Begin("producer-1")
	coord.Begin("producer-1")
	coord.Begin("producer-2")

	txns := coord.GetByProducer("producer-1")
	if len(txns) != 2 {
		t.Errorf("Expected 2 transactions for producer-1, got %d", len(txns))
	}

	txns = coord.GetByProducer("producer-2")
	if len(txns) != 1 {
		t.Errorf("Expected 1 transaction for producer-2, got %d", len(txns))
	}
}

func TestGetStats(t *testing.T) {
	cfg := &config.TransactionConfig{Enabled: true, Timeout: 60}
	coord := NewCoordinator(cfg, newMockStore())

	txn1, _ := coord.Begin("producer-1")
	txn2, _ := coord.Begin("producer-2")
	txn3, _ := coord.Begin("producer-3")

	coord.AddOperation(txn1.ID, Operation{Type: OpProduce, Topic: "t", Payload: []byte("x")})
	coord.Commit(txn1.ID)

	coord.Abort(txn2.ID)

	coord.Prepare(txn3.ID)

	stats := coord.GetStats()
	if stats.CommittedCount != 1 {
		t.Errorf("Expected 1 committed, got %d", stats.CommittedCount)
	}
	if stats.AbortedCount != 1 {
		t.Errorf("Expected 1 aborted, got %d", stats.AbortedCount)
	}
	if stats.PreparedCount != 1 {
		t.Errorf("Expected 1 prepared, got %d", stats.PreparedCount)
	}
}

func TestTransactionStates(t *testing.T) {
	if StateActive != "active" {
		t.Errorf("Expected StateActive 'active', got %s", StateActive)
	}
	if StatePrepared != "prepared" {
		t.Errorf("Expected StatePrepared 'prepared', got %s", StatePrepared)
	}
	if StateCommitted != "committed" {
		t.Errorf("Expected StateCommitted 'committed', got %s", StateCommitted)
	}
	if StateAborted != "aborted" {
		t.Errorf("Expected StateAborted 'aborted', got %s", StateAborted)
	}
}

func TestOperationTypes(t *testing.T) {
	if OpProduce != "produce" {
		t.Errorf("Expected OpProduce 'produce', got %s", OpProduce)
	}
	if OpConsume != "consume" {
		t.Errorf("Expected OpConsume 'consume', got %s", OpConsume)
	}
}

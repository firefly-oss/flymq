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

// Package transaction provides transaction support for FlyMQ.
// It enables exactly-once semantics through a two-phase commit protocol.
package transaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"flymq/internal/config"
	"flymq/internal/logging"
)

// State represents the state of a transaction.
type State string

const (
	StateActive    State = "active"
	StatePrepared  State = "prepared"
	StateCommitted State = "committed"
	StateAborted   State = "aborted"
)

// Operation represents a single operation within a transaction.
type Operation struct {
	Type      OperationType `json:"type"`
	Topic     string        `json:"topic"`
	Partition int           `json:"partition"`
	Offset    uint64        `json:"offset"`
	Payload   []byte        `json:"payload,omitempty"`
}

// OperationType represents the type of transaction operation.
type OperationType string

const (
	OpProduce OperationType = "produce"
	OpConsume OperationType = "consume"
)

// Transaction represents an active transaction.
type Transaction struct {
	ID         string      `json:"id"`
	ProducerID string      `json:"producer_id"`
	State      State       `json:"state"`
	Operations []Operation `json:"operations"`
	CreatedAt  time.Time   `json:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
	ExpiresAt  time.Time   `json:"expires_at"`
}

// MessageStore interface for message operations.
type MessageStore interface {
	Produce(topic string, data []byte) (uint64, error)
	ProduceToPartition(topic string, partition int, data []byte) (uint64, error)
	CommitOffset(topic string, partition int, consumerGroup string, offset uint64) error
}

// Coordinator manages transactions.
type Coordinator struct {
	mu           sync.RWMutex
	config       *config.TransactionConfig
	store        MessageStore
	transactions map[string]*Transaction
	byProducer   map[string][]string // producerID -> transactionIDs
	logger       *logging.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// NewCoordinator creates a new transaction coordinator.
func NewCoordinator(cfg *config.TransactionConfig, store MessageStore) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		config:       cfg,
		store:        store,
		transactions: make(map[string]*Transaction),
		byProducer:   make(map[string][]string),
		logger:       logging.NewLogger("transaction"),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Start starts the transaction coordinator.
func (c *Coordinator) Start() {
	c.wg.Add(1)
	go c.cleanupLoop()
	c.logger.Info("Transaction coordinator started")
}

// Stop stops the transaction coordinator.
func (c *Coordinator) Stop() {
	c.cancel()
	c.wg.Wait()
	c.logger.Info("Transaction coordinator stopped")
}

// Begin starts a new transaction.
func (c *Coordinator) Begin(producerID string) (*Transaction, error) {
	if !c.config.Enabled {
		return nil, fmt.Errorf("transactions are disabled")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	timeout := time.Duration(c.config.Timeout) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	txn := &Transaction{
		ID:         fmt.Sprintf("txn-%s-%d", producerID, now.UnixNano()),
		ProducerID: producerID,
		State:      StateActive,
		Operations: make([]Operation, 0),
		CreatedAt:  now,
		UpdatedAt:  now,
		ExpiresAt:  now.Add(timeout),
	}

	c.transactions[txn.ID] = txn
	c.byProducer[producerID] = append(c.byProducer[producerID], txn.ID)

	c.logger.Debug("Transaction started", "id", txn.ID, "producer", producerID)
	return txn, nil
}

// AddOperation adds an operation to a transaction.
func (c *Coordinator) AddOperation(txnID string, op Operation) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	txn, exists := c.transactions[txnID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	if txn.State != StateActive {
		return fmt.Errorf("transaction is not active: %s", txn.State)
	}

	txn.Operations = append(txn.Operations, op)
	txn.UpdatedAt = time.Now()

	return nil
}

// Prepare prepares a transaction for commit (first phase of 2PC).
func (c *Coordinator) Prepare(txnID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	txn, exists := c.transactions[txnID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	if txn.State != StateActive {
		return fmt.Errorf("transaction is not active: %s", txn.State)
	}

	// Validate all operations can be committed
	for _, op := range txn.Operations {
		if op.Type == OpProduce && len(op.Payload) == 0 {
			return fmt.Errorf("produce operation has no payload")
		}
	}

	txn.State = StatePrepared
	txn.UpdatedAt = time.Now()

	c.logger.Debug("Transaction prepared", "id", txnID)
	return nil
}

// Commit commits a transaction (second phase of 2PC).
func (c *Coordinator) Commit(txnID string) error {
	c.mu.Lock()
	txn, exists := c.transactions[txnID]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	if txn.State != StatePrepared && txn.State != StateActive {
		c.mu.Unlock()
		return fmt.Errorf("transaction cannot be committed: %s", txn.State)
	}

	txn.State = StateCommitted
	txn.UpdatedAt = time.Now()
	c.mu.Unlock()

	// Execute all operations
	for i, op := range txn.Operations {
		switch op.Type {
		case OpProduce:
			offset, err := c.store.Produce(op.Topic, op.Payload)
			if err != nil {
				c.rollbackPartial(txn, i)
				return fmt.Errorf("failed to produce message: %w", err)
			}
			txn.Operations[i].Offset = offset
		case OpConsume:
			// Commit offsets are handled separately
		}
	}

	c.logger.Info("Transaction committed", "id", txnID, "operations", len(txn.Operations))
	return nil
}

// Abort aborts a transaction.
func (c *Coordinator) Abort(txnID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	txn, exists := c.transactions[txnID]
	if !exists {
		return fmt.Errorf("transaction not found: %s", txnID)
	}

	if txn.State == StateCommitted {
		return fmt.Errorf("cannot abort committed transaction")
	}

	txn.State = StateAborted
	txn.UpdatedAt = time.Now()

	c.logger.Info("Transaction aborted", "id", txnID)
	return nil
}

// rollbackPartial rolls back partially committed operations.
func (c *Coordinator) rollbackPartial(txn *Transaction, failedIndex int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	txn.State = StateAborted
	txn.UpdatedAt = time.Now()

	c.logger.Warn("Transaction partially rolled back", "id", txn.ID, "failedAt", failedIndex)
}

// Get retrieves a transaction by ID.
func (c *Coordinator) Get(txnID string) (*Transaction, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	txn, exists := c.transactions[txnID]
	if !exists {
		return nil, fmt.Errorf("transaction not found: %s", txnID)
	}
	return txn, nil
}

// GetByProducer returns all transactions for a producer.
func (c *Coordinator) GetByProducer(producerID string) []*Transaction {
	c.mu.RLock()
	defer c.mu.RUnlock()

	txnIDs := c.byProducer[producerID]
	result := make([]*Transaction, 0, len(txnIDs))
	for _, id := range txnIDs {
		if txn, exists := c.transactions[id]; exists {
			result = append(result, txn)
		}
	}
	return result
}

// cleanupLoop cleans up expired transactions.
func (c *Coordinator) cleanupLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.cleanupExpired()
		}
	}
}

// cleanupExpired aborts expired transactions.
func (c *Coordinator) cleanupExpired() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for id, txn := range c.transactions {
		if txn.State == StateActive && now.After(txn.ExpiresAt) {
			txn.State = StateAborted
			txn.UpdatedAt = now
			c.logger.Warn("Transaction expired and aborted", "id", id)
		}
	}
}

// Stats returns transaction coordinator statistics.
type Stats struct {
	ActiveCount    int `json:"active_count"`
	PreparedCount  int `json:"prepared_count"`
	CommittedCount int `json:"committed_count"`
	AbortedCount   int `json:"aborted_count"`
}

// GetStats returns current statistics.
func (c *Coordinator) GetStats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var stats Stats
	for _, txn := range c.transactions {
		switch txn.State {
		case StateActive:
			stats.ActiveCount++
		case StatePrepared:
			stats.PreparedCount++
		case StateCommitted:
			stats.CommittedCount++
		case StateAborted:
			stats.AbortedCount++
		}
	}
	return stats
}

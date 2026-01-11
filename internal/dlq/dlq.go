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

/*
Package dlq provides dead letter queue functionality for FlyMQ.

OVERVIEW:
=========
Dead Letter Queues (DLQ) handle messages that fail processing. When a message
fails after max retries, it's moved to a DLQ topic for later inspection.

DLQ TOPIC NAMING:
=================
DLQ topics are named: {original_topic}.dlq
Example: "orders" -> "orders.dlq"

RETRY POLICY:
=============
- Exponential backoff between retries
- Configurable max retries (default: 3)
- After max retries, message goes to DLQ

DLQ MESSAGE FORMAT:
===================
DLQ messages include metadata about the failure:
- Original topic and offset
- Failure reason and error message
- Retry count and timestamps

OPERATIONS:
===========
- GetDLQMessages: Inspect failed messages
- ReplayMessage: Retry a single message
- ReplayAll: Retry all messages in DLQ
*/
package dlq

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"flymq/internal/config"
	"flymq/internal/logging"
)

// FailureReason represents why a message was sent to DLQ.
type FailureReason string

const (
	ReasonMaxRetriesExceeded FailureReason = "max_retries_exceeded"
	ReasonValidationFailed   FailureReason = "validation_failed"
	ReasonProcessingError    FailureReason = "processing_error"
	ReasonExpired            FailureReason = "expired"
	ReasonManual             FailureReason = "manual"
)

// DLQMessage represents a message in the dead letter queue.
type DLQMessage struct {
	ID             string            `json:"id"`
	OriginalTopic  string            `json:"original_topic"`
	OriginalOffset uint64            `json:"original_offset"`
	Payload        []byte            `json:"payload"`
	Reason         FailureReason     `json:"reason"`
	ErrorMessage   string            `json:"error_message"`
	RetryCount     int               `json:"retry_count"`
	FirstFailedAt  time.Time         `json:"first_failed_at"`
	LastFailedAt   time.Time         `json:"last_failed_at"`
	Headers        map[string]string `json:"headers,omitempty"`
}

// RetryPolicy defines how messages should be retried.
type RetryPolicy struct {
	MaxRetries    int           `json:"max_retries"`
	InitialDelay  time.Duration `json:"initial_delay"`
	MaxDelay      time.Duration `json:"max_delay"`
	BackoffFactor float64       `json:"backoff_factor"`
}

// DefaultRetryPolicy returns the default retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:    3,
		InitialDelay:  time.Second,
		MaxDelay:      time.Minute,
		BackoffFactor: 2.0,
	}
}

// MessageStore interface for storing messages.
type MessageStore interface {
	Produce(topic string, data []byte) (uint64, error)
	Fetch(topic string, partition int, offset uint64, maxMessages int) ([][]byte, uint64, error)
}

// Manager manages dead letter queues.
type Manager struct {
	mu           sync.RWMutex
	config       *config.DLQConfig
	store        MessageStore
	retryTracker map[string]*retryState // messageID -> retry state
	logger       *logging.Logger
}

// retryState tracks retry attempts for a message.
type retryState struct {
	RetryCount    int
	FirstFailedAt time.Time
	LastFailedAt  time.Time
	NextRetryAt   time.Time
	Policy        RetryPolicy
}

// NewManager creates a new DLQ manager.
func NewManager(cfg *config.DLQConfig, store MessageStore) *Manager {
	return &Manager{
		config:       cfg,
		store:        store,
		retryTracker: make(map[string]*retryState),
		logger:       logging.NewLogger("dlq"),
	}
}

// GetDLQTopic returns the DLQ topic name for a given topic.
func (m *Manager) GetDLQTopic(topic string) string {
	return topic + m.config.TopicSuffix
}

// RecordFailure records a message failure and determines if it should be sent to DLQ.
func (m *Manager) RecordFailure(messageID, topic string, offset uint64, payload []byte, err error) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.retryTracker[messageID]
	if !exists {
		state = &retryState{
			RetryCount:    0,
			FirstFailedAt: time.Now(),
			Policy:        DefaultRetryPolicy(),
		}
		m.retryTracker[messageID] = state
	}

	state.RetryCount++
	state.LastFailedAt = time.Now()

	// Check if max retries exceeded
	if state.RetryCount >= m.config.MaxRetries {
		// Send to DLQ
		dlqMsg := &DLQMessage{
			ID:             messageID,
			OriginalTopic:  topic,
			OriginalOffset: offset,
			Payload:        payload,
			Reason:         ReasonMaxRetriesExceeded,
			ErrorMessage:   err.Error(),
			RetryCount:     state.RetryCount,
			FirstFailedAt:  state.FirstFailedAt,
			LastFailedAt:   state.LastFailedAt,
		}

		if sendErr := m.sendToDLQ(dlqMsg); sendErr != nil {
			return false, sendErr
		}

		delete(m.retryTracker, messageID)
		m.logger.Info("Message sent to DLQ", "messageID", messageID, "topic", topic, "retries", state.RetryCount)
		return true, nil
	}

	// Calculate next retry time
	delay := m.calculateBackoff(state)
	state.NextRetryAt = time.Now().Add(delay)

	m.logger.Debug("Message retry scheduled", "messageID", messageID, "retryCount", state.RetryCount, "nextRetry", state.NextRetryAt)
	return false, nil
}

// calculateBackoff calculates the backoff delay for the next retry.
func (m *Manager) calculateBackoff(state *retryState) time.Duration {
	delay := float64(state.Policy.InitialDelay)
	for i := 1; i < state.RetryCount; i++ {
		delay *= state.Policy.BackoffFactor
	}
	if time.Duration(delay) > state.Policy.MaxDelay {
		return state.Policy.MaxDelay
	}
	return time.Duration(delay)
}

// sendToDLQ sends a message to the dead letter queue.
func (m *Manager) sendToDLQ(msg *DLQMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ message: %w", err)
	}

	dlqTopic := m.GetDLQTopic(msg.OriginalTopic)
	_, err = m.store.Produce(dlqTopic, data)
	if err != nil {
		return fmt.Errorf("failed to produce to DLQ: %w", err)
	}

	return nil
}

// SendToDLQDirect sends a message directly to DLQ without retry tracking.
func (m *Manager) SendToDLQDirect(topic string, offset uint64, payload []byte, reason FailureReason, errMsg string) error {
	msg := &DLQMessage{
		ID:             fmt.Sprintf("%s-%d-%d", topic, offset, time.Now().UnixNano()),
		OriginalTopic:  topic,
		OriginalOffset: offset,
		Payload:        payload,
		Reason:         reason,
		ErrorMessage:   errMsg,
		RetryCount:     0,
		FirstFailedAt:  time.Now(),
		LastFailedAt:   time.Now(),
	}

	return m.sendToDLQ(msg)
}

// GetDLQMessages retrieves messages from a DLQ topic.
func (m *Manager) GetDLQMessages(topic string, offset uint64, maxMessages int) ([]*DLQMessage, uint64, error) {
	dlqTopic := m.GetDLQTopic(topic)
	rawMessages, nextOffset, err := m.store.Fetch(dlqTopic, 0, offset, maxMessages)
	if err != nil {
		return nil, 0, err
	}

	messages := make([]*DLQMessage, 0, len(rawMessages))
	for _, raw := range rawMessages {
		var msg DLQMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			m.logger.Warn("Failed to unmarshal DLQ message", "error", err)
			continue
		}
		messages = append(messages, &msg)
	}

	return messages, nextOffset, nil
}

// ReplayMessage replays a message from DLQ back to the original topic.
func (m *Manager) ReplayMessage(msg *DLQMessage) error {
	_, err := m.store.Produce(msg.OriginalTopic, msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to replay message: %w", err)
	}

	m.logger.Info("Replayed DLQ message", "messageID", msg.ID, "topic", msg.OriginalTopic)
	return nil
}

// ReplayAll replays all messages from a DLQ topic.
func (m *Manager) ReplayAll(topic string) (int, error) {
	dlqTopic := m.GetDLQTopic(topic)
	offset := uint64(0)
	replayed := 0

	for {
		messages, nextOffset, err := m.GetDLQMessages(topic, offset, 100)
		if err != nil {
			return replayed, err
		}

		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			if err := m.ReplayMessage(msg); err != nil {
				m.logger.Error("Failed to replay message", "messageID", msg.ID, "error", err)
				continue
			}
			replayed++
		}

		offset = nextOffset
	}

	m.logger.Info("Replayed all DLQ messages", "topic", dlqTopic, "count", replayed)
	return replayed, nil
}

// GetRetryState returns the retry state for a message.
func (m *Manager) GetRetryState(messageID string) (*retryState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, exists := m.retryTracker[messageID]
	return state, exists
}

// ClearRetryState clears the retry state for a message.
func (m *Manager) ClearRetryState(messageID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.retryTracker, messageID)
}

// ShouldRetry checks if a message should be retried.
func (m *Manager) ShouldRetry(messageID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.retryTracker[messageID]
	if !exists {
		return true // First attempt
	}

	return time.Now().After(state.NextRetryAt)
}

// GetPendingRetries returns all messages pending retry.
func (m *Manager) GetPendingRetries() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var pending []string
	now := time.Now()
	for id, state := range m.retryTracker {
		if now.After(state.NextRetryAt) {
			pending = append(pending, id)
		}
	}
	return pending
}

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
Package delayed provides delayed message delivery functionality for FlyMQ.

OVERVIEW:
=========
Delayed messages are scheduled for future delivery. Use cases include:
- Scheduled notifications (send email in 24 hours)
- Retry with backoff (retry failed operation in 5 minutes)
- Workflow timeouts (cancel order if not confirmed in 1 hour)

IMPLEMENTATION:
===============
Uses a min-heap (priority queue) ordered by scheduled delivery time.
A background goroutine checks for due messages and delivers them.

USAGE:
======

	manager := delayed.NewManager(cfg, broker)
	manager.Start()
	defer manager.Stop()

	// Schedule message for 5 minutes from now
	id, err := manager.Schedule("orders", payload, 5*time.Minute, nil)

	// Cancel if needed
	manager.Cancel(id)
*/
package delayed

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"flymq/internal/config"
	"flymq/internal/logging"
)

// DelayedMessage represents a message scheduled for delayed delivery.
type DelayedMessage struct {
	ID          string            `json:"id"`
	Topic       string            `json:"topic"`
	Payload     []byte            `json:"payload"`
	Headers     map[string]string `json:"headers,omitempty"`
	ScheduledAt time.Time         `json:"scheduled_at"`
	CreatedAt   time.Time         `json:"created_at"`
	index       int               // Index in the heap
}

// MessageStore interface for producing messages.
type MessageStore interface {
	Produce(topic string, data []byte) (uint64, error)
}

// messageHeap implements heap.Interface for delayed messages.
type messageHeap []*DelayedMessage

func (h messageHeap) Len() int           { return len(h) }
func (h messageHeap) Less(i, j int) bool { return h[i].ScheduledAt.Before(h[j].ScheduledAt) }
func (h messageHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *messageHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*DelayedMessage)
	item.index = n
	*h = append(*h, item)
}

func (h *messageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// Manager manages delayed message delivery.
type Manager struct {
	mu       sync.RWMutex
	config   *config.DelayedConfig
	store    MessageStore
	messages messageHeap
	byID     map[string]*DelayedMessage
	logger   *logging.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	notify   chan struct{}
}

// NewManager creates a new delayed message manager.
func NewManager(cfg *config.DelayedConfig, store MessageStore) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		config:   cfg,
		store:    store,
		messages: make(messageHeap, 0),
		byID:     make(map[string]*DelayedMessage),
		logger:   logging.NewLogger("delayed"),
		ctx:      ctx,
		cancel:   cancel,
		notify:   make(chan struct{}, 1),
	}
	heap.Init(&m.messages)
	return m
}

// Start starts the delayed message delivery process.
func (m *Manager) Start() {
	m.wg.Add(1)
	go m.deliveryLoop()
	m.logger.Info("Delayed message manager started")
}

// Stop stops the delayed message delivery process.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.logger.Info("Delayed message manager stopped")
}

// Schedule schedules a message for delayed delivery.
func (m *Manager) Schedule(topic string, payload []byte, delay time.Duration, headers map[string]string) (string, error) {
	if !m.config.Enabled {
		return "", fmt.Errorf("delayed message delivery is disabled")
	}

	maxDelay := time.Duration(m.config.MaxDelay) * time.Second
	if delay > maxDelay {
		return "", fmt.Errorf("delay %v exceeds maximum allowed delay %v", delay, maxDelay)
	}

	now := time.Now()
	msg := &DelayedMessage{
		ID:          fmt.Sprintf("delayed-%d", now.UnixNano()),
		Topic:       topic,
		Payload:     payload,
		Headers:     headers,
		ScheduledAt: now.Add(delay),
		CreatedAt:   now,
	}

	m.mu.Lock()
	heap.Push(&m.messages, msg)
	m.byID[msg.ID] = msg
	m.mu.Unlock()

	// Notify the delivery loop
	select {
	case m.notify <- struct{}{}:
	default:
	}

	m.logger.Debug("Scheduled delayed message", "id", msg.ID, "topic", topic, "scheduledAt", msg.ScheduledAt)
	return msg.ID, nil
}

// ScheduleAt schedules a message for delivery at a specific time.
func (m *Manager) ScheduleAt(topic string, payload []byte, deliverAt time.Time, headers map[string]string) (string, error) {
	delay := time.Until(deliverAt)
	if delay < 0 {
		delay = 0
	}
	return m.Schedule(topic, payload, delay, headers)
}

// Cancel cancels a scheduled message.
func (m *Manager) Cancel(messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg, exists := m.byID[messageID]
	if !exists {
		return fmt.Errorf("message not found: %s", messageID)
	}

	// Remove from heap
	heap.Remove(&m.messages, msg.index)
	delete(m.byID, messageID)

	m.logger.Debug("Cancelled delayed message", "id", messageID)
	return nil
}

// Get retrieves a scheduled message by ID.
func (m *Manager) Get(messageID string) (*DelayedMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	msg, exists := m.byID[messageID]
	if !exists {
		return nil, fmt.Errorf("message not found: %s", messageID)
	}
	return msg, nil
}

// List returns all scheduled messages.
func (m *Manager) List() []*DelayedMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*DelayedMessage, len(m.messages))
	copy(result, m.messages)
	return result
}

// Count returns the number of scheduled messages.
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.messages)
}

// deliveryLoop runs the message delivery process.
func (m *Manager) deliveryLoop() {
	defer m.wg.Done()

	timer := time.NewTimer(time.Hour) // Initial long timer
	defer timer.Stop()

	for {
		m.mu.RLock()
		var nextDelivery time.Time
		if len(m.messages) > 0 {
			nextDelivery = m.messages[0].ScheduledAt
		}
		m.mu.RUnlock()

		if !nextDelivery.IsZero() {
			delay := time.Until(nextDelivery)
			if delay <= 0 {
				m.deliverDueMessages()
				continue
			}
			timer.Reset(delay)
		} else {
			timer.Reset(time.Hour)
		}

		select {
		case <-m.ctx.Done():
			return
		case <-timer.C:
			m.deliverDueMessages()
		case <-m.notify:
			// New message scheduled, recalculate timer
		}
	}
}

// deliverDueMessages delivers all messages that are due.
func (m *Manager) deliverDueMessages() {
	now := time.Now()

	for {
		m.mu.Lock()
		if len(m.messages) == 0 || m.messages[0].ScheduledAt.After(now) {
			m.mu.Unlock()
			return
		}

		msg := heap.Pop(&m.messages).(*DelayedMessage)
		delete(m.byID, msg.ID)
		m.mu.Unlock()

		// Deliver the message
		if _, err := m.store.Produce(msg.Topic, msg.Payload); err != nil {
			m.logger.Error("Failed to deliver delayed message", "id", msg.ID, "topic", msg.Topic, "error", err)
			// Re-schedule for retry
			m.mu.Lock()
			msg.ScheduledAt = time.Now().Add(time.Second)
			heap.Push(&m.messages, msg)
			m.byID[msg.ID] = msg
			m.mu.Unlock()
		} else {
			m.logger.Debug("Delivered delayed message", "id", msg.ID, "topic", msg.Topic)
		}
	}
}

// Stats returns delayed message manager statistics.
type Stats struct {
	PendingCount int       `json:"pending_count"`
	NextDelivery time.Time `json:"next_delivery,omitempty"`
}

// GetStats returns current statistics.
func (m *Manager) GetStats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := Stats{
		PendingCount: len(m.messages),
	}
	if len(m.messages) > 0 {
		stats.NextDelivery = m.messages[0].ScheduledAt
	}
	return stats
}

// MarshalJSON implements json.Marshaler for DelayedMessage.
func (msg *DelayedMessage) MarshalJSON() ([]byte, error) {
	type Alias DelayedMessage
	return json.Marshal(&struct {
		*Alias
		ScheduledAt string `json:"scheduled_at"`
		CreatedAt   string `json:"created_at"`
	}{
		Alias:       (*Alias)(msg),
		ScheduledAt: msg.ScheduledAt.Format(time.RFC3339),
		CreatedAt:   msg.CreatedAt.Format(time.RFC3339),
	})
}

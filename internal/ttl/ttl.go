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

// Package ttl provides message TTL (Time-To-Live) and expiration functionality for FlyMQ.
package ttl

import (
	"context"
	"sync"
	"time"

	"flymq/internal/config"
	"flymq/internal/logging"
)

// MessageMetadata contains TTL-related metadata for a message.
type MessageMetadata struct {
	MessageID  string    `json:"message_id"`
	Topic      string    `json:"topic"`
	Partition  int       `json:"partition"`
	Offset     uint64    `json:"offset"`
	CreatedAt  time.Time `json:"created_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	TTLSeconds int64     `json:"ttl_seconds"`
}

// IsExpired checks if the message has expired.
func (m *MessageMetadata) IsExpired() bool {
	if m.TTLSeconds == 0 {
		return false // No TTL set, never expires
	}
	return time.Now().After(m.ExpiresAt)
}

// TimeToExpiry returns the duration until the message expires.
func (m *MessageMetadata) TimeToExpiry() time.Duration {
	if m.TTLSeconds == 0 {
		return time.Duration(1<<63 - 1) // Max duration
	}
	return time.Until(m.ExpiresAt)
}

// MessageStore interface for message operations.
type MessageStore interface {
	DeleteMessage(topic string, partition int, offset uint64) error
	GetMessageMetadata(topic string, partition int, offset uint64) (*MessageMetadata, error)
	ListMessages(topic string, partition int, startOffset, endOffset uint64) ([]uint64, error)
}

// Manager manages message TTL and expiration.
type Manager struct {
	mu              sync.RWMutex
	config          *config.TTLConfig
	store           MessageStore
	topicTTLs       map[string]time.Duration        // topic -> default TTL
	expirationIndex map[time.Time][]MessageMetadata // expiration time -> messages
	logger          *logging.Logger
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
}

// NewManager creates a new TTL manager.
func NewManager(cfg *config.TTLConfig, store MessageStore) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		config:          cfg,
		store:           store,
		topicTTLs:       make(map[string]time.Duration),
		expirationIndex: make(map[time.Time][]MessageMetadata),
		logger:          logging.NewLogger("ttl"),
		ctx:             ctx,
		cancel:          cancel,
	}
}

// Start starts the TTL cleanup process.
func (m *Manager) Start() {
	m.wg.Add(1)
	go m.cleanupLoop()
	m.logger.Info("TTL manager started", "cleanupInterval", m.config.CleanupInterval)
}

// Stop stops the TTL cleanup process.
func (m *Manager) Stop() {
	m.cancel()
	m.wg.Wait()
	m.logger.Info("TTL manager stopped")
}

// SetTopicTTL sets the default TTL for a topic.
func (m *Manager) SetTopicTTL(topic string, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topicTTLs[topic] = ttl
	m.logger.Info("Set topic TTL", "topic", topic, "ttl", ttl)
}

// GetTopicTTL returns the TTL for a topic.
func (m *Manager) GetTopicTTL(topic string) time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if ttl, exists := m.topicTTLs[topic]; exists {
		return ttl
	}
	return time.Duration(m.config.DefaultTTL) * time.Second
}

// RegisterMessage registers a message for TTL tracking.
func (m *Manager) RegisterMessage(meta MessageMetadata) {
	if meta.TTLSeconds == 0 {
		// Use topic default TTL
		topicTTL := m.GetTopicTTL(meta.Topic)
		if topicTTL == 0 {
			return // No TTL, don't track
		}
		meta.TTLSeconds = int64(topicTTL.Seconds())
		meta.ExpiresAt = meta.CreatedAt.Add(topicTTL)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Round to nearest second for indexing
	expiryKey := meta.ExpiresAt.Truncate(time.Second)
	m.expirationIndex[expiryKey] = append(m.expirationIndex[expiryKey], meta)
}

// cleanupLoop runs the periodic cleanup process.
func (m *Manager) cleanupLoop() {
	defer m.wg.Done()

	interval := time.Duration(m.config.CleanupInterval) * time.Second
	if interval == 0 {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.cleanup()
		}
	}
}

// cleanup removes expired messages.
func (m *Manager) cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	expiredCount := 0

	// Find all expired entries
	for expiryTime, messages := range m.expirationIndex {
		if expiryTime.After(now) {
			continue
		}

		for _, meta := range messages {
			if err := m.store.DeleteMessage(meta.Topic, meta.Partition, meta.Offset); err != nil {
				m.logger.Warn("Failed to delete expired message",
					"topic", meta.Topic,
					"partition", meta.Partition,
					"offset", meta.Offset,
					"error", err)
				continue
			}
			expiredCount++
		}

		delete(m.expirationIndex, expiryTime)
	}

	if expiredCount > 0 {
		m.logger.Info("Cleaned up expired messages", "count", expiredCount)
	}
}

// GetExpiredMessages returns a list of expired messages for a topic.
func (m *Manager) GetExpiredMessages(topic string) []MessageMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := time.Now()
	var expired []MessageMetadata

	for expiryTime, messages := range m.expirationIndex {
		if expiryTime.After(now) {
			continue
		}

		for _, meta := range messages {
			if meta.Topic == topic {
				expired = append(expired, meta)
			}
		}
	}

	return expired
}

// GetPendingExpirations returns the count of messages pending expiration.
func (m *Manager) GetPendingExpirations() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, messages := range m.expirationIndex {
		count += len(messages)
	}
	return count
}

// RemoveFromTracking removes a message from TTL tracking (e.g., when consumed).
func (m *Manager) RemoveFromTracking(topic string, partition int, offset uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for expiryTime, messages := range m.expirationIndex {
		for i, meta := range messages {
			if meta.Topic == topic && meta.Partition == partition && meta.Offset == offset {
				// Remove from slice
				m.expirationIndex[expiryTime] = append(messages[:i], messages[i+1:]...)
				if len(m.expirationIndex[expiryTime]) == 0 {
					delete(m.expirationIndex, expiryTime)
				}
				return
			}
		}
	}
}

// CalculateExpiry calculates the expiration time for a message.
func (m *Manager) CalculateExpiry(topic string, createdAt time.Time, messageTTL int64) time.Time {
	var ttl time.Duration
	if messageTTL > 0 {
		ttl = time.Duration(messageTTL) * time.Second
	} else {
		ttl = m.GetTopicTTL(topic)
	}

	if ttl == 0 {
		return time.Time{} // No expiry
	}

	return createdAt.Add(ttl)
}

// IsMessageExpired checks if a specific message is expired.
func (m *Manager) IsMessageExpired(topic string, partition int, offset uint64) (bool, error) {
	meta, err := m.store.GetMessageMetadata(topic, partition, offset)
	if err != nil {
		return false, err
	}
	return meta.IsExpired(), nil
}

// Stats returns TTL manager statistics.
type Stats struct {
	PendingExpirations int              `json:"pending_expirations"`
	TopicTTLs          map[string]int64 `json:"topic_ttls"`
}

// GetStats returns current TTL manager statistics.
func (m *Manager) GetStats() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	topicTTLs := make(map[string]int64)
	for topic, ttl := range m.topicTTLs {
		topicTTLs[topic] = int64(ttl.Seconds())
	}

	return Stats{
		PendingExpirations: m.GetPendingExpirations(),
		TopicTTLs:          topicTTLs,
	}
}

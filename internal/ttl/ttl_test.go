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

package ttl

import (
	"testing"
	"time"

	"flymq/internal/config"
)

// mockStore implements MessageStore for testing
type mockStore struct {
	deleted []struct {
		topic     string
		partition int
		offset    uint64
	}
}

func newMockStore() *mockStore {
	return &mockStore{}
}

func (m *mockStore) DeleteMessage(topic string, partition int, offset uint64) error {
	m.deleted = append(m.deleted, struct {
		topic     string
		partition int
		offset    uint64
	}{topic, partition, offset})
	return nil
}

func (m *mockStore) GetMessageMetadata(topic string, partition int, offset uint64) (*MessageMetadata, error) {
	return &MessageMetadata{
		Topic:      topic,
		Partition:  partition,
		Offset:     offset,
		CreatedAt:  time.Now().Add(-time.Hour),
		ExpiresAt:  time.Now().Add(-time.Minute),
		TTLSeconds: 3600,
	}, nil
}

func (m *mockStore) ListMessages(topic string, partition int, startOffset, endOffset uint64) ([]uint64, error) {
	return nil, nil
}

func TestNewManager(t *testing.T) {
	cfg := &config.TTLConfig{
		DefaultTTL:      3600,
		CleanupInterval: 60,
	}
	mgr := NewManager(cfg, newMockStore())
	if mgr == nil {
		t.Fatal("Expected non-nil manager")
	}
}

func TestSetTopicTTL(t *testing.T) {
	cfg := &config.TTLConfig{DefaultTTL: 3600, CleanupInterval: 60}
	mgr := NewManager(cfg, newMockStore())

	mgr.SetTopicTTL("orders", time.Hour)

	ttl := mgr.GetTopicTTL("orders")
	if ttl != time.Hour {
		t.Errorf("Expected TTL 1h, got %v", ttl)
	}
}

func TestGetTopicTTLDefault(t *testing.T) {
	cfg := &config.TTLConfig{DefaultTTL: 3600, CleanupInterval: 60}
	mgr := NewManager(cfg, newMockStore())

	ttl := mgr.GetTopicTTL("unknown-topic")
	if ttl != time.Hour {
		t.Errorf("Expected default TTL 1h, got %v", ttl)
	}
}

func TestMessageMetadataIsExpired(t *testing.T) {
	// Not expired
	meta := &MessageMetadata{
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(time.Hour),
		TTLSeconds: 3600,
	}
	if meta.IsExpired() {
		t.Error("Expected message to not be expired")
	}

	// Expired
	meta = &MessageMetadata{
		CreatedAt:  time.Now().Add(-2 * time.Hour),
		ExpiresAt:  time.Now().Add(-time.Hour),
		TTLSeconds: 3600,
	}
	if !meta.IsExpired() {
		t.Error("Expected message to be expired")
	}

	// No TTL
	meta = &MessageMetadata{
		CreatedAt:  time.Now().Add(-24 * time.Hour),
		TTLSeconds: 0,
	}
	if meta.IsExpired() {
		t.Error("Expected message with no TTL to never expire")
	}
}

func TestTimeToExpiry(t *testing.T) {
	meta := &MessageMetadata{
		ExpiresAt:  time.Now().Add(time.Hour),
		TTLSeconds: 3600,
	}

	tte := meta.TimeToExpiry()
	if tte < 59*time.Minute || tte > 61*time.Minute {
		t.Errorf("Expected ~1h time to expiry, got %v", tte)
	}
}

func TestRegisterMessage(t *testing.T) {
	cfg := &config.TTLConfig{DefaultTTL: 3600, CleanupInterval: 60}
	mgr := NewManager(cfg, newMockStore())

	meta := MessageMetadata{
		MessageID:  "msg-1",
		Topic:      "orders",
		Partition:  0,
		Offset:     0,
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(time.Hour),
		TTLSeconds: 3600,
	}

	mgr.RegisterMessage(meta)

	count := mgr.GetPendingExpirations()
	if count != 1 {
		t.Errorf("Expected 1 pending expiration, got %d", count)
	}
}

func TestCalculateExpiry(t *testing.T) {
	cfg := &config.TTLConfig{DefaultTTL: 3600, CleanupInterval: 60}
	mgr := NewManager(cfg, newMockStore())

	now := time.Now()

	// With message TTL
	expiry := mgr.CalculateExpiry("orders", now, 7200)
	expected := now.Add(2 * time.Hour)
	if expiry.Sub(expected) > time.Second {
		t.Errorf("Expected expiry at %v, got %v", expected, expiry)
	}

	// With topic default TTL
	expiry = mgr.CalculateExpiry("orders", now, 0)
	expected = now.Add(time.Hour)
	if expiry.Sub(expected) > time.Second {
		t.Errorf("Expected expiry at %v, got %v", expected, expiry)
	}
}

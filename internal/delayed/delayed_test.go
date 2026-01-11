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

package delayed

import (
	"testing"
	"time"

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

func TestNewManager(t *testing.T) {
	cfg := &config.DelayedConfig{
		Enabled:  true,
		MaxDelay: 86400,
	}
	mgr := NewManager(cfg, newMockStore())
	if mgr == nil {
		t.Fatal("Expected non-nil manager")
	}
}

func TestSchedule(t *testing.T) {
	cfg := &config.DelayedConfig{
		Enabled:  true,
		MaxDelay: 86400,
	}
	mgr := NewManager(cfg, newMockStore())

	id, err := mgr.Schedule("test-topic", []byte("payload"), time.Hour, nil)
	if err != nil {
		t.Fatalf("Schedule failed: %v", err)
	}
	if id == "" {
		t.Error("Expected non-empty message ID")
	}
}

func TestScheduleDisabled(t *testing.T) {
	cfg := &config.DelayedConfig{
		Enabled:  false,
		MaxDelay: 86400,
	}
	mgr := NewManager(cfg, newMockStore())

	_, err := mgr.Schedule("test-topic", []byte("payload"), time.Hour, nil)
	if err == nil {
		t.Error("Expected error when delayed delivery is disabled")
	}
}

func TestScheduleExceedsMaxDelay(t *testing.T) {
	cfg := &config.DelayedConfig{
		Enabled:  true,
		MaxDelay: 60, // 60 seconds max
	}
	mgr := NewManager(cfg, newMockStore())

	_, err := mgr.Schedule("test-topic", []byte("payload"), time.Hour, nil)
	if err == nil {
		t.Error("Expected error when delay exceeds max")
	}
}

func TestGet(t *testing.T) {
	cfg := &config.DelayedConfig{Enabled: true, MaxDelay: 86400}
	mgr := NewManager(cfg, newMockStore())

	id, _ := mgr.Schedule("test-topic", []byte("payload"), time.Hour, nil)

	msg, err := mgr.Get(id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if msg.ID != id {
		t.Errorf("Expected ID %s, got %s", id, msg.ID)
	}
	if msg.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", msg.Topic)
	}
}

func TestGetNonExistent(t *testing.T) {
	cfg := &config.DelayedConfig{Enabled: true, MaxDelay: 86400}
	mgr := NewManager(cfg, newMockStore())

	_, err := mgr.Get("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent message")
	}
}

func TestCancel(t *testing.T) {
	cfg := &config.DelayedConfig{Enabled: true, MaxDelay: 86400}
	mgr := NewManager(cfg, newMockStore())

	id, _ := mgr.Schedule("test-topic", []byte("payload"), time.Hour, nil)

	err := mgr.Cancel(id)
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	_, err = mgr.Get(id)
	if err == nil {
		t.Error("Expected error after cancellation")
	}
}

func TestCancelNonExistent(t *testing.T) {
	cfg := &config.DelayedConfig{Enabled: true, MaxDelay: 86400}
	mgr := NewManager(cfg, newMockStore())

	err := mgr.Cancel("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent message")
	}
}

func TestList(t *testing.T) {
	cfg := &config.DelayedConfig{Enabled: true, MaxDelay: 86400}
	mgr := NewManager(cfg, newMockStore())

	mgr.Schedule("topic-1", []byte("payload1"), time.Hour, nil)
	mgr.Schedule("topic-2", []byte("payload2"), 2*time.Hour, nil)

	messages := mgr.List()
	if len(messages) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(messages))
	}
}

func TestCount(t *testing.T) {
	cfg := &config.DelayedConfig{Enabled: true, MaxDelay: 86400}
	mgr := NewManager(cfg, newMockStore())

	if mgr.Count() != 0 {
		t.Error("Expected count 0 initially")
	}

	mgr.Schedule("topic", []byte("payload"), time.Hour, nil)
	if mgr.Count() != 1 {
		t.Errorf("Expected count 1, got %d", mgr.Count())
	}
}

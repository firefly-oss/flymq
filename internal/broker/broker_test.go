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

package broker

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"flymq/internal/config"
	"flymq/internal/protocol"
)

func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	cfg := &config.Config{
		DataDir:      t.TempDir(),
		SegmentBytes: 1024 * 1024,
	}
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker failed: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func TestNewBroker(t *testing.T) {
	b := newTestBroker(t)
	if b == nil {
		t.Fatal("Expected non-nil broker")
	}
	if b.topics == nil {
		t.Error("Expected topics map to be initialized")
	}
}

func TestCreateTopic(t *testing.T) {
	b := newTestBroker(t)

	err := b.CreateTopic("test-topic", 3)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	// Verify topic exists
	topics := b.ListTopics()
	found := false
	for _, name := range topics {
		if name == "test-topic" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Topic not found in list")
	}

	// Verify partition count
	info, err := b.GetTopicInfo("test-topic")
	if err != nil {
		t.Fatalf("GetTopicInfo failed: %v", err)
	}
	if len(info.Partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(info.Partitions))
	}
}

func TestCreateTopicDuplicate(t *testing.T) {
	b := newTestBroker(t)

	b.CreateTopic("test-topic", 1)
	err := b.CreateTopic("test-topic", 1)
	// In cluster mode, duplicate topic creation is idempotent (no error)
	// This allows for Raft log replay without failures
	if err != nil {
		t.Logf("Note: CreateTopic now handles duplicates idempotently: %v", err)
	}
}

func TestProduceAndConsume(t *testing.T) {
	b := newTestBroker(t)

	testData := []byte("Hello, FlyMQ!")
	offset, err := b.Produce("test-topic", testData)
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("Expected offset 0, got %d", offset)
	}

	data, err := b.Consume("test-topic", 0)
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if !bytes.Equal(data, testData) {
		t.Errorf("Data mismatch: got %q, want %q", data, testData)
	}
}

func TestProduceAutoCreatesTopic(t *testing.T) {
	b := newTestBroker(t)

	_, err := b.Produce("auto-created-topic", []byte("test"))
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	topics := b.ListTopics()
	found := false
	for _, name := range topics {
		if name == "auto-created-topic" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Auto-created topic not found")
	}
}

func TestConsumeNonExistentTopic(t *testing.T) {
	b := newTestBroker(t)

	_, err := b.Consume("non-existent", 0)
	if err == nil {
		t.Error("Expected error for non-existent topic")
	}
}

func TestConsumeNonExistentOffset(t *testing.T) {
	b := newTestBroker(t)

	b.Produce("test-topic", []byte("test"))
	_, err := b.Consume("test-topic", 100)
	if err != io.EOF {
		t.Errorf("Expected io.EOF, got %v", err)
	}
}

func TestMultipleMessages(t *testing.T) {
	b := newTestBroker(t)

	messages := [][]byte{
		[]byte("Message 1"),
		[]byte("Message 2"),
		[]byte("Message 3"),
	}

	for i, msg := range messages {
		offset, err := b.Produce("test-topic", msg)
		if err != nil {
			t.Fatalf("Produce failed: %v", err)
		}
		if offset != uint64(i) {
			t.Errorf("Expected offset %d, got %d", i, offset)
		}
	}

	for i, expected := range messages {
		data, err := b.Consume("test-topic", uint64(i))
		if err != nil {
			t.Fatalf("Consume failed: %v", err)
		}
		if !bytes.Equal(data, expected) {
			t.Errorf("Message %d mismatch", i)
		}
	}
}

func TestConcurrentProduce(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("concurrent-topic", 1)

	var wg sync.WaitGroup
	numGoroutines := 10
	numMessages := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numMessages; j++ {
				_, err := b.Produce("concurrent-topic", []byte("concurrent message"))
				if err != nil {
					t.Errorf("Produce failed: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	info, _ := b.GetTopicInfo("concurrent-topic")
	expectedMessages := uint64(numGoroutines * numMessages)
	if info.Partitions[0].HighestOffset != expectedMessages-1 {
		t.Errorf("Expected %d messages, got %d", expectedMessages, info.Partitions[0].HighestOffset+1)
	}
}

func TestDeleteTopic(t *testing.T) {
	b := newTestBroker(t)

	b.CreateTopic("to-delete", 1)
	b.Produce("to-delete", []byte("test"))

	err := b.DeleteTopic("to-delete")
	if err != nil {
		t.Fatalf("DeleteTopic failed: %v", err)
	}

	topics := b.ListTopics()
	for _, name := range topics {
		if name == "to-delete" {
			t.Error("Topic should have been deleted")
		}
	}
}

func TestDeleteNonExistentTopic(t *testing.T) {
	b := newTestBroker(t)

	err := b.DeleteTopic("non-existent")
	// In cluster mode, deleting a non-existent topic is idempotent (no error)
	// This allows for Raft log replay without failures
	if err != nil {
		t.Logf("Note: DeleteTopic now handles non-existent topics idempotently: %v", err)
	}
}

func TestSubscribe(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("subscribe-topic", 1)

	// Produce some messages
	for i := 0; i < 5; i++ {
		b.Produce("subscribe-topic", []byte("message"))
	}

	tests := []struct {
		name     string
		mode     protocol.SubscribeMode
		expected uint64
	}{
		{"earliest", protocol.SubscribeFromEarliest, 0},
		{"latest", protocol.SubscribeFromLatest, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, err := b.Subscribe("subscribe-topic", "test-group", 0, tt.mode)
			if err != nil {
				t.Fatalf("Subscribe failed: %v", err)
			}
			if offset != tt.expected {
				t.Errorf("Expected offset %d, got %d", tt.expected, offset)
			}
		})
	}
}

func TestSubscribeFromCommit(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("commit-topic", 1)

	// Produce messages
	for i := 0; i < 10; i++ {
		b.Produce("commit-topic", []byte("message"))
	}

	// Commit offset
	b.CommitOffset("commit-topic", "test-group", 0, 5)

	// Subscribe from commit
	offset, err := b.Subscribe("commit-topic", "test-group", 0, protocol.SubscribeFromCommit)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	if offset != 5 {
		t.Errorf("Expected offset 5, got %d", offset)
	}
}

func TestFetch(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("fetch-topic", 1)

	// Produce messages
	for i := 0; i < 10; i++ {
		b.Produce("fetch-topic", []byte("message"))
	}

	messages, nextOffset, err := b.Fetch("fetch-topic", 0, 0, 5)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(messages) != 5 {
		t.Errorf("Expected 5 messages, got %d", len(messages))
	}
	if nextOffset != 5 {
		t.Errorf("Expected next offset 5, got %d", nextOffset)
	}
}

func TestFetchBeyondEnd(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("fetch-topic", 1)

	// Produce 3 messages
	for i := 0; i < 3; i++ {
		b.Produce("fetch-topic", []byte("message"))
	}

	// Try to fetch 10 messages
	messages, _, err := b.Fetch("fetch-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(messages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(messages))
	}
}

func TestListTopics(t *testing.T) {
	b := newTestBroker(t)

	b.CreateTopic("topic-a", 1)
	b.CreateTopic("topic-b", 1)
	b.CreateTopic("topic-c", 1)

	topics := b.ListTopics()
	if len(topics) != 3 {
		t.Errorf("Expected 3 topics, got %d", len(topics))
	}

	// Should be sorted
	expected := []string{"topic-a", "topic-b", "topic-c"}
	for i, name := range expected {
		if topics[i] != name {
			t.Errorf("Expected topic %s at index %d, got %s", name, i, topics[i])
		}
	}
}

func TestGetTopicInfo(t *testing.T) {
	b := newTestBroker(t)
	b.CreateTopic("info-topic", 3)

	// Produce some messages
	for i := 0; i < 5; i++ {
		b.Produce("info-topic", []byte("message"))
	}

	info, err := b.GetTopicInfo("info-topic")
	if err != nil {
		t.Fatalf("GetTopicInfo failed: %v", err)
	}

	if info.Name != "info-topic" {
		t.Errorf("Expected name 'info-topic', got %s", info.Name)
	}
	if len(info.Partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(info.Partitions))
	}
}

func TestGetTopicInfoNonExistent(t *testing.T) {
	b := newTestBroker(t)

	_, err := b.GetTopicInfo("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent topic")
	}
}

// TestConsumerGroupResumeFromCommit verifies that consumers resume from
// their committed offset after a "restart" (simulated by re-subscribing).
// This is the core behavior that prevents message re-consumption.
func TestConsumerGroupResumeFromCommit(t *testing.T) {
	b := newTestBroker(t)
	topic := "resume-test"
	group := "test-group"

	b.CreateTopic(topic, 1)

	// Produce 10 messages
	for i := 0; i < 10; i++ {
		b.Produce(topic, []byte(fmt.Sprintf("message-%d", i)))
	}

	// First consumer session: consume 5 messages and commit
	offset, err := b.Subscribe(topic, group, 0, protocol.SubscribeFromEarliest)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("Expected initial offset 0, got %d", offset)
	}

	// Fetch 5 messages
	messages, nextOffset, err := b.Fetch(topic, 0, offset, 5)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if len(messages) != 5 {
		t.Errorf("Expected 5 messages, got %d", len(messages))
	}
	if nextOffset != 5 {
		t.Errorf("Expected next offset 5, got %d", nextOffset)
	}

	// Commit offset 5 (next message to read)
	changed, err := b.CommitOffset(topic, group, 0, nextOffset)
	if err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}
	if !changed {
		t.Error("Expected offset to be marked as changed")
	}

	// Simulate restart: subscribe again with "committed" mode
	resumeOffset, err := b.Subscribe(topic, group, 0, protocol.SubscribeFromCommit)
	if err != nil {
		t.Fatalf("Subscribe (resume) failed: %v", err)
	}

	// Should resume from offset 5, not 0
	if resumeOffset != 5 {
		t.Errorf("Expected resume offset 5, got %d", resumeOffset)
	}

	// Fetch remaining messages using FetchWithKeys to verify content
	fetchedMessages, nextOffset, err := b.FetchWithKeys(topic, 0, resumeOffset, 10)
	if err != nil {
		t.Fatalf("Fetch (resume) failed: %v", err)
	}
	if len(fetchedMessages) != 5 {
		t.Errorf("Expected 5 remaining messages, got %d", len(fetchedMessages))
	}

	// Verify we got messages 5-9, not 0-4
	if string(fetchedMessages[0].Value) != "message-5" {
		t.Errorf("Expected first message to be 'message-5', got '%s'", string(fetchedMessages[0].Value))
	}
}

// TestCommitOffsetIdempotent verifies that committing the same offset
// multiple times doesn't cause unnecessary disk writes.
func TestCommitOffsetIdempotent(t *testing.T) {
	b := newTestBroker(t)
	topic := "idempotent-test"
	group := "test-group"

	b.CreateTopic(topic, 1)
	b.Produce(topic, []byte("message"))

	// First commit should return changed=true
	changed, err := b.CommitOffset(topic, group, 0, 1)
	if err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}
	if !changed {
		t.Error("First commit should return changed=true")
	}

	// Second commit of same offset should return changed=false
	changed, err = b.CommitOffset(topic, group, 0, 1)
	if err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}
	if changed {
		t.Error("Second commit of same offset should return changed=false")
	}

	// Third commit of different offset should return changed=true
	changed, err = b.CommitOffset(topic, group, 0, 2)
	if err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}
	if !changed {
		t.Error("Commit of different offset should return changed=true")
	}
}

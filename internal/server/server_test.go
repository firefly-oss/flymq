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

package server

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/protocol"
)

// MockBroker implements the Broker interface for testing
type MockBroker struct {
	mu       sync.RWMutex
	topics   map[string][]Message
	offsets  map[string]map[string]uint64 // topic -> groupID -> offset
	metadata map[string]interface{}
}

type Message struct {
	Data   []byte
	Offset uint64
}

func NewMockBroker() *MockBroker {
	return &MockBroker{
		topics:   make(map[string][]Message),
		offsets:  make(map[string]map[string]uint64),
		metadata: make(map[string]interface{}),
	}
}

func (b *MockBroker) Produce(topic string, data []byte) (uint64, error) {
	return b.ProduceWithKey(topic, nil, data)
}

func (b *MockBroker) ProduceWithKey(topic string, key, data []byte) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[topic]; !ok {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	offset := uint64(len(b.topics[topic]))
	b.topics[topic] = append(b.topics[topic], Message{Data: data, Offset: offset})
	return offset, nil
}

func (b *MockBroker) Consume(topic string, offset uint64) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	messages, ok := b.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	if int(offset) >= len(messages) {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}

	return messages[offset].Data, nil
}

func (b *MockBroker) CreateTopic(topic string, partitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[topic]; ok {
		return fmt.Errorf("topic already exists: %s", topic)
	}

	b.topics[topic] = make([]Message, 0)
	b.offsets[topic] = make(map[string]uint64)
	return nil
}

func (b *MockBroker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[name]; !ok {
		return fmt.Errorf("topic not found: %s", name)
	}

	delete(b.topics, name)
	delete(b.offsets, name)
	return nil
}

func (b *MockBroker) GetTopicMetadata(topic string) (interface{}, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.topics[topic]; !ok {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	return map[string]interface{}{
		"topic":      topic,
		"partitions": 1,
		"messages":   len(b.topics[topic]),
	}, nil
}

func (b *MockBroker) Subscribe(topic, groupID string, partition int, mode protocol.SubscribeMode) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[topic]; !ok {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	switch mode {
	case protocol.SubscribeFromEarliest:
		return 0, nil
	case protocol.SubscribeFromLatest:
		return uint64(len(b.topics[topic])), nil
	case protocol.SubscribeFromCommit:
		if offset, ok := b.offsets[topic][groupID]; ok {
			return offset, nil
		}
		return 0, nil
	default:
		return 0, nil
	}
}

func (b *MockBroker) CommitOffset(topic, groupID string, partition int, offset uint64) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.offsets[topic]; !ok {
		b.offsets[topic] = make(map[string]uint64)
	}
	key := groupID
	currentOffset, exists := b.offsets[topic][key]
	changed := !exists || currentOffset != offset
	if changed {
		b.offsets[topic][key] = offset
	}
	return changed, nil
}

func (b *MockBroker) Fetch(topic string, partition int, offset uint64, maxMessages int) ([][]byte, uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	messages, ok := b.topics[topic]
	if !ok {
		return nil, 0, fmt.Errorf("topic not found: %s", topic)
	}

	start := int(offset)
	if start >= len(messages) {
		return [][]byte{}, uint64(len(messages)), nil
	}

	end := start + maxMessages
	if end > len(messages) {
		end = len(messages)
	}

	result := make([][]byte, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, messages[i].Data)
	}

	return result, uint64(end), nil
}

func (b *MockBroker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topics := make([]string, 0, len(b.topics))
	for topic := range b.topics {
		topics = append(topics, topic)
	}
	return topics
}

// Extended consumer-group helpers to satisfy server.Broker interface
func (b *MockBroker) ListConsumerGroups() []*broker.ConsumerGroupInfo {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []*broker.ConsumerGroupInfo
	for topic, groups := range b.offsets {
		for gid, off := range groups {
			offCopy := map[int]uint64{0: off}
			out = append(out, &broker.ConsumerGroupInfo{GroupID: gid, Topic: topic, Offsets: offCopy, Members: 1})
		}
	}
	return out
}

func (b *MockBroker) GetConsumerGroup(topic, groupID string) (*broker.ConsumerGroupInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	groups, ok := b.offsets[topic]
	if !ok {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}
	off, ok := groups[groupID]
	if !ok {
		return nil, fmt.Errorf("group not found: %s", groupID)
	}
	return &broker.ConsumerGroupInfo{GroupID: groupID, Topic: topic, Offsets: map[int]uint64{0: off}, Members: 1}, nil
}

func (b *MockBroker) DeleteConsumerGroup(topic, groupID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.offsets[topic]; !ok {
		return fmt.Errorf("topic not found: %s", topic)
	}
	delete(b.offsets[topic], groupID)
	return nil
}

func (b *MockBroker) GetTopicInfo(topic string) (*broker.TopicInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	msgs, ok := b.topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}
	var high uint64
	if len(msgs) > 0 {
		high = uint64(len(msgs) - 1)
	} else {
		high = 0
	}
	info := &broker.TopicInfo{Name: topic, Partitions: []broker.PartitionInfo{{ID: 0, LowestOffset: 0, HighestOffset: high}}}
	return info, nil
}

func (b *MockBroker) ConsumeWithKey(topic string, offset uint64) (key []byte, value []byte, err error) {
	return b.ConsumeFromPartitionWithKey(topic, 0, offset)
}

func (b *MockBroker) ConsumeFromPartitionWithKey(topic string, partition int, offset uint64) (key []byte, value []byte, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	messages, ok := b.topics[topic]
	if !ok {
		return nil, nil, fmt.Errorf("topic not found: %s", topic)
	}

	if offset >= uint64(len(messages)) {
		return nil, nil, fmt.Errorf("offset out of range: %d", offset)
	}

	return nil, messages[offset].Data, nil
}

func (b *MockBroker) FetchWithKeys(topic string, partition int, offset uint64, maxMessages int) ([]broker.FetchedMessage, uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	messages, ok := b.topics[topic]
	if !ok {
		return nil, 0, fmt.Errorf("topic not found: %s", topic)
	}

	if offset >= uint64(len(messages)) {
		return nil, offset, nil
	}

	start := int(offset)
	end := start + maxMessages
	if end > len(messages) {
		end = len(messages)
	}

	result := make([]broker.FetchedMessage, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, broker.FetchedMessage{
			Key:    nil,
			Value:  messages[i].Data,
			Offset: uint64(i),
		})
	}

	return result, uint64(end), nil
}

func (b *MockBroker) RegisterSchema(name, schemaType, definition, compatibility string) error {
	return nil // Mock implementation - just succeed
}

func (b *MockBroker) DeleteSchema(name string, version int) error {
	return nil // Mock implementation - just succeed
}

func (b *MockBroker) IsClusterMode() bool {
	return false // Mock is always standalone mode
}

func (b *MockBroker) ProposeCreateUser(username, passwordHash string, roles []string) error {
	return fmt.Errorf("not in cluster mode")
}

func (b *MockBroker) ProposeDeleteUser(username string) error {
	return fmt.Errorf("not in cluster mode")
}

func (b *MockBroker) ProposeUpdateUser(username string, roles []string, enabled *bool, passwordHash string) error {
	return fmt.Errorf("not in cluster mode")
}

func (b *MockBroker) ProposeSetACL(topic string, public bool, allowedUsers, allowedRoles []string) error {
	return fmt.Errorf("not in cluster mode")
}

func (b *MockBroker) ProposeDeleteACL(topic string) error {
	return fmt.Errorf("not in cluster mode")
}

func (b *MockBroker) GetZeroCopyInfo(topic string, partition int, offset uint64) (*os.File, int64, int64, error) {
	// Mock implementation - zero-copy not supported in tests
	return nil, 0, 0, fmt.Errorf("zero-copy not supported in mock")
}

func TestNewServer(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()

	server := NewServer(cfg, broker)
	if server == nil {
		t.Fatal("NewServer returned nil")
	}
	if server.config != cfg {
		t.Error("Server config not set correctly")
	}
	if server.broker != broker {
		t.Error("Server broker not set correctly")
	}
}

func TestServerStartStop(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	server := NewServer(cfg, broker)

	if err := server.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify server is running
	if !server.running {
		t.Error("Server not marked as running")
	}

	// Stop should be idempotent
	if err := server.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if server.running {
		t.Error("Server still marked as running after stop")
	}

	// Second stop should be safe
	if err := server.Stop(); err != nil {
		t.Fatalf("Second Stop failed: %v", err)
	}
}

func TestServerIsTLS(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	server := NewServer(cfg, broker)

	// Without TLS config
	if server.IsTLS() {
		t.Error("IsTLS() should return false without TLS config")
	}
}

func TestServerConnection(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	server := NewServer(cfg, broker)

	if err := server.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer server.Stop()

	// Get actual address
	addr := server.ln.Addr().String()

	// Connect to server
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Connection should be accepted
	if conn == nil {
		t.Error("Connection is nil")
	}
}

func TestHandleMessageUnknownOpcode(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	msg := &protocol.Message{
		Header:  protocol.Header{Op: 255}, // Unknown opcode
		Payload: []byte{},
	}

	err := server.handleMessage(&buf, msg)
	if err == nil {
		t.Error("Expected error for unknown opcode")
	}
}

func TestHandleCreateTopic(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	// Use binary protocol
	payload := protocol.EncodeBinaryCreateTopicRequest(&protocol.BinaryCreateTopicRequest{
		Topic:      "test-topic",
		Partitions: 3,
	})

	err := server.handleCreateTopic(&buf, payload, protocol.FlagBinary)
	if err != nil {
		t.Fatalf("handleCreateTopic failed: %v", err)
	}

	// Verify topic was created
	topics := broker.ListTopics()
	found := false
	for _, topic := range topics {
		if topic == "test-topic" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Topic was not created")
	}

	// Verify binary response
	msg, err := protocol.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	resp, err := protocol.DecodeBinaryCreateTopicResponse(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if !resp.Success {
		t.Error("Expected success response")
	}
}

func TestHandleProduce(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	broker.CreateTopic("test-topic", 1)
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	// Use binary protocol
	payload := protocol.EncodeBinaryProduceRequest(&protocol.BinaryProduceRequest{
		Topic: "test-topic",
		Value: []byte("hello"),
	})

	err := server.handleProduce(&buf, payload, protocol.FlagBinary)
	if err != nil {
		t.Fatalf("handleProduce failed: %v", err)
	}

	// Parse binary response
	msg, err := protocol.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	meta, err := protocol.DecodeRecordMetadata(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if meta.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", meta.Offset)
	}
	if meta.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", meta.Topic)
	}
}

func TestHandleConsume(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	broker.CreateTopic("test-topic", 1)
	broker.Produce("test-topic", []byte("test message"))
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	// Use binary protocol
	payload := protocol.EncodeBinaryConsumeRequest(&protocol.BinaryConsumeRequest{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    0,
	})

	err := server.handleConsume(&buf, payload, protocol.FlagBinary)
	if err != nil {
		t.Fatalf("handleConsume failed: %v", err)
	}

	msg, err := protocol.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	resp, err := protocol.DecodeBinaryConsumeResponse(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if string(resp.Value) != "test message" {
		t.Errorf("Expected 'test message', got %s", resp.Value)
	}
}

func TestHandleFetch(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	broker.CreateTopic("test-topic", 1)
	broker.Produce("test-topic", []byte("msg1"))
	broker.Produce("test-topic", []byte("msg2"))
	broker.Produce("test-topic", []byte("msg3"))
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	// Use binary protocol
	payload := protocol.EncodeBinaryFetchRequest(&protocol.BinaryFetchRequest{
		Topic:       "test-topic",
		Partition:   0,
		Offset:      0,
		MaxMessages: 10,
	})

	err := server.handleFetch(&buf, payload, protocol.FlagBinary)
	if err != nil {
		t.Fatalf("handleFetch failed: %v", err)
	}

	msg, err := protocol.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	resp, err := protocol.DecodeBinaryFetchResponse(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.Messages) != 3 {
		t.Errorf("Expected 3 messages, got %d", len(resp.Messages))
	}
	if resp.NextOffset != 3 {
		t.Errorf("Expected next offset 3, got %d", resp.NextOffset)
	}
}

func TestHandleSubscribe(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	broker.CreateTopic("test-topic", 1)
	broker.Produce("test-topic", []byte("msg1"))
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	// Use binary protocol
	payload := protocol.EncodeBinarySubscribeRequest(&protocol.BinarySubscribeRequest{
		Topic:     "test-topic",
		GroupID:   "group1",
		Partition: 0,
		Mode:      "earliest",
	})

	err := server.handleSubscribe(&buf, payload, protocol.FlagBinary)
	if err != nil {
		t.Fatalf("handleSubscribe failed: %v", err)
	}

	msg, err := protocol.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	resp, err := protocol.DecodeBinarySubscribeResponse(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.Offset != 0 {
		t.Errorf("Expected offset 0 for earliest, got %d", resp.Offset)
	}
}

func TestHandleListTopics(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	broker.CreateTopic("topic1", 1)
	broker.CreateTopic("topic2", 1)
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	err := server.handleListTopics(&buf)
	if err != nil {
		t.Fatalf("handleListTopics failed: %v", err)
	}

	msg, err := protocol.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}

	// Use binary decode
	resp, err := protocol.DecodeBinaryListTopicsResponse(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(resp.Topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(resp.Topics))
	}
}

func TestHandleDeleteTopic(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	broker.CreateTopic("test-topic", 1)
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	// Use binary protocol
	payload := protocol.EncodeBinaryDeleteTopicRequest(&protocol.BinaryDeleteTopicRequest{
		Topic: "test-topic",
	})

	err := server.handleDeleteTopic(&buf, payload, protocol.FlagBinary)
	if err != nil {
		t.Fatalf("handleDeleteTopic failed: %v", err)
	}

	// Verify topic was deleted
	topics := broker.ListTopics()
	for _, topic := range topics {
		if topic == "test-topic" {
			t.Error("Topic was not deleted")
		}
	}
}

func TestHandleCommit(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	// Use binary protocol
	payload := protocol.EncodeBinaryCommitRequest(&protocol.BinaryCommitRequest{
		Topic:     "test-topic",
		GroupID:   "group1",
		Partition: 0,
		Offset:    5,
	})

	err := server.handleCommit(&buf, payload, protocol.FlagBinary)
	if err != nil {
		t.Fatalf("handleCommit failed: %v", err)
	}
}

func TestHandleProduceDelayed(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	broker.CreateTopic("test-topic", 1)
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	// Use binary protocol
	payload := protocol.EncodeBinaryProduceDelayedRequest(&protocol.BinaryProduceDelayedRequest{
		Topic:   "test-topic",
		Data:    []byte("delayed"),
		DelayMs: 1000,
	})

	err := server.handleProduceDelayed(&buf, payload)
	if err != nil {
		t.Fatalf("handleProduceDelayed failed: %v", err)
	}
}

func TestHandleProduceWithTTL(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	broker.CreateTopic("test-topic", 1)
	server := NewServer(cfg, broker)

	var buf bytes.Buffer
	// Use binary protocol
	payload := protocol.EncodeBinaryProduceWithTTLRequest(&protocol.BinaryProduceWithTTLRequest{
		Topic: "test-topic",
		Data:  []byte("ttl message"),
		TTLMs: 60000,
	})

	err := server.handleProduceWithTTL(&buf, payload)
	if err != nil {
		t.Fatalf("handleProduceWithTTL failed: %v", err)
	}
}

func TestHandleTransactionOperations(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	broker.CreateTopic("test-topic", 1)
	server := NewServer(cfg, broker)

	// Begin transaction - uses empty binary request
	var buf bytes.Buffer
	err := server.handleBeginTx(&buf, []byte{})
	if err != nil {
		t.Fatalf("handleBeginTx failed: %v", err)
	}

	msg, _ := protocol.ReadMessage(&buf)
	beginResp, err := protocol.DecodeBinaryTxnResponse(msg.Payload)
	if err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if beginResp.TxnID == "" {
		t.Error("Expected non-empty transaction ID")
	}

	// Produce in transaction
	buf.Reset()
	payload := protocol.EncodeBinaryTxnProduceRequest(&protocol.BinaryTxnProduceRequest{
		TxnID: beginResp.TxnID,
		Topic: "test-topic",
		Data:  []byte("tx msg"),
	})
	err = server.handleProduceTx(&buf, payload)
	if err != nil {
		t.Fatalf("handleProduceTx failed: %v", err)
	}

	// Commit transaction
	buf.Reset()
	payload = protocol.EncodeBinaryTxnRequest(&protocol.BinaryTxnRequest{TxnID: beginResp.TxnID})
	err = server.handleCommitTx(&buf, payload)
	if err != nil {
		t.Fatalf("handleCommitTx failed: %v", err)
	}

	// Abort transaction (separate)
	buf.Reset()
	err = server.handleAbortTx(&buf, payload)
	if err != nil {
		t.Fatalf("handleAbortTx failed: %v", err)
	}
}

func TestHandleDLQOperations(t *testing.T) {
	cfg := &config.Config{
		BindAddr: "127.0.0.1:0",
		DataDir:  t.TempDir(),
	}
	broker := NewMockBroker()
	server := NewServer(cfg, broker)

	// Get DLQ messages
	var buf bytes.Buffer
	payload := protocol.EncodeBinaryDLQRequest(&protocol.BinaryDLQRequest{
		Topic:       "test-topic",
		MaxMessages: 10,
	})
	err := server.handleGetDLQMessages(&buf, payload)
	if err != nil {
		t.Fatalf("handleGetDLQMessages failed: %v", err)
	}

	// Replay DLQ
	buf.Reset()
	replayPayload := protocol.EncodeBinaryReplayDLQRequest(&protocol.BinaryReplayDLQRequest{
		Topic:     "test-topic",
		MessageID: "msg-1",
	})
	err = server.handleReplayDLQ(&buf, replayPayload)
	if err != nil {
		t.Fatalf("handleReplayDLQ failed: %v", err)
	}

	// Purge DLQ
	buf.Reset()
	purgePayload := protocol.EncodeBinaryPurgeDLQRequest(&protocol.BinaryPurgeDLQRequest{
		Topic: "test-topic",
	})
	err = server.handlePurgeDLQ(&buf, purgePayload)
	if err != nil {
		t.Fatalf("handlePurgeDLQ failed: %v", err)
	}
}

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
Data Replication for FlyMQ.

OVERVIEW:
=========
Replication ensures data durability by copying messages to multiple
nodes. Uses a leader-follower model similar to Kafka.

REPLICATION FLOW:
=================
1. Producer sends message to partition leader
2. Leader writes to local log
3. Followers fetch from leader (pull-based)
4. Leader waits for MinISR acknowledgments
5. Message is committed and visible to consumers

IN-SYNC REPLICAS (ISR):
=======================
A replica is "in-sync" if:
- It has fetched all messages up to the high watermark
- It has sent a fetch request within replica.lag.time.max.ms

CONFIGURATION:
==============
- ReplicationFactor: Total replicas per partition (default: 3)
- MinISR: Minimum replicas for write acknowledgment (default: 2)
*/
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"flymq/internal/logging"
)

// ReplicationConfig holds configuration for replication.
type ReplicationConfig struct {
	NodeID            string
	ReplicationFactor int
	MinISR            int
	FetchInterval     time.Duration
	FetchMaxBytes     int
	Timeout           time.Duration
}

// DefaultReplicationConfig returns default replication configuration.
func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		ReplicationFactor: 3,
		MinISR:            2,
		FetchInterval:     100 * time.Millisecond,
		FetchMaxBytes:     1024 * 1024, // 1MB
		Timeout:           10 * time.Second,
	}
}

// ReplicaFetcher fetches data from the leader to replicate locally.
type ReplicaFetcher struct {
	mu            sync.RWMutex
	config        ReplicationConfig
	topic         string
	partition     int
	leaderAddr    string
	highWatermark uint64
	logger        *logging.Logger

	conn   net.Conn
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Callback to write fetched data
	writeCallback func(offset uint64, data []byte) error
}

// FetchRequest represents a request to fetch data from leader.
type FetchRequest struct {
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	StartOffset uint64 `json:"start_offset"`
	MaxBytes    int    `json:"max_bytes"`
}

// FetchResponse represents a response from the leader.
type FetchResponse struct {
	Topic         string        `json:"topic"`
	Partition     int           `json:"partition"`
	HighWatermark uint64        `json:"high_watermark"`
	Records       []FetchRecord `json:"records"`
	Error         string        `json:"error,omitempty"`
}

// FetchRecord represents a single record in a fetch response.
type FetchRecord struct {
	Offset uint64 `json:"offset"`
	Data   []byte `json:"data"`
}

// NewReplicaFetcher creates a new replica fetcher.
func NewReplicaFetcher(config ReplicationConfig, topic string, partition int, leaderAddr string) *ReplicaFetcher {
	return &ReplicaFetcher{
		config:     config,
		topic:      topic,
		partition:  partition,
		leaderAddr: leaderAddr,
		stopCh:     make(chan struct{}),
		logger:     logging.NewLogger("replication"),
	}
}

// SetWriteCallback sets the callback for writing fetched data.
func (rf *ReplicaFetcher) SetWriteCallback(cb func(offset uint64, data []byte) error) {
	rf.writeCallback = cb
}

// Start begins fetching from the leader.
func (rf *ReplicaFetcher) Start(ctx context.Context, startOffset uint64) error {
	rf.mu.Lock()
	rf.highWatermark = startOffset
	rf.mu.Unlock()

	rf.wg.Add(1)
	go rf.fetchLoop(ctx)
	return nil
}

// Stop stops the fetcher.
func (rf *ReplicaFetcher) Stop() error {
	close(rf.stopCh)
	rf.wg.Wait()
	if rf.conn != nil {
		rf.conn.Close()
	}
	return nil
}

// HighWatermark returns the current high watermark.
func (rf *ReplicaFetcher) HighWatermark() uint64 {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.highWatermark
}

// UpdateLeader updates the leader address.
func (rf *ReplicaFetcher) UpdateLeader(leaderAddr string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.leaderAddr != leaderAddr {
		rf.leaderAddr = leaderAddr
		if rf.conn != nil {
			rf.conn.Close()
			rf.conn = nil
		}
	}
}

func (rf *ReplicaFetcher) fetchLoop(ctx context.Context) {
	defer rf.wg.Done()

	ticker := time.NewTicker(rf.config.FetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rf.stopCh:
			return
		case <-ticker.C:
			if err := rf.fetch(); err != nil {
				rf.logger.Error("Fetch error", "error", err, "topic", rf.topic, "partition", rf.partition)
			}
		}
	}
}

func (rf *ReplicaFetcher) fetch() error {
	if err := rf.ensureConnection(); err != nil {
		return err
	}

	rf.mu.RLock()
	startOffset := rf.highWatermark
	rf.mu.RUnlock()

	req := FetchRequest{
		Topic:       rf.topic,
		Partition:   rf.partition,
		StartOffset: startOffset,
		MaxBytes:    rf.config.FetchMaxBytes,
	}

	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	// Send request
	if err := rf.sendRequest(data); err != nil {
		rf.conn.Close()
		rf.conn = nil
		return err
	}

	// Read response
	resp, err := rf.readResponse()
	if err != nil {
		rf.conn.Close()
		rf.conn = nil
		return err
	}

	if resp.Error != "" {
		return fmt.Errorf("fetch error: %s", resp.Error)
	}

	// Process records
	for _, record := range resp.Records {
		if rf.writeCallback != nil {
			if err := rf.writeCallback(record.Offset, record.Data); err != nil {
				return err
			}
		}
	}

	// Update high watermark
	if len(resp.Records) > 0 {
		rf.mu.Lock()
		rf.highWatermark = resp.Records[len(resp.Records)-1].Offset + 1
		rf.mu.Unlock()
	}

	return nil
}

func (rf *ReplicaFetcher) ensureConnection() error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", rf.leaderAddr, rf.config.Timeout)
	if err != nil {
		return fmt.Errorf("failed to connect to leader %s: %w", rf.leaderAddr, err)
	}

	rf.conn = conn
	return nil
}

func (rf *ReplicaFetcher) sendRequest(data []byte) error {
	// Simple length-prefixed protocol
	header := make([]byte, 4)
	header[0] = byte(len(data) >> 24)
	header[1] = byte(len(data) >> 16)
	header[2] = byte(len(data) >> 8)
	header[3] = byte(len(data))

	rf.conn.SetWriteDeadline(time.Now().Add(rf.config.Timeout))
	if _, err := rf.conn.Write(header); err != nil {
		return err
	}
	_, err := rf.conn.Write(data)
	return err
}

func (rf *ReplicaFetcher) readResponse() (*FetchResponse, error) {
	rf.conn.SetReadDeadline(time.Now().Add(rf.config.Timeout))

	// Read length
	header := make([]byte, 4)
	if _, err := io.ReadFull(rf.conn, header); err != nil {
		return nil, err
	}
	length := uint32(header[0])<<24 | uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3])

	// Read body
	body := make([]byte, length)
	if _, err := io.ReadFull(rf.conn, body); err != nil {
		return nil, err
	}

	var resp FetchResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// ReplicationManager manages replication for all partitions.
type ReplicationManager struct {
	mu       sync.RWMutex
	config   ReplicationConfig
	fetchers map[string]*ReplicaFetcher // key: "topic-partition"
	logger   *logging.Logger
	stopCh   chan struct{}
}

// NewReplicationManager creates a new replication manager.
func NewReplicationManager(config ReplicationConfig) *ReplicationManager {
	return &ReplicationManager{
		config:   config,
		fetchers: make(map[string]*ReplicaFetcher),
		logger:   logging.NewLogger("replication-mgr"),
		stopCh:   make(chan struct{}),
	}
}

// StartReplication starts replication for a partition.
func (rm *ReplicationManager) StartReplication(ctx context.Context, topic string, partition int, leaderAddr string, startOffset uint64, writeCallback func(uint64, []byte) error) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if _, exists := rm.fetchers[key]; exists {
		return fmt.Errorf("replication already running for %s", key)
	}

	fetcher := NewReplicaFetcher(rm.config, topic, partition, leaderAddr)
	fetcher.SetWriteCallback(writeCallback)

	if err := fetcher.Start(ctx, startOffset); err != nil {
		return err
	}

	rm.fetchers[key] = fetcher
	rm.logger.Info("Started replication", "topic", topic, "partition", partition, "leader", leaderAddr)
	return nil
}

// StopReplication stops replication for a partition.
func (rm *ReplicationManager) StopReplication(topic string, partition int) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	fetcher, exists := rm.fetchers[key]
	if !exists {
		return nil
	}

	if err := fetcher.Stop(); err != nil {
		return err
	}

	delete(rm.fetchers, key)
	rm.logger.Info("Stopped replication", "topic", topic, "partition", partition)
	return nil
}

// UpdateLeader updates the leader for a partition.
func (rm *ReplicationManager) UpdateLeader(topic string, partition int, newLeader string) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if fetcher, exists := rm.fetchers[key]; exists {
		fetcher.UpdateLeader(newLeader)
	}
}

// Stop stops all replication.
func (rm *ReplicationManager) Stop() error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	close(rm.stopCh)

	for key, fetcher := range rm.fetchers {
		if err := fetcher.Stop(); err != nil {
			rm.logger.Error("Failed to stop fetcher", "key", key, "error", err)
		}
	}

	rm.fetchers = make(map[string]*ReplicaFetcher)
	return nil
}

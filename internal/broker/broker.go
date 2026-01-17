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
Package broker implements the core message broker logic for FlyMQ.

ARCHITECTURE OVERVIEW:
======================
The broker is the heart of FlyMQ. It manages topics, partitions, and
coordinates message production and consumption.

HIERARCHY:
==========

	Broker
	 └── Topics (map[string]*Topic)
	      └── Partitions ([]*Partition)
	           └── Log (storage.Log)
	                └── Segments

TOPICS AND PARTITIONS:
======================
- A Topic is a named stream of messages (e.g., "orders", "user-events")
- Each Topic has one or more Partitions for parallelism
- Each Partition is an independent, ordered log of messages
- Messages within a partition are strictly ordered
- Messages across partitions have no ordering guarantee

WHY PARTITIONS?
===============
Partitions enable horizontal scaling:
1. Multiple consumers can read different partitions in parallel
2. Partitions can be distributed across multiple brokers (in clustered mode)
3. Each partition can be replicated for fault tolerance

CONSUMER GROUPS:
================
Consumer groups enable load balancing across consumers:
- Multiple consumers in a group share the partitions
- Each partition is consumed by exactly one consumer in the group
- If a consumer fails, its partitions are reassigned

OFFSET MANAGEMENT:
==================
- Each message has an offset (sequence number) within its partition
- Consumers track their position using offsets
- Committed offsets are persisted for crash recovery
- Consumers can seek to any offset (replay, skip, etc.)

THREAD SAFETY:
==============
- The broker uses RWMutex for concurrent access
- Topic operations (create, delete) require write lock
- Message operations (produce, consume) use read lock
- Each partition's log is independently thread-safe

DATA DIRECTORY STRUCTURE:
=========================

	{DataDir}/
	 ├── orders/           # Topic "orders"
	 │    ├── 0/           # Partition 0
	 │    │    ├── 0.store
	 │    │    └── 0.index
	 │    └── 1/           # Partition 1
	 │         ├── 0.store
	 │         └── 0.index
	 └── __consumer_offsets/  # Internal topic for offset storage
*/
package broker

import (
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"flymq/internal/config"
	"flymq/internal/logging"
	"flymq/internal/protocol"
	"flymq/internal/storage"
)

// Partition represents a single partition within a topic.
// Each partition is an independent, ordered log of messages.
type Partition struct {
	ID  int          // Partition number (0-indexed)
	Log *storage.Log // The underlying storage log
}

// Topic represents a named message stream with one or more partitions.
type Topic struct {
	Name       string       // Topic name (e.g., "orders")
	Partitions []*Partition // Ordered list of partitions
	rrCounter  uint64       // Round-robin counter for partition selection
	mu         sync.RWMutex // Protects partition access
}

// SchemaRegistryApplier is an interface for applying schema operations.
// This provides a way for the broker to apply schema changes without
// directly depending on the schema package.
type SchemaRegistryApplier interface {
	ApplyRegister(name, schemaType, definition, compatibility string) error
	ApplyDelete(name string, version int) error
}

// Broker is the central component that manages topics and message flow.
// It coordinates producers, consumers, and storage.
type Broker struct {
	config        *config.Config        // Server configuration
	topics        map[string]*Topic     // Topic registry
	consumers     *ConsumerGroupManager // Manages consumer groups and offsets
	mu            sync.RWMutex          // Protects topics map
	logger        *logging.Logger       // Broker-level logging
	cluster       Cluster               // Cluster interface
	schemaApplier SchemaRegistryApplier // Schema applier for cluster replication
}

// Cluster interface defines methods needed by the broker for cluster operations.
type Cluster interface {
	GetMembers() []*ClusterMember
	IsLeader() bool
	LeaderID() string
	LeaderAddr() string
	GetRaftState() RaftState
	ProposeCreateTopic(name string, partitions int) error
	ProposeDeleteTopic(name string) error
	ProposeMessage(topic string, partition int, key, value []byte) (int64, error)
	ProposeRegisterSchema(name, schemaType, definition, compatibility string) error
	ProposeDeleteSchema(name string, version int) error
	// User management cluster operations
	ProposeCreateUser(username, passwordHash string, roles []string) error
	ProposeDeleteUser(username string) error
	ProposeUpdateUser(username string, roles []string, enabled *bool, passwordHash string) error
	// ACL management cluster operations
	ProposeSetACL(topic string, public bool, allowedUsers, allowedRoles []string) error
	ProposeDeleteACL(topic string) error

	// Partition-level leadership for horizontal scaling
	// These methods enable routing to partition leaders instead of Raft leader
	IsPartitionLeader(topic string, partition int) bool
	GetPartitionLeaderInfo(topic string, partition int) (*PartitionLeaderInfo, bool)
	GetAllPartitionLeaders(topic string) []*PartitionLeaderInfo
	GetPartitionAssignment(topic string, partition int) (*PartitionAssignment, bool)
	GetTopicPartitionAssignments(topic string) []*PartitionAssignment
	ProposeAssignPartition(topic string, partition int, leader, leaderAddr string, replicas []string, replicaAddrs map[string]string) error

	// Partition management operations
	TriggerRebalance() error
	ReassignPartition(topic string, partition int, newLeader string, replicas []string) error

	// Node information for inter-node communication
	NodeID() string
	NodeAddr() string // Client-facing address of this node
}

// PartitionAssignment represents the full assignment of a partition including replicas and ISR.
type PartitionAssignment struct {
	Topic        string            `json:"topic"`
	Partition    int               `json:"partition"`
	Leader       string            `json:"leader"`       // Node ID of the partition leader
	LeaderAddr   string            `json:"leader_addr"`  // Client-facing address of leader
	Replicas     []string          `json:"replicas"`     // Node IDs of all replicas (including leader)
	ISR          []string          `json:"isr"`          // In-Sync Replicas (node IDs)
	State        string            `json:"state"`        // "online", "offline", "reassigning", "syncing"
	ReplicaAddrs map[string]string `json:"replica_addrs"` // Node ID -> client address
	Epoch        uint64            `json:"epoch"`        // Incremented on leader change for fencing
}

// PartitionLeaderInfo contains routing information for a partition leader.
// Used by clients and brokers to route requests to the correct node.
type PartitionLeaderInfo struct {
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	LeaderID    string `json:"leader_id"`
	LeaderAddr  string `json:"leader_addr"`  // Client-facing address (host:port)
	Epoch       uint64 `json:"epoch"`        // For leader fencing
	IsLocalNode bool   `json:"-"`            // True if this node is the leader
}

// RaftState represents the current Raft consensus state.
type RaftState struct {
	Term        uint64
	CommitIndex uint64
	LastApplied uint64
	State       string // "follower", "candidate", "leader"
	LeaderID    string
	LogLength   int
}

// ClusterMember represents a node in the cluster with full metadata.
type ClusterMember struct {
	ID          string
	Address     string
	ClusterAddr string
	AdminAddr   string
	Status      string
	RaftState   string
	IsLeader    bool
	JoinedAt    string
	LastSeen    string
	Uptime      string
	Stats       *NodeStats
}

// NodeStats contains runtime statistics for a node.
type NodeStats struct {
	MemoryUsedMB     float64
	MemoryAllocMB    float64
	MemorySysMB      float64
	Goroutines       int
	NumGC            uint32
	TopicCount       int
	PartitionCount   int
	MessagesReceived int64
	MessagesSent     int64
	BytesReceived    int64
	BytesSent        int64
}

// TopicMetadata contains detailed information about a topic.
type TopicMetadata struct {
	Name       string              `json:"name"`
	Partitions []PartitionMetadata `json:"partitions"`
}

// PartitionMetadata contains detailed information about a partition.
type PartitionMetadata struct {
	ID            int    `json:"id"`
	LowestOffset  uint64 `json:"lowest_offset"`
	HighestOffset uint64 `json:"highest_offset"`
	MessageCount  uint64 `json:"message_count"`
}

// SetCluster sets the cluster instance.
func (b *Broker) SetCluster(c Cluster) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cluster = c
}

// SetSchemaApplier sets the schema applier instance for cluster replication.
func (b *Broker) SetSchemaApplier(sa SchemaRegistryApplier) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.schemaApplier = sa
}

// RegisterSchema registers a schema in the cluster.
// In cluster mode, routes through Raft consensus for replication.
func (b *Broker) RegisterSchema(name, schemaType, definition, compatibility string) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	// If we have a cluster, route through Raft
	if cluster != nil {
		return cluster.ProposeRegisterSchema(name, schemaType, definition, compatibility)
	}

	// Standalone mode - register directly
	return b.ApplyRegisterSchema(name, schemaType, definition, compatibility)
}

// DeleteSchema deletes a schema version from the cluster.
// In cluster mode, routes through Raft consensus for replication.
func (b *Broker) DeleteSchema(name string, version int) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	// If we have a cluster, route through Raft
	if cluster != nil {
		return cluster.ProposeDeleteSchema(name, version)
	}

	// Standalone mode - delete directly
	return b.ApplyDeleteSchema(name, version)
}

// ApplyRegisterSchema directly registers a schema (called by Raft apply or standalone mode).
func (b *Broker) ApplyRegisterSchema(name, schemaType, definition, compatibility string) error {
	b.mu.RLock()
	sa := b.schemaApplier
	b.mu.RUnlock()

	if sa == nil {
		b.logger.Warn("Schema applier not set, cannot register schema", "name", name)
		return nil // Not an error - schema registry may not be configured
	}

	if err := sa.ApplyRegister(name, schemaType, definition, compatibility); err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}

	b.logger.Info("Registered schema", "name", name, "type", schemaType)
	return nil
}

// ApplyDeleteSchema directly deletes a schema (called by Raft apply or standalone mode).
func (b *Broker) ApplyDeleteSchema(name string, version int) error {
	b.mu.RLock()
	sa := b.schemaApplier
	b.mu.RUnlock()

	if sa == nil {
		b.logger.Warn("Schema applier not set, cannot delete schema", "name", name)
		return nil // Not an error - schema registry may not be configured
	}

	if err := sa.ApplyDelete(name, version); err != nil {
		return fmt.Errorf("failed to delete schema: %w", err)
	}

	b.logger.Info("Deleted schema", "name", name, "version", version)
	return nil
}

// ProposeCreateUser proposes a user creation through the cluster.
// The password should already be hashed before calling this method.
func (b *Broker) ProposeCreateUser(username, passwordHash string, roles []string) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	if cluster == nil {
		return fmt.Errorf("not in cluster mode")
	}

	return cluster.ProposeCreateUser(username, passwordHash, roles)
}

// ProposeDeleteUser proposes a user deletion through the cluster.
func (b *Broker) ProposeDeleteUser(username string) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	if cluster == nil {
		return fmt.Errorf("not in cluster mode")
	}

	return cluster.ProposeDeleteUser(username)
}

// ProposeUpdateUser proposes a user update through the cluster.
// The password should already be hashed before calling this method.
func (b *Broker) ProposeUpdateUser(username string, roles []string, enabled *bool, passwordHash string) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	if cluster == nil {
		return fmt.Errorf("not in cluster mode")
	}

	return cluster.ProposeUpdateUser(username, roles, enabled, passwordHash)
}

// ProposeSetACL proposes an ACL setting through the cluster.
func (b *Broker) ProposeSetACL(topic string, public bool, allowedUsers, allowedRoles []string) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	if cluster == nil {
		return fmt.Errorf("not in cluster mode")
	}

	return cluster.ProposeSetACL(topic, public, allowedUsers, allowedRoles)
}

// ProposeDeleteACL proposes an ACL deletion through the cluster.
func (b *Broker) ProposeDeleteACL(topic string) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	if cluster == nil {
		return fmt.Errorf("not in cluster mode")
	}

	return cluster.ProposeDeleteACL(topic)
}

// IsClusterMode returns true if the broker is running in cluster mode.
func (b *Broker) IsClusterMode() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.cluster != nil
}

// TriggerRebalance triggers a partition rebalance to distribute leaders evenly.
// Returns an error if not in cluster mode or if the rebalance fails.
func (b *Broker) TriggerRebalance() error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	if cluster == nil {
		return fmt.Errorf("partition rebalancing is only available in cluster mode")
	}

	return cluster.TriggerRebalance()
}

// ReassignPartition reassigns a partition to a new leader and/or replicas.
// Returns an error if not in cluster mode or if the reassignment fails.
func (b *Broker) ReassignPartition(topic string, partition int, newLeader string, replicas []string) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	if cluster == nil {
		return fmt.Errorf("partition reassignment is only available in cluster mode")
	}

	return cluster.ReassignPartition(topic, partition, newLeader, replicas)
}

// GetClusterInfo returns basic information about the cluster.
func (b *Broker) GetClusterInfo() (int, string, []string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.cluster == nil {
		return 1, b.config.NodeID, []string{b.config.NodeID}
	}

	members := b.cluster.GetMembers()
	nodeIDs := make([]string, len(members))
	for i, m := range members {
		nodeIDs[i] = m.ID
	}

	return len(members), b.cluster.LeaderID(), nodeIDs
}

// GetClusterInfoFull returns detailed cluster information including all member metadata.
func (b *Broker) GetClusterInfoFull() (int, string, []*ClusterMember) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.cluster == nil {
		// Standalone mode - return self info
		return 1, b.config.NodeID, []*ClusterMember{{
			ID:        b.config.NodeID,
			Address:   b.config.BindAddr,
			Status:    "alive",
			RaftState: "standalone",
			IsLeader:  true,
		}}
	}

	members := b.cluster.GetMembers()
	return len(members), b.cluster.LeaderID(), members
}

// GetRaftState returns the current Raft consensus state, or nil if not in cluster mode.
func (b *Broker) GetRaftState() *RaftState {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.cluster == nil {
		return nil
	}

	state := b.cluster.GetRaftState()
	return &state
}

// NewBroker creates a new broker instance and loads existing topics from disk.
// This is called once at server startup.
func NewBroker(cfg *config.Config) (*Broker, error) {
	consumers, err := NewConsumerGroupManager(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group manager: %w", err)
	}

	b := &Broker{
		config:    cfg,
		topics:    make(map[string]*Topic),
		consumers: consumers,
		logger:    logging.NewLogger("broker"),
	}

	// Log performance configuration for debugging
	b.logger.Info("Performance configuration",
		"acks", cfg.Performance.Acks,
		"sync_interval_ms", cfg.Performance.SyncIntervalMs,
		"sync_batch_size", cfg.Performance.SyncBatchSize,
		"write_buffer_size", cfg.Performance.WriteBufferSize,
		"num_io_workers", cfg.Performance.NumIOWorkers,
		"async_io", cfg.Performance.AsyncIO,
		"zero_copy", cfg.Performance.ZeroCopy,
		"compression", cfg.Performance.Compression,
		"compression_level", cfg.Performance.CompressionLevel,
		"compression_min_size", cfg.Performance.CompressionMinSize,
		"large_message_threshold", cfg.Performance.LargeMessageThreshold,
	)

	// Load existing topics from disk for crash recovery
	if err := b.loadTopics(); err != nil {
		return nil, err
	}
	return b, nil
}

// internalDirectories lists directories in the data directory that are not topics.
// These are used for internal storage (schemas, raft state, etc.)
var internalDirectories = map[string]bool{
	"schemas": true, // Schema registry storage
	"raft":    true, // Raft consensus state
	"cluster": true, // Cluster metadata
}

// loadTopics scans the data directory and loads all existing topics.
// This enables the broker to recover state after a restart.
func (b *Broker) loadTopics() error {
	entries, err := os.ReadDir(b.config.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(b.config.DataDir, 0755)
		}
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		topicName := entry.Name()

		// Skip internal directories that are not topics
		if internalDirectories[topicName] {
			b.logger.Debug("Skipping internal directory", "dir", topicName)
			continue
		}

		// Load partitions for this topic
		pEntries, err := os.ReadDir(filepath.Join(b.config.DataDir, topicName))
		if err != nil {
			continue
		}

		topic := &Topic{
			Name:       topicName,
			Partitions: make([]*Partition, 0),
		}

		for _, pEntry := range pEntries {
			if !pEntry.IsDir() {
				continue
			}
			// Parse partition ID from directory name (must be a valid integer)
			var pID int
			if _, err := fmt.Sscanf(pEntry.Name(), "%d", &pID); err != nil {
				// Skip directories that aren't valid partition IDs
				b.logger.Debug("Skipping non-partition directory", "dir", pEntry.Name())
				continue
			}

			pDir := filepath.Join(b.config.DataDir, topicName, pEntry.Name())
			logConfig := storage.Config{
				Segment: storage.SegmentConfig{
					MaxStoreBytes: uint64(b.config.SegmentBytes),
					MaxIndexBytes: 1024 * 1024, // 1MB index
					InitialOffset: 0,
				},
				Performance: storage.PerformanceConfig{
					Acks:             b.config.Performance.Acks,
					SyncIntervalMs:   b.config.Performance.SyncIntervalMs,
					SyncBatchSize:    b.config.Performance.SyncBatchSize,
					WriteBufferSize:  b.config.Performance.WriteBufferSize,
					Compression:      b.config.Performance.Compression,
					CompressionLevel: b.config.Performance.CompressionLevel,
					CompressionMin:   b.config.Performance.CompressionMinSize,
				},
			}
			log, err := storage.NewLog(pDir, logConfig)
			if err != nil {
				return err
			}
			topic.Partitions = append(topic.Partitions, &Partition{
				ID:  pID,
				Log: log,
			})
		}

		// Only register topics that have at least one partition
		// This prevents empty directories from appearing as topics
		if len(topic.Partitions) > 0 {
			b.topics[topicName] = topic
		} else {
			b.logger.Debug("Skipping directory with no partitions", "dir", topicName)
		}
	}
	return nil
}

// CreateTopic creates a new topic with the specified number of partitions.
// In cluster mode, this routes through Raft consensus.
// Each partition gets its own directory and storage log.
// Returns an error if the topic already exists.
func (b *Broker) CreateTopic(name string, partitions int) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	// If we have a cluster, route through Raft
	if cluster != nil {
		return cluster.ProposeCreateTopic(name, partitions)
	}

	// Standalone mode - create directly
	return b.ApplyCreateTopic(name, partitions)
}

// ApplyCreateTopic directly creates a topic (called by Raft apply or standalone mode).
// This is the actual implementation that creates storage.
func (b *Broker) ApplyCreateTopic(name string, partitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		// In cluster mode, this is idempotent - topic may already exist from replay
		b.logger.Debug("Topic already exists, skipping", "topic", name)
		return nil
	}

	topic := &Topic{
		Name:       name,
		Partitions: make([]*Partition, partitions),
	}

	// Create each partition with its own storage log
	for i := 0; i < partitions; i++ {
		pDir := filepath.Join(b.config.DataDir, name, fmt.Sprintf("%d", i))
		if err := os.MkdirAll(pDir, 0755); err != nil {
			return err
		}
		logConfig := storage.Config{
			Segment: storage.SegmentConfig{
				MaxStoreBytes: uint64(b.config.SegmentBytes),
				MaxIndexBytes: 1024 * 1024,
				InitialOffset: 0,
			},
			Performance: storage.PerformanceConfig{
				Acks:             b.config.Performance.Acks,
				SyncIntervalMs:   b.config.Performance.SyncIntervalMs,
				SyncBatchSize:    b.config.Performance.SyncBatchSize,
				WriteBufferSize:  b.config.Performance.WriteBufferSize,
				Compression:      b.config.Performance.Compression,
				CompressionLevel: b.config.Performance.CompressionLevel,
				CompressionMin:   b.config.Performance.CompressionMinSize,
			},
		}
		log, err := storage.NewLog(pDir, logConfig)
		if err != nil {
			return err
		}
		topic.Partitions[i] = &Partition{
			ID:  i,
			Log: log,
		}
	}

	b.topics[name] = topic
	b.logger.Info("Created topic", "topic", name, "partitions", partitions)
	return nil
}

// Produce writes a message to a topic and returns the assigned offset.
// If the topic doesn't exist, it's auto-created with 1 partition.
// In cluster mode, this routes through Raft consensus for replication.
// Uses round-robin partition selection when no key is provided.
func (b *Broker) Produce(topic string, data []byte) (uint64, error) {
	return b.ProduceWithKey(topic, nil, data)
}

// ProduceWithKey writes a message to a topic with an optional key.
// If key is provided, the partition is determined by hashing the key.
// If key is nil, round-robin partition selection is used.
// In cluster mode, uses partition-level leadership for horizontal scaling.
// Each partition can have a different leader, distributing write load across nodes.
func (b *Broker) ProduceWithKey(topic string, key, data []byte) (uint64, error) {
	offset, _, err := b.ProduceWithKeyAndPartition(topic, key, data)
	return offset, err
}

// ProduceWithKeyAndPartition writes a message to a topic with an optional key
// and returns both the offset and the partition the message was written to.
// This is useful when the caller needs to know the partition for the response.
func (b *Broker) ProduceWithKeyAndPartition(topic string, key, data []byte) (uint64, int, error) {
	b.mu.RLock()
	t, exists := b.topics[topic]
	cluster := b.cluster
	b.mu.RUnlock()

	// In cluster mode, use partition-level leadership (like Kafka)
	// Each partition can have a different leader for horizontal scaling
	if cluster != nil {
		// Auto-create topic if needed (via Raft for metadata consistency)
		if !exists {
			// Only the Raft leader can create topics
			if !cluster.IsLeader() {
				leaderAddr := cluster.LeaderAddr()
				if leaderAddr != "" {
					return 0, -1, fmt.Errorf("not leader: leader_addr=%s", leaderAddr)
				}
				return 0, -1, fmt.Errorf("not leader: no leader elected")
			}

			if err := cluster.ProposeCreateTopic(topic, 6); err != nil {
				// Ignore "already exists" - might be concurrent create
				if !strings.Contains(err.Error(), "already exists") &&
					!strings.Contains(err.Error(), "not leader") {
					return 0, -1, fmt.Errorf("failed to create topic: %w", err)
				}
			}

			// Wait for topic to be applied locally (Raft apply is async)
			for i := 0; i < 50; i++ {
				b.mu.RLock()
				t = b.topics[topic]
				exists = t != nil
				b.mu.RUnlock()

				if exists && t != nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			if !exists || t == nil {
				return 0, -1, fmt.Errorf("topic %s not ready after create (timeout)", topic)
			}
		}

		// Select partition based on key or round-robin
		partition := b.selectPartition(t, key)

		// Check if we're the partition leader (not Raft leader)
		// This enables horizontal scaling - different partitions can have different leaders
		leaderInfo, hasAssignment := cluster.GetPartitionLeaderInfo(topic, partition)

		if hasAssignment {
			// We have partition assignment metadata
			if !leaderInfo.IsLocalNode {
				// Not the partition leader - return error with partition leader address
				if leaderInfo.LeaderAddr != "" {
					return 0, partition, fmt.Errorf("not partition leader: partition=%d leader_addr=%s",
						partition, leaderInfo.LeaderAddr)
				}
				return 0, partition, fmt.Errorf("not partition leader: partition=%d leader=%s",
					partition, leaderInfo.LeaderID)
			}
			// We are the partition leader - write locally
		} else {
			// No partition assignment yet - fall back to Raft leader behavior
			// This happens during initial topic creation before assignments are distributed
			if !cluster.IsLeader() {
				leaderAddr := cluster.LeaderAddr()
				if leaderAddr != "" {
					return 0, partition, fmt.Errorf("not leader: leader_addr=%s", leaderAddr)
				}
				return 0, partition, fmt.Errorf("not leader: no leader elected")
			}
		}

		// Write directly to local log (partition leader fast path)
		p := t.Partitions[partition]
		record := storage.EncodeRecord(key, data)
		offset, err := p.Log.Append(record)
		return offset, partition, err
	}

	// Standalone mode
	if !exists {
		// Auto-create topic with 1 partition
		if err := b.CreateTopic(topic, 1); err != nil {
			return 0, -1, err
		}
		b.mu.RLock()
		t = b.topics[topic]
		b.mu.RUnlock()
	}

	// Select partition based on key or round-robin
	partition := b.selectPartition(t, key)

	// Standalone mode - write directly using record format
	p := t.Partitions[partition]
	record := storage.EncodeRecord(key, data)
	offset, err := p.Log.Append(record)
	return offset, partition, err
}

// selectPartition determines which partition to write to.
// If key is provided, uses consistent hashing (FNV-1a).
// If key is nil, uses round-robin for even distribution.
func (b *Broker) selectPartition(t *Topic, key []byte) int {
	numPartitions := len(t.Partitions)
	if numPartitions == 0 {
		return 0
	}
	if numPartitions == 1 {
		return 0
	}

	if key != nil && len(key) > 0 {
		// Key-based partitioning using FNV-1a hash
		// This ensures messages with the same key go to the same partition
		h := fnv.New32a()
		h.Write(key)
		return int(h.Sum32() % uint32(numPartitions))
	}

	// Round-robin for even distribution when no key
	counter := atomic.AddUint64(&t.rrCounter, 1)
	return int((counter - 1) % uint64(numPartitions))
}

// ApplyProduceMessage directly writes a message (called by Raft apply or standalone mode).
// Returns the offset where the message was stored.
// In cluster mode, topics are auto-created if they don't exist.
func (b *Broker) ApplyProduceMessage(topic string, partition int, key, value []byte, timestamp int64) (int64, error) {
	b.mu.RLock()
	t, exists := b.topics[topic]
	b.mu.RUnlock()

	if !exists {
		// Auto-create topic with enough partitions
		numPartitions := partition + 1
		if numPartitions < 1 {
			numPartitions = 1
		}
		if err := b.CreateTopic(topic, numPartitions); err != nil {
			// Topic might already exist from a concurrent create, ignore "already exists" errors
			if err.Error() != fmt.Sprintf("topic already exists: %s", topic) {
				return 0, fmt.Errorf("failed to auto-create topic %s: %w", topic, err)
			}
		}
		b.mu.RLock()
		t = b.topics[topic]
		b.mu.RUnlock()
	}

	// Ensure topic is valid
	if t == nil {
		return 0, fmt.Errorf("topic %s not ready", topic)
	}

	if partition >= len(t.Partitions) || partition < 0 {
		return 0, fmt.Errorf("partition %d not found for topic %s", partition, topic)
	}

	p := t.Partitions[partition]
	// Store as record format with key+value
	record := storage.EncodeRecord(key, value)
	offset, err := p.Log.Append(record)
	if err != nil {
		return 0, err
	}

	return int64(offset), nil
}

func (b *Broker) Consume(topic string, offset uint64) ([]byte, error) {
	key, value, err := b.ConsumeWithKey(topic, offset)
	_ = key // Ignore key for backward compatibility
	return value, err
}

// ConsumeWithKey reads a message and returns both key and value.
func (b *Broker) ConsumeWithKey(topic string, offset uint64) (key, value []byte, err error) {
	return b.ConsumeFromPartitionWithKey(topic, 0, offset)
}

// ConsumeFromPartitionWithKey reads a message from a specific partition and returns both key and value.
func (b *Broker) ConsumeFromPartitionWithKey(topic string, partition int, offset uint64) (key, value []byte, err error) {
	b.mu.RLock()
	t, exists := b.topics[topic]
	b.mu.RUnlock()

	if !exists {
		return nil, nil, fmt.Errorf("topic not found")
	}

	if partition < 0 || partition >= len(t.Partitions) {
		return nil, nil, fmt.Errorf("partition %d out of range", partition)
	}

	p := t.Partitions[partition]
	data, err := p.Log.Read(offset)
	if err != nil {
		return nil, nil, err
	}

	// Decode record format
	record, err := storage.DecodeRecord(data)
	if err != nil {
		return nil, nil, err
	}

	return record.Key, record.Value, nil
}

// GetZeroCopyInfo returns file and position info for zero-copy reads.
// This allows using sendfile() to transfer data directly to network.
func (b *Broker) GetZeroCopyInfo(topic string, partition int, offset uint64) (*os.File, int64, int64, error) {
	b.mu.RLock()
	t, exists := b.topics[topic]
	b.mu.RUnlock()

	if !exists {
		return nil, 0, 0, fmt.Errorf("topic not found")
	}

	if partition < 0 || partition >= len(t.Partitions) {
		return nil, 0, 0, fmt.Errorf("partition %d out of range", partition)
	}

	p := t.Partitions[partition]
	return p.Log.GetZeroCopyInfo(offset)
}

// GetTopicMetadata returns detailed metadata about a topic.
func (b *Broker) GetTopicMetadata(topic string) (interface{}, error) {
	b.mu.RLock()
	t, exists := b.topics[topic]
	b.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	meta := &TopicMetadata{
		Name:       t.Name,
		Partitions: make([]PartitionMetadata, len(t.Partitions)),
	}

	for i, p := range t.Partitions {
		lowest, _ := p.Log.LowestOffset()
		highest, _ := p.Log.HighestOffset()
		msgCount := uint64(0)
		if highest >= lowest {
			msgCount = highest - lowest + 1
		}
		meta.Partitions[i] = PartitionMetadata{
			ID:            p.ID,
			LowestOffset:  lowest,
			HighestOffset: highest,
			MessageCount:  msgCount,
		}
	}

	return meta, nil
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range b.topics {
		for _, p := range t.Partitions {
			p.Log.Close()
		}
	}
	return nil
}

// Subscribe returns the starting offset for a consumer based on the mode.
func (b *Broker) Subscribe(topic, groupID string, partition int, mode protocol.SubscribeMode) (uint64, error) {
	b.mu.RLock()
	t, exists := b.topics[topic]
	b.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	if partition >= len(t.Partitions) {
		return 0, fmt.Errorf("partition %d not found", partition)
	}

	p := t.Partitions[partition]

	switch mode {
	case protocol.SubscribeFromEarliest:
		return p.Log.LowestOffset()
	case protocol.SubscribeFromLatest:
		high, err := p.Log.HighestOffset()
		if err != nil {
			return 0, err
		}
		return high + 1, nil // Start from next message
	case protocol.SubscribeFromCommit:
		if offset, exists := b.consumers.GetCommittedOffset(topic, groupID, partition); exists {
			return offset, nil
		}
		// No committed offset, start from earliest
		return p.Log.LowestOffset()
	default:
		return 0, fmt.Errorf("unknown subscribe mode: %s", mode)
	}
}

// CommitOffset commits the consumer offset.
// Returns (changed, error) where changed indicates if the offset was actually updated.
func (b *Broker) CommitOffset(topic, groupID string, partition int, offset uint64) (bool, error) {
	return b.consumers.CommitOffset(topic, groupID, partition, offset)
}

// FetchedMessage represents a message with its key and value.
type FetchedMessage struct {
	Key    []byte
	Value  []byte
	Offset uint64
}

// Fetch retrieves multiple messages starting from an offset.
// For backward compatibility, returns only values. Use FetchWithKeys to get keys.
func (b *Broker) Fetch(topic string, partition int, offset uint64, maxMessages int) ([][]byte, uint64, error) {
	messages, nextOffset, err := b.FetchWithKeys(topic, partition, offset, maxMessages)
	if err != nil {
		return nil, 0, err
	}

	// Extract just the values for backward compatibility
	values := make([][]byte, len(messages))
	for i, msg := range messages {
		values[i] = msg.Value
	}
	return values, nextOffset, nil
}

// FetchWithKeys retrieves multiple messages with their keys starting from an offset.
func (b *Broker) FetchWithKeys(topic string, partition int, offset uint64, maxMessages int) ([]FetchedMessage, uint64, error) {
	b.mu.RLock()
	t, exists := b.topics[topic]
	b.mu.RUnlock()

	if !exists {
		return nil, 0, fmt.Errorf("topic not found: %s", topic)
	}

	if partition >= len(t.Partitions) {
		return nil, 0, fmt.Errorf("partition %d not found", partition)
	}

	p := t.Partitions[partition]
	var messages []FetchedMessage
	currentOffset := offset

	for i := 0; i < maxMessages; i++ {
		data, err := p.Log.Read(currentOffset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, err
		}

		// Decode record format
		record, err := storage.DecodeRecord(data)
		if err != nil {
			return nil, 0, err
		}

		messages = append(messages, FetchedMessage{
			Key:    record.Key,
			Value:  record.Value,
			Offset: currentOffset,
		})
		currentOffset++
	}

	return messages, currentOffset, nil
}

// ListTopics returns all topic names.
func (b *Broker) ListTopics() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topics := make([]string, 0, len(b.topics))
	for name := range b.topics {
		// Skip internal topics
		if !strings.HasPrefix(name, "__") {
			topics = append(topics, name)
		}
	}
	sort.Strings(topics)
	return topics
}

// DeleteTopic removes a topic and all its data.
// In cluster mode, this routes through Raft consensus.
func (b *Broker) DeleteTopic(name string) error {
	b.mu.RLock()
	cluster := b.cluster
	b.mu.RUnlock()

	// If we have a cluster, route through Raft
	if cluster != nil {
		return cluster.ProposeDeleteTopic(name)
	}

	// Standalone mode - delete directly
	return b.ApplyDeleteTopic(name)
}

// ApplyDeleteTopic directly deletes a topic (called by Raft apply or standalone mode).
func (b *Broker) ApplyDeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, exists := b.topics[name]
	if !exists {
		// In cluster mode, this is idempotent
		b.logger.Debug("Topic not found, skipping delete", "topic", name)
		return nil
	}

	// Close all partition logs
	for _, p := range t.Partitions {
		if err := p.Log.Remove(); err != nil {
			return err
		}
	}

	// Remove topic directory
	topicDir := filepath.Join(b.config.DataDir, name)
	if err := os.RemoveAll(topicDir); err != nil {
		return err
	}

	delete(b.topics, name)
	b.logger.Info("Deleted topic", "topic", name)
	return nil
}

// GetTopicInfo returns detailed information about a topic.
func (b *Broker) GetTopicInfo(topic string) (*TopicInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t, exists := b.topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}

	info := &TopicInfo{
		Name:       t.Name,
		Partitions: make([]PartitionInfo, len(t.Partitions)),
	}

	for i, p := range t.Partitions {
		low, _ := p.Log.LowestOffset()
		high, _ := p.Log.HighestOffset()
		var msgCount uint64
		if high >= low {
			msgCount = high - low + 1
		}
		info.Partitions[i] = PartitionInfo{
			ID:            p.ID,
			LowestOffset:  low,
			HighestOffset: high,
			MessageCount:  msgCount,
		}
	}

	return info, nil
}

// TopicInfo contains metadata about a topic.
type TopicInfo struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

// PartitionInfo contains metadata about a partition.
type PartitionInfo struct {
	ID            int    `json:"id"`
	LowestOffset  uint64 `json:"lowest_offset"`
	HighestOffset uint64 `json:"highest_offset"`
	MessageCount  uint64 `json:"message_count"`
}

// GetTopicMessageCount returns the total number of messages in a topic across all partitions.
func (b *Broker) GetTopicMessageCount(topic string) (int64, error) {
	info, err := b.GetTopicInfo(topic)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, p := range info.Partitions {
		// Message count is high - low + 1 (if there are any messages)
		if p.HighestOffset >= p.LowestOffset {
			total += int64(p.HighestOffset - p.LowestOffset + 1)
		}
	}
	return total, nil
}

// ConsumerGroupInfo contains information about a consumer group.
type ConsumerGroupInfo struct {
	GroupID string         `json:"group_id"`
	Topic   string         `json:"topic"`
	Offsets map[int]uint64 `json:"offsets"`
	Members int            `json:"members"`
}

// ListConsumerGroups returns all consumer groups.
func (b *Broker) ListConsumerGroups() []*ConsumerGroupInfo {
	groups := b.consumers.ListGroups()
	result := make([]*ConsumerGroupInfo, 0, len(groups))

	for _, g := range groups {
		g.mu.RLock()
		offsets := make(map[int]uint64)
		for k, v := range g.Offsets {
			offsets[k] = v
		}
		g.mu.RUnlock()

		result = append(result, &ConsumerGroupInfo{
			GroupID: g.GroupID,
			Topic:   g.Topic,
			Offsets: offsets,
			Members: 0, // Active members tracking not implemented yet
		})
	}
	return result
}

// GetConsumerGroup returns information about a specific consumer group.
func (b *Broker) GetConsumerGroup(topic, groupID string) (*ConsumerGroupInfo, error) {
	group, exists := b.consumers.GetGroup(topic, groupID)
	if !exists {
		return nil, fmt.Errorf("consumer group not found: %s:%s", topic, groupID)
	}

	group.mu.RLock()
	offsets := make(map[int]uint64)
	for k, v := range group.Offsets {
		offsets[k] = v
	}
	group.mu.RUnlock()

	return &ConsumerGroupInfo{
		GroupID: group.GroupID,
		Topic:   group.Topic,
		Offsets: offsets,
		Members: 0,
	}, nil
}

// DeleteConsumerGroup removes a consumer group.
func (b *Broker) DeleteConsumerGroup(topic, groupID string) error {
	return b.consumers.DeleteGroup(topic, groupID)
}

// ============================================================================
// ReplicationApplier Implementation for Partition Data Replication
// ============================================================================

// WriteReplicatedData writes replicated data to a partition's log.
// This is called by followers when receiving data from partition leaders.
func (b *Broker) WriteReplicatedData(topic string, partition int, offset uint64, data []byte) error {
	b.mu.RLock()
	t, exists := b.topics[topic]
	b.mu.RUnlock()

	if !exists {
		return fmt.Errorf("topic not found: %s", topic)
	}

	if partition >= len(t.Partitions) || partition < 0 {
		return fmt.Errorf("partition %d not found for topic %s", partition, topic)
	}

	p := t.Partitions[partition]

	// Write the data at the specified offset
	// The storage layer should handle offset validation
	_, err := p.Log.Append(data)
	if err != nil {
		return fmt.Errorf("failed to write replicated data: %w", err)
	}

	return nil
}

// GetHighWatermark returns the highest offset written to a partition.
// This is used to determine where to start replication from.
func (b *Broker) GetHighWatermark(topic string, partition int) (uint64, error) {
	b.mu.RLock()
	t, exists := b.topics[topic]
	b.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("topic not found: %s", topic)
	}

	if partition >= len(t.Partitions) || partition < 0 {
		return 0, fmt.Errorf("partition %d not found for topic %s", partition, topic)
	}

	p := t.Partitions[partition]
	return p.Log.HighestOffset()
}

// GetClusterMetadata returns partition-to-node mappings for smart client routing.
// If topic is empty, returns metadata for all topics.
func (b *Broker) GetClusterMetadata(topic string) (*protocol.BinaryClusterMetadataResponse, error) {
	if b.cluster == nil {
		// Standalone mode - return local node as leader for all partitions
		return b.getStandaloneMetadata(topic)
	}

	return b.getClusterMetadata(topic)
}

// getStandaloneMetadata returns metadata for standalone mode.
func (b *Broker) getStandaloneMetadata(topic string) (*protocol.BinaryClusterMetadataResponse, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	resp := &protocol.BinaryClusterMetadataResponse{
		ClusterID: "standalone",
		Topics:    make([]protocol.TopicMetadata, 0),
	}

	// Get local address from config
	localAddr := b.config.BindAddr
	nodeID := b.config.NodeID

	if topic != "" {
		// Single topic
		t, exists := b.topics[topic]
		if !exists {
			return nil, fmt.Errorf("topic not found: %s", topic)
		}

		partitions := make([]protocol.PartitionMetadata, len(t.Partitions))
		for i := range t.Partitions {
			partitions[i] = protocol.PartitionMetadata{
				Partition:  int32(i),
				LeaderID:   nodeID,
				LeaderAddr: localAddr,
				Epoch:      1,
				State:      "online",
				Replicas:   []string{nodeID},
				ISR:        []string{nodeID},
			}
		}
		resp.Topics = append(resp.Topics, protocol.TopicMetadata{
			Topic:      topic,
			Partitions: partitions,
		})
	} else {
		// All topics
		for name, t := range b.topics {
			partitions := make([]protocol.PartitionMetadata, len(t.Partitions))
			for i := range t.Partitions {
				partitions[i] = protocol.PartitionMetadata{
					Partition:  int32(i),
					LeaderID:   nodeID,
					LeaderAddr: localAddr,
					Epoch:      1,
					State:      "online",
					Replicas:   []string{nodeID},
					ISR:        []string{nodeID},
				}
			}
			resp.Topics = append(resp.Topics, protocol.TopicMetadata{
				Topic:      name,
				Partitions: partitions,
			})
		}
	}

	return resp, nil
}

// getClusterMetadata returns metadata from the cluster.
func (b *Broker) getClusterMetadata(topic string) (*protocol.BinaryClusterMetadataResponse, error) {
	resp := &protocol.BinaryClusterMetadataResponse{
		ClusterID: b.config.NodeID, // Use node ID as cluster ID for now
		Topics:    make([]protocol.TopicMetadata, 0),
	}

	b.mu.RLock()
	topics := make([]string, 0)
	if topic != "" {
		if _, exists := b.topics[topic]; !exists {
			b.mu.RUnlock()
			return nil, fmt.Errorf("topic not found: %s", topic)
		}
		topics = append(topics, topic)
	} else {
		for name := range b.topics {
			topics = append(topics, name)
		}
	}
	b.mu.RUnlock()

	// Get partition metadata from cluster using full assignment info
	for _, topicName := range topics {
		b.mu.RLock()
		t := b.topics[topicName]
		numPartitions := len(t.Partitions)
		b.mu.RUnlock()

		partitions := make([]protocol.PartitionMetadata, numPartitions)
		for i := 0; i < numPartitions; i++ {
			assignment, ok := b.cluster.GetPartitionAssignment(topicName, i)
			if ok && assignment != nil {
				partitions[i] = protocol.PartitionMetadata{
					Partition:  int32(i),
					LeaderID:   assignment.Leader,
					LeaderAddr: assignment.LeaderAddr,
					Epoch:      assignment.Epoch,
					State:      assignment.State,
					Replicas:   assignment.Replicas,
					ISR:        assignment.ISR,
				}
			} else {
				// No assignment info available - use leader info as fallback
				info, infoOk := b.cluster.GetPartitionLeaderInfo(topicName, i)
				if infoOk && info != nil {
					partitions[i] = protocol.PartitionMetadata{
						Partition:  int32(i),
						LeaderID:   info.LeaderID,
						LeaderAddr: info.LeaderAddr,
						Epoch:      info.Epoch,
						State:      "online",
						Replicas:   []string{info.LeaderID},
						ISR:        []string{info.LeaderID},
					}
				} else {
					// No info available - use empty values with offline state
					partitions[i] = protocol.PartitionMetadata{
						Partition:  int32(i),
						LeaderID:   "",
						LeaderAddr: "",
						Epoch:      0,
						State:      "offline",
						Replicas:   []string{},
						ISR:        []string{},
					}
				}
			}
		}

		resp.Topics = append(resp.Topics, protocol.TopicMetadata{
			Topic:      topicName,
			Partitions: partitions,
		})
	}

	return resp, nil
}

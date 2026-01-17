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
Package cluster provides distributed clustering for FlyMQ.

ARCHITECTURE:
=============
FlyMQ uses a multi-component clustering approach:

1. RAFT CONSENSUS: Leader election and log replication for metadata
2. MEMBERSHIP: Gossip-based failure detection and member discovery
3. PARTITIONS: Distributed partition assignment and leadership
4. REPLICATION: Data replication between partition replicas

CLUSTER TOPOLOGY:
=================

	┌─────────────────────────────────────────────────────────────┐
	│                      Cluster                                 │
	│  ┌─────────┐    ┌─────────┐    ┌─────────┐                  │
	│  │ Node 1  │◄──►│ Node 2  │◄──►│ Node 3  │  (Raft + Gossip) │
	│  │ Leader  │    │Follower │    │Follower │                  │
	│  └─────────┘    └─────────┘    └─────────┘                  │
	│       │              │              │                        │
	│  ┌────┴────┐    ┌────┴────┐    ┌────┴────┐                  │
	│  │ P0 (L)  │    │ P0 (R)  │    │ P1 (L)  │  (Partitions)    │
	│  │ P1 (R)  │    │ P1 (R)  │    │ P0 (R)  │                  │
	│  └─────────┘    └─────────┘    └─────────┘                  │
	└─────────────────────────────────────────────────────────────┘

FAULT TOLERANCE:
================
- Raft ensures metadata consistency with majority quorum
- Partition replication ensures data durability
- Gossip detects node failures within seconds
- Automatic leader election for failed partition leaders

CONFIGURATION:
==============
- ReplicationFactor: Number of replicas per partition (default: 3)
- MinISR: Minimum in-sync replicas for writes (default: 2)
*/
package cluster

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"flymq/internal/banner"
	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/crypto"
	"flymq/internal/logging"
)

// ClusterConfig holds configuration for the cluster.
type ClusterConfig struct {
	NodeID                   string
	ClusterAddr              string // Address to bind for cluster traffic
	AdvertiseCluster         string // Address to advertise to other nodes (auto-detected if empty)
	Peers                    []string
	DataDir                  string
	ReplicationFactor        int
	MinISR                   int
	EncryptionEnabled        bool   // Whether encryption is enabled
	EncryptionKeyFingerprint string // Fingerprint of encryption key for cluster validation
}

// CommandApplier is the interface for applying committed commands.
type CommandApplier interface {
	ApplyCreateTopic(name string, partitions int) error
	ApplyDeleteTopic(name string) error
	ApplyProduceMessage(topic string, partition int, key, value []byte, timestamp int64) (int64, error)
	ApplyRegisterSchema(name, schemaType, definition, compatibility string) error
	ApplyDeleteSchema(name string, version int) error
}

// AuthApplier is the interface for applying user and ACL commands from Raft log.
type AuthApplier interface {
	ApplyCreateUser(username, passwordHash string, roles []string) error
	ApplyDeleteUser(username string) error
	ApplyUpdateUser(username string, roles []string, enabled *bool, passwordHash string) error
	ApplySetACL(topic string, public bool, allowedUsers, allowedRoles []string)
	ApplyDeleteACL(topic string)
}

// ReplicationApplier is the interface for applying replicated data to local storage.
// This is used by followers to write data received from partition leaders.
type ReplicationApplier interface {
	// WriteReplicatedData writes replicated data to a partition's log.
	// Returns the local offset where the data was written.
	WriteReplicatedData(topic string, partition int, offset uint64, data []byte) error

	// GetHighWatermark returns the highest offset written to a partition.
	GetHighWatermark(topic string, partition int) (uint64, error)
}

// Cluster coordinates all cluster components.
type Cluster struct {
	mu sync.RWMutex

	config      ClusterConfig
	raft        *RaftNode
	membership  *MembershipManager
	partitions  *PartitionManager
	replication *ReplicationManager
	distributor *PartitionDistributor // For smart partition distribution
	transport   *TCPTransport
	discovery   *DiscoveryService      // For mDNS service discovery
	applier     CommandApplier     // For applying committed commands
	authApplier AuthApplier        // For applying user/ACL commands
	replApplier ReplicationApplier // For applying replicated partition data

	logger *logging.Logger
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewCluster creates a new cluster instance.
func NewCluster(cfg *config.Config) (*Cluster, error) {
	// Get the resolved advertise cluster address (auto-detects if not set)
	advertiseCluster := cfg.GetAdvertiseCluster()

	// Compute encryption key fingerprint for cluster validation
	var encryptionFingerprint string
	if cfg.Security.EncryptionEnabled && cfg.Security.EncryptionKey != "" {
		encryptionFingerprint = crypto.KeyFingerprint(cfg.Security.EncryptionKey)
	}

	clusterConfig := ClusterConfig{
		NodeID:                   cfg.NodeID,
		ClusterAddr:              cfg.ClusterAddr,
		AdvertiseCluster:         advertiseCluster,
		Peers:                    cfg.Peers,
		DataDir:                  cfg.DataDir,
		ReplicationFactor:        3,
		MinISR:                   2,
		EncryptionEnabled:        cfg.Security.EncryptionEnabled,
		EncryptionKeyFingerprint: encryptionFingerprint,
	}

	c := &Cluster{
		config: clusterConfig,
		logger: logging.NewLogger("cluster"),
		stopCh: make(chan struct{}),
	}

	c.logger.Info("Cluster configuration",
		"node_id", clusterConfig.NodeID,
		"cluster_addr", clusterConfig.ClusterAddr,
		"advertise_cluster", advertiseCluster,
		"peers", clusterConfig.Peers)

	// Initialize service discovery if enabled
	if cfg.Discovery.Enabled {
		discoveryConfig := DiscoveryConfig{
			NodeID:      cfg.NodeID,
			ClusterID:   cfg.Discovery.ClusterID,
			ClusterAddr: advertiseCluster,
			ClientAddr:  cfg.GetAdvertiseAddr(),
			Version:     banner.Version,
			Enabled:     true,
		}
		c.discovery = NewDiscoveryService(discoveryConfig)
		c.logger.Info("Service discovery enabled",
			"cluster_id", cfg.Discovery.ClusterID)
	}

	// Initialize transport
	c.transport = NewTCPTransport(clusterConfig.ClusterAddr, 10*time.Second)

	// Initialize Raft
	raftConfig := RaftConfig{
		NodeID:            clusterConfig.NodeID,
		ClusterAddr:       clusterConfig.ClusterAddr,
		AdvertiseAddr:     clusterConfig.AdvertiseCluster,
		ClientAddr:        cfg.GetAdvertiseAddr(), // Client-facing address for failover
		DataDir:           clusterConfig.DataDir,
		Peers:             clusterConfig.Peers,
		ElectionTimeout:   2000 * time.Millisecond,
		HeartbeatInterval: 200 * time.Millisecond,
		// Enable async replication for high performance (similar to Kafka's acks=1)
		// Messages are acknowledged after local write, replicated in background
		AsyncReplication:         true,
		ReplicationTimeout:       5 * time.Second,
		EncryptionEnabled:        clusterConfig.EncryptionEnabled,
		EncryptionKeyFingerprint: clusterConfig.EncryptionKeyFingerprint,
	}

	applyCh := make(chan LogEntry, 10000)
	raft, err := NewRaftNode(raftConfig, c.transport, applyCh)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}
	c.raft = raft

	// Initialize membership manager
	membershipConfig := MembershipConfig{
		NodeID:                   clusterConfig.NodeID,
		Address:                  cfg.BindAddr,
		ClusterAddr:              clusterConfig.ClusterAddr,
		AdvertiseAddr:            clusterConfig.AdvertiseCluster,
		DataDir:                  clusterConfig.DataDir,
		GossipInterval:           1 * time.Second,
		SuspectTimeout:           5 * time.Second,
		DeadTimeout:              30 * time.Second,
		Peers:                    clusterConfig.Peers,
		EncryptionEnabled:        clusterConfig.EncryptionEnabled,
		EncryptionKeyFingerprint: clusterConfig.EncryptionKeyFingerprint,
	}

	membership, err := NewMembershipManager(membershipConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create membership manager: %w", err)
	}
	c.membership = membership

	// Initialize partition manager
	partitions, err := NewPartitionManager(clusterConfig.NodeID, clusterConfig.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create partition manager: %w", err)
	}
	c.partitions = partitions

	// Initialize partition distributor for smart load balancing
	c.distributor = NewPartitionDistributor(partitions)

	// Initialize replication manager
	replicationConfig := ReplicationConfig{
		NodeID:            clusterConfig.NodeID,
		ReplicationFactor: clusterConfig.ReplicationFactor,
		MinISR:            clusterConfig.MinISR,
		FetchInterval:     100 * time.Millisecond,
		FetchMaxBytes:     1024 * 1024,
		Timeout:           10 * time.Second,
	}
	c.replication = NewReplicationManager(replicationConfig)

	// Set up callbacks
	c.partitions.SetLeaderChangeCallback(c.onLeaderChange)

	return c, nil
}

// Start starts the cluster.
func (c *Cluster) Start(ctx context.Context) error {
	c.logger.Info("Starting cluster", "node_id", c.config.NodeID, "cluster_addr", c.config.ClusterAddr)

	// Start service discovery if enabled
	if c.discovery != nil {
		if err := c.discovery.Start(); err != nil {
			c.logger.Warn("Failed to start service discovery", "error", err)
			// Non-fatal: continue without discovery
		} else {
			c.logger.Info("Service discovery started")
		}
	}

	// Start membership manager
	if err := c.membership.Start(); err != nil {
		return fmt.Errorf("failed to start membership: %w", err)
	}

	// Set up stats collector for Raft to exchange stats with peers
	c.raft.SetStatsCollector(c.collectRaftStats)

	// Set up peer activity callback to update membership last seen times
	c.raft.SetPeerActivityCallback(func(peerID string) {
		c.membership.UpdateLastSeen(peerID)
	})

	// Start Raft
	if err := c.raft.Start(ctx); err != nil {
		return fmt.Errorf("failed to start raft: %w", err)
	}

	// Start watching for membership changes
	c.wg.Add(1)
	go c.watchMembership(ctx)

	// Start applying Raft log entries
	c.wg.Add(1)
	go c.applyLoop(ctx)

	c.logger.Info("Cluster started successfully")
	return nil
}

// Stop stops the cluster.
func (c *Cluster) Stop() error {
	c.logger.Info("Stopping cluster")
	close(c.stopCh)

	c.wg.Wait()

	// Stop service discovery
	if c.discovery != nil {
		if err := c.discovery.Stop(); err != nil {
			c.logger.Error("Failed to stop discovery", "error", err)
		}
	}

	if err := c.replication.Stop(); err != nil {
		c.logger.Error("Failed to stop replication", "error", err)
	}

	if err := c.raft.Stop(); err != nil {
		c.logger.Error("Failed to stop raft", "error", err)
	}

	if err := c.membership.Stop(); err != nil {
		c.logger.Error("Failed to stop membership", "error", err)
	}

	c.logger.Info("Cluster stopped")
	return nil
}

// SetApplier sets the command applier (typically the broker).
func (c *Cluster) SetApplier(applier CommandApplier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applier = applier
}

// SetAuthApplier sets the auth applier for user/ACL commands.
func (c *Cluster) SetAuthApplier(applier AuthApplier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.authApplier = applier
}

// SetReplicationApplier sets the replication applier for partition data.
func (c *Cluster) SetReplicationApplier(applier ReplicationApplier) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replApplier = applier
}

// IsLeader returns true if this node is the cluster leader.
func (c *Cluster) IsLeader() bool {
	return c.raft.IsLeader()
}

// LeaderID returns the current leader ID.
func (c *Cluster) LeaderID() string {
	return c.raft.LeaderID()
}

// LeaderAddr returns the client address of the current leader.
// This is the address clients should connect to for operations.
// Returns empty string if leader is unknown.
func (c *Cluster) LeaderAddr() string {
	// First try to get the leader's client address directly from Raft
	// This is the most reliable source as it's shared in AppendEntries messages
	if addr := c.raft.LeaderClientAddr(); addr != "" {
		return addr
	}

	// Fall back to membership lookup
	leaderID := c.raft.LeaderID()
	if leaderID == "" {
		return ""
	}
	if member, ok := c.membership.GetMember(leaderID); ok {
		return member.Address
	}
	return ""
}

// notLeaderError returns a formatted error with leader info for client failover.
// Format: "not leader: leader_id=<id> leader_addr=<addr>"
func (c *Cluster) notLeaderError() error {
	leaderID := c.raft.LeaderID()
	leaderAddr := c.LeaderAddr()
	c.logger.Debug("notLeaderError called",
		"leader_id", leaderID,
		"leader_addr", leaderAddr,
		"leader_client_addr", c.raft.LeaderClientAddr())
	if leaderAddr != "" {
		return fmt.Errorf("not leader: leader_id=%s leader_addr=%s", leaderID, leaderAddr)
	}
	return fmt.Errorf("not leader: leader_id=%s", leaderID)
}

// ProposeCreateTopic proposes a topic creation to the cluster.
// This must be called on the leader node.
func (c *Cluster) ProposeCreateTopic(name string, partitions int) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewCreateTopicCommand(name, partitions)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing create topic", "topic", name, "partitions", partitions)
	return c.raft.Apply(data)
}

// ProposeDeleteTopic proposes a topic deletion to the cluster.
func (c *Cluster) ProposeDeleteTopic(name string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewDeleteTopicCommand(name)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing delete topic", "topic", name)
	return c.raft.Apply(data)
}

// IsSingleNode returns true if this is a single-node cluster (no peers).
// Single-node clusters can use a fast path that bypasses Raft for writes.
func (c *Cluster) IsSingleNode() bool {
	return c.raft.IsSingleNode()
}

// ProposeMessage proposes a message production to the cluster.
// Returns the offset of the produced message.
func (c *Cluster) ProposeMessage(topic string, partition int, key, value []byte) (int64, error) {
	if !c.raft.IsLeader() {
		return 0, c.notLeaderError()
	}

	// Fast path for single-node cluster: apply directly without Raft overhead
	c.mu.RLock()
	applier := c.applier
	c.mu.RUnlock()

	isSingleNode := c.raft.IsSingleNode()
	if isSingleNode && applier != nil {
		timestamp := time.Now().UnixNano()
		offset, err := applier.ApplyProduceMessage(topic, partition, key, value, timestamp)
		if err != nil {
			return 0, err
		}
		return offset, nil
	}

	// Log if we're not using fast path (for debugging)
	if !isSingleNode {
		c.logger.Debug("Using Raft path for message", "topic", topic, "partition", partition)
	}

	cmd, err := NewProduceMessageCommand(topic, partition, key, value)
	if err != nil {
		return 0, fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode command: %w", err)
	}

	// Apply through Raft - the offset will be the commit index
	if err := c.raft.Apply(data); err != nil {
		return 0, err
	}

	// Return the commit index as a pseudo-offset
	// The actual offset is determined when the command is applied
	return int64(c.raft.CommitIndex()), nil
}

// ProposeRegisterSchema proposes a schema registration to the cluster.
func (c *Cluster) ProposeRegisterSchema(name, schemaType, definition, compatibility string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewRegisterSchemaCommand(name, schemaType, definition, compatibility)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing register schema", "name", name, "type", schemaType)
	return c.raft.Apply(data)
}

// ProposeDeleteSchema proposes a schema deletion to the cluster.
func (c *Cluster) ProposeDeleteSchema(name string, version int) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewDeleteSchemaCommand(name, version)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing delete schema", "name", name, "version", version)
	return c.raft.Apply(data)
}

// ProposeCreateUser proposes a user creation to the cluster.
// The password should already be hashed before calling this method.
func (c *Cluster) ProposeCreateUser(username, passwordHash string, roles []string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewCreateUserCommand(username, passwordHash, roles)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing create user", "username", username)
	return c.raft.Apply(data)
}

// ProposeDeleteUser proposes a user deletion to the cluster.
func (c *Cluster) ProposeDeleteUser(username string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewDeleteUserCommand(username)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing delete user", "username", username)
	return c.raft.Apply(data)
}

// ProposeUpdateUser proposes a user update to the cluster.
// The password should already be hashed before calling this method.
func (c *Cluster) ProposeUpdateUser(username string, roles []string, enabled *bool, passwordHash string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewUpdateUserCommand(username, roles, enabled, passwordHash)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing update user", "username", username)
	return c.raft.Apply(data)
}

// ProposeSetACL proposes an ACL setting to the cluster.
func (c *Cluster) ProposeSetACL(topic string, public bool, allowedUsers, allowedRoles []string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewSetACLCommand(topic, public, allowedUsers, allowedRoles)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing set ACL", "topic", topic)
	return c.raft.Apply(data)
}

// ProposeDeleteACL proposes an ACL deletion to the cluster.
func (c *Cluster) ProposeDeleteACL(topic string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewDeleteACLCommand(topic)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing delete ACL", "topic", topic)
	return c.raft.Apply(data)
}

// ============================================================================
// Partition Assignment Proposals for Horizontal Scaling
// ============================================================================

// ProposeAssignPartition proposes a partition assignment to the cluster.
// This is used when creating topics or rebalancing partitions.
func (c *Cluster) ProposeAssignPartition(topic string, partition int, leader, leaderAddr string, replicas []string, replicaAddrs map[string]string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewAssignPartitionCommand(topic, partition, leader, leaderAddr, replicas, replicaAddrs)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing assign partition",
		"topic", topic,
		"partition", partition,
		"leader", leader)
	return c.raft.Apply(data)
}

// ProposeUpdatePartitionLeader proposes a partition leader change to the cluster.
// This is used during failover or rebalancing.
func (c *Cluster) ProposeUpdatePartitionLeader(topic string, partition int, newLeader, newLeaderAddr, reason string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewUpdatePartitionLeaderCommand(topic, partition, newLeader, newLeaderAddr, reason)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing update partition leader",
		"topic", topic,
		"partition", partition,
		"new_leader", newLeader,
		"reason", reason)
	return c.raft.Apply(data)
}

// ProposeUpdatePartitionISR proposes an ISR update to the cluster.
func (c *Cluster) ProposeUpdatePartitionISR(topic string, partition int, isr []string) error {
	if !c.raft.IsLeader() {
		return c.notLeaderError()
	}

	cmd, err := NewUpdatePartitionISRCommand(topic, partition, isr)
	if err != nil {
		return fmt.Errorf("failed to create command: %w", err)
	}

	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode command: %w", err)
	}

	c.logger.Info("Proposing update partition ISR",
		"topic", topic,
		"partition", partition,
		"isr", isr)
	return c.raft.Apply(data)
}

// GetPartitionLeader returns the leader info for a specific partition (internal type).
func (c *Cluster) GetPartitionLeader(topic string, partition int) (*PartitionLeaderInfo, bool) {
	return c.partitions.GetPartitionLeader(topic, partition)
}

// GetPartitionLeaderInfo returns the leader info for a specific partition (broker interface type).
func (c *Cluster) GetPartitionLeaderInfo(topic string, partition int) (*broker.PartitionLeaderInfo, bool) {
	info, ok := c.partitions.GetPartitionLeader(topic, partition)
	if !ok {
		return nil, false
	}
	return &broker.PartitionLeaderInfo{
		Topic:       info.Topic,
		Partition:   info.Partition,
		LeaderID:    info.LeaderID,
		LeaderAddr:  info.LeaderAddr,
		Epoch:       info.Epoch,
		IsLocalNode: info.IsLocalNode,
	}, true
}

// IsPartitionLeader returns true if this node is the leader for the partition.
func (c *Cluster) IsPartitionLeader(topic string, partition int) bool {
	return c.partitions.IsPartitionLeader(topic, partition)
}

// GetAllPartitionLeaders returns leader info for all partitions of a topic (broker interface type).
func (c *Cluster) GetAllPartitionLeaders(topic string) []*broker.PartitionLeaderInfo {
	internalLeaders := c.partitions.GetAllPartitionLeaders(topic)
	result := make([]*broker.PartitionLeaderInfo, len(internalLeaders))
	for i, info := range internalLeaders {
		result[i] = &broker.PartitionLeaderInfo{
			Topic:       info.Topic,
			Partition:   info.Partition,
			LeaderID:    info.LeaderID,
			LeaderAddr:  info.LeaderAddr,
			Epoch:       info.Epoch,
			IsLocalNode: info.IsLocalNode,
		}
	}
	return result
}

// GetClusterPartitionMetadata returns metadata for all partitions across all topics.
func (c *Cluster) GetClusterPartitionMetadata() map[string][]*PartitionLeaderInfo {
	return c.partitions.GetClusterPartitionMetadata()
}

// GetPartitionAssignment returns the full partition assignment including State, Replicas, and ISR.
func (c *Cluster) GetPartitionAssignment(topic string, partition int) (*broker.PartitionAssignment, bool) {
	assignment, ok := c.partitions.GetAssignment(topic, partition)
	if !ok {
		return nil, false
	}
	return &broker.PartitionAssignment{
		Topic:        assignment.Topic,
		Partition:    assignment.Partition,
		Leader:       assignment.Leader,
		LeaderAddr:   assignment.LeaderAddr,
		Replicas:     assignment.Replicas,
		ISR:          assignment.ISR,
		State:        assignment.State.String(),
		ReplicaAddrs: assignment.ReplicaAddrs,
		Epoch:        assignment.Epoch,
	}, true
}

// GetTopicPartitionAssignments returns all partition assignments for a topic.
func (c *Cluster) GetTopicPartitionAssignments(topic string) []*broker.PartitionAssignment {
	assignments := c.partitions.GetTopicAssignments(topic)
	result := make([]*broker.PartitionAssignment, len(assignments))
	for i, a := range assignments {
		result[i] = &broker.PartitionAssignment{
			Topic:        a.Topic,
			Partition:    a.Partition,
			Leader:       a.Leader,
			LeaderAddr:   a.LeaderAddr,
			Replicas:     a.Replicas,
			ISR:          a.ISR,
			State:        a.State.String(),
			ReplicaAddrs: a.ReplicaAddrs,
			Epoch:        a.Epoch,
		}
	}
	return result
}

// NodeID returns this node's ID.
func (c *Cluster) NodeID() string {
	return c.config.NodeID
}

// NodeAddr returns this node's client-facing address.
func (c *Cluster) NodeAddr() string {
	return c.membership.Self().Address
}

// GetRaftState returns the current Raft consensus state.
func (c *Cluster) GetRaftState() broker.RaftState {
	return broker.RaftState{
		Term:        c.raft.CurrentTerm(),
		CommitIndex: c.raft.CommitIndex(),
		LastApplied: c.raft.LastApplied(),
		State:       c.raft.State().String(),
		LeaderID:    c.raft.LeaderID(),
		LogLength:   c.raft.LogLength(),
	}
}

// GetLeaderDistribution returns the current distribution of partition leaders across nodes.
func (c *Cluster) GetLeaderDistribution() map[string]int {
	return c.distributor.GetLeaderDistribution()
}

// IsPartitionBalanced returns true if partition leaders are evenly distributed.
func (c *Cluster) IsPartitionBalanced() bool {
	nodes := c.getActiveNodeIDs()
	return c.distributor.IsBalanced(nodes)
}

// ComputePartitionDistribution computes an optimal distribution for a new topic.
func (c *Cluster) ComputePartitionDistribution(topic string, numPartitions int) *DistributionPlan {
	nodes := c.getActiveNodeIDs()
	return c.distributor.ComputeDistribution(topic, numPartitions, nodes, c.config.ReplicationFactor)
}

// TriggerRebalance triggers a partition rebalance across the cluster.
// This should only be called on the Raft leader.
func (c *Cluster) TriggerRebalance() error {
	if !c.raft.IsLeader() {
		return fmt.Errorf("only the Raft leader can trigger rebalance")
	}

	nodes := c.getActiveNodeIDs()
	plan := c.distributor.ComputeRebalance(nodes, c.config.ReplicationFactor)

	if len(plan.Assignments) == 0 {
		c.logger.Info("No rebalancing needed")
		return nil
	}

	c.logger.Info("Triggering partition rebalance", "moves", len(plan.Assignments))

	// Apply each reassignment through Raft
	for _, placement := range plan.Assignments {
		// Get the leader address from membership
		leaderAddr := ""
		if member, ok := c.membership.GetMember(placement.Leader); ok {
			leaderAddr = member.Address
		}

		if err := c.ProposeUpdatePartitionLeader(placement.Topic, placement.Partition, placement.Leader, leaderAddr, "rebalance"); err != nil {
			c.logger.Error("Failed to propose partition reassignment",
				"topic", placement.Topic,
				"partition", placement.Partition,
				"new_leader", placement.Leader,
				"error", err)
			return err
		}
	}

	return nil
}

// ReassignPartition reassigns a partition to a new leader and/or replicas.
// This should only be called on the Raft leader.
func (c *Cluster) ReassignPartition(topic string, partition int, newLeader string, replicas []string) error {
	if !c.raft.IsLeader() {
		return fmt.Errorf("only the Raft leader can reassign partitions")
	}

	// Validate the new leader is in the replicas list
	leaderInReplicas := false
	for _, r := range replicas {
		if r == newLeader {
			leaderInReplicas = true
			break
		}
	}
	if !leaderInReplicas && len(replicas) > 0 {
		return fmt.Errorf("new leader %s must be in the replicas list", newLeader)
	}

	// Validate all nodes exist in the cluster
	for _, nodeID := range replicas {
		if _, ok := c.membership.GetMember(nodeID); !ok {
			return fmt.Errorf("node %s not found in cluster", nodeID)
		}
	}

	// Get the leader address from membership
	leaderAddr := ""
	if member, ok := c.membership.GetMember(newLeader); ok {
		leaderAddr = member.Address
	}

	c.logger.Info("Reassigning partition",
		"topic", topic,
		"partition", partition,
		"new_leader", newLeader,
		"replicas", replicas)

	// Propose the partition assignment through Raft
	return c.ProposeUpdatePartitionLeader(topic, partition, newLeader, leaderAddr, "manual_reassignment")
}

// getActiveNodeIDs returns the IDs of all active cluster nodes.
func (c *Cluster) getActiveNodeIDs() []string {
	members := c.membership.GetMembers()
	nodes := make([]string, 0, len(members))
	for _, member := range members {
		if member.Status == MemberAlive {
			nodes = append(nodes, member.ID)
		}
	}
	return nodes
}

// GetMembers returns all cluster members with full metadata.
// This includes self, peers from config, and dynamically discovered members.
func (c *Cluster) GetMembers() []*broker.ClusterMember {
	// Start with membership data
	members := c.membership.GetMembers()
	leaderID := c.raft.LeaderID()
	raftState := c.raft.State().String()

	// Collect self stats
	selfStats := c.collectStats()

	// Build a map to track unique members
	memberMap := make(map[string]*broker.ClusterMember)

	// Add self first with full metadata
	selfMember := c.membership.Self()
	memberMap[c.config.NodeID] = &broker.ClusterMember{
		ID:          c.config.NodeID,
		Address:     selfMember.Address,
		ClusterAddr: c.config.ClusterAddr,
		AdminAddr:   selfMember.AdminAddr,
		Status:      selfMember.Status.String(),
		RaftState:   raftState,
		IsLeader:    c.config.NodeID == leaderID,
		JoinedAt:    selfMember.JoinedAt.UTC().Format(time.RFC3339),
		LastSeen:    selfMember.LastSeen.UTC().Format(time.RFC3339),
		Uptime:      time.Since(selfMember.JoinedAt).String(),
		Stats:       selfStats,
	}

	// Get peer stats from Raft
	peerStats := c.raft.GetPeerStats()

	// Add members from membership
	for _, m := range members {
		if m.ID == c.config.NodeID {
			continue // Skip self, already added
		}
		if _, exists := memberMap[m.ID]; !exists {
			member := &broker.ClusterMember{
				ID:          m.ID,
				Address:     m.Address,
				ClusterAddr: m.ClusterAddr,
				AdminAddr:   m.AdminAddr,
				Status:      m.Status.String(),
				RaftState:   m.RaftState,
				IsLeader:    m.ID == leaderID,
				JoinedAt:    m.JoinedAt.UTC().Format(time.RFC3339),
				LastSeen:    m.LastSeen.UTC().Format(time.RFC3339),
				Uptime:      time.Since(m.JoinedAt).String(),
			}
			// Add stats from Raft peer exchange if available
			if stats, ok := peerStats[m.ID]; ok {
				member.Stats = convertRaftNodeStats(stats)
				member.RaftState = stats.RaftState
			}
			memberMap[m.ID] = member
		}
	}

	// Add peers from Raft config as well (they're the real cluster members)
	for _, peer := range c.config.Peers {
		// Extract node ID from peer address (e.g., "flymq-2:9093" -> "flymq-2")
		nodeID := peer
		if colonIdx := strings.LastIndex(peer, ":"); colonIdx > 0 {
			nodeID = peer[:colonIdx]
		}
		if _, exists := memberMap[nodeID]; !exists {
			member := &broker.ClusterMember{
				ID:          nodeID,
				Address:     peer,
				ClusterAddr: peer,
				Status:      "alive",
				IsLeader:    nodeID == leaderID,
			}
			// Add stats from Raft peer exchange if available
			if stats, ok := peerStats[nodeID]; ok {
				member.Stats = convertRaftNodeStats(stats)
				member.RaftState = stats.RaftState
			}
			memberMap[nodeID] = member
		}
	}

	// Convert map to slice
	result := make([]*broker.ClusterMember, 0, len(memberMap))
	for _, m := range memberMap {
		result = append(result, m)
	}
	return result
}

// collectStats collects runtime statistics for this node.
func (c *Cluster) collectStats() *broker.NodeStats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return &broker.NodeStats{
		MemoryUsedMB:  float64(memStats.HeapInuse) / 1024 / 1024,
		MemoryAllocMB: float64(memStats.Alloc) / 1024 / 1024,
		MemorySysMB:   float64(memStats.Sys) / 1024 / 1024,
		Goroutines:    runtime.NumGoroutine(),
		NumGC:         memStats.NumGC,
		// TopicCount and PartitionCount will be filled by broker if available
	}
}

// collectRaftStats collects stats for exchange via Raft AppendEntries.
// This is the StatsCollector callback used by the Raft node.
func (c *Cluster) collectRaftStats() *NodeStats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	selfMember := c.membership.Self()

	return &NodeStats{
		NodeID:         c.config.NodeID,
		Address:        selfMember.Address,
		ClusterAddr:    c.config.ClusterAddr,
		RaftState:      c.raft.State().String(),
		MemoryUsedMB:   float64(memStats.HeapInuse) / 1024 / 1024,
		MemoryAllocMB:  float64(memStats.Alloc) / 1024 / 1024,
		MemorySysMB:    float64(memStats.Sys) / 1024 / 1024,
		Goroutines:     runtime.NumGoroutine(),
		NumGC:          memStats.NumGC,
		TopicCount:     0, // Will be filled by broker if callback is provided
		PartitionCount: 0, // Will be filled by broker if callback is provided
		Uptime:         time.Since(selfMember.JoinedAt).String(),
	}
}

// convertRaftNodeStats converts Raft NodeStats to broker NodeStats.
func convertRaftNodeStats(stats *NodeStats) *broker.NodeStats {
	if stats == nil {
		return nil
	}
	return &broker.NodeStats{
		MemoryUsedMB:   stats.MemoryUsedMB,
		MemoryAllocMB:  stats.MemoryAllocMB,
		MemorySysMB:    stats.MemorySysMB,
		Goroutines:     stats.Goroutines,
		NumGC:          stats.NumGC,
		TopicCount:     stats.TopicCount,
		PartitionCount: stats.PartitionCount,
	}
}

func (c *Cluster) watchMembership(ctx context.Context) {
	defer c.wg.Done()

	joinCh := c.membership.JoinEvents()
	leaveCh := c.membership.LeaveEvents()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case member := <-joinCh:
			c.handleMemberJoin(member)
		case member := <-leaveCh:
			c.handleMemberLeave(member)
		}
	}
}

func (c *Cluster) handleMemberJoin(member *Member) {
	c.logger.Info("Handling member join", "member_id", member.ID)

	// If we're the leader, we may need to rebalance partitions
	if c.raft.IsLeader() {
		c.rebalancePartitions()
	}
}

func (c *Cluster) handleMemberLeave(member *Member) {
	c.logger.Info("Handling member leave", "member_id", member.ID)

	// If we're the leader, elect new leaders for affected partitions
	if c.raft.IsLeader() {
		c.electNewLeaders(member.ID)
	}
}

func (c *Cluster) electNewLeaders(failedNodeID string) {
	// Find partitions where the failed node was the leader
	for _, assignment := range c.partitions.GetLeaderPartitions() {
		if assignment.Leader == failedNodeID {
			newLeader, err := c.partitions.ElectLeader(assignment.Topic, assignment.Partition)
			if err != nil {
				c.logger.Error("Failed to elect new leader", "topic", assignment.Topic, "partition", assignment.Partition, "error", err)
			} else {
				c.logger.Info("Elected new leader", "topic", assignment.Topic, "partition", assignment.Partition, "leader", newLeader)
			}
		}
	}
}

func (c *Cluster) rebalancePartitions() {
	// Simple rebalancing: distribute partitions evenly across members
	members := c.membership.GetMembers()
	if len(members) == 0 {
		return
	}

	c.logger.Info("Rebalancing partitions", "member_count", len(members))

	// Get all unassigned partitions and distribute them round-robin
	// For now, we only reassign partitions that have no leader
	for _, assignment := range c.partitions.GetLeaderPartitions() {
		if assignment.Leader == "" {
			// Find the member with the fewest partitions
			// Simple approach: assign to first available member
			for _, member := range members {
				if member.Status == MemberAlive {
					if err := c.partitions.UpdateLeaderWithAddr(assignment.Topic, assignment.Partition, member.ID, member.Address); err != nil {
						c.logger.Error("Failed to assign partition", "error", err)
						continue
					}
					c.logger.Info("Assigned partition to member",
						"topic", assignment.Topic,
						"partition", assignment.Partition,
						"member", member.ID)
					break
				}
			}
		}
	}
}

// autoAssignPartitions distributes partition leadership across cluster members.
// This is called when a new topic is created to enable horizontal scaling.
// Partitions are distributed round-robin across available nodes.
func (c *Cluster) autoAssignPartitions(topic string, numPartitions int) {
	members := c.membership.GetMembers()
	if len(members) == 0 {
		// Single node - assign all partitions to self
		selfMember := c.membership.Self()
		for i := 0; i < numPartitions; i++ {
			replicas := []string{c.config.NodeID}
			replicaAddrs := map[string]string{c.config.NodeID: selfMember.Address}
			if err := c.partitions.CreateAssignmentWithAddr(
				topic, i,
				c.config.NodeID, selfMember.Address,
				replicas, replicaAddrs,
			); err != nil {
				c.logger.Error("Failed to assign partition", "topic", topic, "partition", i, "error", err)
			} else {
				c.logger.Info("Assigned partition to self",
					"topic", topic, "partition", i, "leader", c.config.NodeID)
			}
		}
		return
	}

	// Build list of alive members including self
	aliveMembers := make([]*Member, 0, len(members)+1)
	selfMember := c.membership.Self()
	aliveMembers = append(aliveMembers, selfMember)
	for _, m := range members {
		if m.Status == MemberAlive && m.ID != c.config.NodeID {
			aliveMembers = append(aliveMembers, m)
		}
	}

	if len(aliveMembers) == 0 {
		c.logger.Warn("No alive members for partition assignment", "topic", topic)
		return
	}

	c.logger.Info("Auto-assigning partitions for horizontal scaling",
		"topic", topic,
		"partitions", numPartitions,
		"nodes", len(aliveMembers))

	// Distribute partitions round-robin across nodes
	// This ensures even distribution of partition leadership
	for i := 0; i < numPartitions; i++ {
		leaderIdx := i % len(aliveMembers)
		leader := aliveMembers[leaderIdx]

		// Build replica list (for now, just the leader - replication will be added in Phase 4)
		// In a full implementation, we'd select replicationFactor nodes
		replicas := []string{leader.ID}
		replicaAddrs := map[string]string{leader.ID: leader.Address}

		// Add additional replicas for fault tolerance (up to replicationFactor)
		replicationFactor := c.config.ReplicationFactor
		if replicationFactor > len(aliveMembers) {
			replicationFactor = len(aliveMembers)
		}
		for j := 1; j < replicationFactor; j++ {
			replicaIdx := (leaderIdx + j) % len(aliveMembers)
			replica := aliveMembers[replicaIdx]
			replicas = append(replicas, replica.ID)
			replicaAddrs[replica.ID] = replica.Address
		}

		if err := c.partitions.CreateAssignmentWithAddr(
			topic, i,
			leader.ID, leader.Address,
			replicas, replicaAddrs,
		); err != nil {
			c.logger.Error("Failed to assign partition",
				"topic", topic, "partition", i, "error", err)
		} else {
			c.logger.Info("Assigned partition",
				"topic", topic,
				"partition", i,
				"leader", leader.ID,
				"leader_addr", leader.Address,
				"replicas", replicas)
		}
	}
}

func (c *Cluster) onLeaderChange(topic string, partition int, newLeader string) {
	c.logger.Info("Partition leader changed", "topic", topic, "partition", partition, "new_leader", newLeader)

	// Check if we're a replica for this partition
	assignment, ok := c.partitions.GetAssignment(topic, partition)
	if !ok {
		c.logger.Warn("Partition assignment not found", "topic", topic, "partition", partition)
		return
	}

	isReplica := false
	for _, replica := range assignment.Replicas {
		if replica == c.config.NodeID {
			isReplica = true
			break
		}
	}

	if !isReplica {
		// We're not a replica for this partition, nothing to do
		return
	}

	if newLeader == c.config.NodeID {
		// We became the leader - stop replication (we're now the source)
		if err := c.replication.StopReplication(topic, partition); err != nil {
			c.logger.Error("Failed to stop replication", "topic", topic, "partition", partition, "error", err)
		} else {
			c.logger.Info("Stopped replication - we are now the leader",
				"topic", topic, "partition", partition)
		}
	} else {
		// We're a follower - start replicating from the new leader
		member, ok := c.membership.GetMember(newLeader)
		if !ok {
			c.logger.Error("Leader member not found", "leader", newLeader)
			return
		}

		// Get the high watermark to start replication from
		var startOffset uint64
		c.mu.RLock()
		replApplier := c.replApplier
		c.mu.RUnlock()

		if replApplier != nil {
			hwm, err := replApplier.GetHighWatermark(topic, partition)
			if err != nil {
				c.logger.Warn("Failed to get high watermark, starting from 0",
					"topic", topic, "partition", partition, "error", err)
			} else {
				startOffset = hwm
			}
		}

		// Create write callback that writes to local storage
		writeCallback := func(offset uint64, data []byte) error {
			c.mu.RLock()
			applier := c.replApplier
			c.mu.RUnlock()

			if applier == nil {
				return fmt.Errorf("replication applier not set")
			}
			return applier.WriteReplicatedData(topic, partition, offset, data)
		}

		// Start replication with a context that's cancelled when the cluster stops
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			<-c.stopCh
			cancel()
		}()
		if err := c.replication.StartReplication(ctx, topic, partition, member.Address, startOffset, writeCallback); err != nil {
			c.logger.Error("Failed to start replication",
				"topic", topic,
				"partition", partition,
				"leader", newLeader,
				"leader_addr", member.Address,
				"error", err)
		} else {
			c.logger.Info("Started replication from new leader",
				"topic", topic,
				"partition", partition,
				"leader", newLeader,
				"leader_addr", member.Address,
				"start_offset", startOffset)
		}
	}
}

func (c *Cluster) applyLoop(ctx context.Context) {
	defer c.wg.Done()

	c.logger.Info("Starting apply loop")

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case entry := <-c.raft.applyCh:
			c.applyEntry(entry)
		}
	}
}

// applyEntry applies a committed log entry to the state machine.
func (c *Cluster) applyEntry(entry LogEntry) {
	c.mu.RLock()
	applier := c.applier
	c.mu.RUnlock()

	if applier == nil {
		c.logger.Warn("No applier set, cannot apply entry", "index", entry.Index)
		return
	}

	if len(entry.Command) == 0 {
		return // Empty command (e.g., heartbeat)
	}

	cmd, err := DecodeCommand(entry.Command)
	if err != nil {
		c.logger.Error("Failed to decode command", "error", err, "index", entry.Index)
		return
	}

	c.logger.Debug("Applying command", "type", cmd.Type, "index", entry.Index)

	switch cmd.Type {
	case CmdCreateTopic:
		payload, err := cmd.GetCreateTopicPayload()
		if err != nil {
			c.logger.Error("Failed to get create topic payload", "error", err)
			return
		}
		if err := applier.ApplyCreateTopic(payload.Name, payload.Partitions); err != nil {
			c.logger.Error("Failed to apply create topic", "topic", payload.Name, "error", err)
		} else {
			c.logger.Info("Applied create topic", "topic", payload.Name, "partitions", payload.Partitions)

			// Auto-assign partitions to nodes for horizontal scaling
			// This distributes partition leadership across cluster members
			c.autoAssignPartitions(payload.Name, payload.Partitions)
		}

	case CmdDeleteTopic:
		payload, err := cmd.GetDeleteTopicPayload()
		if err != nil {
			c.logger.Error("Failed to get delete topic payload", "error", err)
			return
		}
		if err := applier.ApplyDeleteTopic(payload.Name); err != nil {
			c.logger.Error("Failed to apply delete topic", "topic", payload.Name, "error", err)
		} else {
			c.logger.Info("Applied delete topic", "topic", payload.Name)
		}

	case CmdProduceMessage:
		payload, err := cmd.GetProduceMessagePayload()
		if err != nil {
			c.logger.Error("Failed to get produce message payload", "error", err)
			return
		}
		offset, err := applier.ApplyProduceMessage(payload.Topic, payload.Partition, payload.Key, payload.Value, payload.Timestamp)
		if err != nil {
			c.logger.Error("Failed to apply produce message", "topic", payload.Topic, "partition", payload.Partition, "error", err)
		} else {
			c.logger.Debug("Applied produce message", "topic", payload.Topic, "partition", payload.Partition, "offset", offset)
		}

	case CmdRegisterSchema:
		payload, err := cmd.GetRegisterSchemaPayload()
		if err != nil {
			c.logger.Error("Failed to get register schema payload", "error", err)
			return
		}
		if err := applier.ApplyRegisterSchema(payload.Name, payload.Type, payload.Definition, payload.Compatibility); err != nil {
			c.logger.Error("Failed to apply register schema", "name", payload.Name, "error", err)
		} else {
			c.logger.Info("Applied register schema", "name", payload.Name, "type", payload.Type)
		}

	case CmdDeleteSchema:
		payload, err := cmd.GetDeleteSchemaPayload()
		if err != nil {
			c.logger.Error("Failed to get delete schema payload", "error", err)
			return
		}
		if err := applier.ApplyDeleteSchema(payload.Name, payload.Version); err != nil {
			c.logger.Error("Failed to apply delete schema", "name", payload.Name, "version", payload.Version, "error", err)
		} else {
			c.logger.Info("Applied delete schema", "name", payload.Name, "version", payload.Version)
		}

	case CmdCreateUser:
		c.applyUserCommand(cmd)

	case CmdDeleteUser:
		c.applyUserCommand(cmd)

	case CmdUpdateUser:
		c.applyUserCommand(cmd)

	case CmdSetACL:
		c.applyACLCommand(cmd)

	case CmdDeleteACL:
		c.applyACLCommand(cmd)

	// Partition assignment commands for horizontal scaling
	case CmdAssignPartition:
		c.applyPartitionCommand(cmd)

	case CmdUpdatePartitionLeader:
		c.applyPartitionCommand(cmd)

	case CmdUpdatePartitionISR:
		c.applyPartitionCommand(cmd)

	default:
		c.logger.Warn("Unknown command type", "type", cmd.Type)
	}
}

// applyUserCommand applies user management commands.
func (c *Cluster) applyUserCommand(cmd *Command) {
	c.mu.RLock()
	authApplier := c.authApplier
	c.mu.RUnlock()

	if authApplier == nil {
		c.logger.Warn("No auth applier set, cannot apply user command", "type", cmd.Type)
		return
	}

	switch cmd.Type {
	case CmdCreateUser:
		payload, err := cmd.GetCreateUserPayload()
		if err != nil {
			c.logger.Error("Failed to get create user payload", "error", err)
			return
		}
		if err := authApplier.ApplyCreateUser(payload.Username, payload.PasswordHash, payload.Roles); err != nil {
			c.logger.Error("Failed to apply create user", "username", payload.Username, "error", err)
		} else {
			c.logger.Info("Applied create user", "username", payload.Username)
		}

	case CmdDeleteUser:
		payload, err := cmd.GetDeleteUserPayload()
		if err != nil {
			c.logger.Error("Failed to get delete user payload", "error", err)
			return
		}
		if err := authApplier.ApplyDeleteUser(payload.Username); err != nil {
			c.logger.Error("Failed to apply delete user", "username", payload.Username, "error", err)
		} else {
			c.logger.Info("Applied delete user", "username", payload.Username)
		}

	case CmdUpdateUser:
		payload, err := cmd.GetUpdateUserPayload()
		if err != nil {
			c.logger.Error("Failed to get update user payload", "error", err)
			return
		}
		if err := authApplier.ApplyUpdateUser(payload.Username, payload.Roles, payload.Enabled, payload.PasswordHash); err != nil {
			c.logger.Error("Failed to apply update user", "username", payload.Username, "error", err)
		} else {
			c.logger.Info("Applied update user", "username", payload.Username)
		}
	}
}

// applyACLCommand applies ACL management commands.
func (c *Cluster) applyACLCommand(cmd *Command) {
	c.mu.RLock()
	authApplier := c.authApplier
	c.mu.RUnlock()

	if authApplier == nil {
		c.logger.Warn("No auth applier set, cannot apply ACL command", "type", cmd.Type)
		return
	}

	switch cmd.Type {
	case CmdSetACL:
		payload, err := cmd.GetSetACLPayload()
		if err != nil {
			c.logger.Error("Failed to get set ACL payload", "error", err)
			return
		}
		authApplier.ApplySetACL(payload.Topic, payload.Public, payload.AllowedUsers, payload.AllowedRoles)
		c.logger.Info("Applied set ACL", "topic", payload.Topic)

	case CmdDeleteACL:
		payload, err := cmd.GetDeleteACLPayload()
		if err != nil {
			c.logger.Error("Failed to get delete ACL payload", "error", err)
			return
		}
		authApplier.ApplyDeleteACL(payload.Topic)
		c.logger.Info("Applied delete ACL", "topic", payload.Topic)
	}
}

// applyPartitionCommand applies partition assignment commands.
// These commands manage partition-level leadership for horizontal scaling.
func (c *Cluster) applyPartitionCommand(cmd *Command) {
	switch cmd.Type {
	case CmdAssignPartition:
		payload, err := cmd.GetAssignPartitionPayload()
		if err != nil {
			c.logger.Error("Failed to get assign partition payload", "error", err)
			return
		}
		// Create or update the partition assignment
		if err := c.partitions.CreateAssignmentWithAddr(
			payload.Topic,
			payload.Partition,
			payload.Leader,
			payload.LeaderAddr,
			payload.Replicas,
			payload.ReplicaAddrs,
		); err != nil {
			c.logger.Error("Failed to apply assign partition",
				"topic", payload.Topic,
				"partition", payload.Partition,
				"error", err)
		} else {
			c.logger.Info("Applied assign partition",
				"topic", payload.Topic,
				"partition", payload.Partition,
				"leader", payload.Leader,
				"leader_addr", payload.LeaderAddr)
		}

	case CmdUpdatePartitionLeader:
		payload, err := cmd.GetUpdatePartitionLeaderPayload()
		if err != nil {
			c.logger.Error("Failed to get update partition leader payload", "error", err)
			return
		}
		if err := c.partitions.UpdateLeaderWithAddr(
			payload.Topic,
			payload.Partition,
			payload.NewLeader,
			payload.NewLeaderAddr,
		); err != nil {
			c.logger.Error("Failed to apply update partition leader",
				"topic", payload.Topic,
				"partition", payload.Partition,
				"error", err)
		} else {
			c.logger.Info("Applied update partition leader",
				"topic", payload.Topic,
				"partition", payload.Partition,
				"new_leader", payload.NewLeader,
				"reason", payload.Reason)
		}

	case CmdUpdatePartitionISR:
		payload, err := cmd.GetUpdatePartitionISRPayload()
		if err != nil {
			c.logger.Error("Failed to get update partition ISR payload", "error", err)
			return
		}
		if err := c.partitions.UpdateISR(
			payload.Topic,
			payload.Partition,
			payload.ISR,
		); err != nil {
			c.logger.Error("Failed to apply update partition ISR",
				"topic", payload.Topic,
				"partition", payload.Partition,
				"error", err)
		} else {
			c.logger.Info("Applied update partition ISR",
				"topic", payload.Topic,
				"partition", payload.Partition,
				"isr", payload.ISR)
		}
	}
}

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

	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/logging"
)

// ClusterConfig holds configuration for the cluster.
type ClusterConfig struct {
	NodeID            string
	ClusterAddr       string // Address to bind for cluster traffic
	AdvertiseCluster  string // Address to advertise to other nodes (auto-detected if empty)
	Peers             []string
	DataDir           string
	ReplicationFactor int
	MinISR            int
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

// Cluster coordinates all cluster components.
type Cluster struct {
	mu sync.RWMutex

	config      ClusterConfig
	raft        *RaftNode
	membership  *MembershipManager
	partitions  *PartitionManager
	replication *ReplicationManager
	transport   *TCPTransport
	applier     CommandApplier // For applying committed commands
	authApplier AuthApplier    // For applying user/ACL commands

	logger *logging.Logger
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewCluster creates a new cluster instance.
func NewCluster(cfg *config.Config) (*Cluster, error) {
	// Get the resolved advertise cluster address (auto-detects if not set)
	advertiseCluster := cfg.GetAdvertiseCluster()

	clusterConfig := ClusterConfig{
		NodeID:            cfg.NodeID,
		ClusterAddr:       cfg.ClusterAddr,
		AdvertiseCluster:  advertiseCluster,
		Peers:             cfg.Peers,
		DataDir:           cfg.DataDir,
		ReplicationFactor: 3,
		MinISR:            2,
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
		AsyncReplication:   true,
		ReplicationTimeout: 5 * time.Second,
	}

	applyCh := make(chan LogEntry, 10000)
	raft, err := NewRaftNode(raftConfig, c.transport, applyCh)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}
	c.raft = raft

	// Initialize membership manager
	membershipConfig := MembershipConfig{
		NodeID:         clusterConfig.NodeID,
		Address:        cfg.BindAddr,
		ClusterAddr:    clusterConfig.ClusterAddr,
		AdvertiseAddr:  clusterConfig.AdvertiseCluster,
		DataDir:        clusterConfig.DataDir,
		GossipInterval: 1 * time.Second,
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    30 * time.Second,
		Peers:          clusterConfig.Peers,
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

// GetPartitionLeader returns the leader for a partition.
func (c *Cluster) GetPartitionLeader(topic string, partition int) (string, error) {
	assignment, ok := c.partitions.GetAssignment(topic, partition)
	if !ok {
		return "", fmt.Errorf("partition not found: %s-%d", topic, partition)
	}
	return assignment.Leader, nil
}

// IsPartitionLeader returns true if this node is the leader for the partition.
func (c *Cluster) IsPartitionLeader(topic string, partition int) bool {
	assignment, ok := c.partitions.GetAssignment(topic, partition)
	if !ok {
		return false
	}
	return assignment.Leader == c.config.NodeID
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
					if err := c.partitions.UpdateLeader(assignment.Topic, assignment.Partition, member.ID); err != nil {
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

func (c *Cluster) onLeaderChange(topic string, partition int, newLeader string) {
	c.logger.Info("Partition leader changed", "topic", topic, "partition", partition, "new_leader", newLeader)

	// If we're no longer the leader, we might need to start following the new leader
	// In Raft-based replication, this is handled by the Raft consensus layer
	// The actual data replication happens through Raft log replication
	if newLeader != c.config.NodeID {
		member, ok := c.membership.GetMember(newLeader)
		if !ok {
			c.logger.Error("Leader member not found", "leader", newLeader)
			return
		}
		c.logger.Debug("Following new partition leader",
			"topic", topic,
			"partition", partition,
			"leader", newLeader,
			"leader_address", member.Address)
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

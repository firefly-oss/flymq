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
Partition Management for FlyMQ.

OVERVIEW:
=========
Topics are divided into partitions for parallelism and scalability.
Each partition has a leader and multiple replicas for fault tolerance.

PARTITION STATES:
=================
- ONLINE: Partition is serving requests
- OFFLINE: Partition is unavailable
- REASSIGNING: Partition is being moved to another node
- SYNCING: Replica is catching up with leader

LEADER ELECTION:
================
When a partition leader fails:
1. Controller detects failure via membership
2. Controller selects new leader from ISR (In-Sync Replicas)
3. New leader is announced to all nodes
4. Clients are redirected to new leader

PARTITION ASSIGNMENT:
=====================
Partitions are assigned to nodes using consistent hashing
to minimize reassignment when nodes join/leave.
*/
package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"flymq/internal/logging"
)

// PartitionState represents the state of a partition.
type PartitionState int

const (
	PartitionOnline PartitionState = iota
	PartitionOffline
	PartitionReassigning
	PartitionSyncing
)

func (s PartitionState) String() string {
	switch s {
	case PartitionOnline:
		return "online"
	case PartitionOffline:
		return "offline"
	case PartitionReassigning:
		return "reassigning"
	case PartitionSyncing:
		return "syncing"
	default:
		return "unknown"
	}
}

// PartitionAssignment represents the assignment of a partition.
// This is the core structure for partition-level leadership distribution.
// Unlike Raft leadership (which is cluster-wide), partition leadership
// can be distributed across different nodes for horizontal scaling.
type PartitionAssignment struct {
	Topic     string         `json:"topic"`
	Partition int            `json:"partition"`
	Leader    string         `json:"leader"`    // Node ID of the partition leader
	Replicas  []string       `json:"replicas"`  // Node IDs of all replicas (including leader)
	ISR       []string       `json:"isr"`       // In-Sync Replicas (node IDs)
	State     PartitionState `json:"state"`

	// Node addresses for direct routing (populated from membership)
	LeaderAddr   string            `json:"leader_addr,omitempty"`   // Client-facing address of leader
	ReplicaAddrs map[string]string `json:"replica_addrs,omitempty"` // Node ID -> client address
	Epoch        uint64            `json:"epoch"`                   // Incremented on leader change for fencing
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

// ReassignmentPlan represents a plan to reassign partitions.
type ReassignmentPlan struct {
	Assignments []PartitionReassignment `json:"assignments"`
}

// PartitionReassignment represents a single partition reassignment.
type PartitionReassignment struct {
	Topic            string   `json:"topic"`
	Partition        int      `json:"partition"`
	OldReplicas      []string `json:"old_replicas"`
	NewReplicas      []string `json:"new_replicas"`
	AddingReplicas   []string `json:"adding_replicas"`
	RemovingReplicas []string `json:"removing_replicas"`
}

// PartitionManager manages partition assignments across the cluster.
type PartitionManager struct {
	mu          sync.RWMutex
	assignments map[string]map[int]*PartitionAssignment // topic -> partition -> assignment
	dataDir     string
	nodeID      string
	logger      *logging.Logger

	// Callbacks
	onLeaderChange func(topic string, partition int, newLeader string)
}

// NewPartitionManager creates a new partition manager.
func NewPartitionManager(nodeID, dataDir string) (*PartitionManager, error) {
	pm := &PartitionManager{
		assignments: make(map[string]map[int]*PartitionAssignment),
		dataDir:     dataDir,
		nodeID:      nodeID,
		logger:      logging.NewLogger("partition"),
	}

	if err := pm.loadAssignments(); err != nil {
		pm.logger.Warn("Failed to load partition assignments", "error", err)
	}

	return pm, nil
}

// SetLeaderChangeCallback sets the callback for leader changes.
func (pm *PartitionManager) SetLeaderChangeCallback(cb func(topic string, partition int, newLeader string)) {
	pm.onLeaderChange = cb
}

// GetAssignment returns the assignment for a partition.
func (pm *PartitionManager) GetAssignment(topic string, partition int) (*PartitionAssignment, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			return assignment, true
		}
	}
	return nil, false
}

// GetTopicAssignments returns all assignments for a topic.
func (pm *PartitionManager) GetTopicAssignments(topic string) []*PartitionAssignment {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var assignments []*PartitionAssignment
	if topicAssignments, ok := pm.assignments[topic]; ok {
		for _, assignment := range topicAssignments {
			assignments = append(assignments, assignment)
		}
	}
	return assignments
}

// GetLeaderPartitions returns partitions where this node is the leader.
func (pm *PartitionManager) GetLeaderPartitions() []*PartitionAssignment {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var assignments []*PartitionAssignment
	for _, topicAssignments := range pm.assignments {
		for _, assignment := range topicAssignments {
			if assignment.Leader == pm.nodeID {
				assignments = append(assignments, assignment)
			}
		}
	}
	return assignments
}

// GetReplicaPartitions returns partitions where this node is a replica.
func (pm *PartitionManager) GetReplicaPartitions() []*PartitionAssignment {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var assignments []*PartitionAssignment
	for _, topicAssignments := range pm.assignments {
		for _, assignment := range topicAssignments {
			for _, replica := range assignment.Replicas {
				if replica == pm.nodeID {
					assignments = append(assignments, assignment)
					break
				}
			}
		}
	}
	return assignments
}

// CreateAssignment creates a new partition assignment.
// The leader and replicas are node IDs. Addresses are populated separately
// via UpdateReplicaAddresses when membership information is available.
func (pm *PartitionManager) CreateAssignment(topic string, partition int, leader string, replicas []string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, ok := pm.assignments[topic]; !ok {
		pm.assignments[topic] = make(map[int]*PartitionAssignment)
	}

	assignment := &PartitionAssignment{
		Topic:        topic,
		Partition:    partition,
		Leader:       leader,
		Replicas:     replicas,
		ISR:          replicas, // Initially all replicas are in-sync
		State:        PartitionOnline,
		ReplicaAddrs: make(map[string]string),
		Epoch:        1, // Start at epoch 1
	}

	pm.assignments[topic][partition] = assignment
	pm.logger.Info("Created partition assignment", "topic", topic, "partition", partition, "leader", leader, "epoch", 1)

	return pm.saveAssignments()
}

// CreateAssignmentWithAddr creates a new partition assignment with leader address.
// This is the preferred method when the leader address is known.
func (pm *PartitionManager) CreateAssignmentWithAddr(topic string, partition int, leader, leaderAddr string, replicas []string, replicaAddrs map[string]string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, ok := pm.assignments[topic]; !ok {
		pm.assignments[topic] = make(map[int]*PartitionAssignment)
	}

	assignment := &PartitionAssignment{
		Topic:        topic,
		Partition:    partition,
		Leader:       leader,
		LeaderAddr:   leaderAddr,
		Replicas:     replicas,
		ISR:          replicas,
		State:        PartitionOnline,
		ReplicaAddrs: replicaAddrs,
		Epoch:        1,
	}

	pm.assignments[topic][partition] = assignment
	pm.logger.Info("Created partition assignment with address",
		"topic", topic, "partition", partition, "leader", leader, "leader_addr", leaderAddr)

	return pm.saveAssignments()
}

// UpdateLeader updates the leader for a partition.
// This increments the epoch to fence stale leaders.
func (pm *PartitionManager) UpdateLeader(topic string, partition int, newLeader string) error {
	return pm.UpdateLeaderWithAddr(topic, partition, newLeader, "")
}

// UpdateLeaderWithAddr updates the leader for a partition with the new leader's address.
// This increments the epoch to fence stale leaders.
func (pm *PartitionManager) UpdateLeaderWithAddr(topic string, partition int, newLeader, newLeaderAddr string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			oldLeader := assignment.Leader
			oldEpoch := assignment.Epoch
			assignment.Leader = newLeader
			assignment.Epoch++ // Increment epoch on leader change for fencing

			// Update leader address if provided
			if newLeaderAddr != "" {
				assignment.LeaderAddr = newLeaderAddr
			} else if addr, ok := assignment.ReplicaAddrs[newLeader]; ok {
				// Try to get address from replica addresses
				assignment.LeaderAddr = addr
			}

			pm.logger.Info("Updated partition leader",
				"topic", topic,
				"partition", partition,
				"old_leader", oldLeader,
				"new_leader", newLeader,
				"old_epoch", oldEpoch,
				"new_epoch", assignment.Epoch)

			if pm.onLeaderChange != nil {
				go pm.onLeaderChange(topic, partition, newLeader)
			}

			return pm.saveAssignments()
		}
	}

	return fmt.Errorf("partition not found: %s-%d", topic, partition)
}

// UpdateISR updates the in-sync replicas for a partition.
func (pm *PartitionManager) UpdateISR(topic string, partition int, isr []string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			assignment.ISR = isr
			return pm.saveAssignments()
		}
	}

	return fmt.Errorf("partition not found: %s-%d", topic, partition)
}

// Reassign initiates a partition reassignment.
func (pm *PartitionManager) Reassign(plan *ReassignmentPlan) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, reassignment := range plan.Assignments {
		if topicAssignments, ok := pm.assignments[reassignment.Topic]; ok {
			if assignment, ok := topicAssignments[reassignment.Partition]; ok {
				assignment.State = PartitionReassigning
				assignment.Replicas = reassignment.NewReplicas

				// If leader is being removed, elect new leader from new replicas
				leaderRemoved := true
				for _, replica := range reassignment.NewReplicas {
					if replica == assignment.Leader {
						leaderRemoved = false
						break
					}
				}

				if leaderRemoved && len(reassignment.NewReplicas) > 0 {
					assignment.Leader = reassignment.NewReplicas[0]
				}

				pm.logger.Info("Partition reassignment started",
					"topic", reassignment.Topic,
					"partition", reassignment.Partition,
					"new_replicas", reassignment.NewReplicas)
			}
		}
	}

	return pm.saveAssignments()
}

// CompleteReassignment marks a reassignment as complete.
func (pm *PartitionManager) CompleteReassignment(topic string, partition int) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			assignment.State = PartitionOnline
			assignment.ISR = assignment.Replicas
			pm.logger.Info("Partition reassignment completed", "topic", topic, "partition", partition)
			return pm.saveAssignments()
		}
	}

	return fmt.Errorf("partition not found: %s-%d", topic, partition)
}

// ElectLeader elects a new leader for a partition from the ISR.
func (pm *PartitionManager) ElectLeader(topic string, partition int) (string, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			if len(assignment.ISR) == 0 {
				return "", fmt.Errorf("no in-sync replicas available for %s-%d", topic, partition)
			}

			// Prefer first ISR member as new leader
			newLeader := assignment.ISR[0]
			assignment.Leader = newLeader
			pm.logger.Info("Elected new leader", "topic", topic, "partition", partition, "leader", newLeader)

			if pm.onLeaderChange != nil {
				go pm.onLeaderChange(topic, partition, newLeader)
			}

			return newLeader, pm.saveAssignments()
		}
	}

	return "", fmt.Errorf("partition not found: %s-%d", topic, partition)
}

func (pm *PartitionManager) loadAssignments() error {
	assignmentFile := filepath.Join(pm.dataDir, "partition_assignments.json")
	data, err := os.ReadFile(assignmentFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var assignments []*PartitionAssignment
	if err := json.Unmarshal(data, &assignments); err != nil {
		return err
	}

	for _, assignment := range assignments {
		if _, ok := pm.assignments[assignment.Topic]; !ok {
			pm.assignments[assignment.Topic] = make(map[int]*PartitionAssignment)
		}
		pm.assignments[assignment.Topic][assignment.Partition] = assignment
	}

	return nil
}

func (pm *PartitionManager) saveAssignments() error {
	if err := os.MkdirAll(pm.dataDir, 0755); err != nil {
		return err
	}

	var assignments []*PartitionAssignment
	for _, topicAssignments := range pm.assignments {
		for _, assignment := range topicAssignments {
			assignments = append(assignments, assignment)
		}
	}

	data, err := json.MarshalIndent(assignments, "", "  ")
	if err != nil {
		return err
	}

	// Use atomic write: write to temp file, sync, then rename
	// This ensures the assignments file is never corrupted even if the server crashes
	assignmentFile := filepath.Join(pm.dataDir, "partition_assignments.json")
	tempFile := assignmentFile + ".tmp"

	f, err := os.Create(tempFile)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tempFile)
		return err
	}

	// Sync to ensure data is on disk before rename
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tempFile)
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(tempFile)
		return err
	}

	// Atomic rename - this is atomic on POSIX systems
	return os.Rename(tempFile, assignmentFile)
}

// ============================================================================
// Partition Routing Methods for Horizontal Scaling
// ============================================================================

// GetPartitionLeader returns the leader info for a specific partition.
// This is used for routing produce/consume requests to the correct node.
func (pm *PartitionManager) GetPartitionLeader(topic string, partition int) (*PartitionLeaderInfo, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			return &PartitionLeaderInfo{
				Topic:       topic,
				Partition:   partition,
				LeaderID:    assignment.Leader,
				LeaderAddr:  assignment.LeaderAddr,
				Epoch:       assignment.Epoch,
				IsLocalNode: assignment.Leader == pm.nodeID,
			}, true
		}
	}
	return nil, false
}

// IsPartitionLeader returns true if this node is the leader for the partition.
func (pm *PartitionManager) IsPartitionLeader(topic string, partition int) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			return assignment.Leader == pm.nodeID
		}
	}
	return false
}

// GetAllPartitionLeaders returns leader info for all partitions of a topic.
// Used by clients to build a routing table.
func (pm *PartitionManager) GetAllPartitionLeaders(topic string) []*PartitionLeaderInfo {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var leaders []*PartitionLeaderInfo
	if topicAssignments, ok := pm.assignments[topic]; ok {
		for _, assignment := range topicAssignments {
			leaders = append(leaders, &PartitionLeaderInfo{
				Topic:       topic,
				Partition:   assignment.Partition,
				LeaderID:    assignment.Leader,
				LeaderAddr:  assignment.LeaderAddr,
				Epoch:       assignment.Epoch,
				IsLocalNode: assignment.Leader == pm.nodeID,
			})
		}
	}
	return leaders
}

// UpdateLeaderAddress updates the client-facing address for a partition leader.
// Called when membership information is updated.
func (pm *PartitionManager) UpdateLeaderAddress(topic string, partition int, leaderAddr string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			assignment.LeaderAddr = leaderAddr
			return pm.saveAssignments()
		}
	}
	return fmt.Errorf("partition not found: %s-%d", topic, partition)
}

// UpdateReplicaAddresses updates the addresses for all replicas of a partition.
func (pm *PartitionManager) UpdateReplicaAddresses(topic string, partition int, addrs map[string]string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			assignment.ReplicaAddrs = addrs
			// Also update leader address if present
			if leaderAddr, ok := addrs[assignment.Leader]; ok {
				assignment.LeaderAddr = leaderAddr
			}
			return pm.saveAssignments()
		}
	}
	return fmt.Errorf("partition not found: %s-%d", topic, partition)
}

// GetClusterPartitionMetadata returns metadata for all partitions across all topics.
// This is used for the cluster metadata protocol response.
func (pm *PartitionManager) GetClusterPartitionMetadata() map[string][]*PartitionLeaderInfo {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	result := make(map[string][]*PartitionLeaderInfo)
	for topic, topicAssignments := range pm.assignments {
		for _, assignment := range topicAssignments {
			result[topic] = append(result[topic], &PartitionLeaderInfo{
				Topic:       topic,
				Partition:   assignment.Partition,
				LeaderID:    assignment.Leader,
				LeaderAddr:  assignment.LeaderAddr,
				Epoch:       assignment.Epoch,
				IsLocalNode: assignment.Leader == pm.nodeID,
			})
		}
	}
	return result
}

// IncrementEpoch increments the epoch for a partition (called on leader change).
func (pm *PartitionManager) IncrementEpoch(topic string, partition int) (uint64, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			assignment.Epoch++
			if err := pm.saveAssignments(); err != nil {
				return 0, err
			}
			return assignment.Epoch, nil
		}
	}
	return 0, fmt.Errorf("partition not found: %s-%d", topic, partition)
}

// ============================================================================
// Smart Partition Distribution for Horizontal Scaling
// ============================================================================

// PartitionDistributor handles intelligent distribution of partition leaders
// across cluster nodes to achieve load balancing.
type PartitionDistributor struct {
	pm     *PartitionManager
	logger *logging.Logger
}

// NewPartitionDistributor creates a new partition distributor.
func NewPartitionDistributor(pm *PartitionManager) *PartitionDistributor {
	return &PartitionDistributor{
		pm:     pm,
		logger: logging.NewLogger("partition-distributor"),
	}
}

// DistributionPlan represents a plan for distributing partitions.
type DistributionPlan struct {
	Assignments []PartitionPlacement
}

// PartitionPlacement represents where a partition should be placed.
type PartitionPlacement struct {
	Topic     string
	Partition int
	Leader    string   // Node ID of the leader
	Replicas  []string // Node IDs of replicas (including leader)
}

// ComputeDistribution computes an optimal distribution of partitions across nodes.
// Uses a round-robin approach with rack awareness (if available).
func (pd *PartitionDistributor) ComputeDistribution(topic string, numPartitions int, nodes []string, replicationFactor int) *DistributionPlan {
	if len(nodes) == 0 {
		return &DistributionPlan{}
	}

	// Ensure replication factor doesn't exceed node count
	if replicationFactor > len(nodes) {
		replicationFactor = len(nodes)
	}

	plan := &DistributionPlan{
		Assignments: make([]PartitionPlacement, numPartitions),
	}

	// Round-robin distribution of leaders
	for i := 0; i < numPartitions; i++ {
		leaderIdx := i % len(nodes)
		leader := nodes[leaderIdx]

		// Select replicas (including leader) using round-robin from leader position
		replicas := make([]string, replicationFactor)
		for j := 0; j < replicationFactor; j++ {
			replicaIdx := (leaderIdx + j) % len(nodes)
			replicas[j] = nodes[replicaIdx]
		}

		plan.Assignments[i] = PartitionPlacement{
			Topic:     topic,
			Partition: i,
			Leader:    leader,
			Replicas:  replicas,
		}
	}

	return plan
}

// ComputeRebalance computes a rebalancing plan when nodes change.
// This minimizes partition movement while achieving balance.
func (pd *PartitionDistributor) ComputeRebalance(nodes []string, replicationFactor int) *DistributionPlan {
	pd.pm.mu.RLock()
	defer pd.pm.mu.RUnlock()

	if len(nodes) == 0 {
		return &DistributionPlan{}
	}

	// Count current leader assignments per node
	leaderCounts := make(map[string]int)
	for _, node := range nodes {
		leaderCounts[node] = 0
	}

	// Collect all current assignments
	var allAssignments []*PartitionAssignment
	for _, topicAssignments := range pd.pm.assignments {
		for _, assignment := range topicAssignments {
			allAssignments = append(allAssignments, assignment)
			if _, ok := leaderCounts[assignment.Leader]; ok {
				leaderCounts[assignment.Leader]++
			}
		}
	}

	if len(allAssignments) == 0 {
		return &DistributionPlan{}
	}

	// Calculate target leaders per node
	totalPartitions := len(allAssignments)
	targetPerNode := totalPartitions / len(nodes)
	remainder := totalPartitions % len(nodes)

	// Find overloaded and underloaded nodes
	type nodeLoad struct {
		nodeID string
		count  int
		target int
	}

	nodeLoads := make([]nodeLoad, 0, len(nodes))
	for i, node := range nodes {
		target := targetPerNode
		if i < remainder {
			target++
		}
		nodeLoads = append(nodeLoads, nodeLoad{
			nodeID: node,
			count:  leaderCounts[node],
			target: target,
		})
	}

	// Build rebalance plan
	plan := &DistributionPlan{
		Assignments: make([]PartitionPlacement, 0),
	}

	// Find partitions that need to move (leader on dead node or overloaded node)
	for _, assignment := range allAssignments {
		needsReassignment := false
		newLeader := assignment.Leader

		// Check if current leader is still in the cluster
		leaderAlive := false
		for _, node := range nodes {
			if node == assignment.Leader {
				leaderAlive = true
				break
			}
		}

		if !leaderAlive {
			// Leader is dead, need to elect new leader from replicas
			needsReassignment = true
			for _, replica := range assignment.Replicas {
				for _, node := range nodes {
					if replica == node {
						newLeader = replica
						break
					}
				}
				if newLeader != assignment.Leader {
					break
				}
			}
			// If no replica is alive, pick any available node
			if newLeader == assignment.Leader && len(nodes) > 0 {
				newLeader = nodes[0]
			}
		}

		if needsReassignment {
			// Compute new replicas
			newReplicas := pd.selectReplicas(newLeader, nodes, replicationFactor)
			plan.Assignments = append(plan.Assignments, PartitionPlacement{
				Topic:     assignment.Topic,
				Partition: assignment.Partition,
				Leader:    newLeader,
				Replicas:  newReplicas,
			})
		}
	}

	return plan
}

// selectReplicas selects replicas for a partition, starting with the leader.
func (pd *PartitionDistributor) selectReplicas(leader string, nodes []string, replicationFactor int) []string {
	if replicationFactor > len(nodes) {
		replicationFactor = len(nodes)
	}

	replicas := make([]string, 0, replicationFactor)
	replicas = append(replicas, leader)

	// Add other nodes as replicas
	for _, node := range nodes {
		if len(replicas) >= replicationFactor {
			break
		}
		if node != leader {
			replicas = append(replicas, node)
		}
	}

	return replicas
}

// GetLeaderDistribution returns the current distribution of leaders across nodes.
func (pd *PartitionDistributor) GetLeaderDistribution() map[string]int {
	pd.pm.mu.RLock()
	defer pd.pm.mu.RUnlock()

	distribution := make(map[string]int)
	for _, topicAssignments := range pd.pm.assignments {
		for _, assignment := range topicAssignments {
			distribution[assignment.Leader]++
		}
	}
	return distribution
}

// IsBalanced returns true if partition leaders are evenly distributed.
// Allows for a difference of 1 partition between nodes (due to remainder).
func (pd *PartitionDistributor) IsBalanced(nodes []string) bool {
	if len(nodes) == 0 {
		return true
	}

	distribution := pd.GetLeaderDistribution()

	// Calculate expected range
	totalPartitions := 0
	for _, count := range distribution {
		totalPartitions += count
	}

	if totalPartitions == 0 {
		return true
	}

	minExpected := totalPartitions / len(nodes)
	maxExpected := minExpected + 1

	for _, node := range nodes {
		count := distribution[node]
		if count < minExpected || count > maxExpected {
			return false
		}
	}

	return true
}

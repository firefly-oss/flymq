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
type PartitionAssignment struct {
	Topic     string         `json:"topic"`
	Partition int            `json:"partition"`
	Leader    string         `json:"leader"`
	Replicas  []string       `json:"replicas"`
	ISR       []string       `json:"isr"` // In-Sync Replicas
	State     PartitionState `json:"state"`
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
func (pm *PartitionManager) CreateAssignment(topic string, partition int, leader string, replicas []string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, ok := pm.assignments[topic]; !ok {
		pm.assignments[topic] = make(map[int]*PartitionAssignment)
	}

	assignment := &PartitionAssignment{
		Topic:     topic,
		Partition: partition,
		Leader:    leader,
		Replicas:  replicas,
		ISR:       replicas, // Initially all replicas are in-sync
		State:     PartitionOnline,
	}

	pm.assignments[topic][partition] = assignment
	pm.logger.Info("Created partition assignment", "topic", topic, "partition", partition, "leader", leader)

	return pm.saveAssignments()
}

// UpdateLeader updates the leader for a partition.
func (pm *PartitionManager) UpdateLeader(topic string, partition int, newLeader string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if topicAssignments, ok := pm.assignments[topic]; ok {
		if assignment, ok := topicAssignments[partition]; ok {
			oldLeader := assignment.Leader
			assignment.Leader = newLeader
			pm.logger.Info("Updated partition leader", "topic", topic, "partition", partition, "old_leader", oldLeader, "new_leader", newLeader)

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

	assignmentFile := filepath.Join(pm.dataDir, "partition_assignments.json")
	return os.WriteFile(assignmentFile, data, 0644)
}

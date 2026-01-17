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

package cluster

import (
	"testing"
	"time"
)

func TestMemberStatusString(t *testing.T) {
	tests := []struct {
		status   MemberStatus
		expected string
	}{
		{MemberAlive, "alive"},
		{MemberSuspect, "suspect"},
		{MemberDead, "dead"},
		{MemberLeft, "left"},
		{MemberStatus(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.status.String(); got != tt.expected {
			t.Errorf("MemberStatus(%d).String() = %s, want %s", tt.status, got, tt.expected)
		}
	}
}

func TestPartitionStateString(t *testing.T) {
	tests := []struct {
		state    PartitionState
		expected string
	}{
		{PartitionOnline, "online"},
		{PartitionOffline, "offline"},
		{PartitionReassigning, "reassigning"},
		{PartitionSyncing, "syncing"},
		{PartitionState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("PartitionState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{Follower, "follower"},
		{Candidate, "candidate"},
		{Leader, "leader"},
		{NodeState(99), "unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("NodeState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

func TestDefaultMembershipConfig(t *testing.T) {
	cfg := DefaultMembershipConfig()

	if cfg.GossipInterval != 1*time.Second {
		t.Errorf("GossipInterval = %v, want 1s", cfg.GossipInterval)
	}
	if cfg.SuspectTimeout != 5*time.Second {
		t.Errorf("SuspectTimeout = %v, want 5s", cfg.SuspectTimeout)
	}
	if cfg.DeadTimeout != 30*time.Second {
		t.Errorf("DeadTimeout = %v, want 30s", cfg.DeadTimeout)
	}
}

func TestDefaultRaftConfig(t *testing.T) {
	cfg := DefaultRaftConfig()

	if cfg.ElectionTimeout != 150*time.Millisecond {
		t.Errorf("ElectionTimeout = %v, want 150ms", cfg.ElectionTimeout)
	}
	if cfg.HeartbeatInterval != 50*time.Millisecond {
		t.Errorf("HeartbeatInterval = %v, want 50ms", cfg.HeartbeatInterval)
	}
}

func TestNewMembershipManager(t *testing.T) {
	dir := t.TempDir()

	config := MembershipConfig{
		NodeID:         "node-1",
		Address:        "127.0.0.1:9000",
		ClusterAddr:    "127.0.0.1:9001",
		DataDir:        dir,
		GossipInterval: 1 * time.Second,
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    30 * time.Second,
	}

	mm, err := NewMembershipManager(config)
	if err != nil {
		t.Fatalf("NewMembershipManager failed: %v", err)
	}

	// Check self is added
	self := mm.Self()
	if self == nil {
		t.Fatal("Self() returned nil")
	}
	if self.ID != "node-1" {
		t.Errorf("Self().ID = %s, want node-1", self.ID)
	}
	if self.Status != MemberAlive {
		t.Errorf("Self().Status = %v, want MemberAlive", self.Status)
	}

	// Check members includes self
	members := mm.GetMembers()
	if len(members) != 1 {
		t.Errorf("GetMembers() returned %d members, want 1", len(members))
	}
}

func TestMembershipManagerJoinLeave(t *testing.T) {
	dir := t.TempDir()

	config := MembershipConfig{
		NodeID:         "node-1",
		Address:        "127.0.0.1:9000",
		ClusterAddr:    "127.0.0.1:9001",
		DataDir:        dir,
		GossipInterval: 1 * time.Second,
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    30 * time.Second,
	}

	mm, err := NewMembershipManager(config)
	if err != nil {
		t.Fatalf("NewMembershipManager failed: %v", err)
	}

	// Join a new member
	newMember := &Member{
		ID:          "node-2",
		Address:     "127.0.0.1:9002",
		ClusterAddr: "127.0.0.1:9003",
		Metadata:    make(map[string]string),
	}

	if err := mm.Join(newMember); err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	// Check member was added
	members := mm.GetMembers()
	if len(members) != 2 {
		t.Errorf("GetMembers() returned %d members, want 2", len(members))
	}

	// Get specific member
	member, ok := mm.GetMember("node-2")
	if !ok {
		t.Fatal("GetMember(node-2) returned false")
	}
	if member.Address != "127.0.0.1:9002" {
		t.Errorf("member.Address = %s, want 127.0.0.1:9002", member.Address)
	}

	// Leave
	if err := mm.Leave("node-2"); err != nil {
		t.Fatalf("Leave failed: %v", err)
	}

	// Check member status changed
	member, ok = mm.GetMember("node-2")
	if !ok {
		t.Fatal("GetMember(node-2) returned false after leave")
	}
	if member.Status != MemberLeft {
		t.Errorf("member.Status = %v, want MemberLeft", member.Status)
	}

	// GetMembers should not include left members
	members = mm.GetMembers()
	if len(members) != 1 {
		t.Errorf("GetMembers() returned %d members after leave, want 1", len(members))
	}

	// GetAllMembers should include all
	allMembers := mm.GetAllMembers()
	if len(allMembers) != 2 {
		t.Errorf("GetAllMembers() returned %d members, want 2", len(allMembers))
	}
}

func TestMembershipManagerUpdateLastSeen(t *testing.T) {
	dir := t.TempDir()

	config := MembershipConfig{
		NodeID:         "node-1",
		Address:        "127.0.0.1:9000",
		ClusterAddr:    "127.0.0.1:9001",
		DataDir:        dir,
		GossipInterval: 1 * time.Second,
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    30 * time.Second,
	}

	mm, err := NewMembershipManager(config)
	if err != nil {
		t.Fatalf("NewMembershipManager failed: %v", err)
	}

	// Join a member
	newMember := &Member{
		ID:          "node-2",
		Address:     "127.0.0.1:9002",
		ClusterAddr: "127.0.0.1:9003",
		Metadata:    make(map[string]string),
	}
	mm.Join(newMember)

	// Get initial last seen
	member, _ := mm.GetMember("node-2")
	initialLastSeen := member.LastSeen

	// Wait a bit and update
	time.Sleep(10 * time.Millisecond)
	mm.UpdateLastSeen("node-2")

	// Check last seen was updated
	member, _ = mm.GetMember("node-2")
	if !member.LastSeen.After(initialLastSeen) {
		t.Error("LastSeen was not updated")
	}
}

func TestMembershipManagerStartStop(t *testing.T) {
	dir := t.TempDir()

	config := MembershipConfig{
		NodeID:         "node-1",
		Address:        "127.0.0.1:9000",
		ClusterAddr:    "127.0.0.1:9001",
		DataDir:        dir,
		GossipInterval: 100 * time.Millisecond,
		SuspectTimeout: 500 * time.Millisecond,
		DeadTimeout:    1 * time.Second,
	}

	mm, err := NewMembershipManager(config)
	if err != nil {
		t.Fatalf("NewMembershipManager failed: %v", err)
	}

	if err := mm.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	if err := mm.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestMembershipManagerEventChannels(t *testing.T) {
	dir := t.TempDir()

	config := MembershipConfig{
		NodeID:         "node-1",
		Address:        "127.0.0.1:9000",
		ClusterAddr:    "127.0.0.1:9001",
		DataDir:        dir,
		GossipInterval: 1 * time.Second,
		SuspectTimeout: 5 * time.Second,
		DeadTimeout:    30 * time.Second,
	}

	mm, err := NewMembershipManager(config)
	if err != nil {
		t.Fatalf("NewMembershipManager failed: %v", err)
	}

	joinCh := mm.JoinEvents()
	leaveCh := mm.LeaveEvents()

	// Join a member
	newMember := &Member{
		ID:          "node-2",
		Address:     "127.0.0.1:9002",
		ClusterAddr: "127.0.0.1:9003",
		Metadata:    make(map[string]string),
	}

	go mm.Join(newMember)

	select {
	case member := <-joinCh:
		if member.ID != "node-2" {
			t.Errorf("Join event member.ID = %s, want node-2", member.ID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for join event")
	}

	// Leave
	go mm.Leave("node-2")

	select {
	case member := <-leaveCh:
		if member.ID != "node-2" {
			t.Errorf("Leave event member.ID = %s, want node-2", member.ID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for leave event")
	}
}

func TestNewPartitionManager(t *testing.T) {
	dir := t.TempDir()

	pm, err := NewPartitionManager("node-1", dir)
	if err != nil {
		t.Fatalf("NewPartitionManager failed: %v", err)
	}

	// Initially no assignments
	assignments := pm.GetLeaderPartitions()
	if len(assignments) != 0 {
		t.Errorf("GetLeaderPartitions() returned %d, want 0", len(assignments))
	}
}

func TestPartitionManagerCreateAssignment(t *testing.T) {
	dir := t.TempDir()

	pm, err := NewPartitionManager("node-1", dir)
	if err != nil {
		t.Fatalf("NewPartitionManager failed: %v", err)
	}

	// Create assignment
	err = pm.CreateAssignment("test-topic", 0, "node-1", []string{"node-1", "node-2", "node-3"})
	if err != nil {
		t.Fatalf("CreateAssignment failed: %v", err)
	}

	// Get assignment
	assignment, ok := pm.GetAssignment("test-topic", 0)
	if !ok {
		t.Fatal("GetAssignment returned false")
	}
	if assignment.Leader != "node-1" {
		t.Errorf("assignment.Leader = %s, want node-1", assignment.Leader)
	}
	if len(assignment.Replicas) != 3 {
		t.Errorf("len(assignment.Replicas) = %d, want 3", len(assignment.Replicas))
	}
	if assignment.State != PartitionOnline {
		t.Errorf("assignment.State = %v, want PartitionOnline", assignment.State)
	}

	// Get topic assignments
	topicAssignments := pm.GetTopicAssignments("test-topic")
	if len(topicAssignments) != 1 {
		t.Errorf("GetTopicAssignments returned %d, want 1", len(topicAssignments))
	}

	// Get leader partitions
	leaderPartitions := pm.GetLeaderPartitions()
	if len(leaderPartitions) != 1 {
		t.Errorf("GetLeaderPartitions returned %d, want 1", len(leaderPartitions))
	}
}

func TestPartitionManagerUpdateLeader(t *testing.T) {
	dir := t.TempDir()

	pm, err := NewPartitionManager("node-1", dir)
	if err != nil {
		t.Fatalf("NewPartitionManager failed: %v", err)
	}

	// Set up callback
	var callbackCalled bool
	var callbackTopic string
	var callbackPartition int
	var callbackLeader string

	pm.SetLeaderChangeCallback(func(topic string, partition int, newLeader string) {
		callbackCalled = true
		callbackTopic = topic
		callbackPartition = partition
		callbackLeader = newLeader
	})

	// Create assignment
	pm.CreateAssignment("test-topic", 0, "node-1", []string{"node-1", "node-2", "node-3"})

	// Update leader
	err = pm.UpdateLeader("test-topic", 0, "node-2")
	if err != nil {
		t.Fatalf("UpdateLeader failed: %v", err)
	}

	// Check assignment updated
	assignment, _ := pm.GetAssignment("test-topic", 0)
	if assignment.Leader != "node-2" {
		t.Errorf("assignment.Leader = %s, want node-2", assignment.Leader)
	}

	// Wait for callback
	time.Sleep(50 * time.Millisecond)
	if !callbackCalled {
		t.Error("Leader change callback was not called")
	}
	if callbackTopic != "test-topic" || callbackPartition != 0 || callbackLeader != "node-2" {
		t.Errorf("Callback args: topic=%s, partition=%d, leader=%s", callbackTopic, callbackPartition, callbackLeader)
	}

	// Update non-existent partition
	err = pm.UpdateLeader("nonexistent", 0, "node-2")
	if err == nil {
		t.Error("Expected error for non-existent partition")
	}
}

func TestPartitionManagerUpdateISR(t *testing.T) {
	dir := t.TempDir()

	pm, err := NewPartitionManager("node-1", dir)
	if err != nil {
		t.Fatalf("NewPartitionManager failed: %v", err)
	}

	pm.CreateAssignment("test-topic", 0, "node-1", []string{"node-1", "node-2", "node-3"})

	// Update ISR
	err = pm.UpdateISR("test-topic", 0, []string{"node-1", "node-2"})
	if err != nil {
		t.Fatalf("UpdateISR failed: %v", err)
	}

	assignment, _ := pm.GetAssignment("test-topic", 0)
	if len(assignment.ISR) != 2 {
		t.Errorf("len(assignment.ISR) = %d, want 2", len(assignment.ISR))
	}
}

func TestPartitionManagerElectLeader(t *testing.T) {
	dir := t.TempDir()

	pm, err := NewPartitionManager("node-1", dir)
	if err != nil {
		t.Fatalf("NewPartitionManager failed: %v", err)
	}

	pm.CreateAssignment("test-topic", 0, "node-1", []string{"node-1", "node-2", "node-3"})

	// Elect new leader
	newLeader, err := pm.ElectLeader("test-topic", 0)
	if err != nil {
		t.Fatalf("ElectLeader failed: %v", err)
	}

	// Should elect first ISR member
	if newLeader != "node-1" {
		t.Errorf("newLeader = %s, want node-1", newLeader)
	}

	// Update ISR and elect again
	pm.UpdateISR("test-topic", 0, []string{"node-2", "node-3"})
	newLeader, err = pm.ElectLeader("test-topic", 0)
	if err != nil {
		t.Fatalf("ElectLeader failed: %v", err)
	}
	if newLeader != "node-2" {
		t.Errorf("newLeader = %s, want node-2", newLeader)
	}

	// Empty ISR should fail
	pm.UpdateISR("test-topic", 0, []string{})
	_, err = pm.ElectLeader("test-topic", 0)
	if err == nil {
		t.Error("Expected error for empty ISR")
	}
}

func TestPartitionManagerReassign(t *testing.T) {
	dir := t.TempDir()

	pm, err := NewPartitionManager("node-1", dir)
	if err != nil {
		t.Fatalf("NewPartitionManager failed: %v", err)
	}

	pm.CreateAssignment("test-topic", 0, "node-1", []string{"node-1", "node-2"})

	// Reassign
	plan := &ReassignmentPlan{
		Assignments: []PartitionReassignment{
			{
				Topic:       "test-topic",
				Partition:   0,
				OldReplicas: []string{"node-1", "node-2"},
				NewReplicas: []string{"node-2", "node-3"},
			},
		},
	}

	err = pm.Reassign(plan)
	if err != nil {
		t.Fatalf("Reassign failed: %v", err)
	}

	assignment, _ := pm.GetAssignment("test-topic", 0)
	if assignment.State != PartitionReassigning {
		t.Errorf("assignment.State = %v, want PartitionReassigning", assignment.State)
	}
	if len(assignment.Replicas) != 2 {
		t.Errorf("len(assignment.Replicas) = %d, want 2", len(assignment.Replicas))
	}

	// Complete reassignment
	err = pm.CompleteReassignment("test-topic", 0)
	if err != nil {
		t.Fatalf("CompleteReassignment failed: %v", err)
	}

	assignment, _ = pm.GetAssignment("test-topic", 0)
	if assignment.State != PartitionOnline {
		t.Errorf("assignment.State = %v, want PartitionOnline", assignment.State)
	}
}

func TestPartitionManagerGetReplicaPartitions(t *testing.T) {
	dir := t.TempDir()

	pm, err := NewPartitionManager("node-2", dir)
	if err != nil {
		t.Fatalf("NewPartitionManager failed: %v", err)
	}

	pm.CreateAssignment("test-topic", 0, "node-1", []string{"node-1", "node-2", "node-3"})
	pm.CreateAssignment("test-topic", 1, "node-3", []string{"node-3", "node-4"})

	replicas := pm.GetReplicaPartitions()
	if len(replicas) != 1 {
		t.Errorf("GetReplicaPartitions returned %d, want 1", len(replicas))
	}
}

// ============================================================================
// User and ACL Command Tests
// ============================================================================

func TestUserCommandEncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		cmdType CommandType
		payload interface{}
	}{
		{
			name:    "CreateUser",
			cmdType: CmdCreateUser,
			payload: &CreateUserPayload{
				Username:     "testuser",
				PasswordHash: "hashed_password_123",
				Roles:        []string{"admin", "user"},
			},
		},
		{
			name:    "DeleteUser",
			cmdType: CmdDeleteUser,
			payload: &DeleteUserPayload{
				Username: "testuser",
			},
		},
		{
			name:    "UpdateUser with roles",
			cmdType: CmdUpdateUser,
			payload: &UpdateUserPayload{
				Username: "testuser",
				Roles:    []string{"viewer"},
			},
		},
		{
			name:    "UpdateUser with enabled",
			cmdType: CmdUpdateUser,
			payload: &UpdateUserPayload{
				Username: "testuser",
				Enabled:  boolPtr(false),
			},
		},
		{
			name:    "UpdateUser with password",
			cmdType: CmdUpdateUser,
			payload: &UpdateUserPayload{
				Username:     "testuser",
				PasswordHash: "new_hashed_password",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cmd *Command
			var err error

			switch p := tt.payload.(type) {
			case *CreateUserPayload:
				cmd, err = NewCreateUserCommand(p.Username, p.PasswordHash, p.Roles)
			case *DeleteUserPayload:
				cmd, err = NewDeleteUserCommand(p.Username)
			case *UpdateUserPayload:
				cmd, err = NewUpdateUserCommand(p.Username, p.Roles, p.Enabled, p.PasswordHash)
			}

			if err != nil {
				t.Fatalf("Failed to create command: %v", err)
			}

			if cmd.Type != tt.cmdType {
				t.Errorf("Command type = %s, want %s", cmd.Type, tt.cmdType)
			}

			// Verify payload can be decoded
			switch tt.cmdType {
			case CmdCreateUser:
				decoded, err := DecodeCreateUserPayload(cmd.Payload)
				if err != nil {
					t.Fatalf("Failed to decode payload: %v", err)
				}
				original := tt.payload.(*CreateUserPayload)
				if decoded.Username != original.Username {
					t.Errorf("Username = %s, want %s", decoded.Username, original.Username)
				}
				if decoded.PasswordHash != original.PasswordHash {
					t.Errorf("PasswordHash = %s, want %s", decoded.PasswordHash, original.PasswordHash)
				}
			case CmdDeleteUser:
				decoded, err := DecodeDeleteUserPayload(cmd.Payload)
				if err != nil {
					t.Fatalf("Failed to decode payload: %v", err)
				}
				original := tt.payload.(*DeleteUserPayload)
				if decoded.Username != original.Username {
					t.Errorf("Username = %s, want %s", decoded.Username, original.Username)
				}
			case CmdUpdateUser:
				decoded, err := DecodeUpdateUserPayload(cmd.Payload)
				if err != nil {
					t.Fatalf("Failed to decode payload: %v", err)
				}
				original := tt.payload.(*UpdateUserPayload)
				if decoded.Username != original.Username {
					t.Errorf("Username = %s, want %s", decoded.Username, original.Username)
				}
			}
		})
	}
}

func TestACLCommandEncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		cmdType CommandType
		payload interface{}
	}{
		{
			name:    "SetACL public",
			cmdType: CmdSetACL,
			payload: &SetACLPayload{
				Topic:  "public-topic",
				Public: true,
			},
		},
		{
			name:    "SetACL with users and roles",
			cmdType: CmdSetACL,
			payload: &SetACLPayload{
				Topic:        "private-topic",
				Public:       false,
				AllowedUsers: []string{"user1", "user2"},
				AllowedRoles: []string{"admin"},
			},
		},
		{
			name:    "DeleteACL",
			cmdType: CmdDeleteACL,
			payload: &DeleteACLPayload{
				Topic: "some-topic",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cmd *Command
			var err error

			switch p := tt.payload.(type) {
			case *SetACLPayload:
				cmd, err = NewSetACLCommand(p.Topic, p.Public, p.AllowedUsers, p.AllowedRoles)
			case *DeleteACLPayload:
				cmd, err = NewDeleteACLCommand(p.Topic)
			}

			if err != nil {
				t.Fatalf("Failed to create command: %v", err)
			}

			if cmd.Type != tt.cmdType {
				t.Errorf("Command type = %s, want %s", cmd.Type, tt.cmdType)
			}

			// Verify payload can be decoded
			switch tt.cmdType {
			case CmdSetACL:
				decoded, err := DecodeSetACLPayload(cmd.Payload)
				if err != nil {
					t.Fatalf("Failed to decode payload: %v", err)
				}
				original := tt.payload.(*SetACLPayload)
				if decoded.Topic != original.Topic {
					t.Errorf("Topic = %s, want %s", decoded.Topic, original.Topic)
				}
				if decoded.Public != original.Public {
					t.Errorf("Public = %v, want %v", decoded.Public, original.Public)
				}
			case CmdDeleteACL:
				decoded, err := DecodeDeleteACLPayload(cmd.Payload)
				if err != nil {
					t.Fatalf("Failed to decode payload: %v", err)
				}
				original := tt.payload.(*DeleteACLPayload)
				if decoded.Topic != original.Topic {
					t.Errorf("Topic = %s, want %s", decoded.Topic, original.Topic)
				}
			}
		})
	}
}

// Helper function for bool pointer
func boolPtr(b bool) *bool {
	return &b
}

// ============================================================================
// Partition Distribution Tests
// ============================================================================

func TestPartitionDistributor_ComputeDistribution(t *testing.T) {
	// Create a partition manager
	pm, err := NewPartitionManager("test-node", t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	distributor := NewPartitionDistributor(pm)

	tests := []struct {
		name              string
		topic             string
		numPartitions     int
		nodes             []string
		replicationFactor int
		wantLeaders       map[string]int // expected leader count per node
	}{
		{
			name:              "3 partitions across 3 nodes",
			topic:             "test-topic",
			numPartitions:     3,
			nodes:             []string{"node1", "node2", "node3"},
			replicationFactor: 3,
			wantLeaders:       map[string]int{"node1": 1, "node2": 1, "node3": 1},
		},
		{
			name:              "6 partitions across 3 nodes",
			topic:             "test-topic",
			numPartitions:     6,
			nodes:             []string{"node1", "node2", "node3"},
			replicationFactor: 3,
			wantLeaders:       map[string]int{"node1": 2, "node2": 2, "node3": 2},
		},
		{
			name:              "5 partitions across 3 nodes (uneven)",
			topic:             "test-topic",
			numPartitions:     5,
			nodes:             []string{"node1", "node2", "node3"},
			replicationFactor: 2,
			wantLeaders:       map[string]int{"node1": 2, "node2": 2, "node3": 1},
		},
		{
			name:              "single node cluster",
			topic:             "test-topic",
			numPartitions:     3,
			nodes:             []string{"node1"},
			replicationFactor: 1,
			wantLeaders:       map[string]int{"node1": 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := distributor.ComputeDistribution(tt.topic, tt.numPartitions, tt.nodes, tt.replicationFactor)

			if len(plan.Assignments) != tt.numPartitions {
				t.Errorf("Got %d assignments, want %d", len(plan.Assignments), tt.numPartitions)
			}

			// Count leaders per node
			leaderCounts := make(map[string]int)
			for _, assignment := range plan.Assignments {
				leaderCounts[assignment.Leader]++

				// Verify replicas include leader
				hasLeader := false
				for _, replica := range assignment.Replicas {
					if replica == assignment.Leader {
						hasLeader = true
						break
					}
				}
				if !hasLeader {
					t.Errorf("Partition %d replicas don't include leader %s", assignment.Partition, assignment.Leader)
				}

				// Verify replication factor
				expectedRF := tt.replicationFactor
				if expectedRF > len(tt.nodes) {
					expectedRF = len(tt.nodes)
				}
				if len(assignment.Replicas) != expectedRF {
					t.Errorf("Partition %d has %d replicas, want %d", assignment.Partition, len(assignment.Replicas), expectedRF)
				}
			}

			// Verify leader distribution
			for node, wantCount := range tt.wantLeaders {
				if leaderCounts[node] != wantCount {
					t.Errorf("Node %s has %d leaders, want %d", node, leaderCounts[node], wantCount)
				}
			}
		})
	}
}

func TestPartitionDistributor_IsBalanced(t *testing.T) {
	pm, err := NewPartitionManager("test-node", t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	// Create some assignments
	_ = pm.CreateAssignment("topic1", 0, "node1", []string{"node1", "node2"})
	_ = pm.CreateAssignment("topic1", 1, "node2", []string{"node2", "node3"})
	_ = pm.CreateAssignment("topic1", 2, "node3", []string{"node3", "node1"})

	distributor := NewPartitionDistributor(pm)

	// Should be balanced with 3 nodes (1 leader each)
	if !distributor.IsBalanced([]string{"node1", "node2", "node3"}) {
		t.Error("Expected balanced distribution")
	}

	// Add another partition to node1 (now unbalanced)
	_ = pm.CreateAssignment("topic1", 3, "node1", []string{"node1", "node2"})

	// Still balanced because 4/3 = 1 with remainder 1, so max is 2
	if !distributor.IsBalanced([]string{"node1", "node2", "node3"}) {
		t.Error("Expected balanced distribution with 4 partitions across 3 nodes")
	}

	// Add two more to node1 (now definitely unbalanced)
	_ = pm.CreateAssignment("topic1", 4, "node1", []string{"node1", "node2"})
	_ = pm.CreateAssignment("topic1", 5, "node1", []string{"node1", "node2"})

	// Now node1 has 4, others have 1 each - unbalanced
	if distributor.IsBalanced([]string{"node1", "node2", "node3"}) {
		t.Error("Expected unbalanced distribution")
	}
}

func TestPartitionDistributor_GetLeaderDistribution(t *testing.T) {
	pm, err := NewPartitionManager("test-node", t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	// Create assignments
	_ = pm.CreateAssignment("topic1", 0, "node1", []string{"node1"})
	_ = pm.CreateAssignment("topic1", 1, "node1", []string{"node1"})
	_ = pm.CreateAssignment("topic1", 2, "node2", []string{"node2"})
	_ = pm.CreateAssignment("topic2", 0, "node3", []string{"node3"})

	distributor := NewPartitionDistributor(pm)
	distribution := distributor.GetLeaderDistribution()

	if distribution["node1"] != 2 {
		t.Errorf("node1 has %d leaders, want 2", distribution["node1"])
	}
	if distribution["node2"] != 1 {
		t.Errorf("node2 has %d leaders, want 1", distribution["node2"])
	}
	if distribution["node3"] != 1 {
		t.Errorf("node3 has %d leaders, want 1", distribution["node3"])
	}
}

func TestPartitionManager_GetPartitionLeader(t *testing.T) {
	pm, err := NewPartitionManager("test-node", t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	// Create an assignment with address
	err = pm.CreateAssignmentWithAddr("test-topic", 0, "leader-node", "localhost:9092",
		[]string{"leader-node", "replica1"}, map[string]string{"replica1": "localhost:9093"})
	if err != nil {
		t.Fatalf("Failed to create assignment: %v", err)
	}

	// Get the leader info
	info, ok := pm.GetPartitionLeader("test-topic", 0)
	if !ok {
		t.Fatal("Expected to find partition leader")
	}

	if info.LeaderID != "leader-node" {
		t.Errorf("LeaderID = %s, want leader-node", info.LeaderID)
	}
	if info.LeaderAddr != "localhost:9092" {
		t.Errorf("LeaderAddr = %s, want localhost:9092", info.LeaderAddr)
	}
	if info.Topic != "test-topic" {
		t.Errorf("Topic = %s, want test-topic", info.Topic)
	}
	if info.Partition != 0 {
		t.Errorf("Partition = %d, want 0", info.Partition)
	}

	// Test non-existent partition
	_, ok = pm.GetPartitionLeader("test-topic", 99)
	if ok {
		t.Error("Expected not to find non-existent partition")
	}
}

func TestPartitionManager_UpdateLeader(t *testing.T) {
	pm, err := NewPartitionManager("test-node", t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create partition manager: %v", err)
	}

	// Create initial assignment
	err = pm.CreateAssignmentWithAddr("test-topic", 0, "node1", "localhost:9092",
		[]string{"node1", "node2"}, map[string]string{"node2": "localhost:9093"})
	if err != nil {
		t.Fatalf("Failed to create assignment: %v", err)
	}

	// Update leader with address
	err = pm.UpdateLeaderWithAddr("test-topic", 0, "node2", "localhost:9093")
	if err != nil {
		t.Fatalf("Failed to update leader: %v", err)
	}

	// Verify the update
	info, ok := pm.GetPartitionLeader("test-topic", 0)
	if !ok {
		t.Fatal("Expected to find partition leader")
	}

	if info.LeaderID != "node2" {
		t.Errorf("LeaderID = %s, want node2", info.LeaderID)
	}
	if info.LeaderAddr != "localhost:9093" {
		t.Errorf("LeaderAddr = %s, want localhost:9093", info.LeaderAddr)
	}
}

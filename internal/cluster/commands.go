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
Cluster Commands for Raft Log Replication.

OVERVIEW:
=========
Commands represent operations that must be replicated across the cluster
via Raft consensus. When a command is committed, it is applied to the
local state machine on each node.

COMMAND TYPES:
==============
Topic Management:
- CreateTopic: Creates a new topic with specified partitions
- DeleteTopic: Removes a topic and its data
- ProduceMessage: Appends a message to a topic partition

Schema Management:
- RegisterSchema: Registers a new schema version
- DeleteSchema: Removes a schema version

User Management:
- CreateUser: Creates a new user with hashed password and roles
- DeleteUser: Removes a user from the system
- UpdateUser: Updates user roles, enabled status, or password

ACL Management:
- SetACL: Sets access control for a topic
- DeleteACL: Removes access control for a topic

FLOW:
=====
1. Client sends request to any node
2. If not leader, redirect to leader
3. Leader proposes command to Raft
4. Raft replicates to majority
5. Command is committed and applied on all nodes

SECURITY NOTES:
===============
- Passwords are hashed on the leader before replication
- Password hashes (not plaintext) are stored in the Raft log
- ACL changes are atomic and consistent across all nodes
*/
package cluster

import (
	"encoding/json"
	"fmt"
	"time"
)

// CommandType identifies the type of cluster command.
type CommandType string

const (
	CmdCreateTopic       CommandType = "create_topic"
	CmdDeleteTopic       CommandType = "delete_topic"
	CmdUpdateTopicConfig CommandType = "update_topic_config"
	CmdProduceMessage    CommandType = "produce_message"
	CmdRegisterSchema    CommandType = "register_schema"
	CmdDeleteSchema      CommandType = "delete_schema"

	// User management commands for cluster replication
	CmdCreateUser CommandType = "create_user"
	CmdDeleteUser CommandType = "delete_user"
	CmdUpdateUser CommandType = "update_user"

	// ACL management commands for cluster replication
	CmdSetACL    CommandType = "set_acl"
	CmdDeleteACL CommandType = "delete_acl"

	// Partition assignment commands for horizontal scaling
	// These commands manage partition-level leadership distribution
	CmdAssignPartition       CommandType = "assign_partition"        // Assign partition to a node
	CmdUpdatePartitionLeader CommandType = "update_partition_leader" // Change partition leader
	CmdUpdatePartitionISR    CommandType = "update_partition_isr"    // Update in-sync replicas
)

// Command represents a cluster operation to be replicated via Raft.
type Command struct {
	Type    CommandType     `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// CreateTopicPayload contains data for creating a topic.
type CreateTopicPayload struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

// DeleteTopicPayload contains data for deleting a topic.
type DeleteTopicPayload struct {
	Name string `json:"name"`
}

// UpdateTopicConfigPayload contains data for updating topic configuration.
type UpdateTopicConfigPayload struct {
	Name           string `json:"name"`
	RetentionBytes int64  `json:"retention_bytes,omitempty"`
	RetentionMs    int64  `json:"retention_ms,omitempty"`
}

// ProduceMessagePayload contains data for producing a message.
type ProduceMessagePayload struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Key       []byte `json:"key,omitempty"`
	Value     []byte `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

// RegisterSchemaPayload contains data for registering a schema.
type RegisterSchemaPayload struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Definition    string `json:"definition"`
	Compatibility string `json:"compatibility"`
}

// DeleteSchemaPayload contains data for deleting a schema.
type DeleteSchemaPayload struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// CreateUserPayload contains data for creating a user.
type CreateUserPayload struct {
	Username     string   `json:"username"`
	PasswordHash string   `json:"password_hash"` // Already hashed password
	Roles        []string `json:"roles"`
}

// DeleteUserPayload contains data for deleting a user.
type DeleteUserPayload struct {
	Username string `json:"username"`
}

// UpdateUserPayload contains data for updating a user.
type UpdateUserPayload struct {
	Username     string   `json:"username"`
	Roles        []string `json:"roles,omitempty"`
	Enabled      *bool    `json:"enabled,omitempty"`
	PasswordHash string   `json:"password_hash,omitempty"` // Already hashed password
}

// SetACLPayload contains data for setting an ACL.
type SetACLPayload struct {
	Topic        string   `json:"topic"`
	Public       bool     `json:"public"`
	AllowedUsers []string `json:"allowed_users,omitempty"`
	AllowedRoles []string `json:"allowed_roles,omitempty"`
}

// DeleteACLPayload contains data for deleting an ACL.
type DeleteACLPayload struct {
	Topic string `json:"topic"`
}

// ============================================================================
// Partition Assignment Payloads for Horizontal Scaling
// ============================================================================

// AssignPartitionPayload contains data for assigning a partition to nodes.
// This is used when creating a topic or rebalancing partitions.
type AssignPartitionPayload struct {
	Topic        string            `json:"topic"`
	Partition    int               `json:"partition"`
	Leader       string            `json:"leader"`        // Node ID of the leader
	LeaderAddr   string            `json:"leader_addr"`   // Client-facing address of leader
	Replicas     []string          `json:"replicas"`      // Node IDs of all replicas
	ReplicaAddrs map[string]string `json:"replica_addrs"` // Node ID -> client address
}

// UpdatePartitionLeaderPayload contains data for changing a partition leader.
// This is used during failover or rebalancing.
type UpdatePartitionLeaderPayload struct {
	Topic         string `json:"topic"`
	Partition     int    `json:"partition"`
	NewLeader     string `json:"new_leader"`      // Node ID of new leader
	NewLeaderAddr string `json:"new_leader_addr"` // Client-facing address of new leader
	Reason        string `json:"reason"`          // Why the leader changed (failover, rebalance, etc.)
}

// UpdatePartitionISRPayload contains data for updating in-sync replicas.
type UpdatePartitionISRPayload struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	ISR       []string `json:"isr"` // Node IDs of in-sync replicas
}

// NewCreateTopicCommand creates a command for topic creation.
func NewCreateTopicCommand(name string, partitions int) (*Command, error) {
	payload, err := json.Marshal(CreateTopicPayload{
		Name:       name,
		Partitions: partitions,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdCreateTopic,
		Payload: payload,
	}, nil
}

// NewDeleteTopicCommand creates a command for topic deletion.
func NewDeleteTopicCommand(name string) (*Command, error) {
	payload, err := json.Marshal(DeleteTopicPayload{
		Name: name,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdDeleteTopic,
		Payload: payload,
	}, nil
}

// NewProduceMessageCommand creates a command for producing a message.
func NewProduceMessageCommand(topic string, partition int, key, value []byte) (*Command, error) {
	payload, err := json.Marshal(ProduceMessagePayload{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdProduceMessage,
		Payload: payload,
	}, nil
}

// Encode serializes the command to bytes for Raft log.
func (c *Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

// DecodeCommand deserializes bytes to a Command.
func DecodeCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("failed to decode command: %w", err)
	}
	return &cmd, nil
}

// GetCreateTopicPayload extracts CreateTopicPayload from command.
func (c *Command) GetCreateTopicPayload() (*CreateTopicPayload, error) {
	if c.Type != CmdCreateTopic {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdCreateTopic, c.Type)
	}
	var payload CreateTopicPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetDeleteTopicPayload extracts DeleteTopicPayload from command.
func (c *Command) GetDeleteTopicPayload() (*DeleteTopicPayload, error) {
	if c.Type != CmdDeleteTopic {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdDeleteTopic, c.Type)
	}
	var payload DeleteTopicPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetProduceMessagePayload extracts ProduceMessagePayload from command.
func (c *Command) GetProduceMessagePayload() (*ProduceMessagePayload, error) {
	if c.Type != CmdProduceMessage {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdProduceMessage, c.Type)
	}
	var payload ProduceMessagePayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// NewRegisterSchemaCommand creates a command for schema registration.
func NewRegisterSchemaCommand(name, schemaType, definition, compatibility string) (*Command, error) {
	payload, err := json.Marshal(RegisterSchemaPayload{
		Name:          name,
		Type:          schemaType,
		Definition:    definition,
		Compatibility: compatibility,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdRegisterSchema,
		Payload: payload,
	}, nil
}

// NewDeleteSchemaCommand creates a command for schema deletion.
func NewDeleteSchemaCommand(name string, version int) (*Command, error) {
	payload, err := json.Marshal(DeleteSchemaPayload{
		Name:    name,
		Version: version,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdDeleteSchema,
		Payload: payload,
	}, nil
}

// GetRegisterSchemaPayload extracts RegisterSchemaPayload from command.
func (c *Command) GetRegisterSchemaPayload() (*RegisterSchemaPayload, error) {
	if c.Type != CmdRegisterSchema {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdRegisterSchema, c.Type)
	}
	var payload RegisterSchemaPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetDeleteSchemaPayload extracts DeleteSchemaPayload from command.
func (c *Command) GetDeleteSchemaPayload() (*DeleteSchemaPayload, error) {
	if c.Type != CmdDeleteSchema {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdDeleteSchema, c.Type)
	}
	var payload DeleteSchemaPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// NewCreateUserCommand creates a command for user creation.
func NewCreateUserCommand(username, passwordHash string, roles []string) (*Command, error) {
	payload, err := json.Marshal(CreateUserPayload{
		Username:     username,
		PasswordHash: passwordHash,
		Roles:        roles,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdCreateUser,
		Payload: payload,
	}, nil
}

// NewDeleteUserCommand creates a command for user deletion.
func NewDeleteUserCommand(username string) (*Command, error) {
	payload, err := json.Marshal(DeleteUserPayload{
		Username: username,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdDeleteUser,
		Payload: payload,
	}, nil
}

// NewUpdateUserCommand creates a command for user update.
func NewUpdateUserCommand(username string, roles []string, enabled *bool, passwordHash string) (*Command, error) {
	payload, err := json.Marshal(UpdateUserPayload{
		Username:     username,
		Roles:        roles,
		Enabled:      enabled,
		PasswordHash: passwordHash,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdUpdateUser,
		Payload: payload,
	}, nil
}

// NewSetACLCommand creates a command for setting an ACL.
func NewSetACLCommand(topic string, public bool, allowedUsers, allowedRoles []string) (*Command, error) {
	payload, err := json.Marshal(SetACLPayload{
		Topic:        topic,
		Public:       public,
		AllowedUsers: allowedUsers,
		AllowedRoles: allowedRoles,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdSetACL,
		Payload: payload,
	}, nil
}

// NewDeleteACLCommand creates a command for deleting an ACL.
func NewDeleteACLCommand(topic string) (*Command, error) {
	payload, err := json.Marshal(DeleteACLPayload{
		Topic: topic,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdDeleteACL,
		Payload: payload,
	}, nil
}

// GetCreateUserPayload extracts CreateUserPayload from command.
func (c *Command) GetCreateUserPayload() (*CreateUserPayload, error) {
	if c.Type != CmdCreateUser {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdCreateUser, c.Type)
	}
	var payload CreateUserPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetDeleteUserPayload extracts DeleteUserPayload from command.
func (c *Command) GetDeleteUserPayload() (*DeleteUserPayload, error) {
	if c.Type != CmdDeleteUser {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdDeleteUser, c.Type)
	}
	var payload DeleteUserPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetUpdateUserPayload extracts UpdateUserPayload from command.
func (c *Command) GetUpdateUserPayload() (*UpdateUserPayload, error) {
	if c.Type != CmdUpdateUser {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdUpdateUser, c.Type)
	}
	var payload UpdateUserPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetSetACLPayload extracts SetACLPayload from command.
func (c *Command) GetSetACLPayload() (*SetACLPayload, error) {
	if c.Type != CmdSetACL {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdSetACL, c.Type)
	}
	var payload SetACLPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetDeleteACLPayload extracts DeleteACLPayload from command.
func (c *Command) GetDeleteACLPayload() (*DeleteACLPayload, error) {
	if c.Type != CmdDeleteACL {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdDeleteACL, c.Type)
	}
	var payload DeleteACLPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// ============================================================================
// Standalone Decode Functions
// ============================================================================

// DecodeCreateUserPayload decodes a CreateUserPayload from JSON bytes.
func DecodeCreateUserPayload(data []byte) (*CreateUserPayload, error) {
	var payload CreateUserPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// DecodeDeleteUserPayload decodes a DeleteUserPayload from JSON bytes.
func DecodeDeleteUserPayload(data []byte) (*DeleteUserPayload, error) {
	var payload DeleteUserPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// DecodeUpdateUserPayload decodes an UpdateUserPayload from JSON bytes.
func DecodeUpdateUserPayload(data []byte) (*UpdateUserPayload, error) {
	var payload UpdateUserPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// DecodeSetACLPayload decodes a SetACLPayload from JSON bytes.
func DecodeSetACLPayload(data []byte) (*SetACLPayload, error) {
	var payload SetACLPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// DecodeDeleteACLPayload decodes a DeleteACLPayload from JSON bytes.
func DecodeDeleteACLPayload(data []byte) (*DeleteACLPayload, error) {
	var payload DeleteACLPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// ============================================================================
// Partition Assignment Command Constructors
// ============================================================================

// NewAssignPartitionCommand creates a command for partition assignment.
func NewAssignPartitionCommand(topic string, partition int, leader, leaderAddr string, replicas []string, replicaAddrs map[string]string) (*Command, error) {
	payload, err := json.Marshal(AssignPartitionPayload{
		Topic:        topic,
		Partition:    partition,
		Leader:       leader,
		LeaderAddr:   leaderAddr,
		Replicas:     replicas,
		ReplicaAddrs: replicaAddrs,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdAssignPartition,
		Payload: payload,
	}, nil
}

// NewUpdatePartitionLeaderCommand creates a command for changing partition leader.
func NewUpdatePartitionLeaderCommand(topic string, partition int, newLeader, newLeaderAddr, reason string) (*Command, error) {
	payload, err := json.Marshal(UpdatePartitionLeaderPayload{
		Topic:         topic,
		Partition:     partition,
		NewLeader:     newLeader,
		NewLeaderAddr: newLeaderAddr,
		Reason:        reason,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdUpdatePartitionLeader,
		Payload: payload,
	}, nil
}

// NewUpdatePartitionISRCommand creates a command for updating ISR.
func NewUpdatePartitionISRCommand(topic string, partition int, isr []string) (*Command, error) {
	payload, err := json.Marshal(UpdatePartitionISRPayload{
		Topic:     topic,
		Partition: partition,
		ISR:       isr,
	})
	if err != nil {
		return nil, err
	}
	return &Command{
		Type:    CmdUpdatePartitionISR,
		Payload: payload,
	}, nil
}

// GetAssignPartitionPayload extracts AssignPartitionPayload from command.
func (c *Command) GetAssignPartitionPayload() (*AssignPartitionPayload, error) {
	if c.Type != CmdAssignPartition {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdAssignPartition, c.Type)
	}
	var payload AssignPartitionPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetUpdatePartitionLeaderPayload extracts UpdatePartitionLeaderPayload from command.
func (c *Command) GetUpdatePartitionLeaderPayload() (*UpdatePartitionLeaderPayload, error) {
	if c.Type != CmdUpdatePartitionLeader {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdUpdatePartitionLeader, c.Type)
	}
	var payload UpdatePartitionLeaderPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

// GetUpdatePartitionISRPayload extracts UpdatePartitionISRPayload from command.
func (c *Command) GetUpdatePartitionISRPayload() (*UpdatePartitionISRPayload, error) {
	if c.Type != CmdUpdatePartitionISR {
		return nil, fmt.Errorf("invalid command type: expected %s, got %s", CmdUpdatePartitionISR, c.Type)
	}
	var payload UpdatePartitionISRPayload
	if err := json.Unmarshal(c.Payload, &payload); err != nil {
		return nil, err
	}
	return &payload, nil
}

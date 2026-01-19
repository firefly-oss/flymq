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
	"encoding/base64"
	"fmt"
	"runtime"
	"time"

	"flymq/internal/admin"
	"flymq/internal/audit"
	"flymq/internal/auth"
	"flymq/internal/config"
	"flymq/internal/dlq"
)

// SchemaRegistryInfo contains schema metadata returned by the registry.
type SchemaRegistryInfo struct {
	Name       string
	Version    int
	Type       string
	Definition string
}

// SchemaRegistry interface for schema operations.
type SchemaRegistry interface {
	Register(topic string, schemaType, definition string, compat string) error
	Get(topic string, version int) (name string, schemaType string, definition string, err error)
	GetLatest(topic string) (name string, version int, schemaType string, definition string, err error)
	ListSchemas(topic string) ([]SchemaRegistryInfo, error)
	ListTopics() []string
	Delete(topic string, version int) error
}

// AdminHandler implements the admin.AdminHandler interface.
type AdminHandler struct {
	broker         *Broker
	config         *config.Config
	version        string
	startTime      time.Time
	authorizer     *auth.Authorizer
	dlqManager     *dlq.Manager
	auditStore     audit.Store
	schemaRegistry SchemaRegistry
}

// NewAdminHandler creates a new admin handler.
func NewAdminHandler(b *Broker, cfg *config.Config, version string) *AdminHandler {
	return &AdminHandler{
		broker:    b,
		config:    cfg,
		version:   version,
		startTime: time.Now(),
	}
}

// SetAuthorizer sets the authorizer for user/ACL operations.
func (h *AdminHandler) SetAuthorizer(a *auth.Authorizer) {
	h.authorizer = a
}

// SetDLQManager sets the DLQ manager for dead letter queue operations.
func (h *AdminHandler) SetDLQManager(m *dlq.Manager) {
	h.dlqManager = m
}

// SetAuditStore sets the audit store for audit trail operations.
func (h *AdminHandler) SetAuditStore(s audit.Store) {
	h.auditStore = s
}

// SetSchemaRegistry sets the schema registry for schema operations.
func (h *AdminHandler) SetSchemaRegistry(r SchemaRegistry) {
	h.schemaRegistry = r
}

// GetClusterInfo returns cluster information.
func (h *AdminHandler) GetClusterInfo() (*admin.ClusterInfo, error) {
	topics := h.broker.ListTopics()
	uptime := time.Since(h.startTime)

	nodeCount, leaderID, members := h.broker.GetClusterInfoFull()

	// Calculate total messages across all topics
	var totalMessages int64
	for _, name := range topics {
		info, err := h.broker.GetTopicInfo(name)
		if err == nil {
			for _, p := range info.Partitions {
				totalMessages += int64(p.HighestOffset - p.LowestOffset + 1)
			}
		}
	}

	// Get Raft state if available
	var raftTerm, raftCommit uint64
	if raftState := h.broker.GetRaftState(); raftState != nil {
		raftTerm = raftState.Term
		raftCommit = raftState.CommitIndex
	}

	// Convert members to NodeInfo
	nodes := make([]admin.NodeInfo, 0, len(members))
	for _, m := range members {
		var stats *admin.NodeStats
		if m.Stats != nil {
			stats = &admin.NodeStats{
				MemoryUsedMB:     m.Stats.MemoryUsedMB,
				MemoryAllocMB:    m.Stats.MemoryAllocMB,
				MemorySysMB:      m.Stats.MemorySysMB,
				Goroutines:       m.Stats.Goroutines,
				NumGC:            m.Stats.NumGC,
				TopicCount:       m.Stats.TopicCount,
				PartitionCount:   m.Stats.PartitionCount,
				MessagesReceived: m.Stats.MessagesReceived,
				MessagesSent:     m.Stats.MessagesSent,
				BytesReceived:    m.Stats.BytesReceived,
				BytesSent:        m.Stats.BytesSent,
			}
		}
		nodes = append(nodes, admin.NodeInfo{
			ID:          m.ID,
			Address:     m.Address,
			ClusterAddr: m.ClusterAddr,
			AdminAddr:   m.AdminAddr,
			State:       m.Status,
			RaftState:   m.RaftState,
			IsLeader:    m.IsLeader,
			LastSeen:    m.LastSeen,
			Uptime:      m.Uptime,
			JoinedAt:    m.JoinedAt,
			Stats:       stats,
		})
	}

	// Calculate approximate message rate (messages per second)
	// This is an estimate based on total messages and uptime
	var messageRate float64
	uptimeSeconds := uptime.Seconds()
	if uptimeSeconds > 0 && totalMessages > 0 {
		messageRate = float64(totalMessages) / uptimeSeconds
	}

	return &admin.ClusterInfo{
		ClusterID:       h.config.NodeID,
		Version:         h.version,
		NodeCount:       nodeCount,
		LeaderID:        leaderID,
		RaftTerm:        raftTerm,
		RaftCommitIndex: raftCommit,
		TopicCount:      len(topics),
		TotalMessages:   totalMessages,
		MessageRate:     messageRate,
		Uptime:          uptime.String(),
		Nodes:           nodes,
	}, nil
}

// GetNodes returns node information.
func (h *AdminHandler) GetNodes() ([]admin.NodeInfo, error) {
	_, leaderID, members := h.broker.GetClusterInfoFull()
	nodes := make([]admin.NodeInfo, 0, len(members))

	for _, m := range members {
		var stats *admin.NodeStats
		if m.Stats != nil {
			stats = &admin.NodeStats{
				MemoryUsedMB:     m.Stats.MemoryUsedMB,
				MemoryAllocMB:    m.Stats.MemoryAllocMB,
				MemorySysMB:      m.Stats.MemorySysMB,
				Goroutines:       m.Stats.Goroutines,
				NumGC:            m.Stats.NumGC,
				TopicCount:       m.Stats.TopicCount,
				PartitionCount:   m.Stats.PartitionCount,
				MessagesReceived: m.Stats.MessagesReceived,
				MessagesSent:     m.Stats.MessagesSent,
				BytesReceived:    m.Stats.BytesReceived,
				BytesSent:        m.Stats.BytesSent,
			}
		}
		nodes = append(nodes, admin.NodeInfo{
			ID:          m.ID,
			Address:     m.Address,
			ClusterAddr: m.ClusterAddr,
			AdminAddr:   m.AdminAddr,
			State:       m.Status,
			RaftState:   m.RaftState,
			IsLeader:    m.ID == leaderID,
			LastSeen:    m.LastSeen,
			Uptime:      m.Uptime,
			JoinedAt:    m.JoinedAt,
			Stats:       stats,
		})
	}
	return nodes, nil
}

// ListTopics returns all topics.
func (h *AdminHandler) ListTopics() ([]admin.TopicInfo, error) {
	topics := h.broker.ListTopics()
	result := make([]admin.TopicInfo, 0, len(topics))

	for _, name := range topics {
		info, err := h.broker.GetTopicInfo(name)
		if err != nil {
			continue
		}
		result = append(result, admin.TopicInfo{
			Name:       info.Name,
			Partitions: len(info.Partitions),
		})
	}

	return result, nil
}

// CreateTopic creates a new topic.
func (h *AdminHandler) CreateTopic(name string, opts admin.TopicOptions) error {
	partitions := opts.Partitions
	if partitions <= 0 {
		partitions = 1
	}
	return h.broker.CreateTopic(name, partitions)
}

// DeleteTopic deletes a topic.
func (h *AdminHandler) DeleteTopic(name string) error {
	return h.broker.DeleteTopic(name)
}

// GetTopicInfo returns topic information.
func (h *AdminHandler) GetTopicInfo(name string) (*admin.TopicInfo, error) {
	info, err := h.broker.GetTopicInfo(name)
	if err != nil {
		return nil, err
	}
	return &admin.TopicInfo{
		Name:       info.Name,
		Partitions: len(info.Partitions),
	}, nil
}

// ListConsumerGroups returns all consumer groups.
func (h *AdminHandler) ListConsumerGroups() ([]admin.ConsumerGroupInfo, error) {
	groups := h.broker.ListConsumerGroups()
	result := make([]admin.ConsumerGroupInfo, 0, len(groups))

	for _, g := range groups {
		// Calculate lag per partition
		partitions := make([]admin.PartitionLag, 0, len(g.Offsets))
		var totalLag int64

		// Get topic info to compute lag
		topicInfo, _ := h.broker.GetTopicInfo(g.Topic)
		for partition, offset := range g.Offsets {
			var lag int64
			if topicInfo != nil && partition < len(topicInfo.Partitions) {
				highest := topicInfo.Partitions[partition].HighestOffset
				if highest > offset {
					lag = int64(highest - offset)
				}
			}
			totalLag += lag
			partitions = append(partitions, admin.PartitionLag{
				Partition: partition,
				Offset:    int64(offset),
				Lag:       lag,
			})
		}

		result = append(result, admin.ConsumerGroupInfo{
			Name:       g.GroupID,
			Topic:      g.Topic,
			Members:    g.Members,
			State:      "active",
			Lag:        totalLag,
			Partitions: partitions,
		})
	}
	return result, nil
}

// GetConsumerGroup returns consumer group information.
// The name parameter is in format "topic:groupID".
func (h *AdminHandler) GetConsumerGroup(name string) (*admin.ConsumerGroupInfo, error) {
	// Parse name as "topic:groupID" or just "groupID" (search all topics)
	groups := h.broker.ListConsumerGroups()
	for _, g := range groups {
		if g.GroupID == name || fmt.Sprintf("%s:%s", g.Topic, g.GroupID) == name {
			// Calculate lag per partition
			partitions := make([]admin.PartitionLag, 0, len(g.Offsets))
			var totalLag int64

			topicInfo, _ := h.broker.GetTopicInfo(g.Topic)
			for partition, offset := range g.Offsets {
				var lag int64
				if topicInfo != nil && partition < len(topicInfo.Partitions) {
					highest := topicInfo.Partitions[partition].HighestOffset
					if highest > offset {
						lag = int64(highest - offset)
					}
				}
				totalLag += lag
				partitions = append(partitions, admin.PartitionLag{
					Partition: partition,
					Offset:    int64(offset),
					Lag:       lag,
				})
			}

			return &admin.ConsumerGroupInfo{
				Name:       g.GroupID,
				Topic:      g.Topic,
				Members:    g.Members,
				State:      "active",
				Lag:        totalLag,
				Partitions: partitions,
			}, nil
		}
	}
	return nil, fmt.Errorf("consumer group not found: %s", name)
}

// DeleteConsumerGroup deletes a consumer group.
// The name parameter is in format "topic:groupID".
func (h *AdminHandler) DeleteConsumerGroup(name string) error {
	// Parse name - try direct lookup first
	groups := h.broker.ListConsumerGroups()
	for _, g := range groups {
		if g.GroupID == name || fmt.Sprintf("%s:%s", g.Topic, g.GroupID) == name {
			return h.broker.DeleteConsumerGroup(g.Topic, g.GroupID)
		}
	}
	return fmt.Errorf("consumer group not found: %s", name)
}

// ListSchemas returns all schemas.
func (h *AdminHandler) ListSchemas() ([]admin.SchemaInfo, error) {
	if h.schemaRegistry == nil {
		return []admin.SchemaInfo{}, nil
	}

	topics := h.schemaRegistry.ListTopics()
	var result []admin.SchemaInfo

	for _, topic := range topics {
		schemas, err := h.schemaRegistry.ListSchemas(topic)
		if err != nil {
			continue
		}
		for _, s := range schemas {
			result = append(result, admin.SchemaInfo{
				Name:    s.Name,
				Version: s.Version,
				Type:    s.Type,
				Schema:  s.Definition,
			})
		}
	}

	return result, nil
}

// GetSchema returns schema information.
func (h *AdminHandler) GetSchema(name string) (*admin.SchemaInfo, error) {
	if h.schemaRegistry == nil {
		return nil, fmt.Errorf("schema registry not configured")
	}

	schemaName, version, schemaType, definition, err := h.schemaRegistry.GetLatest(name)
	if err != nil {
		return nil, fmt.Errorf("schema not found: %s", name)
	}

	return &admin.SchemaInfo{
		Name:    schemaName,
		Version: version,
		Type:    schemaType,
		Schema:  definition,
	}, nil
}

// RegisterSchema registers a new schema.
func (h *AdminHandler) RegisterSchema(name string, schema []byte) error {
	if h.schemaRegistry == nil {
		return fmt.Errorf("schema registry not configured")
	}

	// Default to JSON schema type and backward compatibility
	return h.schemaRegistry.Register(name, "json", string(schema), "backward")
}

// DeleteSchema deletes a schema.
func (h *AdminHandler) DeleteSchema(name string) error {
	if h.schemaRegistry == nil {
		return fmt.Errorf("schema registry not configured")
	}

	// Get the latest version to delete
	_, version, _, _, err := h.schemaRegistry.GetLatest(name)
	if err != nil {
		return fmt.Errorf("schema not found: %s", name)
	}

	return h.schemaRegistry.Delete(name, version)
}

// ============================================================================
// User Management
// ============================================================================

// ListUsers returns all users.
func (h *AdminHandler) ListUsers() ([]admin.UserInfo, error) {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return nil, fmt.Errorf("authentication is not enabled")
	}

	userStore := h.authorizer.UserStore()
	usernames := userStore.ListUsers()

	result := make([]admin.UserInfo, 0, len(usernames))
	for _, username := range usernames {
		user, exists := userStore.GetUser(username)
		if !exists {
			continue
		}
		perms := userStore.GetUserPermissions(username)
		permStrs := make([]string, len(perms))
		for i, p := range perms {
			permStrs[i] = string(p)
		}
		result = append(result, admin.UserInfo{
			Username:    user.Username,
			Roles:       user.Roles,
			Permissions: permStrs,
			Enabled:     user.Enabled,
			CreatedAt:   user.CreatedAt.Format(time.RFC3339),
			UpdatedAt:   user.UpdatedAt.Format(time.RFC3339),
		})
	}
	return result, nil
}

// GetUser returns user information.
func (h *AdminHandler) GetUser(username string) (*admin.UserInfo, error) {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return nil, fmt.Errorf("authentication is not enabled")
	}

	userStore := h.authorizer.UserStore()
	user, exists := userStore.GetUser(username)
	if !exists {
		return nil, fmt.Errorf("user not found: %s", username)
	}

	perms := userStore.GetUserPermissions(username)
	permStrs := make([]string, len(perms))
	for i, p := range perms {
		permStrs[i] = string(p)
	}

	return &admin.UserInfo{
		Username:    user.Username,
		Roles:       user.Roles,
		Permissions: permStrs,
		Enabled:     user.Enabled,
		CreatedAt:   user.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   user.UpdatedAt.Format(time.RFC3339),
	}, nil
}

// CreateUser creates a new user.
func (h *AdminHandler) CreateUser(username, password string, roles []string) error {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return fmt.Errorf("authentication is not enabled")
	}

	// In cluster mode, route through Raft consensus
	if h.broker.IsClusterMode() {
		passwordHash, err := auth.HashPassword(password)
		if err != nil {
			return fmt.Errorf("failed to hash password: %w", err)
		}
		return h.broker.ProposeCreateUser(username, passwordHash, roles)
	}

	// Standalone mode - create directly
	if err := h.authorizer.UserStore().CreateUser(username, password, roles); err != nil {
		return err
	}
	return h.authorizer.UserStore().Save()
}

// UpdateUser updates a user.
func (h *AdminHandler) UpdateUser(username string, roles []string, enabled *bool) error {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return fmt.Errorf("authentication is not enabled")
	}

	// In cluster mode, route through Raft consensus
	if h.broker.IsClusterMode() {
		return h.broker.ProposeUpdateUser(username, roles, enabled, "")
	}

	// Standalone mode - update directly
	userStore := h.authorizer.UserStore()
	if roles != nil {
		if err := userStore.SetUserRoles(username, roles); err != nil {
			return err
		}
	}
	if enabled != nil {
		if err := userStore.SetUserEnabled(username, *enabled); err != nil {
			return err
		}
	}
	return userStore.Save()
}

// DeleteUser deletes a user.
func (h *AdminHandler) DeleteUser(username string) error {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return fmt.Errorf("authentication is not enabled")
	}

	// In cluster mode, route through Raft consensus
	if h.broker.IsClusterMode() {
		return h.broker.ProposeDeleteUser(username)
	}

	// Standalone mode - delete directly
	if err := h.authorizer.UserStore().DeleteUser(username); err != nil {
		return err
	}
	return h.authorizer.UserStore().Save()
}

// ChangePassword changes a user's password.
func (h *AdminHandler) ChangePassword(username, oldPassword, newPassword string) error {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return fmt.Errorf("authentication is not enabled")
	}

	userStore := h.authorizer.UserStore()

	// Verify old password
	_, err := userStore.Authenticate(username, oldPassword)
	if err != nil {
		return fmt.Errorf("invalid current password")
	}

	// In cluster mode, route through Raft consensus
	if h.broker.IsClusterMode() {
		passwordHash, err := auth.HashPassword(newPassword)
		if err != nil {
			return fmt.Errorf("failed to hash password: %w", err)
		}
		return h.broker.ProposeUpdateUser(username, nil, nil, passwordHash)
	}

	// Standalone mode - update directly
	if err := userStore.UpdatePassword(username, newPassword); err != nil {
		return err
	}
	return userStore.Save()
}

// ============================================================================
// ACL Management
// ============================================================================

// ListACLs returns all ACLs.
func (h *AdminHandler) ListACLs() ([]admin.ACLInfo, error) {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return nil, fmt.Errorf("authentication is not enabled")
	}

	aclStore := h.authorizer.ACLStore()
	acls := aclStore.ListTopicACLs()

	result := make([]admin.ACLInfo, 0, len(acls))
	for _, acl := range acls {
		result = append(result, admin.ACLInfo{
			Topic:        acl.Topic,
			Public:       acl.Public,
			AllowedUsers: acl.AllowedUsers,
			AllowedRoles: acl.AllowedRoles,
		})
	}
	return result, nil
}

// GetACL returns ACL for a topic.
func (h *AdminHandler) GetACL(topic string) (*admin.ACLInfo, error) {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return nil, fmt.Errorf("authentication is not enabled")
	}

	aclStore := h.authorizer.ACLStore()
	acl, exists := aclStore.GetTopicACL(topic)
	if !exists {
		return nil, fmt.Errorf("ACL not found for topic: %s", topic)
	}

	return &admin.ACLInfo{
		Topic:        acl.Topic,
		Public:       acl.Public,
		AllowedUsers: acl.AllowedUsers,
		AllowedRoles: acl.AllowedRoles,
	}, nil
}

// SetACL sets ACL for a topic.
func (h *AdminHandler) SetACL(topic string, public bool, allowedUsers, allowedRoles []string) error {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return fmt.Errorf("authentication is not enabled")
	}

	// In cluster mode, route through Raft consensus
	if h.broker.IsClusterMode() {
		return h.broker.ProposeSetACL(topic, public, allowedUsers, allowedRoles)
	}

	// Standalone mode - set directly
	aclStore := h.authorizer.ACLStore()
	aclStore.ApplySetACL(topic, public, allowedUsers, allowedRoles)
	return aclStore.Save()
}

// DeleteACL deletes ACL for a topic.
func (h *AdminHandler) DeleteACL(topic string) error {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return fmt.Errorf("authentication is not enabled")
	}

	// In cluster mode, route through Raft consensus
	if h.broker.IsClusterMode() {
		return h.broker.ProposeDeleteACL(topic)
	}

	// Standalone mode - delete directly
	aclStore := h.authorizer.ACLStore()
	aclStore.ApplyDeleteACL(topic)
	return aclStore.Save()
}

// ============================================================================
// Role Management
// ============================================================================

// ListRoles returns all available roles.
func (h *AdminHandler) ListRoles() ([]admin.RoleInfo, error) {
	if h.authorizer == nil || !h.authorizer.IsEnabled() {
		return nil, fmt.Errorf("authentication is not enabled")
	}

	userStore := h.authorizer.UserStore()
	roles := userStore.ListRoles()

	result := make([]admin.RoleInfo, 0, len(roles))
	for _, role := range roles {
		permStrs := make([]string, len(role.Permissions))
		for i, p := range role.Permissions {
			permStrs[i] = string(p)
		}
		result = append(result, admin.RoleInfo{
			Name:        role.Name,
			Permissions: permStrs,
			Description: role.Description,
		})
	}
	return result, nil
}

// ============================================================================
// DLQ Management
// ============================================================================

// GetDLQMessages returns messages from a topic's dead letter queue.
func (h *AdminHandler) GetDLQMessages(topic string, maxMessages int) ([]admin.DLQMessage, error) {
	if !h.config.DLQ.Enabled {
		return nil, fmt.Errorf("dead letter queue is not enabled")
	}

	if h.dlqManager == nil {
		return nil, fmt.Errorf("DLQ manager not initialized")
	}

	// Fetch DLQ messages starting from offset 0
	msgs, _, err := h.dlqManager.GetDLQMessages(topic, 0, maxMessages)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch DLQ messages: %w", err)
	}

	result := make([]admin.DLQMessage, 0, len(msgs))
	for _, m := range msgs {
		result = append(result, admin.DLQMessage{
			ID:        m.ID,
			Topic:     m.OriginalTopic,
			Data:      base64.StdEncoding.EncodeToString(m.Payload),
			Error:     m.ErrorMessage,
			Retries:   m.RetryCount,
			Timestamp: m.LastFailedAt.Format(time.RFC3339),
		})
	}
	return result, nil
}

// ReplayDLQMessage replays a message from the DLQ back to the topic.
func (h *AdminHandler) ReplayDLQMessage(topic, messageID string) error {
	if !h.config.DLQ.Enabled {
		return fmt.Errorf("dead letter queue is not enabled")
	}

	if h.dlqManager == nil {
		return fmt.Errorf("DLQ manager not initialized")
	}

	// Fetch all messages to find the one with matching ID
	msgs, _, err := h.dlqManager.GetDLQMessages(topic, 0, 1000)
	if err != nil {
		return fmt.Errorf("failed to fetch DLQ messages: %w", err)
	}

	for _, msg := range msgs {
		if msg.ID == messageID {
			return h.dlqManager.ReplayMessage(msg)
		}
	}

	return fmt.Errorf("message not found: %s", messageID)
}

// ReplayDLQMessageByOffset replays a message from the DLQ back to the topic using its offset.
func (h *AdminHandler) ReplayDLQMessageByOffset(topic string, offset uint64) error {
	if !h.config.DLQ.Enabled {
		return fmt.Errorf("dead letter queue is not enabled")
	}

	if h.dlqManager == nil {
		return fmt.Errorf("DLQ manager not initialized")
	}

	return h.dlqManager.ReplayMessageByID(topic, offset)
}

// PurgeDLQ purges all messages from a topic's DLQ.
func (h *AdminHandler) PurgeDLQ(topic string) error {
	if !h.config.DLQ.Enabled {
		return fmt.Errorf("dead letter queue is not enabled")
	}

	// Purging a DLQ is equivalent to deleting the DLQ topic
	dlqTopic := topic + h.config.DLQ.TopicSuffix
	return h.broker.DeleteTopic(dlqTopic)
}

// ============================================================================
// Metrics
// ============================================================================

// GetMetrics returns metrics in Prometheus format.
func (h *AdminHandler) GetMetrics() (string, error) {
	// Get runtime stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Get broker stats
	topics := h.broker.ListTopics()
	groups := h.broker.ListConsumerGroups()

	// Calculate total messages
	var totalMessages int64
	for _, name := range topics {
		info, err := h.broker.GetTopicInfo(name)
		if err == nil {
			for _, p := range info.Partitions {
				totalMessages += int64(p.HighestOffset - p.LowestOffset + 1)
			}
		}
	}

	// Format as Prometheus metrics
	metrics := fmt.Sprintf(`# HELP flymq_topics_total Total number of topics
# TYPE flymq_topics_total gauge
flymq_topics_total %d

# HELP flymq_consumer_groups_total Total number of consumer groups
# TYPE flymq_consumer_groups_total gauge
flymq_consumer_groups_total %d

# HELP flymq_messages_total Total number of messages across all topics
# TYPE flymq_messages_total counter
flymq_messages_total %d

# HELP flymq_memory_alloc_bytes Current memory allocation in bytes
# TYPE flymq_memory_alloc_bytes gauge
flymq_memory_alloc_bytes %d

# HELP flymq_memory_sys_bytes Total memory obtained from system in bytes
# TYPE flymq_memory_sys_bytes gauge
flymq_memory_sys_bytes %d

# HELP flymq_goroutines Number of goroutines
# TYPE flymq_goroutines gauge
flymq_goroutines %d

# HELP flymq_gc_cycles_total Total number of GC cycles
# TYPE flymq_gc_cycles_total counter
flymq_gc_cycles_total %d

# HELP flymq_uptime_seconds Server uptime in seconds
# TYPE flymq_uptime_seconds counter
flymq_uptime_seconds %.0f
`,
		len(topics),
		len(groups),
		totalMessages,
		m.Alloc,
		m.Sys,
		runtime.NumGoroutine(),
		m.NumGC,
		time.Since(h.startTime).Seconds(),
	)

	return metrics, nil
}

// GetStats returns rich statistics in JSON format for the Admin API.
func (h *AdminHandler) GetStats() (*admin.StatsInfo, error) {
	// Get runtime stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Get broker stats
	topics := h.broker.ListTopics()
	groups := h.broker.ListConsumerGroups()

	// Calculate totals
	var totalMessages int64
	var totalPartitions int
	for _, name := range topics {
		info, err := h.broker.GetTopicInfo(name)
		if err == nil {
			totalPartitions += len(info.Partitions)
			for _, p := range info.Partitions {
				totalMessages += int64(p.HighestOffset - p.LowestOffset + 1)
			}
		}
	}

	// Get cluster info
	nodeCount, leaderID, members := h.broker.GetClusterInfoFull()
	healthyNodes := 0
	for _, m := range members {
		if m.Status == "active" {
			healthyNodes++
		}
	}

	// Build cluster stats
	clusterStats := admin.ClusterStats{
		NodeCount:          nodeCount,
		HealthyNodes:       healthyNodes,
		TopicCount:         len(topics),
		TotalPartitions:    totalPartitions,
		TotalMessages:      totalMessages,
		ConsumerGroupCount: len(groups),
	}

	// Build node stats
	nodes := make([]admin.NodeStatsInfo, 0, len(members))
	for _, member := range members {
		node := admin.NodeStatsInfo{
			NodeID:   member.ID,
			Address:  member.Address,
			State:    member.Status,
			IsLeader: member.ID == leaderID,
			Uptime:   member.Uptime,
		}
		if member.Stats != nil {
			node.MemoryUsedMB = member.Stats.MemoryUsedMB
			node.Goroutines = member.Stats.Goroutines
			node.MessagesReceived = member.Stats.MessagesReceived
			node.MessagesSent = member.Stats.MessagesSent
			node.BytesReceived = member.Stats.BytesReceived
			node.BytesSent = member.Stats.BytesSent
		}
		nodes = append(nodes, node)
	}

	// Build topic stats
	topicStats := make([]admin.TopicStatsInfo, 0, len(topics))
	for _, name := range topics {
		info, err := h.broker.GetTopicInfo(name)
		if err != nil {
			continue
		}
		var msgCount int64
		for _, p := range info.Partitions {
			msgCount += int64(p.HighestOffset - p.LowestOffset + 1)
		}
		topicStats = append(topicStats, admin.TopicStatsInfo{
			Name:         name,
			Partitions:   len(info.Partitions),
			MessageCount: msgCount,
		})
	}

	// Build consumer group stats
	groupStats := make([]admin.ConsumerGroupStats, 0, len(groups))
	for _, g := range groups {
		var totalLag int64
		topicInfo, _ := h.broker.GetTopicInfo(g.Topic)
		for partition, offset := range g.Offsets {
			if topicInfo != nil && partition < len(topicInfo.Partitions) {
				highest := topicInfo.Partitions[partition].HighestOffset
				if highest > offset {
					totalLag += int64(highest - offset)
				}
			}
		}
		groupStats = append(groupStats, admin.ConsumerGroupStats{
			GroupID:  g.GroupID,
			Topic:    g.Topic,
			Members:  g.Members,
			State:    "active",
			TotalLag: totalLag,
		})
	}

	// Build system stats
	systemStats := admin.SystemStats{
		UptimeSeconds: int64(time.Since(h.startTime).Seconds()),
		StartTime:     h.startTime.Format(time.RFC3339),
		GoVersion:     runtime.Version(),
		NumCPU:        runtime.NumCPU(),
		MemoryAllocMB: float64(m.Alloc) / (1024 * 1024),
		MemoryTotalMB: float64(m.TotalAlloc) / (1024 * 1024),
		MemorySysMB:   float64(m.Sys) / (1024 * 1024),
		Goroutines:    runtime.NumGoroutine(),
		NumGC:         m.NumGC,
	}

	return &admin.StatsInfo{
		Cluster:        clusterStats,
		Nodes:          nodes,
		Topics:         topicStats,
		ConsumerGroups: groupStats,
		System:         systemStats,
	}, nil
}

// ============================================================================
// Partition Management Methods (Horizontal Scaling)
// ============================================================================

// GetClusterMetadata returns partition-to-node mappings for smart client routing.
func (h *AdminHandler) GetClusterMetadata(topic string) (*admin.ClusterMetadataInfo, error) {
	metadata, err := h.broker.GetClusterMetadata(topic)
	if err != nil {
		return nil, err
	}

	// Convert protocol types to admin types
	topics := make([]admin.TopicPartitionMetadata, 0, len(metadata.Topics))
	for _, t := range metadata.Topics {
		partitions := make([]admin.PartitionMetadataInfo, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			partitions = append(partitions, admin.PartitionMetadataInfo{
				Partition:  int(p.Partition),
				LeaderID:   p.LeaderID,
				LeaderAddr: p.LeaderAddr,
				Epoch:      p.Epoch,
				State:      p.State,
				Replicas:   p.Replicas,
				ISR:        p.ISR,
			})
		}
		topics = append(topics, admin.TopicPartitionMetadata{
			Topic:      t.Topic,
			Partitions: partitions,
		})
	}

	return &admin.ClusterMetadataInfo{
		ClusterID: metadata.ClusterID,
		Topics:    topics,
	}, nil
}

// GetPartitionAssignments returns detailed partition assignment information.
func (h *AdminHandler) GetPartitionAssignments(topic string) ([]admin.PartitionAssignmentInfo, error) {
	metadata, err := h.broker.GetClusterMetadata(topic)
	if err != nil {
		return nil, err
	}

	var assignments []admin.PartitionAssignmentInfo
	for _, t := range metadata.Topics {
		for _, p := range t.Partitions {
			assignments = append(assignments, admin.PartitionAssignmentInfo{
				Topic:      t.Topic,
				Partition:  int(p.Partition),
				Leader:     p.LeaderID,
				LeaderAddr: p.LeaderAddr,
				Replicas:   p.Replicas,
				ISR:        p.ISR,
				Epoch:      p.Epoch,
				State:      p.State,
			})
		}
	}

	return assignments, nil
}

// GetLeaderDistribution returns the distribution of partition leaders across nodes.
func (h *AdminHandler) GetLeaderDistribution() (map[string]int, error) {
	metadata, err := h.broker.GetClusterMetadata("")
	if err != nil {
		return nil, err
	}

	distribution := make(map[string]int)
	for _, t := range metadata.Topics {
		for _, p := range t.Partitions {
			if p.LeaderID != "" {
				distribution[p.LeaderID]++
			}
		}
	}

	return distribution, nil
}

// TriggerRebalance triggers a partition rebalance to distribute leaders evenly.
func (h *AdminHandler) TriggerRebalance() (*admin.RebalanceResult, error) {
	// Get the distribution before rebalancing
	oldDistribution, err := h.GetLeaderDistribution()
	if err != nil {
		return nil, err
	}

	// Trigger the rebalance through the broker
	if err := h.broker.TriggerRebalance(); err != nil {
		return &admin.RebalanceResult{
			Success:    false,
			Message:    err.Error(),
			Moves:      []admin.PartitionMove{},
			OldLeaders: oldDistribution,
			NewLeaders: oldDistribution,
		}, nil
	}

	// Get the distribution after rebalancing
	newDistribution, err := h.GetLeaderDistribution()
	if err != nil {
		return nil, err
	}

	// Calculate the moves that were made
	moves := []admin.PartitionMove{}
	// Note: Detailed move tracking would require comparing partition assignments before/after
	// For now, we report success based on distribution change

	message := "Rebalance completed successfully."
	if len(oldDistribution) == len(newDistribution) {
		// Check if distribution actually changed
		changed := false
		for node, count := range oldDistribution {
			if newDistribution[node] != count {
				changed = true
				break
			}
		}
		if !changed {
			message = "Rebalance completed. No moves were required."
		}
	}

	return &admin.RebalanceResult{
		Success:    true,
		Message:    message,
		Moves:      moves,
		OldLeaders: oldDistribution,
		NewLeaders: newDistribution,
	}, nil
}

// ReassignPartition reassigns a partition to a new leader and/or replicas.
func (h *AdminHandler) ReassignPartition(topic string, partition int, newLeader string, newReplicas []string) error {
	return h.broker.ReassignPartition(topic, partition, newLeader, newReplicas)
}

// ============================================================================
// Audit Trail Operations
// ============================================================================

// QueryAuditEvents queries audit events based on the provided filter.
func (h *AdminHandler) QueryAuditEvents(filter *admin.AuditQueryFilter) (*admin.AuditQueryResult, error) {
	if h.auditStore == nil {
		return nil, fmt.Errorf("audit trail is not enabled")
	}

	// Convert admin filter to audit filter
	auditFilter := &audit.QueryFilter{
		User:     filter.User,
		Resource: filter.Resource,
		Result:   filter.Result,
		Search:   filter.Search,
		Limit:    filter.Limit,
		Offset:   filter.Offset,
	}

	if filter.StartTime != nil {
		auditFilter.StartTime = filter.StartTime
	}
	if filter.EndTime != nil {
		auditFilter.EndTime = filter.EndTime
	}

	// Convert event types
	for _, et := range filter.EventTypes {
		auditFilter.EventTypes = append(auditFilter.EventTypes, audit.EventType(et))
	}

	// Execute query
	result, err := h.auditStore.Query(auditFilter)
	if err != nil {
		return nil, err
	}

	// Convert to admin result
	events := make([]admin.AuditEventInfo, len(result.Events))
	for i, event := range result.Events {
		events[i] = admin.AuditEventInfo{
			ID:        event.ID,
			Timestamp: event.Timestamp.Format(time.RFC3339),
			Type:      string(event.Type),
			User:      event.User,
			ClientIP:  event.ClientIP,
			Resource:  event.Resource,
			Action:    event.Action,
			Result:    event.Result,
			Details:   event.Details,
			NodeID:    event.NodeID,
		}
	}

	return &admin.AuditQueryResult{
		Events:     events,
		TotalCount: result.TotalCount,
		HasMore:    result.HasMore,
	}, nil
}

// ExportAuditEvents exports audit events in the specified format.
func (h *AdminHandler) ExportAuditEvents(filter *admin.AuditQueryFilter, format string) ([]byte, error) {
	if h.auditStore == nil {
		return nil, fmt.Errorf("audit trail is not enabled")
	}

	// Convert admin filter to audit filter
	auditFilter := &audit.QueryFilter{
		User:     filter.User,
		Resource: filter.Resource,
		Result:   filter.Result,
		Search:   filter.Search,
	}

	if filter.StartTime != nil {
		auditFilter.StartTime = filter.StartTime
	}
	if filter.EndTime != nil {
		auditFilter.EndTime = filter.EndTime
	}

	// Convert event types
	for _, et := range filter.EventTypes {
		auditFilter.EventTypes = append(auditFilter.EventTypes, audit.EventType(et))
	}

	// Determine export format
	exportFormat := audit.ExportJSON
	if format == "csv" {
		exportFormat = audit.ExportCSV
	}

	return h.auditStore.Export(auditFilter, exportFormat)
}

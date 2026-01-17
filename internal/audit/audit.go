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
Package audit provides audit trail functionality for FlyMQ.

OVERVIEW:
=========
The audit package records security-relevant events for compliance and forensics.
All events are stored persistently and can be queried, filtered, and exported.

EVENT CATEGORIES:
=================
- Authentication: login, logout, auth failures
- Authorization: permission checks, access denied
- Data Operations: produce, consume, topic management
- Admin Operations: user management, ACL changes, config changes
- System Events: startup, shutdown, cluster changes

STORAGE:
========
Events are stored in append-only log files with automatic rotation.
Each event is JSON-encoded for easy parsing and export.

QUERY CAPABILITIES:
===================
- Filter by time range
- Filter by event type
- Filter by user
- Filter by resource (topic, user, etc.)
- Full-text search in event details
*/
package audit

import (
	"time"
)

// EventType represents the type of audit event.
type EventType string

const (
	// Authentication events
	EventAuthSuccess    EventType = "auth.success"
	EventAuthFailure    EventType = "auth.failure"
	EventAuthLogout     EventType = "auth.logout"

	// Authorization events
	EventAccessGranted  EventType = "access.granted"
	EventAccessDenied   EventType = "access.denied"

	// Data operation events
	EventProduce        EventType = "data.produce"
	EventConsume        EventType = "data.consume"
	EventTopicCreate    EventType = "topic.create"
	EventTopicDelete    EventType = "topic.delete"

	// Admin operation events
	EventUserCreate     EventType = "user.create"
	EventUserUpdate     EventType = "user.update"
	EventUserDelete     EventType = "user.delete"
	EventUserPassword   EventType = "user.password"
	EventACLSet         EventType = "acl.set"
	EventACLDelete      EventType = "acl.delete"
	EventSchemaRegister EventType = "schema.register"
	EventSchemaDelete   EventType = "schema.delete"

	// Consumer group events
	EventGroupCreate    EventType = "group.create"
	EventGroupDelete    EventType = "group.delete"
	EventOffsetCommit   EventType = "offset.commit"
	EventOffsetReset    EventType = "offset.reset"

	// Transaction events
	EventTxnBegin       EventType = "txn.begin"
	EventTxnCommit      EventType = "txn.commit"
	EventTxnAbort       EventType = "txn.abort"

	// System events
	EventServerStart    EventType = "system.start"
	EventServerStop     EventType = "system.stop"
	EventClusterJoin    EventType = "cluster.join"
	EventClusterLeave   EventType = "cluster.leave"
	EventConfigChange   EventType = "config.change"
)

// Event represents a single audit event.
type Event struct {
	ID        string            `json:"id"`         // Unique event ID
	Timestamp time.Time         `json:"timestamp"`  // When the event occurred
	Type      EventType         `json:"type"`       // Event type
	User      string            `json:"user"`       // Username (empty for system events)
	ClientIP  string            `json:"client_ip"`  // Client IP address
	Resource  string            `json:"resource"`   // Resource affected (topic, user, etc.)
	Action    string            `json:"action"`     // Action performed
	Result    string            `json:"result"`     // success, failure, denied
	Details   map[string]string `json:"details"`    // Additional details
	NodeID    string            `json:"node_id"`    // Cluster node ID
}

// QueryFilter defines filters for querying audit events.
type QueryFilter struct {
	StartTime  *time.Time  // Events after this time
	EndTime    *time.Time  // Events before this time
	EventTypes []EventType // Filter by event types
	User       string      // Filter by username
	Resource   string      // Filter by resource
	Result     string      // Filter by result (success, failure, denied)
	Search     string      // Full-text search in details
	Limit      int         // Maximum number of events to return
	Offset     int         // Offset for pagination
}

// QueryResult contains the result of an audit query.
type QueryResult struct {
	Events     []Event `json:"events"`
	TotalCount int     `json:"total_count"`
	HasMore    bool    `json:"has_more"`
}

// ExportFormat defines the format for exporting audit events.
type ExportFormat string

const (
	ExportJSON ExportFormat = "json"
	ExportCSV  ExportFormat = "csv"
)

// Store defines the interface for audit event storage.
type Store interface {
	// Record stores a new audit event
	Record(event *Event) error

	// Query retrieves events matching the filter
	Query(filter *QueryFilter) (*QueryResult, error)

	// Export exports events to the specified format
	Export(filter *QueryFilter, format ExportFormat) ([]byte, error)

	// Close closes the store
	Close() error
}


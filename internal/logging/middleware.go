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
Logging Middleware for FlyMQ.

OVERVIEW:
=========
Provides structured logging for connections, requests, and operations.
Each log entry includes contextual information for debugging.

CONNECTION LOGGING:
===================
- New connection: client IP, TLS status, connection ID
- Connection closed: duration, bytes transferred
- Connection errors: error details, stack trace

REQUEST LOGGING:
================
- Request received: operation type, topic, partition
- Request completed: latency, result status
- Request failed: error code, error message

CORRELATION:
============
Each connection gets a unique ID for correlating log entries.
Request IDs link related operations across the system.
*/
package logging

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"time"
)

// ConnectionLogger provides detailed logging for connections
type ConnectionLogger struct {
	logger *Logger
}

// NewConnectionLogger creates a new connection logger
func NewConnectionLogger(logger *Logger) *ConnectionLogger {
	return &ConnectionLogger{logger: logger}
}

// LogNewConnection logs a new client connection with details
func (cl *ConnectionLogger) LogNewConnection(conn net.Conn, tlsEnabled bool) {
	connectionID := GenerateConnectionID(conn)
	cl.logger.Info("New client connection established",
		"connection_id", connectionID,
		"remote_addr", conn.RemoteAddr().String(),
		"local_addr", conn.LocalAddr().String(),
		"tls_enabled", tlsEnabled,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogConnectionClosed logs when a connection is closed
func (cl *ConnectionLogger) LogConnectionClosed(conn net.Conn, reason string, duration time.Duration) {
	connectionID := GenerateConnectionID(conn)
	cl.logger.Info("Client connection closed",
		"connection_id", connectionID,
		"remote_addr", conn.RemoteAddr().String(),
		"reason", reason,
		"duration_seconds", duration.Seconds(),
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// MessageLogger provides detailed logging for messages
type MessageLogger struct {
	logger *Logger
}

// NewMessageLogger creates a new message logger
func NewMessageLogger(logger *Logger) *MessageLogger {
	return &MessageLogger{logger: logger}
}

// LogProduce logs a produce operation without exposing payload
func (ml *MessageLogger) LogProduce(topic string, offset uint64, size int, clientID string, latencyMs float64) {
	// Hash the message for tracking without exposing content
	messageID := GenerateMessageID(topic, offset)

	ml.logger.Info("Message produced",
		"message_id", messageID,
		"topic", topic,
		"offset", offset,
		"size_bytes", size,
		"client_id", clientID,
		"latency_ms", latencyMs,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogConsume logs a consume operation
func (ml *MessageLogger) LogConsume(topic string, offset uint64, consumerGroup string, clientID string, latencyMs float64) {
	messageID := GenerateMessageID(topic, offset)

	ml.logger.Info("Message consumed",
		"message_id", messageID,
		"topic", topic,
		"offset", offset,
		"consumer_group", consumerGroup,
		"client_id", clientID,
		"latency_ms", latencyMs,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogBatchProduce logs batch produce operations
func (ml *MessageLogger) LogBatchProduce(topic string, count int, totalSize int, clientID string, latencyMs float64) {
	ml.logger.Info("Batch messages produced",
		"topic", topic,
		"message_count", count,
		"total_size_bytes", totalSize,
		"avg_size_bytes", totalSize/count,
		"client_id", clientID,
		"latency_ms", latencyMs,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// TopicLogger provides detailed logging for topic operations
type TopicLogger struct {
	logger *Logger
}

// NewTopicLogger creates a new topic logger
func NewTopicLogger(logger *Logger) *TopicLogger {
	return &TopicLogger{logger: logger}
}

// LogTopicCreated logs topic creation
func (tl *TopicLogger) LogTopicCreated(topic string, partitions int, replicationFactor int, creator string) {
	tl.logger.Info("Topic created",
		"topic", topic,
		"partitions", partitions,
		"replication_factor", replicationFactor,
		"creator", creator,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogTopicDeleted logs topic deletion
func (tl *TopicLogger) LogTopicDeleted(topic string, deletedBy string, messageCount int64) {
	tl.logger.Info("Topic deleted",
		"topic", topic,
		"deleted_by", deletedBy,
		"message_count", messageCount,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogTopicConfiguration logs topic configuration changes
func (tl *TopicLogger) LogTopicConfiguration(topic string, config map[string]interface{}, changedBy string) {
	tl.logger.Info("Topic configuration updated",
		"topic", topic,
		"config", config,
		"changed_by", changedBy,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// ConsumerLogger provides detailed logging for consumer operations
type ConsumerLogger struct {
	logger *Logger
}

// NewConsumerLogger creates a new consumer logger
func NewConsumerLogger(logger *Logger) *ConsumerLogger {
	return &ConsumerLogger{logger: logger}
}

// LogConsumerJoin logs when a consumer joins a group
func (cl *ConsumerLogger) LogConsumerJoin(consumerID string, group string, topics []string) {
	cl.logger.Info("Consumer joined group",
		"consumer_id", consumerID,
		"consumer_group", group,
		"topics", strings.Join(topics, ","),
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogConsumerLeave logs when a consumer leaves a group
func (cl *ConsumerLogger) LogConsumerLeave(consumerID string, group string, reason string) {
	cl.logger.Info("Consumer left group",
		"consumer_id", consumerID,
		"consumer_group", group,
		"reason", reason,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogRebalance logs consumer group rebalancing
func (cl *ConsumerLogger) LogRebalance(group string, oldAssignments, newAssignments map[string][]int) {
	cl.logger.Info("Consumer group rebalanced",
		"consumer_group", group,
		"old_assignments", oldAssignments,
		"new_assignments", newAssignments,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogCommit logs offset commits
func (cl *ConsumerLogger) LogCommit(consumerID string, group string, topic string, partition int, offset uint64) {
	cl.logger.Info("Offset committed",
		"consumer_id", consumerID,
		"consumer_group", group,
		"topic", topic,
		"partition", partition,
		"offset", offset,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// SecurityLogger provides detailed logging for security events
type SecurityLogger struct {
	logger *Logger
}

// NewSecurityLogger creates a new security logger
func NewSecurityLogger(logger *Logger) *SecurityLogger {
	return &SecurityLogger{logger: logger}
}

// LogAuthentication logs authentication attempts
func (sl *SecurityLogger) LogAuthentication(clientID string, method string, success bool, reason string) {
	level := INFO
	if !success {
		level = WARN
	}

	sl.logger.log(level, "Authentication attempt",
		"client_id", clientID,
		"method", method,
		"success", success,
		"reason", reason,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogAuthorization logs authorization decisions
func (sl *SecurityLogger) LogAuthorization(clientID string, resource string, operation string, allowed bool) {
	level := DEBUG
	if !allowed {
		level = WARN
	}

	sl.logger.log(level, "Authorization check",
		"client_id", clientID,
		"resource", resource,
		"operation", operation,
		"allowed", allowed,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogTLSConnection logs TLS connection details
func (sl *SecurityLogger) LogTLSConnection(conn net.Conn, version uint16, cipherSuite uint16) {
	sl.logger.Info("TLS connection established",
		"remote_addr", conn.RemoteAddr().String(),
		"tls_version", fmt.Sprintf("0x%04x", version),
		"cipher_suite", fmt.Sprintf("0x%04x", cipherSuite),
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogAuthSuccess logs a successful authentication attempt
func (sl *SecurityLogger) LogAuthSuccess(username string, conn interface{}) {
	remoteAddr := "unknown"
	if c, ok := conn.(net.Conn); ok {
		remoteAddr = c.RemoteAddr().String()
	}
	sl.logger.Info("Authentication successful",
		"username", username,
		"remote_addr", remoteAddr,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogAuthFailure logs a failed authentication attempt
func (sl *SecurityLogger) LogAuthFailure(username string, reason string, conn interface{}) {
	remoteAddr := "unknown"
	if c, ok := conn.(net.Conn); ok {
		remoteAddr = c.RemoteAddr().String()
	}
	sl.logger.Warn("Authentication failed",
		"username", username,
		"reason", reason,
		"remote_addr", remoteAddr,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogAuthzFailure logs a failed authorization attempt
func (sl *SecurityLogger) LogAuthzFailure(username string, operation string, resource string, conn interface{}) {
	remoteAddr := "unknown"
	if c, ok := conn.(net.Conn); ok {
		remoteAddr = c.RemoteAddr().String()
	}
	sl.logger.Warn("Authorization denied",
		"username", username,
		"operation", operation,
		"resource", resource,
		"remote_addr", remoteAddr,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogUserCreated logs when a user is created
func (sl *SecurityLogger) LogUserCreated(username string, createdBy string) {
	sl.logger.Info("User created",
		"username", username,
		"created_by", createdBy,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogUserDeleted logs when a user is deleted
func (sl *SecurityLogger) LogUserDeleted(username string, deletedBy string) {
	sl.logger.Info("User deleted",
		"username", username,
		"deleted_by", deletedBy,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogUserUpdated logs when a user is updated
func (sl *SecurityLogger) LogUserUpdated(username string, updatedBy string) {
	sl.logger.Info("User updated",
		"username", username,
		"updated_by", updatedBy,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// PerformanceLogger provides detailed logging for performance metrics
type PerformanceLogger struct {
	logger *Logger
}

// NewPerformanceLogger creates a new performance logger
func NewPerformanceLogger(logger *Logger) *PerformanceLogger {
	return &PerformanceLogger{logger: logger}
}

// LogLatency logs operation latency
func (pl *PerformanceLogger) LogLatency(operation string, latencyMs float64, success bool) {
	level := DEBUG
	if latencyMs > 1000 { // Log as warning if > 1 second
		level = WARN
	}

	pl.logger.log(level, "Operation latency",
		"operation", operation,
		"latency_ms", latencyMs,
		"success", success,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogThroughput logs throughput metrics
func (pl *PerformanceLogger) LogThroughput(topic string, messagesPerSec float64, bytesPerSec float64) {
	pl.logger.Info("Throughput metrics",
		"topic", topic,
		"messages_per_sec", messagesPerSec,
		"bytes_per_sec", bytesPerSec,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// LogResourceUsage logs resource usage
func (pl *PerformanceLogger) LogResourceUsage(cpuPercent float64, memoryMB int64, goroutines int, connections int) {
	pl.logger.Debug("Resource usage",
		"cpu_percent", cpuPercent,
		"memory_mb", memoryMB,
		"goroutines", goroutines,
		"connections", connections,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// ErrorLogger provides detailed error logging
type ErrorLogger struct {
	logger *Logger
}

// NewErrorLogger creates a new error logger
func NewErrorLogger(logger *Logger) *ErrorLogger {
	return &ErrorLogger{logger: logger}
}

// LogError logs errors with context
func (el *ErrorLogger) LogError(err error, operation string, context map[string]interface{}) {
	fields := make([]interface{}, 0, len(context)*2+4)
	fields = append(fields, "operation", operation)
	fields = append(fields, "error", err.Error())

	for k, v := range context {
		fields = append(fields, k, v)
	}

	fields = append(fields, "timestamp", time.Now().UTC().Format(time.RFC3339Nano))

	el.logger.Error("Operation failed", fields...)
}

// LogRecovery logs panic recovery
func (el *ErrorLogger) LogRecovery(panicValue interface{}, stack string, operation string) {
	el.logger.Error("Panic recovered",
		"operation", operation,
		"panic_value", fmt.Sprintf("%v", panicValue),
		"stack_trace", stack,
		"timestamp", time.Now().UTC().Format(time.RFC3339Nano),
	)
}

// Helper functions

// GenerateConnectionID generates a unique ID for a connection
func GenerateConnectionID(conn net.Conn) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s-%s-%d",
		conn.RemoteAddr().String(),
		conn.LocalAddr().String(),
		time.Now().UnixNano())))
	return hex.EncodeToString(hash[:8])
}

// GenerateMessageID generates a unique ID for a message without exposing content
func GenerateMessageID(topic string, offset uint64) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s-%d", topic, offset)))
	return hex.EncodeToString(hash[:8])
}

// SanitizePayload removes sensitive data from payload for logging
func SanitizePayload(data []byte, maxLen int) string {
	if len(data) == 0 {
		return "[empty]"
	}

	// Only show size, not content
	return fmt.Sprintf("[%d bytes]", len(data))
}

// MaskIP partially masks IP addresses for privacy
func MaskIP(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "unknown"
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return addr
	}

	if ip.To4() != nil {
		// IPv4: show first two octets
		parts := strings.Split(host, ".")
		if len(parts) == 4 {
			return fmt.Sprintf("%s.%s.*.*:%s", parts[0], parts[1], port)
		}
	} else {
		// IPv6: show first 32 bits
		parts := strings.Split(host, ":")
		if len(parts) > 2 {
			return fmt.Sprintf("%s:%s:*:%s", parts[0], parts[1], port)
		}
	}

	return addr
}

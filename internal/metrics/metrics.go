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
Package metrics provides Prometheus-compatible metrics for FlyMQ.

METRIC CATEGORIES:
==================
- Messages: produced, consumed, failed, expired, DLQ
- Throughput: messages per second (produce/consume)
- Latency: produce/consume latency histograms
- Connections: active, total
- Topics: count, partitions
- Consumer Groups: count, lag

PROMETHEUS ENDPOINT:
====================
Metrics are exposed at /metrics in Prometheus text format.

EXAMPLE METRICS:
================

	flymq_messages_produced_total 12345
	flymq_messages_consumed_total 12340
	flymq_produce_latency_seconds{quantile="0.99"} 0.005
	flymq_active_connections 42

GRAFANA DASHBOARD:
==================
Import the provided dashboard JSON for visualization.
*/
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"flymq/internal/config"
	"flymq/internal/logging"
)

// Metrics holds all FlyMQ metrics.
type Metrics struct {
	// Message metrics
	MessagesProduced atomic.Uint64
	MessagesConsumed atomic.Uint64
	MessagesFailed   atomic.Uint64
	MessagesExpired  atomic.Uint64
	MessagesDLQ      atomic.Uint64

	// Throughput metrics (per second)
	ProduceThroughput atomic.Uint64
	ConsumeThroughput atomic.Uint64

	// Latency metrics (in microseconds)
	ProduceLatencySum   atomic.Uint64
	ProduceLatencyCount atomic.Uint64
	ConsumeLatencySum   atomic.Uint64
	ConsumeLatencyCount atomic.Uint64

	// Connection metrics
	ActiveConnections atomic.Int64
	TotalConnections  atomic.Uint64

	// Topic metrics
	TopicCount     atomic.Int64
	PartitionCount atomic.Int64

	// Consumer group metrics
	ConsumerGroupCount atomic.Int64

	// Transaction metrics
	TransactionsActive    atomic.Int64
	TransactionsCommitted atomic.Uint64
	TransactionsAborted   atomic.Uint64

	// Cluster metrics
	ClusterNodes atomic.Int64
	IsLeader     atomic.Bool

	// Per-topic metrics
	topicMetrics sync.Map // topic -> *TopicMetrics
}

// TopicMetrics holds metrics for a specific topic.
type TopicMetrics struct {
	MessagesProduced atomic.Uint64
	MessagesConsumed atomic.Uint64
	BytesProduced    atomic.Uint64
	BytesConsumed    atomic.Uint64
}

// Global metrics instance
var globalMetrics = &Metrics{}

// Get returns the global metrics instance.
func Get() *Metrics {
	return globalMetrics
}

// GetTopicMetrics returns metrics for a specific topic.
func (m *Metrics) GetTopicMetrics(topic string) *TopicMetrics {
	if tm, ok := m.topicMetrics.Load(topic); ok {
		return tm.(*TopicMetrics)
	}
	tm := &TopicMetrics{}
	actual, _ := m.topicMetrics.LoadOrStore(topic, tm)
	return actual.(*TopicMetrics)
}

// RecordProduce records a produce operation.
func (m *Metrics) RecordProduce(topic string, bytes int, latency time.Duration) {
	m.MessagesProduced.Add(1)
	m.ProduceLatencySum.Add(uint64(latency.Microseconds()))
	m.ProduceLatencyCount.Add(1)

	tm := m.GetTopicMetrics(topic)
	tm.MessagesProduced.Add(1)
	tm.BytesProduced.Add(uint64(bytes))
}

// RecordConsume records a consume operation.
func (m *Metrics) RecordConsume(topic string, bytes int, latency time.Duration) {
	m.MessagesConsumed.Add(1)
	m.ConsumeLatencySum.Add(uint64(latency.Microseconds()))
	m.ConsumeLatencyCount.Add(1)

	tm := m.GetTopicMetrics(topic)
	tm.MessagesConsumed.Add(1)
	tm.BytesConsumed.Add(uint64(bytes))
}

// RecordError records a failed operation.
func (m *Metrics) RecordError() {
	m.MessagesFailed.Add(1)
}

// RecordExpired records an expired message.
func (m *Metrics) RecordExpired() {
	m.MessagesExpired.Add(1)
}

// RecordDLQ records a message sent to DLQ.
func (m *Metrics) RecordDLQ() {
	m.MessagesDLQ.Add(1)
}

// ConnectionOpened records a new connection.
func (m *Metrics) ConnectionOpened() {
	m.ActiveConnections.Add(1)
	m.TotalConnections.Add(1)
}

// ConnectionClosed records a closed connection.
func (m *Metrics) ConnectionClosed() {
	m.ActiveConnections.Add(-1)
}

// AverageProduceLatency returns the average produce latency in microseconds.
func (m *Metrics) AverageProduceLatency() float64 {
	count := m.ProduceLatencyCount.Load()
	if count == 0 {
		return 0
	}
	return float64(m.ProduceLatencySum.Load()) / float64(count)
}

// AverageConsumeLatency returns the average consume latency in microseconds.
func (m *Metrics) AverageConsumeLatency() float64 {
	count := m.ConsumeLatencyCount.Load()
	if count == 0 {
		return 0
	}
	return float64(m.ConsumeLatencySum.Load()) / float64(count)
}

// Server provides an HTTP server for Prometheus metrics.
type Server struct {
	config *config.MetricsConfig
	server *http.Server
	logger *logging.Logger
}

// NewServer creates a new metrics server.
func NewServer(cfg *config.MetricsConfig) *Server {
	return &Server{
		config: cfg,
		logger: logging.NewLogger("metrics"),
	}
}

// Start starts the metrics HTTP server.
func (s *Server) Start() error {
	if !s.config.Enabled {
		s.logger.Info("Metrics server disabled")
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)

	s.server = &http.Server{
		Addr:    s.config.Addr,
		Handler: mux,
	}

	go func() {
		s.logger.Info("Starting metrics server", "addr", s.config.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("Metrics server error", "error", err)
		}
	}()

	return nil
}

// Stop stops the metrics HTTP server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.logger.Info("Stopping metrics server")
	return s.server.Shutdown(ctx)
}

// handleMetrics handles the /metrics endpoint in Prometheus format.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m := Get()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")

	// Message metrics
	fmt.Fprintf(w, "# HELP flymq_messages_produced_total Total messages produced\n")
	fmt.Fprintf(w, "# TYPE flymq_messages_produced_total counter\n")
	fmt.Fprintf(w, "flymq_messages_produced_total %d\n", m.MessagesProduced.Load())

	fmt.Fprintf(w, "# HELP flymq_messages_consumed_total Total messages consumed\n")
	fmt.Fprintf(w, "# TYPE flymq_messages_consumed_total counter\n")
	fmt.Fprintf(w, "flymq_messages_consumed_total %d\n", m.MessagesConsumed.Load())

	fmt.Fprintf(w, "# HELP flymq_messages_failed_total Total failed messages\n")
	fmt.Fprintf(w, "# TYPE flymq_messages_failed_total counter\n")
	fmt.Fprintf(w, "flymq_messages_failed_total %d\n", m.MessagesFailed.Load())

	fmt.Fprintf(w, "# HELP flymq_messages_expired_total Total expired messages\n")
	fmt.Fprintf(w, "# TYPE flymq_messages_expired_total counter\n")
	fmt.Fprintf(w, "flymq_messages_expired_total %d\n", m.MessagesExpired.Load())

	fmt.Fprintf(w, "# HELP flymq_messages_dlq_total Total messages sent to DLQ\n")
	fmt.Fprintf(w, "# TYPE flymq_messages_dlq_total counter\n")
	fmt.Fprintf(w, "flymq_messages_dlq_total %d\n", m.MessagesDLQ.Load())

	// Latency metrics
	fmt.Fprintf(w, "# HELP flymq_produce_latency_avg_microseconds Average produce latency\n")
	fmt.Fprintf(w, "# TYPE flymq_produce_latency_avg_microseconds gauge\n")
	fmt.Fprintf(w, "flymq_produce_latency_avg_microseconds %.2f\n", m.AverageProduceLatency())

	fmt.Fprintf(w, "# HELP flymq_consume_latency_avg_microseconds Average consume latency\n")
	fmt.Fprintf(w, "# TYPE flymq_consume_latency_avg_microseconds gauge\n")
	fmt.Fprintf(w, "flymq_consume_latency_avg_microseconds %.2f\n", m.AverageConsumeLatency())

	// Connection metrics
	fmt.Fprintf(w, "# HELP flymq_connections_active Current active connections\n")
	fmt.Fprintf(w, "# TYPE flymq_connections_active gauge\n")
	fmt.Fprintf(w, "flymq_connections_active %d\n", m.ActiveConnections.Load())

	fmt.Fprintf(w, "# HELP flymq_connections_total Total connections\n")
	fmt.Fprintf(w, "# TYPE flymq_connections_total counter\n")
	fmt.Fprintf(w, "flymq_connections_total %d\n", m.TotalConnections.Load())

	// Topic metrics
	fmt.Fprintf(w, "# HELP flymq_topics_count Number of topics\n")
	fmt.Fprintf(w, "# TYPE flymq_topics_count gauge\n")
	fmt.Fprintf(w, "flymq_topics_count %d\n", m.TopicCount.Load())

	fmt.Fprintf(w, "# HELP flymq_partitions_count Number of partitions\n")
	fmt.Fprintf(w, "# TYPE flymq_partitions_count gauge\n")
	fmt.Fprintf(w, "flymq_partitions_count %d\n", m.PartitionCount.Load())

	// Consumer group metrics
	fmt.Fprintf(w, "# HELP flymq_consumer_groups_count Number of consumer groups\n")
	fmt.Fprintf(w, "# TYPE flymq_consumer_groups_count gauge\n")
	fmt.Fprintf(w, "flymq_consumer_groups_count %d\n", m.ConsumerGroupCount.Load())

	// Transaction metrics
	fmt.Fprintf(w, "# HELP flymq_transactions_active Active transactions\n")
	fmt.Fprintf(w, "# TYPE flymq_transactions_active gauge\n")
	fmt.Fprintf(w, "flymq_transactions_active %d\n", m.TransactionsActive.Load())

	fmt.Fprintf(w, "# HELP flymq_transactions_committed_total Committed transactions\n")
	fmt.Fprintf(w, "# TYPE flymq_transactions_committed_total counter\n")
	fmt.Fprintf(w, "flymq_transactions_committed_total %d\n", m.TransactionsCommitted.Load())

	fmt.Fprintf(w, "# HELP flymq_transactions_aborted_total Aborted transactions\n")
	fmt.Fprintf(w, "# TYPE flymq_transactions_aborted_total counter\n")
	fmt.Fprintf(w, "flymq_transactions_aborted_total %d\n", m.TransactionsAborted.Load())

	// Cluster metrics
	fmt.Fprintf(w, "# HELP flymq_cluster_nodes Number of cluster nodes\n")
	fmt.Fprintf(w, "# TYPE flymq_cluster_nodes gauge\n")
	fmt.Fprintf(w, "flymq_cluster_nodes %d\n", m.ClusterNodes.Load())

	isLeader := 0
	if m.IsLeader.Load() {
		isLeader = 1
	}
	fmt.Fprintf(w, "# HELP flymq_is_leader Whether this node is the leader\n")
	fmt.Fprintf(w, "# TYPE flymq_is_leader gauge\n")
	fmt.Fprintf(w, "flymq_is_leader %d\n", isLeader)

	// Per-topic metrics
	fmt.Fprintf(w, "# HELP flymq_topic_messages_produced_total Messages produced per topic\n")
	fmt.Fprintf(w, "# TYPE flymq_topic_messages_produced_total counter\n")
	m.topicMetrics.Range(func(key, value interface{}) bool {
		topic := key.(string)
		tm := value.(*TopicMetrics)
		fmt.Fprintf(w, "flymq_topic_messages_produced_total{topic=\"%s\"} %d\n", topic, tm.MessagesProduced.Load())
		return true
	})

	fmt.Fprintf(w, "# HELP flymq_topic_messages_consumed_total Messages consumed per topic\n")
	fmt.Fprintf(w, "# TYPE flymq_topic_messages_consumed_total counter\n")
	m.topicMetrics.Range(func(key, value interface{}) bool {
		topic := key.(string)
		tm := value.(*TopicMetrics)
		fmt.Fprintf(w, "flymq_topic_messages_consumed_total{topic=\"%s\"} %d\n", topic, tm.MessagesConsumed.Load())
		return true
	})
}

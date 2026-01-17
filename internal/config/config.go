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
Package config provides configuration management for FlyMQ.

CONFIGURATION SOURCES (in order of precedence):
===============================================
1. Command-line flags (highest priority)
2. Environment variables (FLYMQ_* prefix)
3. Configuration file (JSON format)
4. Default values (lowest priority)

CONFIGURATION CATEGORIES:
=========================
- Network: bind_addr, cluster_addr, peers, node_id
- Storage: data_dir, retention_bytes, segment_bytes
- Security: TLS, encryption at rest
- Logging: log_level, log_json
- Features: schema, DLQ, TTL, delayed, transactions
- Observability: metrics, tracing, health, admin

EXAMPLE CONFIGURATION FILE:
===========================

	{
	  "bind_addr": ":9092",
	  "data_dir": "/var/lib/flymq",
	  "security": {
	    "tls_enabled": true,
	    "tls_cert_file": "/etc/flymq/server.crt",
	    "tls_key_file": "/etc/flymq/server.key"
	  }
	}

ENVIRONMENT VARIABLES:
======================
All settings can be configured via environment variables with FLYMQ_ prefix.
Example: FLYMQ_BIND_ADDR=":9092" FLYMQ_LOG_LEVEL="debug"
*/
package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// Environment variable names
const (
	EnvBindAddr          = "FLYMQ_BIND_ADDR"
	EnvClusterAddr       = "FLYMQ_CLUSTER_ADDR"
	EnvAdvertiseAddr     = "FLYMQ_ADVERTISE_ADDR"
	EnvAdvertiseCluster  = "FLYMQ_ADVERTISE_CLUSTER"
	EnvDataDir           = "FLYMQ_DATA_DIR"
	EnvPeers             = "FLYMQ_PEERS"
	EnvNodeID            = "FLYMQ_NODE_ID"
	EnvLogLevel          = "FLYMQ_LOG_LEVEL"
	EnvLogJSON           = "FLYMQ_LOG_JSON"
	EnvRetention         = "FLYMQ_RETENTION_BYTES"
	EnvSegmentBytes      = "FLYMQ_SEGMENT_BYTES"
	EnvTLSEnabled        = "FLYMQ_TLS_ENABLED"
	EnvTLSCertFile       = "FLYMQ_TLS_CERT_FILE"
	EnvTLSKeyFile        = "FLYMQ_TLS_KEY_FILE"
	EnvTLSCAFile         = "FLYMQ_TLS_CA_FILE"
	EnvEncryptionEnabled = "FLYMQ_ENCRYPTION_ENABLED"
	EnvEncryptionKey     = "FLYMQ_ENCRYPTION_KEY"

	// Performance configuration
	EnvAcks = "FLYMQ_ACKS" // Acks mode: all, leader, none

	// Schema configuration
	EnvSchemaEnabled     = "FLYMQ_SCHEMA_ENABLED"
	EnvSchemaRegistryDir = "FLYMQ_SCHEMA_REGISTRY_DIR"
	EnvSchemaValidation  = "FLYMQ_SCHEMA_VALIDATION"

	// Dead letter queue configuration
	EnvDLQEnabled    = "FLYMQ_DLQ_ENABLED"
	EnvDLQMaxRetries = "FLYMQ_DLQ_MAX_RETRIES"
	EnvDLQRetryDelay = "FLYMQ_DLQ_RETRY_DELAY"

	// Message TTL configuration
	EnvDefaultTTL         = "FLYMQ_DEFAULT_TTL"
	EnvTTLCleanupInterval = "FLYMQ_TTL_CLEANUP_INTERVAL"

	// Delayed delivery configuration
	EnvDelayedEnabled  = "FLYMQ_DELAYED_ENABLED"
	EnvDelayedMaxDelay = "FLYMQ_DELAYED_MAX_DELAY"

	// Transaction configuration
	EnvTransactionEnabled = "FLYMQ_TRANSACTION_ENABLED"
	EnvTransactionTimeout = "FLYMQ_TRANSACTION_TIMEOUT"

	// Observability configuration
	EnvMetricsEnabled    = "FLYMQ_METRICS_ENABLED"
	EnvMetricsAddr       = "FLYMQ_METRICS_ADDR"
	EnvTracingEnabled    = "FLYMQ_TRACING_ENABLED"
	EnvTracingEndpoint   = "FLYMQ_TRACING_ENDPOINT"
	EnvTracingSampleRate = "FLYMQ_TRACING_SAMPLE_RATE"
	EnvHealthEnabled     = "FLYMQ_HEALTH_ENABLED"
	EnvHealthAddr        = "FLYMQ_HEALTH_ADDR"
	EnvAdminEnabled      = "FLYMQ_ADMIN_ENABLED"
	EnvAdminAddr         = "FLYMQ_ADMIN_ADDR"

	// Authentication configuration
	EnvAuthEnabled       = "FLYMQ_AUTH_ENABLED"
	EnvAuthUserFile      = "FLYMQ_AUTH_USER_FILE"
	EnvAuthACLFile       = "FLYMQ_AUTH_ACL_FILE"
	EnvAuthDefaultPublic = "FLYMQ_AUTH_DEFAULT_PUBLIC"
	EnvAuthAdminUsername = "FLYMQ_AUTH_ADMIN_USERNAME"
	EnvAuthAdminPassword = "FLYMQ_AUTH_ADMIN_PASSWORD"

	// Partition management configuration (horizontal scaling)
	EnvPartitionDistributionStrategy = "FLYMQ_PARTITION_DISTRIBUTION_STRATEGY"
	EnvDefaultReplicationFactor      = "FLYMQ_DEFAULT_REPLICATION_FACTOR"
	EnvDefaultPartitions             = "FLYMQ_DEFAULT_PARTITIONS"
	EnvAutoRebalanceEnabled          = "FLYMQ_AUTO_REBALANCE_ENABLED"
	EnvAutoRebalanceInterval         = "FLYMQ_AUTO_REBALANCE_INTERVAL"
	EnvRebalanceThreshold            = "FLYMQ_REBALANCE_THRESHOLD"
)

// Default paths
var DefaultConfigPaths = []string{
	"/etc/flymq/flymq.conf",
	"$HOME/.config/flymq/flymq.conf",
	"./flymq.conf",
}

// SecurityConfig holds security-related configuration.
type SecurityConfig struct {
	// TLS Configuration
	TLSEnabled  bool   `toml:"tls_enabled" json:"tls_enabled"`     // Enable TLS for client connections
	TLSCertFile string `toml:"tls_cert_file" json:"tls_cert_file"` // Path to TLS certificate file
	TLSKeyFile  string `toml:"tls_key_file" json:"tls_key_file"`   // Path to TLS private key file
	TLSCAFile   string `toml:"tls_ca_file" json:"tls_ca_file"`     // Path to CA certificate for client auth

	// Encryption Configuration
	EncryptionEnabled bool   `toml:"encryption_enabled" json:"encryption_enabled"` // Enable data-at-rest encryption
	EncryptionKey     string `toml:"-" json:"-"`                                   // AES-256 key - MUST be set via FLYMQ_ENCRYPTION_KEY env var (never stored in config files)
}

// SchemaConfig holds schema registry configuration.
type SchemaConfig struct {
	Enabled     bool   `toml:"enabled" json:"enabled"`           // Enable schema validation
	RegistryDir string `toml:"registry_dir" json:"registry_dir"` // Directory for schema storage
	Validation  string `toml:"validation" json:"validation"`     // Validation mode: strict, warn, none
}

// DLQConfig holds dead letter queue configuration.
type DLQConfig struct {
	Enabled     bool   `toml:"enabled" json:"enabled"`           // Enable dead letter queues
	MaxRetries  int    `toml:"max_retries" json:"max_retries"`   // Maximum retry attempts before DLQ
	RetryDelay  int64  `toml:"retry_delay" json:"retry_delay"`   // Delay between retries in milliseconds
	TopicSuffix string `toml:"topic_suffix" json:"topic_suffix"` // Suffix for DLQ topics
}

// TTLConfig holds message TTL configuration.
type TTLConfig struct {
	DefaultTTL      int64 `toml:"default_ttl" json:"default_ttl"`           // Default TTL in seconds (0=no expiry)
	CleanupInterval int64 `toml:"cleanup_interval" json:"cleanup_interval"` // Cleanup interval in seconds
}

// DelayedConfig holds delayed message delivery configuration.
type DelayedConfig struct {
	Enabled  bool  `toml:"enabled" json:"enabled"`     // Enable delayed message delivery
	MaxDelay int64 `toml:"max_delay" json:"max_delay"` // Maximum delay in seconds
}

// TransactionConfig holds transaction configuration.
type TransactionConfig struct {
	Enabled bool  `toml:"enabled" json:"enabled"` // Enable transaction support
	Timeout int64 `toml:"timeout" json:"timeout"` // Transaction timeout in seconds
}

// AuditConfig holds audit trail configuration.
type AuditConfig struct {
	Enabled       bool   `toml:"enabled" json:"enabled"`               // Enable audit trail logging
	LogDir        string `toml:"log_dir" json:"log_dir"`               // Directory for audit logs (default: data_dir/audit)
	MaxFileSize   int64  `toml:"max_file_size" json:"max_file_size"`   // Maximum audit log file size in bytes (default: 100MB)
	RetentionDays int    `toml:"retention_days" json:"retention_days"` // Days to retain audit logs (default: 90)
}

// PartitionConfig holds partition management configuration for horizontal scaling.
//
// IMPORTANT: This configures how partition LEADERS are distributed across cluster nodes,
// NOT how messages are assigned to partitions. Message partitioning uses:
//   - Key-based partitioning: hash(key) % num_partitions (FNV-1a)
//   - Round-robin: when no key is provided
//
// Leader distribution enables horizontal scaling by spreading write load across nodes.
type PartitionConfig struct {
	// LeaderDistributionStrategy determines how partition leaders are distributed across nodes.
	// This affects cluster-level load balancing, not message-to-partition assignment.
	// Options:
	//   - "round-robin" (default): Distribute leaders evenly in order
	//   - "least-loaded": Assign to node with fewest leaders
	//   - "rack-aware": Consider rack placement for fault tolerance
	DistributionStrategy string `toml:"distribution_strategy" json:"distribution_strategy"`

	// DefaultReplicationFactor is the default number of replicas for new topics.
	// Must be <= number of nodes in the cluster. Higher values improve durability.
	DefaultReplicationFactor int `toml:"default_replication_factor" json:"default_replication_factor"`

	// DefaultPartitions is the default number of partitions for new topics.
	// More partitions = more parallelism but more overhead.
	// Messages are assigned to partitions via key-based hashing or round-robin.
	DefaultPartitions int `toml:"default_partitions" json:"default_partitions"`

	// AutoRebalanceEnabled enables automatic partition leader rebalancing when nodes join/leave.
	AutoRebalanceEnabled bool `toml:"auto_rebalance_enabled" json:"auto_rebalance_enabled"`

	// AutoRebalanceInterval is the interval in seconds between automatic rebalance checks.
	AutoRebalanceInterval int64 `toml:"auto_rebalance_interval" json:"auto_rebalance_interval"`

	// RebalanceThreshold is the maximum allowed imbalance ratio before triggering rebalance.
	// For example, 0.2 means rebalance if any node has 20% more leaders than average.
	RebalanceThreshold float64 `toml:"rebalance_threshold" json:"rebalance_threshold"`
}

// MetricsConfig holds Prometheus metrics configuration.
type MetricsConfig struct {
	Enabled bool   `toml:"enabled" json:"enabled"` // Enable Prometheus metrics
	Addr    string `toml:"addr" json:"addr"`       // Metrics HTTP server address
}

// TracingConfig holds OpenTelemetry tracing configuration.
type TracingConfig struct {
	Enabled     bool    `toml:"enabled" json:"enabled"`           // Enable OpenTelemetry tracing
	Endpoint    string  `toml:"endpoint" json:"endpoint"`         // OTLP endpoint
	SampleRate  float64 `toml:"sample_rate" json:"sample_rate"`   // Sampling rate (0.0-1.0)
	ServiceName string  `toml:"service_name" json:"service_name"` // Service name for traces
}

// HealthConfig holds health check configuration.
type HealthConfig struct {
	Enabled bool   `toml:"enabled" json:"enabled"` // Enable health check endpoints
	Addr    string `toml:"addr" json:"addr"`       // Health check HTTP server address

	// TLS Configuration for HTTPS
	TLSEnabled      bool   `toml:"tls_enabled" json:"tls_enabled"`             // Enable HTTPS for health endpoints
	TLSCertFile     string `toml:"tls_cert_file" json:"tls_cert_file"`         // Path to TLS certificate file
	TLSKeyFile      string `toml:"tls_key_file" json:"tls_key_file"`           // Path to TLS private key file
	TLSAutoGenerate bool   `toml:"tls_auto_generate" json:"tls_auto_generate"` // Auto-generate self-signed certificate
	TLSUseAdminCert bool   `toml:"tls_use_admin_cert" json:"tls_use_admin_cert"` // Use Admin API TLS certificate
}

// AdminConfig holds admin API configuration.
type AdminConfig struct {
	Enabled     bool   `toml:"enabled" json:"enabled"`           // Enable admin API
	Addr        string `toml:"addr" json:"addr"`                 // Admin API HTTP server address
	AuthEnabled bool   `toml:"auth_enabled" json:"auth_enabled"` // Require authentication for protected endpoints

	// TLS Configuration for HTTPS
	TLSEnabled      bool   `toml:"tls_enabled" json:"tls_enabled"`             // Enable HTTPS for admin API
	TLSCertFile     string `toml:"tls_cert_file" json:"tls_cert_file"`         // Path to TLS certificate file
	TLSKeyFile      string `toml:"tls_key_file" json:"tls_key_file"`           // Path to TLS private key file
	TLSAutoGenerate bool   `toml:"tls_auto_generate" json:"tls_auto_generate"` // Auto-generate self-signed certificate
}

// ObservabilityConfig holds all observability-related configuration.
type ObservabilityConfig struct {
	Metrics MetricsConfig `toml:"metrics" json:"metrics"`
	Tracing TracingConfig `toml:"tracing" json:"tracing"`
	Health  HealthConfig  `toml:"health" json:"health"`
	Admin   AdminConfig   `toml:"admin" json:"admin"`
}

// AuthConfig holds authentication and authorization configuration.
type AuthConfig struct {
	Enabled        bool   `toml:"enabled" json:"enabled"`                 // Enable authentication
	RBACEnabled    bool   `toml:"rbac_enabled" json:"rbac_enabled"`       // Enable role-based access control (default: true when auth enabled)
	AllowAnonymous bool   `toml:"allow_anonymous" json:"allow_anonymous"` // Allow unauthenticated connections (read-only)
	UserFile       string `toml:"user_file" json:"user_file"`             // Path to user database file
	ACLFile        string `toml:"acl_file" json:"acl_file"`               // Path to ACL database file
	DefaultPublic  bool   `toml:"default_public" json:"default_public"`   // Topics are public by default
	AdminUsername  string `toml:"admin_username" json:"admin_username"`   // Default admin username
	AdminPassword  string `toml:"admin_password" json:"admin_password"`   // Default admin password (created on first startup)
}

// PerformanceConfig holds performance tuning configuration.
// These settings control the trade-off between durability and throughput.
type PerformanceConfig struct {
	// Durability settings - controls fsync behavior
	// "all" - fsync every write (safest, slowest - current behavior)
	// "leader" - batch fsync on interval (balanced)
	// "none" - async writes, fsync on segment rotation only (fastest, least safe)
	Acks string `toml:"acks" json:"acks"`

	// SyncIntervalMs is the interval between batch fsyncs when Acks="leader"
	// Default: 10ms. Lower values = more durable, higher latency
	SyncIntervalMs int `toml:"sync_interval_ms" json:"sync_interval_ms"`

	// SyncBatchSize is the max messages before forcing fsync when Acks="leader"
	// Default: 100. Lower values = more durable, higher latency
	SyncBatchSize int `toml:"sync_batch_size" json:"sync_batch_size"`

	// BinaryProtocol enables binary message encoding instead of JSON
	// Reduces serialization overhead by 50-80%
	BinaryProtocol bool `toml:"binary_protocol" json:"binary_protocol"`

	// AsyncIO enables the async I/O manager for non-blocking writes
	AsyncIO bool `toml:"async_io" json:"async_io"`

	// ZeroCopy enables zero-copy transfers for consumers using sendfile()
	ZeroCopy bool `toml:"zero_copy" json:"zero_copy"`

	// WriteBufferSize is the per-partition write buffer size in bytes
	// Default: 1MB. Larger buffers = better batching, more memory
	WriteBufferSize int `toml:"write_buffer_size" json:"write_buffer_size"`

	// NumIOWorkers is the number of async I/O worker goroutines
	// Default: 4. More workers = better parallelism for multiple partitions
	NumIOWorkers int `toml:"num_io_workers" json:"num_io_workers"`

	// Compression settings for storage and network
	// "none" - no compression (fastest, no CPU overhead)
	// "lz4" - fast compression, moderate ratio (recommended for most cases)
	// "snappy" - very fast, lower compression ratio
	// "zstd" - best ratio, configurable speed/ratio tradeoff
	// "gzip" - good compression ratio, moderate CPU
	Compression string `toml:"compression" json:"compression"`

	// CompressionLevel controls the compression level (1-12 for lz4/zstd, 1-9 for gzip)
	// Higher = better compression, more CPU. Default: 3
	CompressionLevel int `toml:"compression_level" json:"compression_level"`

	// CompressionMinSize is the minimum message size to compress (bytes)
	// Messages smaller than this are not compressed. Default: 1024 (1KB)
	CompressionMinSize int `toml:"compression_min_size" json:"compression_min_size"`

	// LargeMessageThreshold is the size above which zero-copy optimizations are used
	// Default: 65536 (64KB)
	LargeMessageThreshold int `toml:"large_message_threshold" json:"large_message_threshold"`
}

// DiscoveryConfig holds configuration for mDNS service discovery.
type DiscoveryConfig struct {
	Enabled   bool   `toml:"enabled" json:"enabled"`       // Enable mDNS service discovery
	ClusterID string `toml:"cluster_id" json:"cluster_id"` // Cluster identifier for discovery filtering
}

// Config holds the configuration for FlyMQ.
type Config struct {
	// Network
	BindAddr         string   `toml:"bind_addr" json:"bind_addr"`                 // Address to listen for clients
	ClusterAddr      string   `toml:"cluster_addr" json:"cluster_addr"`           // Address for cluster traffic
	AdvertiseAddr    string   `toml:"advertise_addr" json:"advertise_addr"`       // Advertised address for clients (auto-detected if empty)
	AdvertiseCluster string   `toml:"advertise_cluster" json:"advertise_cluster"` // Advertised cluster address (auto-detected if empty)
	Peers            []string `toml:"peers" json:"peers"`                         // Initial cluster peers
	NodeID           string   `toml:"node_id" json:"node_id"`                     // Unique node identifier

	// Storage
	DataDir      string `toml:"data_dir" json:"data_dir"`
	Retention    int64  `toml:"retention_bytes" json:"retention_bytes"` // Max bytes per topic/partition (0=unlimited)
	SegmentBytes int64  `toml:"segment_bytes" json:"segment_bytes"`     // Size of each log segment

	// Logging
	LogLevel string `toml:"log_level" json:"log_level"`
	LogJSON  bool   `toml:"log_json" json:"log_json"`

	// Security
	Security SecurityConfig `toml:"security" json:"security"`

	// Authentication
	Auth AuthConfig `toml:"auth" json:"auth"`

	// Message Handling
	Schema      SchemaConfig      `toml:"schema" json:"schema"`
	DLQ         DLQConfig         `toml:"dlq" json:"dlq"`
	TTL         TTLConfig         `toml:"ttl" json:"ttl"`
	Delayed     DelayedConfig     `toml:"delayed" json:"delayed"`
	Transaction TransactionConfig `toml:"transaction" json:"transaction"`

	// Partition Management (Horizontal Scaling)
	Partition PartitionConfig `toml:"partition" json:"partition"`

	// Service Discovery
	Discovery DiscoveryConfig `toml:"discovery" json:"discovery"`

	// Observability
	Observability ObservabilityConfig `toml:"observability" json:"observability"`

	// Performance tuning
	Performance PerformanceConfig `toml:"performance" json:"performance"`

	// Audit Trail
	Audit AuditConfig `toml:"audit" json:"audit"`

	// Metadata
	ConfigFile string `toml:"-" json:"-"`
}

// DefaultConfig returns defaults.
func DefaultConfig() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		BindAddr:     ":9092",
		ClusterAddr:  ":9093",
		Peers:        []string{},
		NodeID:       hostname,
		DataDir:      GetDefaultDataDir(),
		Retention:    0,                // Unlimited
		SegmentBytes: 1024 * 1024 * 64, // 64MB
		LogLevel:     "info",
		LogJSON:      true, // JSON by default for production/parsing
		Schema: SchemaConfig{
			Enabled:    false,
			Validation: "strict",
		},
		DLQ: DLQConfig{
			Enabled:     false,
			MaxRetries:  3,
			RetryDelay:  1000, // 1 second
			TopicSuffix: ".dlq",
		},
		TTL: TTLConfig{
			DefaultTTL:      0,  // No expiry by default
			CleanupInterval: 60, // 1 minute
		},
		Delayed: DelayedConfig{
			Enabled:  false,
			MaxDelay: 86400, // 24 hours
		},
		Transaction: TransactionConfig{
			Enabled: false,
			Timeout: 60, // 1 minute
		},
		Auth: AuthConfig{
			Enabled:        false,
			RBACEnabled:    true,  // RBAC is enabled by default when auth is enabled
			AllowAnonymous: false, // Require authentication by default (secure default)
			DefaultPublic:  true,  // Backward compatible: topics are public by default
			AdminUsername:  "admin",
		},
		Partition: PartitionConfig{
			DistributionStrategy:    "round-robin",
			DefaultReplicationFactor: 1,  // Single replica by default (increase for production)
			DefaultPartitions:        1,  // Single partition by default
			AutoRebalanceEnabled:     false,
			AutoRebalanceInterval:    300, // 5 minutes
			RebalanceThreshold:       0.2, // 20% imbalance triggers rebalance
		},
		Discovery: DiscoveryConfig{
			Enabled:   false, // Disabled by default, enable for cluster auto-discovery
			ClusterID: "",    // Empty means accept any cluster
		},
		Observability: ObservabilityConfig{
			Metrics: MetricsConfig{
				Enabled: false,
				Addr:    ":9094",
			},
			Tracing: TracingConfig{
				Enabled:     false,
				Endpoint:    "localhost:4317",
				SampleRate:  0.1,
				ServiceName: "flymq",
			},
			Health: HealthConfig{
				Enabled: true,
				Addr:    ":9095",
			},
			Admin: AdminConfig{
				Enabled: false,
				Addr:    ":9096",
			},
		},
		Performance: autoTunePerformance(),
		Audit: AuditConfig{
			Enabled:       true, // Enabled by default for security compliance
			MaxFileSize:   100 * 1024 * 1024, // 100MB
			RetentionDays: 90,
		},
	}
}

// autoTunePerformance returns performance settings optimized for the current system.
func autoTunePerformance() PerformanceConfig {
	numCPU := runtime.NumCPU()

	// Scale workers with CPU count (min 4, max 32)
	numWorkers := numCPU
	if numWorkers < 4 {
		numWorkers = 4
	}
	if numWorkers > 32 {
		numWorkers = 32
	}

	// Larger batch sizes for more CPUs (min 500, max 10000)
	batchSize := 500 + (numCPU * 100)
	if batchSize > 10000 {
		batchSize = 10000
	}

	// Buffer size scales with CPU count (min 2MB, max 64MB)
	bufferSize := 2 * 1024 * 1024 * numCPU
	if bufferSize > 64*1024*1024 {
		bufferSize = 64 * 1024 * 1024
	}
	if bufferSize < 2*1024*1024 {
		bufferSize = 2 * 1024 * 1024
	}

	return PerformanceConfig{
		Acks:                  "leader",   // Balanced durability/performance
		SyncIntervalMs:        5,          // Aggressive 5ms sync interval (was 10ms)
		SyncBatchSize:         batchSize,  // Auto-tuned batch size
		BinaryProtocol:        false,      // JSON by default for compatibility
		AsyncIO:               true,       // Enable async I/O by default
		ZeroCopy:              true,       // Enable zero-copy for consumers
		WriteBufferSize:       bufferSize, // Auto-tuned buffer size
		NumIOWorkers:          numWorkers, // Auto-tuned worker count
		Compression:           "lz4",      // LZ4 for best speed/ratio tradeoff
		CompressionLevel:      3,          // Moderate compression level
		CompressionMinSize:    1024,       // Only compress messages >= 1KB
		LargeMessageThreshold: 64 * 1024,  // Use zero-copy for messages >= 64KB
	}
}

// GetDefaultDataDir returns the default data directory.
func GetDefaultDataDir() string {
	if os.Getuid() == 0 {
		return "/var/lib/flymq"
	}
	if home := os.Getenv("HOME"); home != "" {
		return filepath.Join(home, ".local", "share", "flymq")
	}
	return "./data"
}

// Manager handles configuration loading.
type Manager struct {
	config *Config
	mu     sync.RWMutex
}

var globalManager = &Manager{
	config: DefaultConfig(),
}

// Global returns the global manager.
func Global() *Manager {
	return globalManager
}

// Get returns a copy of current config.
func (m *Manager) Get() *Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cfg := *m.config
	return &cfg
}

// Set updates the config.
func (m *Manager) Set(cfg *Config) {
	m.mu.Lock()
	m.config = cfg
	m.mu.Unlock()
}

// LoadFromFile loads configuration from a JSON file.
func (m *Manager) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return err
	}

	cfg.ConfigFile = path
	m.Set(cfg)
	return nil
}

// LoadFromEnv loads configuration from environment variables.
func (m *Manager) LoadFromEnv() {
	cfg := m.Get()

	if v := os.Getenv(EnvBindAddr); v != "" {
		cfg.BindAddr = v
	}
	if v, ok := os.LookupEnv(EnvClusterAddr); ok {
		// Allow explicitly setting to empty to disable cluster mode
		cfg.ClusterAddr = v
	}
	if v := os.Getenv(EnvAdvertiseAddr); v != "" {
		cfg.AdvertiseAddr = v
	}
	if v := os.Getenv(EnvAdvertiseCluster); v != "" {
		cfg.AdvertiseCluster = v
	}
	if v := os.Getenv(EnvDataDir); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv(EnvNodeID); v != "" {
		cfg.NodeID = v
	}
	if v := os.Getenv(EnvLogLevel); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv(EnvLogJSON); v != "" {
		cfg.LogJSON = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvPeers); v != "" {
		parts := strings.Split(v, ",")
		for _, p := range parts {
			if strings.TrimSpace(p) != "" {
				cfg.Peers = append(cfg.Peers, strings.TrimSpace(p))
			}
		}
	}
	if v := os.Getenv(EnvRetention); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.Retention = i
		}
	}
	if v := os.Getenv(EnvSegmentBytes); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.SegmentBytes = i
		}
	}

	// Security environment variables
	if v := os.Getenv(EnvTLSEnabled); v != "" {
		cfg.Security.TLSEnabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvTLSCertFile); v != "" {
		cfg.Security.TLSCertFile = v
	}
	if v := os.Getenv(EnvTLSKeyFile); v != "" {
		cfg.Security.TLSKeyFile = v
	}
	if v := os.Getenv(EnvTLSCAFile); v != "" {
		cfg.Security.TLSCAFile = v
	}
	if v := os.Getenv(EnvEncryptionEnabled); v != "" {
		cfg.Security.EncryptionEnabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvEncryptionKey); v != "" {
		cfg.Security.EncryptionKey = v
	}

	// Performance environment variables
	if v := os.Getenv(EnvAcks); v != "" {
		cfg.Performance.Acks = v
	}

	// Schema environment variables
	if v := os.Getenv(EnvSchemaEnabled); v != "" {
		cfg.Schema.Enabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvSchemaRegistryDir); v != "" {
		cfg.Schema.RegistryDir = v
	}
	if v := os.Getenv(EnvSchemaValidation); v != "" {
		cfg.Schema.Validation = v
	}

	// DLQ environment variables
	if v := os.Getenv(EnvDLQEnabled); v != "" {
		cfg.DLQ.Enabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvDLQMaxRetries); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.DLQ.MaxRetries = i
		}
	}
	if v := os.Getenv(EnvDLQRetryDelay); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.DLQ.RetryDelay = i
		}
	}

	// TTL environment variables
	if v := os.Getenv(EnvDefaultTTL); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.TTL.DefaultTTL = i
		}
	}
	if v := os.Getenv(EnvTTLCleanupInterval); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.TTL.CleanupInterval = i
		}
	}

	// Delayed delivery environment variables
	if v := os.Getenv(EnvDelayedEnabled); v != "" {
		cfg.Delayed.Enabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvDelayedMaxDelay); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.Delayed.MaxDelay = i
		}
	}

	// Transaction environment variables
	if v := os.Getenv(EnvTransactionEnabled); v != "" {
		cfg.Transaction.Enabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvTransactionTimeout); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.Transaction.Timeout = i
		}
	}

	// Observability environment variables
	if v := os.Getenv(EnvMetricsEnabled); v != "" {
		cfg.Observability.Metrics.Enabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvMetricsAddr); v != "" {
		cfg.Observability.Metrics.Addr = v
	}
	if v := os.Getenv(EnvTracingEnabled); v != "" {
		cfg.Observability.Tracing.Enabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvTracingEndpoint); v != "" {
		cfg.Observability.Tracing.Endpoint = v
	}
	if v := os.Getenv(EnvTracingSampleRate); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.Observability.Tracing.SampleRate = f
		}
	}
	if v := os.Getenv(EnvHealthEnabled); v != "" {
		cfg.Observability.Health.Enabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvHealthAddr); v != "" {
		cfg.Observability.Health.Addr = v
	}
	if v := os.Getenv(EnvAdminEnabled); v != "" {
		cfg.Observability.Admin.Enabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvAdminAddr); v != "" {
		cfg.Observability.Admin.Addr = v
	}

	// Authentication environment variables
	if v := os.Getenv(EnvAuthEnabled); v != "" {
		cfg.Auth.Enabled = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvAuthUserFile); v != "" {
		cfg.Auth.UserFile = v
	}
	if v := os.Getenv(EnvAuthACLFile); v != "" {
		cfg.Auth.ACLFile = v
	}
	if v := os.Getenv(EnvAuthDefaultPublic); v != "" {
		cfg.Auth.DefaultPublic = strings.ToLower(v) == "true" || v == "1"
	}
	if v := os.Getenv(EnvAuthAdminUsername); v != "" {
		cfg.Auth.AdminUsername = v
	}
	if v := os.Getenv(EnvAuthAdminPassword); v != "" {
		cfg.Auth.AdminPassword = v
	}

	m.Set(cfg)
}

// Finalize performs final configuration adjustments after loading.
// This should be called after loading config from file and environment.
func (c *Config) Finalize() {
	// Auto-enable Admin API authentication when global auth is enabled
	if c.Auth.Enabled && !c.Observability.Admin.AuthEnabled {
		c.Observability.Admin.AuthEnabled = true
	}

	// Auto-enable RBAC when auth is enabled (unless explicitly disabled)
	if c.Auth.Enabled && !c.Auth.RBACEnabled {
		c.Auth.RBACEnabled = true
	}
}

// Validate checks configuration validity.
func (c *Config) Validate() error {
	if c.BindAddr == "" {
		return fmt.Errorf("bind_addr is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}

	// Validate TLS configuration
	if c.Security.TLSEnabled {
		if c.Security.TLSCertFile == "" {
			return fmt.Errorf("tls_cert_file is required when TLS is enabled")
		}
		if c.Security.TLSKeyFile == "" {
			return fmt.Errorf("tls_key_file is required when TLS is enabled")
		}
	}

	// Validate encryption configuration
	// SECURITY: Encryption key must ONLY be provided via environment variable
	if c.Security.EncryptionEnabled {
		if c.Security.EncryptionKey == "" {
			return fmt.Errorf("FLYMQ_ENCRYPTION_KEY environment variable is required when encryption is enabled.\n" +
				"  Set it with: export FLYMQ_ENCRYPTION_KEY=<64-char-hex-key>\n" +
				"  Generate a key with: openssl rand -hex 32")
		}
		if len(c.Security.EncryptionKey) != 64 {
			return fmt.Errorf("FLYMQ_ENCRYPTION_KEY must be exactly 64 hex characters (256 bits), got %d characters", len(c.Security.EncryptionKey))
		}
		// Validate hex format
		for i, ch := range c.Security.EncryptionKey {
			if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
				return fmt.Errorf("FLYMQ_ENCRYPTION_KEY contains invalid character '%c' at position %d (must be hex: 0-9, a-f)", ch, i)
			}
		}
	}

	// Validate advanced configuration options

	// Schema validation
	if c.Schema.Enabled {
		validModes := map[string]bool{"strict": true, "lenient": true, "none": true}
		if c.Schema.Validation != "" && !validModes[c.Schema.Validation] {
			return fmt.Errorf("schema.validation must be 'strict', 'lenient', or 'none'")
		}
	}

	// DLQ validation
	if c.DLQ.Enabled {
		if c.DLQ.MaxRetries < 0 {
			return fmt.Errorf("dlq.max_retries must be non-negative")
		}
		if c.DLQ.RetryDelay < 0 {
			return fmt.Errorf("dlq.retry_delay must be non-negative")
		}
	}

	// TTL validation - TTLConfig doesn't have Enabled field, check DefaultTTL
	if c.TTL.DefaultTTL < 0 {
		return fmt.Errorf("ttl.default_ttl must be non-negative")
	}
	if c.TTL.CleanupInterval <= 0 && c.TTL.DefaultTTL > 0 {
		c.TTL.CleanupInterval = 60 // Default to 60 seconds
	}

	// Delayed delivery validation
	if c.Delayed.Enabled {
		if c.Delayed.MaxDelay <= 0 {
			c.Delayed.MaxDelay = 604800 // Default to 7 days in seconds
		}
	}

	// Transaction validation
	if c.Transaction.Enabled {
		if c.Transaction.Timeout <= 0 {
			c.Transaction.Timeout = 60 // Default to 60 seconds
		}
	}

	// Tracing validation
	if c.Observability.Tracing.Enabled {
		if c.Observability.Tracing.SampleRate < 0 || c.Observability.Tracing.SampleRate > 1 {
			return fmt.Errorf("tracing.sample_rate must be between 0 and 1")
		}
	}

	return nil
}

// IsTLSEnabled returns true if TLS is properly configured and enabled.
func (c *Config) IsTLSEnabled() bool {
	return c.Security.TLSEnabled &&
		c.Security.TLSCertFile != "" &&
		c.Security.TLSKeyFile != ""
}

// IsEncryptionEnabled returns true if encryption is properly configured and enabled.
func (c *Config) IsEncryptionEnabled() bool {
	return c.Security.EncryptionEnabled && c.Security.EncryptionKey != ""
}

// IsAuthEnabled returns true if authentication is enabled.
func (c *Config) IsAuthEnabled() bool {
	return c.Auth.Enabled
}

// GetAdvertiseAddr returns the advertise address for client connections.
// If not explicitly set, it returns the bind address.
// If bind address is 0.0.0.0 or ::, it attempts to detect the local IP.
func (c *Config) GetAdvertiseAddr() string {
	if c.AdvertiseAddr != "" {
		return c.AdvertiseAddr
	}
	return resolveAdvertiseAddr(c.BindAddr)
}

// GetAdvertiseCluster returns the advertise address for cluster connections.
// If not explicitly set, it returns the cluster address.
// If cluster address is 0.0.0.0 or ::, it attempts to detect the local IP.
func (c *Config) GetAdvertiseCluster() string {
	if c.AdvertiseCluster != "" {
		return c.AdvertiseCluster
	}
	return resolveAdvertiseAddr(c.ClusterAddr)
}

// resolveAdvertiseAddr resolves an address to an advertisable address.
// If the address binds to all interfaces (0.0.0.0 or ::), it attempts to
// detect the local IP address.
func resolveAdvertiseAddr(addr string) string {
	host, port, err := splitHostPort(addr)
	if err != nil {
		return addr
	}

	// If binding to all interfaces, try to detect local IP
	if host == "" || host == "0.0.0.0" || host == "::" {
		if localIP := detectLocalIP(); localIP != "" {
			return localIP + ":" + port
		}
	}

	return addr
}

// splitHostPort splits an address into host and port.
// Handles addresses like ":9092", "0.0.0.0:9092", "[::]:9092"
func splitHostPort(addr string) (host, port string, err error) {
	// Handle IPv6 addresses
	if strings.HasPrefix(addr, "[") {
		end := strings.Index(addr, "]")
		if end == -1 {
			return "", "", fmt.Errorf("invalid address: %s", addr)
		}
		host = addr[1:end]
		if len(addr) > end+1 && addr[end+1] == ':' {
			port = addr[end+2:]
		}
		return host, port, nil
	}

	// Handle IPv4 addresses and simple port-only addresses
	lastColon := strings.LastIndex(addr, ":")
	if lastColon == -1 {
		return addr, "", nil
	}
	if lastColon == 0 {
		return "", addr[1:], nil
	}
	return addr[:lastColon], addr[lastColon+1:], nil
}

// detectLocalIP attempts to detect the local IP address.
// It prefers non-loopback IPv4 addresses.
func detectLocalIP() string {
	// Try to get the IP by connecting to a known address
	// This doesn't actually make a connection, just determines the route
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer conn.Close()
		if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
			return addr.IP.String()
		}
	}

	// Fallback: iterate through interfaces
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	for _, iface := range interfaces {
		// Skip loopback and down interfaces
		if iface.Flags&net.FlagLoopback != 0 || iface.Flags&net.FlagUp == 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			// Prefer IPv4 non-loopback addresses
			if ip != nil && ip.To4() != nil && !ip.IsLoopback() {
				return ip.String()
			}
		}
	}

	return ""
}

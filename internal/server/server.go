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
Package server implements the FlyMQ TCP server that handles client connections.

ARCHITECTURE OVERVIEW:
======================
The server package is the network-facing component of FlyMQ. It implements a
high-performance TCP server that:

1. Accepts incoming client connections (with optional TLS encryption)
2. Parses the FlyMQ binary protocol messages
3. Routes requests to the appropriate broker handlers
4. Manages connection lifecycle and graceful shutdown

DESIGN PATTERNS USED:
=====================
  - Dependency Injection: The Server receives a Broker interface, allowing for
    easy testing and swapping of broker implementations
  - Interface Segregation: The Broker interface defines only the methods needed
    by the server, not the full broker implementation
  - Concurrent Connection Handling: Each connection is handled in its own goroutine
  - Graceful Shutdown: Uses channels and WaitGroups for clean shutdown

CONNECTION FLOW:
================
1. Client connects to server (TCP or TLS)
2. Server spawns a goroutine to handle the connection
3. Connection handler reads messages in a loop
4. Each message is parsed and routed to the appropriate handler
5. Handler processes request and writes response
6. Loop continues until client disconnects or server shuts down

THREAD SAFETY:
==============
- The server uses sync.RWMutex to protect the running state
- Each connection is handled independently in its own goroutine
- The broker implementation must be thread-safe as it's called concurrently

PERFORMANCE CONSIDERATIONS:
===========================
- Connection handling is non-blocking (one goroutine per connection)
- Read timeouts prevent resource exhaustion from idle connections
- The WaitGroup ensures all connections complete before shutdown

DEVELOPER GUIDE:
================
To add a new operation:
1. Add the opcode to internal/protocol/protocol.go
2. Create request/response structs in this file
3. Add a case to handleMessage() switch statement
4. Implement the handler method (e.g., handleNewOperation)
5. Add corresponding method to the Broker interface if needed
*/
package server

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"flymq/internal/audit"
	"flymq/internal/auth"
	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/crypto"
	"flymq/internal/delayed"
	"flymq/internal/dlq"
	"flymq/internal/logging"
	"flymq/internal/performance"
	"flymq/internal/protocol"
	"flymq/internal/schema"
	"flymq/internal/server/bridge"
	"flymq/internal/server/grpc"
	"flymq/internal/server/ws"
	"flymq/internal/transaction"
	"flymq/internal/ttl"
)

// Broker defines the interface for message broker operations.
// This interface abstracts the broker implementation from the server,
// enabling dependency injection and easier testing.
//
// DESIGN DECISION: Using an interface here follows the Dependency Inversion
// Principle - the server depends on an abstraction, not a concrete implementation.
// This allows us to:
// - Swap broker implementations without changing server code
// - Create mock brokers for testing
// - Implement different broker backends (in-memory, distributed, etc.)
//
// All methods must be thread-safe as they will be called concurrently
// from multiple connection handler goroutines.
type Broker interface {
	// Produce writes a message to a topic and returns the assigned offset.
	// The offset is a monotonically increasing sequence number unique within the topic.
	Produce(topic string, data []byte) (uint64, error)

	// ProduceWithKey writes a message with a key for partition assignment.
	// Messages with the same key are routed to the same partition.
	ProduceWithKey(topic string, key, data []byte) (uint64, error)

	// ProduceWithKeyAndPartition writes a message with a key and returns both offset and partition.
	// This is used when the caller needs to know which partition the message was written to.
	ProduceWithKeyAndPartition(topic string, key, data []byte) (offset uint64, partition int, err error)

	// Consume reads a single message from a topic at the specified offset.
	// Returns the message data or an error if the offset is invalid.
	Consume(topic string, offset uint64) ([]byte, error)

	// ConsumeWithKey reads a single message and its key from a topic at the specified offset.
	// Returns the key, value, or an error if the offset is invalid.
	ConsumeWithKey(topic string, offset uint64) (key []byte, value []byte, err error)

	// ConsumeFromPartitionWithKey reads a message from a specific partition.
	// Returns the key, value, or an error if the offset is invalid.
	ConsumeFromPartitionWithKey(topic string, partition int, offset uint64) (key []byte, value []byte, err error)

	// CreateTopic creates a new topic with the specified number of partitions.
	// Partitions enable parallel consumption and horizontal scaling.
	CreateTopic(topic string, partitions int) error

	// DeleteTopic removes a topic and all its data.
	// This operation is irreversible and should be used with caution.
	DeleteTopic(name string) error

	// GetTopicMetadata returns metadata about a topic (partitions, offsets, etc.)
	// Returns *broker.TopicMetadata with partition details.
	GetTopicMetadata(topic string) (interface{}, error)

	// Subscribe registers a consumer group for a topic partition.
	// The mode determines where to start consuming (earliest, latest, or committed offset).
	// Returns the starting offset for consumption.
	Subscribe(topic, groupID string, partition int, mode protocol.SubscribeMode) (uint64, error)

	// CommitOffset persists the consumer's progress for a topic partition.
	// This enables resuming consumption from the last committed position.
	// Returns (changed, error) where changed indicates if the offset was actually updated.
	CommitOffset(topic, groupID string, partition int, offset uint64) (bool, error)

	// ListConsumerGroups returns all consumer groups.
	ListConsumerGroups() []*broker.ConsumerGroupInfo

	// GetConsumerGroup returns info about a specific consumer group.
	GetConsumerGroup(topic, groupID string) (*broker.ConsumerGroupInfo, error)

	// DeleteConsumerGroup removes a consumer group.
	DeleteConsumerGroup(topic, groupID string) error

	// GetTopicInfo returns lowest/highest offsets per partition.
	GetTopicInfo(topic string) (*broker.TopicInfo, error)

	// GetTopicMessageCount returns the total number of messages in a topic.
	GetTopicMessageCount(topic string) (int64, error)

	// Fetch retrieves multiple messages starting from the given offset.
	// Returns the messages, the next offset to fetch, and any error.
	// This is more efficient than calling Consume repeatedly.
	Fetch(topic string, partition int, offset uint64, maxMessages int) ([][]byte, uint64, error)

	// FetchWithKeys retrieves multiple messages with their keys starting from the given offset.
	// Returns FetchedMessage structs containing key, value, and offset.
	FetchWithKeys(topic string, partition int, offset uint64, maxMessages int, filter string) ([]broker.FetchedMessage, uint64, error)

	// ListTopics returns the names of all topics in the broker.
	ListTopics() []string

	// RegisterSchema registers a schema in the cluster.
	// In cluster mode, routes through Raft consensus for replication.
	RegisterSchema(name, schemaType, definition, compatibility string) error

	// DeleteSchema removes a schema version from the cluster.
	// In cluster mode, routes through Raft consensus for replication.
	DeleteSchema(name string, version int) error

	// IsClusterMode returns true if the broker is running in cluster mode.
	IsClusterMode() bool

	// ProposeCreateUser proposes a user creation through the cluster.
	// The password should already be hashed before calling this method.
	ProposeCreateUser(username, passwordHash string, roles []string) error

	// ProposeDeleteUser proposes a user deletion through the cluster.
	ProposeDeleteUser(username string) error

	// ProposeUpdateUser proposes a user update through the cluster.
	// The password should already be hashed before calling this method.
	ProposeUpdateUser(username string, roles []string, enabled *bool, passwordHash string) error

	// ProposeSetACL proposes an ACL setting through the cluster.
	ProposeSetACL(topic string, public bool, allowedUsers, allowedRoles []string) error

	// ProposeDeleteACL proposes an ACL deletion through the cluster.
	ProposeDeleteACL(topic string) error

	// GetZeroCopyInfo returns file and position info for zero-copy reads.
	// This allows using sendfile() to transfer data directly to network.
	GetZeroCopyInfo(topic string, partition int, offset uint64) (*os.File, int64, int64, error)

	// GetClusterMetadata returns partition-to-node mappings for smart client routing.
	// If topic is empty, returns metadata for all topics.
	GetClusterMetadata(topic string) (*protocol.BinaryClusterMetadataResponse, error)
}

// Server is the main TCP server that handles client connections.
// It implements a concurrent connection model where each client connection
// is handled in its own goroutine.
//
// ARCHITECTURE:
// - Uses a single listener goroutine (acceptLoop) to accept connections
// - Spawns a handler goroutine for each connection (handleConn)
// - Uses channels for graceful shutdown coordination
// - Supports both plain TCP and TLS connections
//
// LIFECYCLE:
// 1. NewServer() - Creates server instance with configuration
// 2. Start() - Begins listening and accepting connections
// 3. (handles connections until...)
// 4. Stop() - Gracefully shuts down, waiting for connections to complete
type Server struct {
	// config holds the server configuration (bind address, TLS settings, etc.)
	config *config.Config

	// broker is the message broker that handles all message operations.
	// This is injected at construction time for flexibility and testability.
	broker Broker

	// logger is the primary logger for server-level events
	logger *logging.Logger

	// Specialized loggers for different aspects of server operation.
	// Using specialized loggers enables fine-grained log filtering and
	// makes it easier to analyze specific types of events.
	connLogger     *logging.ConnectionLogger  // Connection lifecycle events
	msgLogger      *logging.MessageLogger     // Message produce/consume events
	topicLogger    *logging.TopicLogger       // Topic management events
	consumerLogger *logging.ConsumerLogger    // Consumer group events
	securityLogger *logging.SecurityLogger    // Security-related events
	perfLogger     *logging.PerformanceLogger // Performance metrics
	errorLogger    *logging.ErrorLogger       // Error tracking

	// ln is the network listener (TCP or TLS)
	ln net.Listener

	// stopCh is closed to signal all goroutines to stop.
	// This is the primary shutdown coordination mechanism.
	stopCh chan struct{}

	// wg tracks active connection handlers for graceful shutdown.
	// Stop() waits on this to ensure all connections complete.
	wg sync.WaitGroup

	// mu protects the running state from concurrent access
	mu sync.RWMutex

	// running indicates whether the server is accepting connections
	running bool

	// tlsConfig holds TLS configuration when TLS is enabled
	tlsConfig *tls.Config

	// schemaRegistry manages message schemas for validation
	schemaRegistry *schema.Registry

	// schemaValidator validates messages against registered schemas
	schemaValidator *schema.Validator

	// authorizer handles authentication and authorization
	authorizer *auth.Authorizer

	// connAuth tracks authenticated users per connection
	// Maps net.Conn to username string
	connAuth sync.Map

	// connConsumerGroup tracks the consumer group for each connection
	// Maps net.Conn to consumer group string
	connConsumerGroup sync.Map

	// connStartTime tracks when each connection was established.
	// Used for connection duration metrics and debugging.
	// sync.Map is used for concurrent access without explicit locking.
	connStartTime sync.Map

	// asyncIO provides async I/O operations for high-throughput scenarios
	asyncIO *performance.AsyncIOManager

	// bufferPool provides pooled buffers to reduce allocations
	bufferPool *performance.BufferPool

	// pipeline provides high-performance message processing with compression and zero-copy
	pipeline *performance.MessagePipeline

	// delayedManager handles delayed message delivery
	delayedManager *delayed.Manager

	// ttlManager handles message TTL and expiration
	ttlManager *ttl.Manager

	// dlqManager handles dead letter queue operations
	dlqManager *dlq.Manager

	// txnCoordinator handles transaction coordination
	txnCoordinator *transaction.Coordinator

	// auditStore handles audit trail logging
	auditStore audit.Store

	// grpcServer is the gRPC server instance
	grpcServer GRPCServer

	// mqttBridge is the MQTT bridge instance
	mqttBridge *bridge.MQTTBridge

	// wsGateway is the WebSocket gateway instance
	wsGateway *ws.Gateway
}

// GRPCServer defines the interface for the gRPC server to avoid circular dependencies
type GRPCServer interface {
	Start() error
	Stop()
}

// brokerMessageStore adapts the Broker interface to delayed.MessageStore.
type brokerMessageStore struct {
	broker Broker
}

func (s *brokerMessageStore) Produce(topic string, data []byte) (uint64, error) {
	return s.broker.Produce(topic, data)
}

// brokerDLQStore adapts the Broker interface to dlq.MessageStore.
type brokerDLQStore struct {
	broker Broker
}

func (s *brokerDLQStore) Produce(topic string, data []byte) (uint64, error) {
	return s.broker.Produce(topic, data)
}

func (s *brokerDLQStore) Fetch(topic string, partition int, offset uint64, maxMessages int) ([][]byte, uint64, error) {
	return s.broker.Fetch(topic, partition, offset, maxMessages)
}

// brokerTxnStore adapts the Broker interface to transaction.MessageStore.
type brokerTxnStore struct {
	broker Broker
}

func (s *brokerTxnStore) Produce(topic string, data []byte) (uint64, error) {
	return s.broker.Produce(topic, data)
}

func (s *brokerTxnStore) ProduceToPartition(topic string, partition int, data []byte) (uint64, error) {
	// For now, use the default produce which handles partition selection
	return s.broker.Produce(topic, data)
}

func (s *brokerTxnStore) CommitOffset(topic string, partition int, consumerGroup string, offset uint64) error {
	_, err := s.broker.CommitOffset(topic, consumerGroup, partition, offset)
	return err
}

// brokerTTLStore adapts the Broker interface to ttl.MessageStore.
// Note: The broker doesn't currently support message deletion, so this is a no-op store
// that allows TTL tracking but doesn't actually delete expired messages.
type brokerTTLStore struct {
	broker Broker
	logger *logging.Logger
}

func (s *brokerTTLStore) DeleteMessage(topic string, partition int, offset uint64) error {
	// The broker doesn't support message deletion yet
	// Log the deletion request for debugging
	s.logger.Debug("TTL message deletion requested (not implemented)", "topic", topic, "partition", partition, "offset", offset)
	return nil
}

func (s *brokerTTLStore) GetMessageMetadata(topic string, partition int, offset uint64) (*ttl.MessageMetadata, error) {
	// Return a placeholder metadata - actual implementation would need broker support
	return &ttl.MessageMetadata{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}, nil
}

func (s *brokerTTLStore) ListMessages(topic string, partition int, startOffset, endOffset uint64) ([]uint64, error) {
	// Return empty list - actual implementation would need broker support
	return nil, nil
}

// NewServer creates a new Server instance with the given configuration and broker.
//
// INITIALIZATION SEQUENCE:
// 1. Sets up schema registry for message validation (optional feature)
// 2. Creates specialized loggers for different event categories
// 3. Initializes the stop channel for shutdown coordination
//
// DESIGN NOTES:
//   - Schema registry initialization is non-fatal; if it fails, schema validation
//     is simply disabled. This follows the "fail soft" principle for optional features.
//   - Each logger category gets its own logger instance for fine-grained control
//   - The stopCh is created unbuffered; closing it broadcasts to all listeners
//
// PARAMETERS:
// - cfg: Server configuration including bind address, TLS settings, etc.
// - broker: The message broker implementation (must be thread-safe)
//
// RETURNS:
// - A configured Server instance ready to be started with Start()
func NewServer(cfg *config.Config, broker Broker) *Server {
	// Initialize schema registry with data directory.
	// The schema registry stores message schemas for validation.
	// If DataDir is empty, schemas will be stored in memory only.
	schemaDir := ""
	if cfg.DataDir != "" {
		schemaDir = cfg.DataDir + "/schemas"
	}
	registry, err := schema.NewRegistry(schemaDir)
	if err != nil {
		// DESIGN DECISION: Log and continue rather than failing.
		// Schema validation is an optional feature, so we degrade gracefully.
		logging.NewLogger("server").Warn("Failed to initialize schema registry", "error", err)
		registry = nil
	}

	// Create validator only if registry was successfully initialized
	var validator *schema.Validator
	if registry != nil {
		validator = schema.NewValidator(registry)
	}

	// Initialize authentication if enabled
	var authorizer *auth.Authorizer
	if cfg.Auth.Enabled {
		userStore := auth.NewUserStore(cfg.Auth.UserFile)
		if err := userStore.Load(); err != nil {
			logging.NewLogger("server").Warn("Failed to load user store", "error", err)
		}

		// Create default admin user if configured and doesn't exist
		if cfg.Auth.AdminUsername != "" && cfg.Auth.AdminPassword != "" {
			if _, exists := userStore.GetUser(cfg.Auth.AdminUsername); !exists {
				if err := userStore.CreateUser(cfg.Auth.AdminUsername, cfg.Auth.AdminPassword, []string{"admin"}); err != nil {
					logging.NewLogger("server").Error("Failed to create admin user", "error", err)
				} else {
					logging.NewLogger("server").Info("Created default admin user",
						"username", cfg.Auth.AdminUsername,
						"roles", []string{"admin"})
					// Save the user store to persist the admin user
					if err := userStore.Save(); err != nil {
						logging.NewLogger("server").Warn("Failed to save user store", "error", err)
					}
				}
			}
		}

		aclStore := auth.NewACLStore(cfg.Auth.ACLFile)
		aclStore.DefaultPublic = cfg.Auth.DefaultPublic
		if err := aclStore.Load(); err != nil {
			logging.NewLogger("server").Warn("Failed to load ACL store", "error", err)
		}

		// Create authorizer with RBAC enabled (RBAC is always enabled when auth is enabled)
		authorizer = auth.NewAuthorizer(userStore, aclStore, true)
		authorizer.SetAllowAnonymous(cfg.Auth.AllowAnonymous)

		logging.NewLogger("server").Info("Authentication enabled",
			"rbac_enabled", cfg.Auth.RBACEnabled,
			"allow_anonymous", cfg.Auth.AllowAnonymous,
			"default_public", cfg.Auth.DefaultPublic)
	} else {
		// Create a disabled authorizer for consistent API
		authorizer = auth.NewAuthorizer(nil, nil, false)
	}

	// Create specialized loggers for different aspects of server operation.
	// This enables operators to filter logs by category and set different
	// log levels for different concerns (e.g., verbose message logging,
	// minimal connection logging).
	baseLogger := logging.NewLogger("server")

	// Initialize async I/O manager if enabled
	var asyncIO *performance.AsyncIOManager
	if cfg.Performance.AsyncIO {
		asyncIOConfig := performance.AsyncIOConfig{
			NumWorkers:    cfg.Performance.NumIOWorkers,
			QueueSize:     10000,
			BatchSize:     100,
			FlushInterval: time.Duration(cfg.Performance.SyncIntervalMs) * time.Millisecond,
		}
		asyncIO = performance.NewAsyncIOManager(asyncIOConfig)
		asyncIO.Start()
		baseLogger.Info("Async I/O manager started", "workers", asyncIOConfig.NumWorkers)
	}

	// Initialize buffer pool for zero-copy operations
	var bufferPool *performance.BufferPool
	if cfg.Performance.ZeroCopy {
		bufferPool = performance.NewBufferPool(64 * 1024) // 64KB buffers
		baseLogger.Info("Zero-copy buffer pool initialized")
	}

	// Initialize message pipeline with compression and zero-copy
	pipelineConfig := performance.DefaultPipelineConfig()
	switch cfg.Performance.Compression {
	case "lz4":
		pipelineConfig.CompressionType = performance.CompressionLZ4
	case "snappy":
		pipelineConfig.CompressionType = performance.CompressionSnappy
	case "zstd":
		pipelineConfig.CompressionType = performance.CompressionZstd
	case "gzip":
		pipelineConfig.CompressionType = performance.CompressionGzip
	default:
		pipelineConfig.CompressionType = performance.CompressionNone
	}
	pipelineConfig.CompressionLevel = cfg.Performance.CompressionLevel
	pipelineConfig.CompressionMinSize = cfg.Performance.CompressionMinSize
	pipelineConfig.LargeMessageThreshold = cfg.Performance.LargeMessageThreshold
	pipelineConfig.ZeroCopyEnabled = cfg.Performance.ZeroCopy
	pipeline := performance.NewMessagePipeline(pipelineConfig)
	baseLogger.Info("Message pipeline initialized",
		"compression", cfg.Performance.Compression,
		"compression_level", cfg.Performance.CompressionLevel,
		"zero_copy", cfg.Performance.ZeroCopy,
	)

	// Initialize delayed message manager if enabled
	var delayedManager *delayed.Manager
	if cfg.Delayed.Enabled {
		delayedManager = delayed.NewManager(&cfg.Delayed, &brokerMessageStore{broker})
		delayedManager.Start()
		baseLogger.Info("Delayed message manager initialized")
	}

	// Initialize TTL manager if TTL is configured
	var ttlManager *ttl.Manager
	if cfg.TTL.DefaultTTL > 0 || cfg.TTL.CleanupInterval > 0 {
		ttlManager = ttl.NewManager(&cfg.TTL, &brokerTTLStore{broker: broker, logger: baseLogger})
		ttlManager.Start()
		baseLogger.Info("TTL manager initialized", "default_ttl", cfg.TTL.DefaultTTL, "cleanup_interval", cfg.TTL.CleanupInterval)
	}

	// Initialize DLQ manager if enabled
	var dlqManager *dlq.Manager
	if cfg.DLQ.Enabled {
		dlqManager = dlq.NewManager(&cfg.DLQ, &brokerDLQStore{broker})
		baseLogger.Info("DLQ manager initialized")
	}

	// Initialize transaction coordinator if enabled
	var txnCoordinator *transaction.Coordinator
	if cfg.Transaction.Enabled {
		txnCoordinator = transaction.NewCoordinator(&cfg.Transaction, &brokerTxnStore{broker})
		txnCoordinator.Start()
		baseLogger.Info("Transaction coordinator initialized")
	}

	// Initialize audit store for audit trail logging
	var auditStore audit.Store
	if cfg.Audit.Enabled {
		auditDir := cfg.DataDir + "/audit"
		if cfg.Audit.LogDir != "" {
			auditDir = cfg.Audit.LogDir
		}
		store, err := audit.NewFileStore(audit.FileStoreConfig{
			Dir:           auditDir,
			NodeID:        cfg.NodeID,
			MaxFileSize:   cfg.Audit.MaxFileSize,
			RetentionDays: cfg.Audit.RetentionDays,
		})
		if err != nil {
			baseLogger.Warn("Failed to initialize audit store", "error", err)
		} else {
			auditStore = store
			baseLogger.Info("Audit trail enabled", "dir", auditDir)
		}
	}

	s := &Server{
		config:         cfg,
		broker:         broker,
		logger:         baseLogger,
		connLogger:     logging.NewConnectionLogger(logging.NewLogger("connection")),
		msgLogger:      logging.NewMessageLogger(logging.NewLogger("message")),
		topicLogger:    logging.NewTopicLogger(logging.NewLogger("topic")),
		consumerLogger: logging.NewConsumerLogger(logging.NewLogger("consumer")),
		securityLogger: logging.NewSecurityLogger(logging.NewLogger("security")),
		perfLogger:     logging.NewPerformanceLogger(logging.NewLogger("performance")),
		errorLogger:    logging.NewErrorLogger(logging.NewLogger("error")),
		// Create unbuffered channel - closing broadcasts to all receivers
		stopCh:          make(chan struct{}),
		schemaRegistry:  registry,
		schemaValidator: validator,
		authorizer:      authorizer,
		asyncIO:         asyncIO,
		bufferPool:      bufferPool,
		pipeline:        pipeline,
		delayedManager:  delayedManager,
		ttlManager:      ttlManager,
		dlqManager:      dlqManager,
		txnCoordinator:  txnCoordinator,
		auditStore:      auditStore,
	}

	if cfg.GRPC.Enabled {
		// Pass the broker and authorizer
		s.grpcServer = grpc.NewServer(cfg, broker, authorizer, baseLogger)
	}

	// Initialize MQTT bridge (always initialized if needed, but only started if configured)
	// For now we just create it
	s.mqttBridge = bridge.NewMQTTBridge(cfg, broker, authorizer, baseLogger)

	// Initialize WebSocket gateway
	s.wsGateway = ws.NewGateway(cfg, broker, authorizer, baseLogger)

	return s
}

// Start begins accepting client connections on the configured address.
//
// STARTUP SEQUENCE:
// 1. Configure TLS if enabled in configuration
// 2. Create TCP listener (with or without TLS)
// 3. Mark server as running
// 4. Start accept loop in background goroutine
//
// TLS CONFIGURATION:
// When TLS is enabled, the server uses mutual TLS (mTLS) if a CA file is
// provided. This means both server and client must present valid certificates.
// This is the recommended configuration for production deployments.
//
// CONCURRENCY:
// The accept loop runs in its own goroutine, so Start() returns immediately
// after the listener is created. Use Stop() to shut down the server.
//
// RETURNS:
// - nil on success
// - error if listener creation fails or TLS configuration is invalid
func (s *Server) Start() error {
	var ln net.Listener
	var err error

	// Configure TLS if enabled in configuration.
	// TLS provides encryption and optionally mutual authentication.
	if s.config.IsTLSEnabled() {
		tlsCfg, err := crypto.NewServerTLSConfig(crypto.TLSConfig{
			CertFile: s.config.Security.TLSCertFile, // Server certificate
			KeyFile:  s.config.Security.TLSKeyFile,  // Server private key
			CAFile:   s.config.Security.TLSCAFile,   // CA for client verification (mTLS)
		})
		if err != nil {
			return fmt.Errorf("failed to configure TLS: %w", err)
		}
		s.tlsConfig = tlsCfg
		// tls.Listen wraps net.Listen with TLS handshake
		ln, err = tls.Listen("tcp", s.config.BindAddr, tlsCfg)
		if err != nil {
			return err
		}
		s.logger.Info("Server started with TLS", "addr", s.config.BindAddr)
	} else {
		// Plain TCP - suitable for development or when TLS termination
		// is handled by a load balancer/proxy
		ln, err = net.Listen("tcp", s.config.BindAddr)
		if err != nil {
			return err
		}
		s.logger.Info("Server started", "addr", s.config.BindAddr)
	}

	s.ln = ln

	// Mark server as running under lock to prevent race conditions
	// with concurrent Stop() calls
	s.mu.Lock()
	s.running = true
	s.mu.Unlock()

	// Start accept loop in background goroutine.
	// This allows Start() to return immediately while the server
	// continues accepting connections.
	go s.acceptLoop()

	if s.config.GRPC.Enabled && s.grpcServer != nil {
		s.logger.Info("Starting gRPC server", "addr", s.config.GRPC.Addr)
		if err := s.grpcServer.Start(); err != nil {
			s.logger.Error("Failed to start gRPC server", "error", err)
		}
	}

	// Start MQTT bridge (simplified activation for now)
	if s.config.MQTT.Enabled && s.mqttBridge != nil {
		go func() {
			if err := s.mqttBridge.Start(); err != nil {
				s.logger.Error("Failed to start MQTT bridge", "error", err)
			}
		}()
	}

	// Start WebSocket gateway
	if s.config.WS.Enabled && s.wsGateway != nil {
		go func() {
			if err := s.wsGateway.Start(); err != nil {
				s.logger.Error("Failed to start WebSocket gateway", "error", err)
			}
		}()
	}

	return nil
}

// IsTLS returns true if the server is using TLS.
// This is useful for logging and for clients that need to know the connection type.
func (s *Server) IsTLS() bool {
	return s.tlsConfig != nil
}

// GetSchemaRegistry returns the server's schema registry.
// This can be used to wire the schema registry to the broker for cluster replication.
func (s *Server) GetSchemaRegistry() *schema.Registry {
	return s.schemaRegistry
}

// GetAuthorizer returns the server's authorizer.
// This can be used to wire the authorizer to the cluster for user/ACL replication.
func (s *Server) GetAuthorizer() *auth.Authorizer {
	return s.authorizer
}

// GetAuditStore returns the server's audit store.
// This can be used to wire the audit store to the admin handler for REST API access.
func (s *Server) GetAuditStore() audit.Store {
	return s.auditStore
}

// Stop gracefully shuts down the server.
//
// SHUTDOWN SEQUENCE:
// 1. Acquire lock and check if already stopped (idempotent)
// 2. Mark server as not running
// 3. Close stopCh to signal all goroutines to stop
// 4. Close listener to unblock Accept() call
// 5. Release lock
// 6. Wait for all connection handlers to complete
//
// GRACEFUL SHUTDOWN:
// The server waits for all active connections to complete their current
// operations before returning. This ensures no messages are lost during
// shutdown. The timeout for this wait is controlled by the connection
// handlers' read timeouts.
//
// THREAD SAFETY:
// This method is safe to call from multiple goroutines. The first call
// initiates shutdown; subsequent calls return immediately.
func (s *Server) Stop() error {
	s.mu.Lock()
	if !s.running {
		// Already stopped - this is idempotent
		s.mu.Unlock()
		return nil
	}
	s.running = false

	// Close stopCh to broadcast shutdown signal to all goroutines.
	// This is a common Go pattern: closing a channel causes all
	// receivers to immediately receive the zero value.
	close(s.stopCh)

	// Close listener to unblock the Accept() call in acceptLoop.
	// This will cause Accept() to return an error, which we handle
	// by checking stopCh.
	s.ln.Close()
	s.mu.Unlock()

	// Wait for all connection handlers to complete.
	// This ensures graceful shutdown - no connections are forcibly terminated.
	s.wg.Wait()

	// Stop async I/O manager if it was started
	if s.asyncIO != nil {
		s.asyncIO.Stop()
	}

	// Stop delayed message manager if it was started
	if s.delayedManager != nil {
		s.delayedManager.Stop()
		s.logger.Debug("Delayed message manager stopped")
	}

	// Stop TTL manager if it was started
	if s.ttlManager != nil {
		s.ttlManager.Stop()
		s.logger.Debug("TTL manager stopped")
	}

	// Stop transaction coordinator if it was started
	if s.txnCoordinator != nil {
		s.txnCoordinator.Stop()
		s.logger.Debug("Transaction coordinator stopped")
	}

	// Close audit store if it was started
	if s.auditStore != nil {
		s.auditStore.Close()
		s.logger.Debug("Audit store closed")
	}

	// Stop gRPC server if it was started
	if s.grpcServer != nil {
		s.grpcServer.Stop()
		s.logger.Debug("gRPC server stopped")
	}

	// Stop WebSocket gateway if it was started
	if s.wsGateway != nil {
		s.wsGateway.Stop()
		s.logger.Debug("WebSocket gateway stopped")
	}

	// Stop MQTT bridge if it was started
	if s.mqttBridge != nil {
		s.mqttBridge.Stop()
		s.logger.Debug("MQTT bridge stopped")
	}

	s.logger.Info("Server stopped")
	return nil
}

// recordAudit records an audit event if audit logging is enabled.
// This is a helper method that handles nil checks and error logging.
func (s *Server) recordAudit(eventType audit.EventType, user, clientIP, resource, action, result string, details map[string]string) {
	if s.auditStore == nil {
		return
	}

	event := &audit.Event{
		Timestamp: time.Now().UTC(),
		Type:      eventType,
		User:      user,
		ClientIP:  clientIP,
		Resource:  resource,
		Action:    action,
		Result:    result,
		Details:   details,
	}

	if err := s.auditStore.Record(event); err != nil {
		s.logger.Warn("Failed to record audit event", "error", err, "type", eventType)
	}
}

// getClientIP extracts the client IP address from a connection.
func getClientIP(conn net.Conn) string {
	if conn == nil {
		return ""
	}
	addr := conn.RemoteAddr()
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// acceptLoop is the main loop that accepts incoming connections.
// It runs in its own goroutine, started by Start().
//
// DESIGN PATTERN:
// This implements the "accept loop" pattern common in network servers:
// - Single goroutine handles all Accept() calls
// - Each accepted connection spawns its own handler goroutine
// - This provides natural concurrency without explicit thread pools
//
// ERROR HANDLING:
// - If Accept() fails due to shutdown (stopCh closed), exit cleanly
// - If Accept() fails for other reasons, log and continue
// - This makes the server resilient to transient network errors
//
// CONCURRENCY:
//   - wg.Add(1) is called BEFORE spawning the goroutine to prevent
//     race conditions with Stop() waiting on the WaitGroup
func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			// Check if we're shutting down
			select {
			case <-s.stopCh:
				// Normal shutdown - exit cleanly
				return
			default:
				// Transient error - log and continue accepting
				s.logger.Error("Accept error", "error", err)
				continue
			}
		}
		// Add to WaitGroup BEFORE spawning goroutine to prevent race
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// handleConn handles a single client connection.
// It runs in its own goroutine, one per connection.
//
// CONNECTION LIFECYCLE:
// 1. Record connection start time for metrics
// 2. Log connection establishment
// 3. Enter message processing loop
// 4. On exit: log disconnection with duration
//
// MESSAGE LOOP:
// The handler reads messages in a loop until:
// - Server shutdown (stopCh closed)
// - Client disconnects (EOF)
// - Read error occurs
// - Read timeout expires (5 minute idle timeout)
//
// IDLE TIMEOUT:
// The 5-minute read deadline prevents resource exhaustion from
// idle connections. Clients should send periodic heartbeats or
// reconnect if they need longer idle periods.
//
// ERROR HANDLING:
// - EOF is expected (client disconnect) - exit silently
// - Other read errors are logged
// - Handler errors are logged and sent to client as error response
func (s *Server) handleConn(conn net.Conn) {
	// Ensure WaitGroup is decremented and connection is closed on exit
	defer s.wg.Done()
	defer conn.Close()

	// Apply TCP optimizations for low latency and high throughput
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Disable Nagle's algorithm for lower latency
		// This sends data immediately rather than waiting to batch small writes
		tcpConn.SetNoDelay(true)

		// Enable TCP keepalive to detect dead connections
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)

		// Increase socket buffer sizes for higher throughput (4MB each)
		tcpConn.SetReadBuffer(4 * 1024 * 1024)
		tcpConn.SetWriteBuffer(4 * 1024 * 1024)
	}

	// Track connection lifetime for metrics and debugging
	connStart := time.Now()
	s.connStartTime.Store(conn, connStart)
	defer s.connStartTime.Delete(conn)

	// Log new connection with TLS status
	s.connLogger.LogNewConnection(conn, s.IsTLS())

	// Ensure connection close is logged with duration
	defer func() {
		duration := time.Since(connStart)
		s.connLogger.LogConnectionClosed(conn, "client_disconnect", duration)
	}()

	// Main message processing loop
	for {
		// Check for shutdown signal using non-blocking select.
		// This pattern allows us to exit promptly on shutdown
		// without waiting for the next message.
		select {
		case <-s.stopCh:
			return
		default:
			// Continue processing
		}

		// Set read deadline to prevent idle connections from
		// consuming resources indefinitely. The deadline is
		// reset on each iteration, so active connections stay open.
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		// Read and parse the next message from the wire
		msg, err := protocol.ReadMessage(conn)
		if err != nil {
			if err != io.EOF {
				// EOF is expected when client disconnects cleanly
				s.logger.Error("Read error", "error", err, "addr", conn.RemoteAddr())
			}
			return
		}

		// Route message to appropriate handler
		if err := s.handleMessage(conn, msg); err != nil {
			s.logger.Error("Handle error", "error", err)
			// Send error response to client so they know what went wrong
			protocol.WriteError(conn, err)
		}
	}
}

// handleMessage routes incoming messages to the appropriate handler based on opcode.
//
// DESIGN PATTERN: Command Pattern
// Each opcode represents a command that the server can execute. The switch
// statement acts as a command dispatcher, routing each command to its handler.
//
// OPERATION CATEGORIES:
// 1. Core Operations: produce, consume, create/delete topic, subscribe, commit, fetch
// 2. Advanced Operations: delayed messages, TTL, schema validation
// 3. DLQ Operations: get, replay, purge dead letter queue messages
// 4. Transaction Operations: begin, commit, abort, transactional produce
//
// EXTENSIBILITY:
// To add a new operation:
// 1. Define the opcode in protocol.go
// 2. Add a case to this switch statement
// 3. Implement the handler method
//
// ERROR HANDLING:
// Handlers return errors which are sent to the client as error responses.
// Unknown opcodes return an error rather than silently failing.
func (s *Server) handleMessage(conn io.Writer, msg *protocol.Message) error {
	payload := msg.Payload
	var err error

	// Decompress payload if compression flag is set
	if msg.Header.Flags&0x07 != 0 || msg.Header.Flags&protocol.FlagCompressed != 0 {
		payload, err = protocol.DecompressPayload(msg.Payload, msg.Header.Flags)
		if err != nil {
			s.errorLogger.LogError(err, "decompress", nil)
			return err
		}
	}

	switch msg.Header.Op {
	// ========== Core Message Operations ==========
	case protocol.OpProduce:
		return s.handleProduce(conn, payload, msg.Header.Flags)
	case protocol.OpConsume:
		return s.handleConsume(conn, payload, msg.Header.Flags)
	case protocol.OpConsumeZeroCopy:
		return s.handleConsumeZeroCopy(conn, payload)
	case protocol.OpFetch:
		return s.handleFetch(conn, payload, msg.Header.Flags)
	case protocol.OpFetchBinary:
		return s.handleFetchBinary(conn, payload)

	// ========== Topic Management ==========
	case protocol.OpCreateTopic:
		return s.handleCreateTopic(conn, payload, msg.Header.Flags)
	case protocol.OpDeleteTopic:
		return s.handleDeleteTopic(conn, payload, msg.Header.Flags)
	case protocol.OpListTopics:
		return s.handleListTopics(conn)

	// ========== Consumer Group Operations ==========
	case protocol.OpSubscribe:
		return s.handleSubscribe(conn, payload, msg.Header.Flags)
	case protocol.OpCommit:
		return s.handleCommit(conn, payload, msg.Header.Flags)
	case protocol.OpGetOffset:
		return s.handleGetOffset(conn, payload)
	case protocol.OpResetOffset:
		return s.handleResetOffset(conn, payload)
	case protocol.OpListGroups:
		return s.handleListGroups(conn)
	case protocol.OpDescribeGroup:
		return s.handleDescribeGroup(conn, payload)
	case protocol.OpGetLag:
		return s.handleGetLag(conn, payload)
	case protocol.OpDeleteGroup:
		return s.handleDeleteGroup(conn, payload)

	// ========== Advanced Message Operations ==========
	case protocol.OpProduceDelayed:
		return s.handleProduceDelayed(conn, payload)
	case protocol.OpProduceWithTTL:
		return s.handleProduceWithTTL(conn, payload)
	case protocol.OpProduceWithSchema:
		return s.handleProduceWithSchema(conn, payload)

	// ========== Schema Registry Operations ==========
	case protocol.OpRegisterSchema:
		return s.handleRegisterSchema(conn, payload)
	case protocol.OpListSchemas:
		return s.handleListSchemas(conn, payload)
	case protocol.OpValidateSchema:
		return s.handleValidateSchema(conn, payload)
	case protocol.OpGetSchema:
		return s.handleGetSchema(conn, payload)
	case protocol.OpDeleteSchema:
		return s.handleDeleteSchema(conn, payload)

	// ========== Dead Letter Queue Operations ==========
	case protocol.OpGetDLQMessages:
		return s.handleGetDLQMessages(conn, payload)
	case protocol.OpReplayDLQ:
		return s.handleReplayDLQ(conn, payload)
	case protocol.OpPurgeDLQ:
		return s.handlePurgeDLQ(conn, payload)

	// ========== Transaction Operations ==========
	case protocol.OpBeginTx:
		return s.handleBeginTx(conn, payload)
	case protocol.OpCommitTx:
		return s.handleCommitTx(conn, payload)
	case protocol.OpAbortTx:
		return s.handleAbortTx(conn, payload)
	case protocol.OpProduceTx:
		return s.handleProduceTx(conn, payload)

	// ========== Authentication Operations ==========
	case protocol.OpAuth:
		return s.handleAuth(conn, payload)
	case protocol.OpWhoAmI:
		return s.handleWhoAmI(conn)

	// ========== User Management Operations ==========
	case protocol.OpUserCreate:
		return s.handleUserCreate(conn, payload)
	case protocol.OpUserDelete:
		return s.handleUserDelete(conn, payload)
	case protocol.OpUserUpdate:
		return s.handleUserUpdate(conn, payload)
	case protocol.OpUserList:
		return s.handleUserList(conn)
	case protocol.OpUserGet:
		return s.handleUserGet(conn, payload)
	case protocol.OpACLSet:
		return s.handleACLSet(conn, payload)
	case protocol.OpACLGet:
		return s.handleACLGet(conn, payload)
	case protocol.OpACLDelete:
		return s.handleACLDelete(conn, payload)
	case protocol.OpACLList:
		return s.handleACLList(conn)
	case protocol.OpPasswordChange:
		return s.handlePasswordChange(conn, payload)
	case protocol.OpRoleList:
		return s.handleRoleList(conn)

	// ========== Cluster Operations ==========
	case protocol.OpClusterMetadata:
		return s.handleClusterMetadata(conn, payload)

	// ========== Audit Trail Operations ==========
	case protocol.OpAuditQuery:
		return s.handleAuditQuery(conn, payload)
	case protocol.OpAuditExport:
		return s.handleAuditExport(conn, payload)

	default:
		return fmt.Errorf("unknown opcode: %d", msg.Header.Op)
	}
}

// ============================================================================
// Request/Response Types for Core Operations
// ============================================================================
// NOTE: These structs are LEGACY and NOT used for the binary wire protocol.
// The actual wire protocol uses binary encoding via protocol.DecodeBinary*
// and protocol.EncodeBinary* functions in internal/protocol/binary.go.
//
// These structs with json tags are retained for:
// - HTTP admin API endpoints (if implemented)
// - Documentation and reference
// - Internal Go struct serialization where needed
//
// For the binary wire protocol specification, see docs/protocol.md.
// ============================================================================

// ProduceRequest contains the parameters for producing a message to a topic.
type ProduceRequest struct {
	Topic string `json:"topic"`         // Target topic name
	Key   []byte `json:"key,omitempty"` // Optional message key for partitioning
	Data  []byte `json:"data"`          // Message payload (arbitrary bytes)
}

// ProduceResponse contains the result of a produce operation.
type ProduceResponse struct {
	Offset uint64 `json:"offset"` // Assigned offset (sequence number) in the topic
}

// ConsumeRequest contains the parameters for consuming a single message.
type ConsumeRequest struct {
	Topic     string `json:"topic"`     // Topic to consume from
	Partition int    `json:"partition"` // Partition to consume from (default: 0)
	Offset    uint64 `json:"offset"`    // Offset of message to retrieve
}

// ConsumeResponse contains the consumed message data.
type ConsumeResponse struct {
	Key  []byte `json:"key,omitempty"` // Message key (if present)
	Data []byte `json:"data"`          // Message payload
}

// CreateTopicRequest contains the parameters for creating a new topic.
type CreateTopicRequest struct {
	Topic      string `json:"topic"`      // Name of the topic to create
	Partitions int    `json:"partitions"` // Number of partitions (for parallelism)
}

// SubscribeRequest contains the parameters for subscribing to a topic.
// Consumer groups enable multiple consumers to share the workload.
type SubscribeRequest struct {
	Topic     string `json:"topic"`     // Topic to subscribe to
	GroupID   string `json:"group_id"`  // Consumer group identifier
	Partition int    `json:"partition"` // Specific partition to consume
	Mode      string `json:"mode"`      // Start position: "earliest", "latest", or "commit"
}

// SubscribeResponse contains the starting offset for consumption.
type SubscribeResponse struct {
	Offset uint64 `json:"offset"` // Offset to start consuming from
}

// CommitRequest contains the parameters for committing a consumer's offset.
// Committing offsets enables consumers to resume from where they left off.
type CommitRequest struct {
	Topic     string `json:"topic"`     // Topic being consumed
	GroupID   string `json:"group_id"`  // Consumer group identifier
	Partition int    `json:"partition"` // Partition being consumed
	Offset    uint64 `json:"offset"`    // Offset to commit (last processed + 1)
}

// FetchRequest contains the parameters for fetching multiple messages.
// Batch fetching is more efficient than consuming messages one at a time.
type FetchRequest struct {
	Topic       string `json:"topic"`        // Topic to fetch from
	Partition   int    `json:"partition"`    // Partition to fetch from
	Offset      uint64 `json:"offset"`       // Starting offset
	MaxMessages int    `json:"max_messages"` // Maximum messages to return
}

// FetchedMessage represents a message with its key and offset.
// Used in FetchWithKeys responses to provide full message metadata.
type FetchedMessage struct {
	Key    []byte `json:"key,omitempty"` // Message key (if present)
	Value  []byte `json:"value"`         // Message payload
	Offset uint64 `json:"offset"`        // Message offset
}

// FetchResponse contains the fetched messages and next offset.
type FetchResponse struct {
	Messages   []FetchedMessage `json:"messages"`    // Array of fetched messages with keys
	NextOffset uint64           `json:"next_offset"` // Offset for next fetch call
}

// DeleteTopicRequest contains the topic name to delete.
type DeleteTopicRequest struct {
	Topic string `json:"topic"` // Topic to delete
}

// ListTopicsResponse contains the list of all topic names.
type ListTopicsResponse struct {
	Topics []string `json:"topics"` // Array of topic names
}

// ============================================================================
// Core Operation Handlers
// ============================================================================
// These handlers implement the core message queue operations.
// All handlers use binary-only protocol for maximum performance.
// Each handler follows a consistent pattern:
// 1. Parse binary request payload
// 2. Call the broker method
// 3. Log the operation with metrics
// 4. Send binary response
// ============================================================================

// handleProduce handles message production requests.
//
// FLOW:
// 1. Deserialize BinaryProduceRequest from payload
// 2. Call broker.Produce() to write message to topic
// 3. Log operation with latency metrics
// 4. Return binary response with offset
//
// PERFORMANCE:
// - Binary-only protocol reduces serialization overhead by 50-80%
// - Latency is measured from request start to broker completion
// - Metrics are logged for monitoring and alerting
func (s *Server) handleProduce(w io.Writer, payload []byte, flags byte) error {
	start := time.Now()

	var topic string
	var key, data []byte

	// Parse binary request (binary-only protocol)
	binReq, err := protocol.DecodeBinaryProduceRequest(payload)
	if err != nil {
		return err
	}
	topic = binReq.Topic
	key = binReq.Key
	data = binReq.Value

	// Check authorization
	if err := s.checkProduceAuth(w, topic); err != nil {
		s.securityLogger.LogAuthzFailure(s.getConnUsername(w), "produce", topic, w)
		return err
	}

	// Write message to broker and get assigned offset and partition
	// Use key-based partitioning if key is provided
	offset, partition, err := s.broker.ProduceWithKeyAndPartition(topic, key, data)
	if err != nil {
		s.errorLogger.LogError(err, "produce", map[string]interface{}{
			"topic":     topic,
			"partition": partition,
			"key":       len(key) > 0,
			"size":      len(data),
		})
		return err
	}

	// Calculate latency - only log if debug level or sampling
	// Logging every message is too expensive for high-throughput scenarios
	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
	// Sample 1% of messages for logging to reduce overhead
	if offset%100 == 0 {
		clientID := s.getClientID(w)
		s.msgLogger.LogProduce(topic, offset, len(data), clientID, latencyMs)
		s.perfLogger.LogLatency("produce", latencyMs, true)
	}

	// Send RecordMetadata response (Kafka-like)
	keySize := int32(-1)
	if len(key) > 0 {
		keySize = int32(len(key))
	}
	metadata := &protocol.RecordMetadata{
		Topic:     topic,
		Partition: int32(partition),
		Offset:    offset,
		Timestamp: time.Now().UnixMilli(),
		KeySize:   keySize,
		ValueSize: int32(len(data)),
	}
	resp := protocol.EncodeRecordMetadata(metadata)
	return protocol.WriteBinaryMessage(w, protocol.OpProduce, resp)
}

// handleConsume handles single message consumption requests.
//
// FLOW:
// 1. Deserialize BinaryConsumeRequest from payload
// 2. Call broker.ConsumeWithKey() to read message and key at offset
// 3. Log operation with latency metrics
// 4. Return binary response with message data and key
//
// PERFORMANCE: Uses binary-only protocol. For large messages (>64KB), uses buffer
// pools to reduce allocations. For batch consumption, use handleFetch.
func (s *Server) handleConsume(w io.Writer, payload []byte, flags byte) error {
	start := time.Now()

	var topic string
	var partition int
	var offset uint64

	// Parse binary request (binary-only protocol)
	binReq, err := protocol.DecodeBinaryConsumeRequest(payload)
	if err != nil {
		return err
	}
	topic = binReq.Topic
	partition = int(binReq.Partition)
	offset = binReq.Offset

	// Check authorization
	if err := s.checkConsumeAuth(w, topic); err != nil {
		s.securityLogger.LogAuthzFailure(s.getConnUsername(w), "consume", topic, w)
		return err
	}

	// Read message and key from broker at specified offset
	key, data, err := s.broker.ConsumeFromPartitionWithKey(topic, partition, offset)
	if err != nil {
		s.errorLogger.LogError(err, "consume", map[string]interface{}{
			"topic":     topic,
			"partition": partition,
			"offset":    offset,
		})
		return err
	}

	// Calculate latency and log metrics (sample 1% for high throughput)
	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
	if offset%100 == 0 {
		clientID := s.getClientID(w)
		consumerGroup := s.getConsumerGroup(w)
		s.msgLogger.LogConsume(topic, offset, consumerGroup, clientID, latencyMs)
		s.perfLogger.LogLatency("consume", latencyMs, true)
	}

	// Send binary response (binary-only protocol)
	resp := protocol.EncodeBinaryConsumeResponse(&protocol.BinaryConsumeResponse{
		Key:   key,
		Value: data,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpConsume, resp)
}

func (s *Server) handleCreateTopic(w io.Writer, payload []byte, flags byte) error {
	// Parse binary request (binary-only protocol)
	req, err := protocol.DecodeBinaryCreateTopicRequest(payload)
	if err != nil {
		return err
	}
	topic := req.Topic
	partitions := int(req.Partitions)

	// Get connection info for audit
	username := s.getConnUsername(w)
	clientIP := ""
	if conn, ok := w.(net.Conn); ok {
		clientIP = getClientIP(conn)
	}

	// Check admin authorization for topic creation
	if err := s.checkAdminAuth(w, topic); err != nil {
		s.securityLogger.LogAuthzFailure(username, "create_topic", topic, w)
		s.recordAudit(audit.EventAccessDenied, username, clientIP, topic, "create_topic", "denied", nil)
		return err
	}

	// Use partitions from request, default to 1 if not specified
	if partitions == 0 {
		partitions = 1
	}
	if err := s.broker.CreateTopic(topic, partitions); err != nil {
		s.errorLogger.LogError(err, "create_topic", map[string]interface{}{
			"topic":      topic,
			"partitions": partitions,
		})
		s.recordAudit(audit.EventTopicCreate, username, clientIP, topic, "create_topic", "failure", map[string]string{
			"error": err.Error(),
		})
		return err
	}

	// Log topic creation
	creator := s.getClientID(w)
	replicationFactor := s.config.Partition.DefaultReplicationFactor
	s.topicLogger.LogTopicCreated(topic, partitions, replicationFactor, creator)

	// Record audit event for topic creation
	s.recordAudit(audit.EventTopicCreate, username, clientIP, topic, "create_topic", "success", map[string]string{
		"partitions":         fmt.Sprintf("%d", partitions),
		"replication_factor": fmt.Sprintf("%d", replicationFactor),
	})

	// Always respond with binary for performance
	resp := protocol.EncodeBinaryCreateTopicResponse(&protocol.BinaryCreateTopicResponse{Success: true})
	return protocol.WriteBinaryMessage(w, protocol.OpCreateTopic, resp)
}

func (s *Server) handleSubscribe(w io.Writer, payload []byte, flags byte) error {
	// Parse binary request (binary-only protocol)
	req, err := protocol.DecodeBinarySubscribeRequest(payload)
	if err != nil {
		return err
	}
	topic := req.Topic
	groupID := req.GroupID
	partition := int(req.Partition)
	mode := req.Mode

	// Check consume authorization for subscription
	if err := s.checkConsumeAuth(w, topic); err != nil {
		s.securityLogger.LogAuthzFailure(s.getConnUsername(w), "subscribe", topic, w)
		return err
	}

	// Track the consumer group for this connection
	if conn, ok := w.(net.Conn); ok && groupID != "" {
		s.connConsumerGroup.Store(conn, groupID)
	}

	subMode := protocol.SubscribeMode(mode)
	if subMode == "" {
		subMode = protocol.SubscribeFromLatest
	}
	offset, err := s.broker.Subscribe(topic, groupID, partition, subMode)
	if err != nil {
		return err
	}

	// Always respond with binary for performance
	resp := protocol.EncodeBinarySubscribeResponse(&protocol.BinarySubscribeResponse{Offset: offset})
	return protocol.WriteBinaryMessage(w, protocol.OpSubscribe, resp)
}

func (s *Server) handleCommit(w io.Writer, payload []byte, flags byte) error {
	start := time.Now()

	// Parse binary request (binary-only protocol)
	req, err := protocol.DecodeBinaryCommitRequest(payload)
	if err != nil {
		return err
	}
	topic := req.Topic
	groupID := req.GroupID
	partition := int(req.Partition)
	offset := req.Offset

	// Persist the commit (only if offset changed)
	changed, err := s.broker.CommitOffset(topic, groupID, partition, offset)
	if err != nil {
		return err
	}

	// Only log when offset actually changes (avoid spamming logs on auto-commit)
	if changed {
		consumerID := s.getClientID(w)
		s.consumerLogger.LogCommit(consumerID, groupID, topic, partition, offset)
	}

	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
	s.perfLogger.LogLatency("commit", latencyMs, true)

	// Always respond with binary for performance
	resp := protocol.EncodeBinaryCommitResponse(&protocol.BinaryCommitResponse{Success: true})
	return protocol.WriteBinaryMessage(w, protocol.OpCommit, resp)
}

func (s *Server) handleFetch(w io.Writer, payload []byte, flags byte) error {
	// Parse binary request (binary-only protocol)
	req, err := protocol.DecodeBinaryFetchRequest(payload)
	if err != nil {
		return err
	}
	topic := req.Topic
	partition := int(req.Partition)
	offset := req.Offset
	maxMessages := int(req.MaxMessages)

	// Check consume authorization for fetch
	if err := s.checkConsumeAuth(w, topic); err != nil {
		s.securityLogger.LogAuthzFailure(s.getConnUsername(w), "fetch", topic, w)
		return err
	}

	if maxMessages <= 0 {
		maxMessages = 10
	}

	// Use FetchWithKeys to get messages with their keys
	brokerMessages, nextOffset, err := s.broker.FetchWithKeys(topic, partition, offset, maxMessages, req.Filter)
	if err != nil {
		return err
	}

	// Always respond with binary for performance
	binaryMessages := make([]protocol.BinaryFetchMessage, len(brokerMessages))
	for i, m := range brokerMessages {
		binaryMessages[i] = protocol.BinaryFetchMessage{
			Offset: m.Offset,
			Key:    m.Key,
			Value:  m.Value,
		}
	}

	resp := protocol.EncodeBinaryFetchResponse(&protocol.BinaryFetchResponse{
		Messages:   binaryMessages,
		NextOffset: nextOffset,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpFetch, resp)
}

// handleConsumeZeroCopy handles zero-copy consume requests for large messages.
// Uses sendfile() to transfer data directly from disk to network.
func (s *Server) handleConsumeZeroCopy(w io.Writer, payload []byte) error {
	start := time.Now()

	// Decode binary request
	req, err := protocol.DecodeBinaryConsumeRequest(payload)
	if err != nil {
		return err
	}

	// Check authorization
	if err := s.checkConsumeAuth(w, req.Topic); err != nil {
		s.securityLogger.LogAuthzFailure(s.getConnUsername(w), "consume", req.Topic, w)
		return err
	}

	// Get zero-copy info (file handle, offset, length)
	file, dataOffset, dataLen, err := s.broker.GetZeroCopyInfo(req.Topic, int(req.Partition), req.Offset)
	if err != nil {
		return err
	}

	// For zero-copy, we need the actual connection, not just io.Writer
	conn, ok := w.(net.Conn)
	if !ok {
		// Fall back to regular consume if we can't get the connection
		return s.handleConsumeFallback(w, req.Topic, int(req.Partition), req.Offset)
	}

	// Write response header first
	// Format: [8 bytes total length][4 bytes key length][key][4 bytes value length][value via sendfile]
	// For simplicity, we'll send a binary response with the value using zero-copy

	// Read key from storage (keys are typically small)
	key, value, err := s.broker.ConsumeFromPartitionWithKey(req.Topic, int(req.Partition), req.Offset)
	if err != nil {
		return err
	}

	// For large values, use sendfile; for small values, use regular write
	largeThreshold := 64 * 1024 // 64KB
	if len(value) >= largeThreshold && file != nil {
		// Use zero-copy for large values
		reader := performance.NewZeroCopyReader(file, dataOffset, dataLen)

		// Write header with key length, key, then value length
		headerBuf := make([]byte, 4+len(key)+4)
		binary.BigEndian.PutUint32(headerBuf[0:], uint32(len(key)))
		if len(key) > 0 {
			copy(headerBuf[4:], key)
		}
		binary.BigEndian.PutUint32(headerBuf[4+len(key):], uint32(dataLen))

		// Write protocol header
		h := protocol.Header{
			Magic:   protocol.MagicByte,
			Version: protocol.ProtocolVersion,
			Op:      protocol.OpConsumeZeroCopy,
			Flags:   protocol.FlagBinary,
			Length:  uint32(len(headerBuf)) + uint32(dataLen),
		}
		if err := protocol.WriteHeader(conn, h); err != nil {
			return err
		}

		// Write key header
		if _, err := conn.Write(headerBuf); err != nil {
			return err
		}

		// Zero-copy send the value
		if _, err := reader.SendTo(conn); err != nil {
			return err
		}
	} else {
		// Use regular binary response for small values
		resp := protocol.EncodeBinaryConsumeResponse(&protocol.BinaryConsumeResponse{
			Key:   key,
			Value: value,
		})
		if err := protocol.WriteBinaryMessage(conn, protocol.OpConsumeZeroCopy, resp); err != nil {
			return err
		}
	}

	// Log metrics
	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
	s.perfLogger.LogLatency("consume_zerocopy", latencyMs, true)

	return nil
}

// handleConsumeFallback is used when zero-copy is not available.
func (s *Server) handleConsumeFallback(w io.Writer, topic string, partition int, offset uint64) error {
	key, value, err := s.broker.ConsumeFromPartitionWithKey(topic, partition, offset)
	if err != nil {
		return err
	}

	resp := protocol.EncodeBinaryConsumeResponse(&protocol.BinaryConsumeResponse{
		Key:   key,
		Value: value,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpConsumeZeroCopy, resp)
}

// handleFetchBinary handles binary-encoded batch fetch requests for high performance.
func (s *Server) handleFetchBinary(w io.Writer, payload []byte) error {
	// Decode binary request
	req, err := protocol.DecodeBinaryFetchRequest(payload)
	if err != nil {
		return err
	}

	// Check authorization
	if err := s.checkConsumeAuth(w, req.Topic); err != nil {
		s.securityLogger.LogAuthzFailure(s.getConnUsername(w), "fetch_binary", req.Topic, w)
		return err
	}

	if req.MaxMessages <= 0 {
		req.MaxMessages = 10
	}

	// Fetch messages from broker
	brokerMessages, nextOffset, err := s.broker.FetchWithKeys(req.Topic, int(req.Partition), req.Offset, int(req.MaxMessages), req.Filter)
	if err != nil {
		return err
	}

	// Convert to binary format
	binaryMessages := make([]protocol.BinaryFetchMessage, len(brokerMessages))
	for i, m := range brokerMessages {
		binaryMessages[i] = protocol.BinaryFetchMessage{
			Offset: m.Offset,
			Key:    m.Key,
			Value:  m.Value,
		}
	}

	// Encode and send binary response
	resp := protocol.EncodeBinaryFetchResponse(&protocol.BinaryFetchResponse{
		Messages:   binaryMessages,
		NextOffset: nextOffset,
	})

	return protocol.WriteBinaryMessage(w, protocol.OpFetchBinary, resp)
}

func (s *Server) handleListTopics(w io.Writer) error {
	topics := s.broker.ListTopics()
	// Always respond with binary for performance
	resp := protocol.EncodeBinaryListTopicsResponse(&protocol.BinaryListTopicsResponse{Topics: topics})
	return protocol.WriteBinaryMessage(w, protocol.OpListTopics, resp)
}

func (s *Server) handleDeleteTopic(w io.Writer, payload []byte, flags byte) error {
	// Parse binary request (binary-only protocol)
	req, err := protocol.DecodeBinaryDeleteTopicRequest(payload)
	if err != nil {
		return err
	}
	topic := req.Topic

	// Get connection info for audit
	username := s.getConnUsername(w)
	clientIP := ""
	if conn, ok := w.(net.Conn); ok {
		clientIP = getClientIP(conn)
	}

	// Check admin authorization for topic deletion
	if err := s.checkAdminAuth(w, topic); err != nil {
		s.securityLogger.LogAuthzFailure(username, "delete_topic", topic, w)
		s.recordAudit(audit.EventAccessDenied, username, clientIP, topic, "delete_topic", "denied", nil)
		return err
	}

	// Get message count before deletion for logging
	messageCount, _ := s.broker.GetTopicMessageCount(topic)

	if err := s.broker.DeleteTopic(topic); err != nil {
		s.errorLogger.LogError(err, "delete_topic", map[string]interface{}{
			"topic": topic,
		})
		s.recordAudit(audit.EventTopicDelete, username, clientIP, topic, "delete_topic", "failure", map[string]string{
			"error": err.Error(),
		})
		return err
	}

	// Log topic deletion
	deletedBy := s.getClientID(w)
	s.topicLogger.LogTopicDeleted(topic, deletedBy, messageCount)

	// Record audit event for topic deletion
	s.recordAudit(audit.EventTopicDelete, username, clientIP, topic, "delete_topic", "success", map[string]string{
		"message_count": fmt.Sprintf("%d", messageCount),
	})

	// Always respond with binary for performance
	resp := protocol.EncodeBinaryDeleteTopicResponse(&protocol.BinaryDeleteTopicResponse{Success: true})
	return protocol.WriteBinaryMessage(w, protocol.OpDeleteTopic, resp)
}

// ============================================================================
// Consumer Group Handlers (Binary Only)
// ============================================================================

func (s *Server) handleGetOffset(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryGetOffsetRequest(payload)
	if err != nil {
		return err
	}
	// Authorization: consume permission is sufficient for offset queries
	if err := s.checkConsumeAuth(w, req.Topic); err != nil {
		return err
	}
	info, err := s.broker.GetConsumerGroup(req.Topic, req.GroupID)
	if err != nil {
		return err
	}
	var committed uint64
	if off, ok := info.Offsets[int(req.Partition)]; ok {
		committed = off
	}
	resp := protocol.EncodeBinaryGetOffsetResponse(&protocol.BinaryGetOffsetResponse{Offset: committed})
	return protocol.WriteBinaryMessage(w, protocol.OpGetOffset, resp)
}

func (s *Server) handleResetOffset(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryResetOffsetRequest(payload)
	if err != nil {
		return err
	}
	if err := s.checkAdminAuth(w, req.Topic); err != nil {
		return err
	}
	// Determine target offset
	var target uint64
	switch protocol.SubscribeMode(req.Mode) {
	case protocol.SubscribeFromEarliest:
		info, err := s.broker.GetTopicInfo(req.Topic)
		if err != nil {
			return err
		}
		if int(req.Partition) >= len(info.Partitions) {
			return fmt.Errorf("partition %d not found", req.Partition)
		}
		target = info.Partitions[req.Partition].LowestOffset
	case protocol.SubscribeFromLatest:
		info, err := s.broker.GetTopicInfo(req.Topic)
		if err != nil {
			return err
		}
		if int(req.Partition) >= len(info.Partitions) {
			return fmt.Errorf("partition %d not found", req.Partition)
		}
		// Next offset after highest
		high := info.Partitions[req.Partition].HighestOffset
		target = high + 1
	case "offset":
		target = req.Offset
	default:
		return fmt.Errorf("unknown reset mode: %s", req.Mode)
	}
	if _, err := s.broker.CommitOffset(req.Topic, req.GroupID, int(req.Partition), target); err != nil {
		return err
	}
	resp := protocol.EncodeBinarySimpleBoolResponse(&protocol.BinarySimpleBoolResponse{Success: true})
	return protocol.WriteBinaryMessage(w, protocol.OpResetOffset, resp)
}

func (s *Server) handleListGroups(w io.Writer) error {
	// Admin or read permission could be required; use admin for safety
	if err := s.checkUserManagementAuth(w); err != nil {
		return err
	}
	groups := s.broker.ListConsumerGroups()
	out := protocol.BinaryListGroupsResponse{Groups: make([]struct {
		Topic   string
		GroupID string
		Members uint32
		Offsets []protocol.BinaryGroupOffsets
	}, 0, len(groups))}
	for _, g := range groups {
		var offsets []protocol.BinaryGroupOffsets
		for p, off := range g.Offsets {
			offsets = append(offsets, protocol.BinaryGroupOffsets{Partition: int32(p), Offset: off})
		}
		out.Groups = append(out.Groups, struct {
			Topic   string
			GroupID string
			Members uint32
			Offsets []protocol.BinaryGroupOffsets
		}{Topic: g.Topic, GroupID: g.GroupID, Members: uint32(g.Members), Offsets: offsets})
	}
	resp := protocol.EncodeBinaryListGroupsResponse(&out)
	return protocol.WriteBinaryMessage(w, protocol.OpListGroups, resp)
}

func (s *Server) handleDescribeGroup(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryDescribeGroupRequest(payload)
	if err != nil {
		return err
	}
	if err := s.checkUserManagementAuth(w); err != nil {
		return err
	}
	g, err := s.broker.GetConsumerGroup(req.Topic, req.GroupID)
	if err != nil {
		return err
	}
	var offsets []protocol.BinaryGroupOffsets
	for p, off := range g.Offsets {
		offsets = append(offsets, protocol.BinaryGroupOffsets{Partition: int32(p), Offset: off})
	}
	resp := protocol.EncodeBinaryDescribeGroupResponse(&protocol.BinaryDescribeGroupResponse{Topic: g.Topic, GroupID: g.GroupID, Members: uint32(g.Members), Offsets: offsets})
	return protocol.WriteBinaryMessage(w, protocol.OpDescribeGroup, resp)
}

func (s *Server) handleGetLag(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryGetLagRequest(payload)
	if err != nil {
		return err
	}
	if err := s.checkConsumeAuth(w, req.Topic); err != nil {
		return err
	}
	g, err := s.broker.GetConsumerGroup(req.Topic, req.GroupID)
	if err != nil {
		return err
	}
	committed := uint64(0)
	if off, ok := g.Offsets[int(req.Partition)]; ok {
		committed = off
	}
	info, err := s.broker.GetTopicInfo(req.Topic)
	if err != nil {
		return err
	}
	if int(req.Partition) >= len(info.Partitions) {
		return fmt.Errorf("partition %d not found", req.Partition)
	}
	high := info.Partitions[req.Partition].HighestOffset
	latest := high + 1
	var lag uint64
	if latest >= committed {
		lag = latest - committed
	} else {
		lag = 0
	}
	resp := protocol.EncodeBinaryGetLagResponse(&protocol.BinaryGetLagResponse{CurrentOffset: committed, CommittedOffset: committed, LatestOffset: latest, Lag: lag})
	return protocol.WriteBinaryMessage(w, protocol.OpGetLag, resp)
}

func (s *Server) handleDeleteGroup(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryDeleteGroupRequest(payload)
	if err != nil {
		return err
	}
	if err := s.checkUserManagementAuth(w); err != nil {
		return err
	}
	if err := s.broker.DeleteConsumerGroup(req.Topic, req.GroupID); err != nil {
		return err
	}
	resp := protocol.EncodeBinarySimpleBoolResponse(&protocol.BinarySimpleBoolResponse{Success: true})
	return protocol.WriteBinaryMessage(w, protocol.OpDeleteGroup, resp)
}

// ============================================================================
// Advanced Request/Response Types
// ============================================================================

type ProduceDelayedRequest struct {
	Topic   string `json:"topic"`
	Data    []byte `json:"data"`
	DelayMs int64  `json:"delay_ms"`
}

type ProduceWithTTLRequest struct {
	Topic string `json:"topic"`
	Data  []byte `json:"data"`
	TTLMs int64  `json:"ttl_ms"`
}

type ProduceWithSchemaRequest struct {
	Topic      string `json:"topic"`
	Data       []byte `json:"data"`
	SchemaName string `json:"schema_name"`
}

type RegisterSchemaRequest struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Schema []byte `json:"schema"`
}

type GetDLQMessagesRequest struct {
	Topic       string `json:"topic"`
	MaxMessages int    `json:"max_messages"`
}

type ReplayDLQRequest struct {
	Topic     string `json:"topic"`
	MessageID string `json:"message_id"`
}

type PurgeDLQRequest struct {
	Topic string `json:"topic"`
}

type BeginTxResponse struct {
	TxnID string `json:"txn_id"`
}

type TxnRequest struct {
	TxnID string `json:"txn_id"`
}

type TxnProduceRequest struct {
	TxnID string `json:"txn_id"`
	Topic string `json:"topic"`
	Data  []byte `json:"data"`
}

// ============================================================================
// Advanced Handlers
// ============================================================================

func (s *Server) handleProduceDelayed(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryProduceDelayedRequest(payload)
	if err != nil {
		return err
	}

	// Use delayed message manager if available
	if s.delayedManager != nil {
		delay := time.Duration(req.DelayMs) * time.Millisecond
		messageID, err := s.delayedManager.Schedule(req.Topic, req.Data, delay, nil)
		if err != nil {
			return err
		}
		s.logger.Debug("Delayed message scheduled", "topic", req.Topic, "delay_ms", req.DelayMs, "message_id", messageID)
		metadata := &protocol.RecordMetadata{
			Topic:     req.Topic,
			Partition: 0,
			Offset:    0, // Offset will be assigned when message is delivered
			Timestamp: time.Now().UnixMilli(),
			KeySize:   -1,
			ValueSize: int32(len(req.Data)),
		}
		return protocol.WriteBinaryMessage(w, protocol.OpProduceDelayed, protocol.EncodeRecordMetadata(metadata))
	}

	// Fallback: produce immediately if delayed manager not enabled
	offset, err := s.broker.Produce(req.Topic, req.Data)
	if err != nil {
		return err
	}
	s.logger.Debug("Delayed message produced immediately (delayed delivery disabled)", "topic", req.Topic, "delay_ms", req.DelayMs)
	metadata := &protocol.RecordMetadata{
		Topic:     req.Topic,
		Partition: 0,
		Offset:    offset,
		Timestamp: time.Now().UnixMilli(),
		KeySize:   -1,
		ValueSize: int32(len(req.Data)),
	}
	return protocol.WriteBinaryMessage(w, protocol.OpProduceDelayed, protocol.EncodeRecordMetadata(metadata))
}

func (s *Server) handleProduceWithTTL(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryProduceWithTTLRequest(payload)
	if err != nil {
		return err
	}

	// Produce the message
	offset, err := s.broker.Produce(req.Topic, req.Data)
	if err != nil {
		return err
	}

	// Register message with TTL manager if available
	if s.ttlManager != nil {
		now := time.Now()
		ttlSeconds := req.TTLMs / 1000
		meta := ttl.MessageMetadata{
			MessageID:  fmt.Sprintf("%s-%d-%d", req.Topic, 0, offset),
			Topic:      req.Topic,
			Partition:  0,
			Offset:     offset,
			CreatedAt:  now,
			ExpiresAt:  now.Add(time.Duration(req.TTLMs) * time.Millisecond),
			TTLSeconds: ttlSeconds,
		}
		s.ttlManager.RegisterMessage(meta)
		s.logger.Debug("Message with TTL registered", "topic", req.Topic, "ttl_ms", req.TTLMs, "offset", offset)
	} else {
		s.logger.Debug("Message with TTL produced (TTL tracking disabled)", "topic", req.Topic, "ttl_ms", req.TTLMs)
	}

	metadata := &protocol.RecordMetadata{
		Topic:     req.Topic,
		Partition: 0,
		Offset:    offset,
		Timestamp: time.Now().UnixMilli(),
		KeySize:   -1,
		ValueSize: int32(len(req.Data)),
	}
	return protocol.WriteBinaryMessage(w, protocol.OpProduceWithTTL, protocol.EncodeRecordMetadata(metadata))
}

func (s *Server) handleProduceWithSchema(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryProduceWithSchemaRequest(payload)
	if err != nil {
		return err
	}

	// Validate message against schema if schema validation is enabled
	if s.config.Schema.Enabled && s.schemaValidator != nil {
		result := s.schemaValidator.Validate(req.SchemaName, req.Data)
		if !result.Valid {
			errMsg := fmt.Sprintf("schema validation failed: %v", result.Errors)
			s.logger.Warn("Schema validation failed", "topic", req.Topic, "schema", req.SchemaName, "errors", result.Errors)
			return protocol.WriteBinaryMessage(w, protocol.OpError, []byte(errMsg))
		}
	}

	offset, err := s.broker.Produce(req.Topic, req.Data)
	if err != nil {
		return err
	}
	s.logger.Debug("Message with schema produced", "topic", req.Topic, "schema", req.SchemaName)
	metadata := &protocol.RecordMetadata{
		Topic:     req.Topic,
		Partition: 0,
		Offset:    offset,
		Timestamp: time.Now().UnixMilli(),
		KeySize:   -1,
		ValueSize: int32(len(req.Data)),
	}
	return protocol.WriteBinaryMessage(w, protocol.OpProduceWithSchema, protocol.EncodeRecordMetadata(metadata))
}

func (s *Server) handleRegisterSchema(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryRegisterSchemaRequest(payload)
	if err != nil {
		return err
	}

	if s.schemaRegistry == nil {
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte("schema registry not initialized"))
	}

	// Validate schema type
	switch req.Type {
	case "json", "avro", "protobuf", "proto":
		// Valid types
	default:
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte(fmt.Sprintf("unsupported schema type: %s", req.Type)))
	}

	// Register the schema through broker (routes through Raft in cluster mode)
	if err := s.broker.RegisterSchema(req.Name, req.Type, string(req.Schema), "backward"); err != nil {
		s.logger.Error("Failed to register schema", "name", req.Name, "error", err)
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte(err.Error()))
	}

	// Get the registered schema to return version info
	registeredSchema, err := s.schemaRegistry.GetLatest(req.Name)
	version := 1
	if err == nil && registeredSchema != nil {
		version = registeredSchema.Version
	}

	s.logger.Info("Schema registered", "name", req.Name, "type", req.Type, "version", version)
	resp := protocol.EncodeBinarySchemaResponse(&protocol.BinarySchemaResponse{
		Name:    req.Name,
		Version: uint32(version),
		Type:    req.Type,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpRegisterSchema, resp)
}

func (s *Server) handleListSchemas(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryListSchemasRequest(payload)
	if err != nil {
		// Handle empty payload - list all schemas
		req = &protocol.BinaryListSchemasRequest{Topic: ""}
	}

	if s.schemaRegistry == nil {
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte("schema registry not initialized"))
	}

	schemas, err := s.schemaRegistry.List(req.Topic)
	if err != nil {
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte(err.Error()))
	}

	schemaList := make([]protocol.BinarySchemaInfo, 0, len(schemas))
	for _, sch := range schemas {
		schemaList = append(schemaList, protocol.BinarySchemaInfo{
			Name:      sch.ID,
			Type:      string(sch.Type),
			Version:   uint32(sch.Version),
			CreatedAt: sch.CreatedAt.Unix(),
		})
	}

	resp := protocol.EncodeBinaryListSchemasResponse(&protocol.BinaryListSchemasResponse{Schemas: schemaList})
	return protocol.WriteBinaryMessage(w, protocol.OpListSchemas, resp)
}

func (s *Server) handleValidateSchema(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryValidateSchemaRequest(payload)
	if err != nil {
		return err
	}

	if s.schemaValidator == nil {
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte("schema validator not initialized"))
	}

	result := s.schemaValidator.Validate(req.Name, req.Message)
	resp := protocol.EncodeBinaryValidateSchemaResponse(&protocol.BinaryValidateSchemaResponse{
		Valid:  result.Valid,
		Errors: result.Errors,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpValidateSchema, resp)
}

func (s *Server) handleGetSchema(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryGetSchemaRequest(payload)
	if err != nil {
		return err
	}

	if s.schemaRegistry == nil {
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte("schema registry not initialized"))
	}

	var sch *schema.Schema
	if req.Version > 0 {
		sch, err = s.schemaRegistry.Get(req.Topic, int(req.Version))
	} else {
		sch, err = s.schemaRegistry.GetLatest(req.Topic)
	}

	if err != nil {
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte(err.Error()))
	}

	resp := protocol.EncodeBinaryGetSchemaResponse(&protocol.BinaryGetSchemaResponse{
		ID:            sch.ID,
		Topic:         sch.Topic,
		Version:       uint32(sch.Version),
		Type:          string(sch.Type),
		Definition:    sch.Definition,
		Compatibility: string(sch.Compatibility),
		CreatedAt:     sch.CreatedAt.Unix(),
	})
	return protocol.WriteBinaryMessage(w, protocol.OpGetSchema, resp)
}

func (s *Server) handleDeleteSchema(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryDeleteSchemaRequest(payload)
	if err != nil {
		return err
	}

	if s.schemaRegistry == nil {
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte("schema registry not initialized"))
	}

	// Delete the schema through broker (routes through Raft in cluster mode)
	if err := s.broker.DeleteSchema(req.Topic, int(req.Version)); err != nil {
		return protocol.WriteBinaryMessage(w, protocol.OpError, []byte(err.Error()))
	}

	s.logger.Info("Schema deleted", "topic", req.Topic, "version", req.Version)
	resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{Success: true, Message: "OK"})
	return protocol.WriteBinaryMessage(w, protocol.OpDeleteSchema, resp)
}

func (s *Server) handleGetDLQMessages(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryDLQRequest(payload)
	if err != nil {
		return err
	}

	// Use DLQ manager if available
	if s.dlqManager != nil {
		messages, _, err := s.dlqManager.GetDLQMessages(req.Topic, 0, int(req.MaxMessages))
		if err != nil {
			s.logger.Warn("Failed to get DLQ messages", "topic", req.Topic, "error", err)
			// Return empty response on error
			resp := protocol.EncodeBinaryDLQResponse(&protocol.BinaryDLQResponse{Messages: nil})
			return protocol.WriteBinaryMessage(w, protocol.OpGetDLQMessages, resp)
		}

		// Convert DLQ messages to protocol format
		protoMessages := make([]protocol.BinaryDLQMessage, len(messages))
		for i, msg := range messages {
			protoMessages[i] = protocol.BinaryDLQMessage{
				ID:      msg.ID,
				Data:    msg.Payload,
				Error:   msg.ErrorMessage,
				Retries: int32(msg.RetryCount),
			}
		}
		s.logger.Debug("Retrieved DLQ messages", "topic", req.Topic, "count", len(messages))
		resp := protocol.EncodeBinaryDLQResponse(&protocol.BinaryDLQResponse{Messages: protoMessages})
		return protocol.WriteBinaryMessage(w, protocol.OpGetDLQMessages, resp)
	}

	// DLQ manager not enabled
	s.logger.Debug("DLQ manager not enabled, returning empty response", "topic", req.Topic)
	resp := protocol.EncodeBinaryDLQResponse(&protocol.BinaryDLQResponse{Messages: nil})
	return protocol.WriteBinaryMessage(w, protocol.OpGetDLQMessages, resp)
}

func (s *Server) handleReplayDLQ(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryReplayDLQRequest(payload)
	if err != nil {
		return err
	}

	// Use DLQ manager if available
	if s.dlqManager != nil {
		// Get the DLQ message by ID
		messages, _, err := s.dlqManager.GetDLQMessages(req.Topic, 0, 1000)
		if err != nil {
			s.logger.Warn("Failed to get DLQ messages for replay", "topic", req.Topic, "error", err)
			resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{Success: false, Message: err.Error()})
			return protocol.WriteBinaryMessage(w, protocol.OpReplayDLQ, resp)
		}

		// Find the message with the matching ID
		var targetMsg *dlq.DLQMessage
		for _, msg := range messages {
			if msg.ID == req.MessageID {
				targetMsg = msg
				break
			}
		}

		if targetMsg == nil {
			s.logger.Warn("DLQ message not found", "topic", req.Topic, "message_id", req.MessageID)
			resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{Success: false, Message: "message not found"})
			return protocol.WriteBinaryMessage(w, protocol.OpReplayDLQ, resp)
		}

		// Replay the message
		if err := s.dlqManager.ReplayMessage(targetMsg); err != nil {
			s.logger.Warn("Failed to replay DLQ message", "topic", req.Topic, "message_id", req.MessageID, "error", err)
			resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{Success: false, Message: err.Error()})
			return protocol.WriteBinaryMessage(w, protocol.OpReplayDLQ, resp)
		}

		s.logger.Info("DLQ message replayed", "topic", req.Topic, "message_id", req.MessageID)
		resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{Success: true, Message: "OK"})
		return protocol.WriteBinaryMessage(w, protocol.OpReplayDLQ, resp)
	}

	// DLQ manager not enabled
	s.logger.Debug("DLQ manager not enabled", "topic", req.Topic)
	resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{Success: false, Message: "DLQ not enabled"})
	return protocol.WriteBinaryMessage(w, protocol.OpReplayDLQ, resp)
}

func (s *Server) handlePurgeDLQ(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryPurgeDLQRequest(payload)
	if err != nil {
		return err
	}

	// Use DLQ manager if available
	if s.dlqManager != nil {
		// Get the DLQ topic name
		dlqTopic := s.dlqManager.GetDLQTopic(req.Topic)

		// Delete the DLQ topic to purge all messages
		if err := s.broker.DeleteTopic(dlqTopic); err != nil {
			s.logger.Warn("Failed to purge DLQ", "topic", req.Topic, "dlq_topic", dlqTopic, "error", err)
			resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{Success: false, Message: err.Error()})
			return protocol.WriteBinaryMessage(w, protocol.OpPurgeDLQ, resp)
		}

		s.logger.Info("DLQ purged", "topic", req.Topic, "dlq_topic", dlqTopic)
		resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{Success: true, Message: "OK"})
		return protocol.WriteBinaryMessage(w, protocol.OpPurgeDLQ, resp)
	}

	// DLQ manager not enabled
	s.logger.Debug("DLQ manager not enabled", "topic", req.Topic)
	resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{Success: false, Message: "DLQ not enabled"})
	return protocol.WriteBinaryMessage(w, protocol.OpPurgeDLQ, resp)
}

func (s *Server) handleBeginTx(w io.Writer, payload []byte) error {
	// Use transaction coordinator if available
	if s.txnCoordinator != nil {
		// Use a producer ID based on connection (simplified for now)
		producerID := fmt.Sprintf("producer-%d", time.Now().UnixNano())
		txn, err := s.txnCoordinator.Begin(producerID)
		if err != nil {
			s.logger.Warn("Failed to begin transaction", "error", err)
			resp := protocol.EncodeBinaryTxnResponse(&protocol.BinaryTxnResponse{TxnID: "", Success: false})
			return protocol.WriteBinaryMessage(w, protocol.OpBeginTx, resp)
		}
		s.logger.Debug("Transaction started", "txn_id", txn.ID, "producer_id", producerID)
		resp := protocol.EncodeBinaryTxnResponse(&protocol.BinaryTxnResponse{TxnID: txn.ID, Success: true})
		return protocol.WriteBinaryMessage(w, protocol.OpBeginTx, resp)
	}

	// Fallback: generate a simple transaction ID
	txnID := fmt.Sprintf("txn-%d", time.Now().UnixNano())
	s.logger.Debug("Transaction started (coordinator disabled)", "txn_id", txnID)
	resp := protocol.EncodeBinaryTxnResponse(&protocol.BinaryTxnResponse{TxnID: txnID, Success: true})
	return protocol.WriteBinaryMessage(w, protocol.OpBeginTx, resp)
}

func (s *Server) handleCommitTx(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryTxnRequest(payload)
	if err != nil {
		return err
	}

	// Use transaction coordinator if available
	if s.txnCoordinator != nil {
		if err := s.txnCoordinator.Commit(req.TxnID); err != nil {
			s.logger.Warn("Failed to commit transaction", "txn_id", req.TxnID, "error", err)
			resp := protocol.EncodeBinaryTxnResponse(&protocol.BinaryTxnResponse{TxnID: req.TxnID, Success: false})
			return protocol.WriteBinaryMessage(w, protocol.OpCommitTx, resp)
		}
		s.logger.Debug("Transaction committed", "txn_id", req.TxnID)
		resp := protocol.EncodeBinaryTxnResponse(&protocol.BinaryTxnResponse{TxnID: req.TxnID, Success: true})
		return protocol.WriteBinaryMessage(w, protocol.OpCommitTx, resp)
	}

	// Fallback: just acknowledge
	s.logger.Debug("Transaction committed (coordinator disabled)", "txn_id", req.TxnID)
	resp := protocol.EncodeBinaryTxnResponse(&protocol.BinaryTxnResponse{TxnID: req.TxnID, Success: true})
	return protocol.WriteBinaryMessage(w, protocol.OpCommitTx, resp)
}

func (s *Server) handleAbortTx(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryTxnRequest(payload)
	if err != nil {
		return err
	}

	// Use transaction coordinator if available
	if s.txnCoordinator != nil {
		if err := s.txnCoordinator.Abort(req.TxnID); err != nil {
			s.logger.Warn("Failed to abort transaction", "txn_id", req.TxnID, "error", err)
			resp := protocol.EncodeBinaryTxnResponse(&protocol.BinaryTxnResponse{TxnID: req.TxnID, Success: false})
			return protocol.WriteBinaryMessage(w, protocol.OpAbortTx, resp)
		}
		s.logger.Debug("Transaction aborted", "txn_id", req.TxnID)
		resp := protocol.EncodeBinaryTxnResponse(&protocol.BinaryTxnResponse{TxnID: req.TxnID, Success: true})
		return protocol.WriteBinaryMessage(w, protocol.OpAbortTx, resp)
	}

	// Fallback: just acknowledge
	s.logger.Debug("Transaction aborted (coordinator disabled)", "txn_id", req.TxnID)
	resp := protocol.EncodeBinaryTxnResponse(&protocol.BinaryTxnResponse{TxnID: req.TxnID, Success: true})
	return protocol.WriteBinaryMessage(w, protocol.OpAbortTx, resp)
}

func (s *Server) handleProduceTx(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryTxnProduceRequest(payload)
	if err != nil {
		return err
	}

	// Use transaction coordinator if available
	if s.txnCoordinator != nil {
		// Add the produce operation to the transaction
		op := transaction.Operation{
			Type:    transaction.OpProduce,
			Topic:   req.Topic,
			Payload: req.Data,
		}
		if err := s.txnCoordinator.AddOperation(req.TxnID, op); err != nil {
			s.logger.Warn("Failed to add operation to transaction", "txn_id", req.TxnID, "error", err)
			return err
		}
		s.logger.Debug("Transactional message added", "txn_id", req.TxnID, "topic", req.Topic)
		metadata := &protocol.RecordMetadata{
			Topic:     req.Topic,
			Partition: 0,
			Offset:    0, // Offset will be assigned on commit
			Timestamp: time.Now().UnixMilli(),
			KeySize:   -1,
			ValueSize: int32(len(req.Data)),
		}
		return protocol.WriteBinaryMessage(w, protocol.OpProduceTx, protocol.EncodeRecordMetadata(metadata))
	}

	// Fallback: produce immediately
	offset, err := s.broker.Produce(req.Topic, req.Data)
	if err != nil {
		return err
	}
	s.logger.Debug("Transactional message produced (coordinator disabled)", "txn_id", req.TxnID, "topic", req.Topic)
	metadata := &protocol.RecordMetadata{
		Topic:     req.Topic,
		Partition: 0,
		Offset:    offset,
		Timestamp: time.Now().UnixMilli(),
		KeySize:   -1,
		ValueSize: int32(len(req.Data)),
	}
	return protocol.WriteBinaryMessage(w, protocol.OpProduceTx, protocol.EncodeRecordMetadata(metadata))
}

// ============================================================================
// Authentication Handlers
// ============================================================================

// handleAuth handles authentication requests.
// Clients send username/password and receive a success/failure response.
func (s *Server) handleAuth(w io.Writer, payload []byte) error {
	// Parse binary auth request
	req, err := protocol.DecodeBinaryAuthRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid auth request: %w", err))
	}

	// Get client IP for audit logging
	clientIP := ""
	if conn, ok := w.(net.Conn); ok {
		clientIP = getClientIP(conn)
	}

	// Authenticate the user
	user, err := s.authorizer.Authenticate(req.Username, req.Password)
	if err != nil {
		s.securityLogger.LogAuthFailure(req.Username, "invalid_credentials", w)
		// Record audit event for failed authentication
		s.recordAudit(audit.EventAuthFailure, req.Username, clientIP, "", "authenticate", "failure", map[string]string{
			"reason": err.Error(),
		})
		resp := protocol.EncodeBinaryAuthResponse(&protocol.BinaryAuthResponse{
			Success: false,
			Error:   err.Error(),
		})
		return protocol.WriteBinaryMessage(w, protocol.OpAuthResponse, resp)
	}

	// Store the authenticated user for this connection
	if conn, ok := w.(net.Conn); ok {
		s.connAuth.Store(conn, req.Username)
	}

	// Log successful authentication
	s.securityLogger.LogAuthSuccess(req.Username, w)

	// Record audit event for successful authentication
	s.recordAudit(audit.EventAuthSuccess, req.Username, clientIP, "", "authenticate", "success", map[string]string{
		"roles": fmt.Sprintf("%v", user.Roles),
	})

	// Get user roles for response
	resp := protocol.EncodeBinaryAuthResponse(&protocol.BinaryAuthResponse{
		Success:  true,
		Username: user.Username,
		Roles:    user.Roles,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpAuthResponse, resp)
}

// handleWhoAmI returns information about the currently authenticated user.
func (s *Server) handleWhoAmI(w io.Writer) error {
	username := ""
	if conn, ok := w.(net.Conn); ok {
		if u, exists := s.connAuth.Load(conn); exists {
			username = u.(string)
		}
	}

	// Build binary response using auth response format
	if username == "" {
		resp := protocol.EncodeBinaryAuthResponse(&protocol.BinaryAuthResponse{
			Success:  false, // Not authenticated
			Username: "",
			Roles:    nil,
		})
		return protocol.WriteBinaryMessage(w, protocol.OpWhoAmI, resp)
	}

	user, exists := s.authorizer.UserStore().GetUser(username)
	if !exists {
		resp := protocol.EncodeBinaryAuthResponse(&protocol.BinaryAuthResponse{
			Success:  false,
			Username: "",
			Roles:    nil,
		})
		return protocol.WriteBinaryMessage(w, protocol.OpWhoAmI, resp)
	}

	resp := protocol.EncodeBinaryAuthResponse(&protocol.BinaryAuthResponse{
		Success:  true, // Authenticated
		Username: user.Username,
		Roles:    user.Roles,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpWhoAmI, resp)
}

// getConnUsername returns the authenticated username for a connection.
func (s *Server) getConnUsername(w io.Writer) string {
	if conn, ok := w.(net.Conn); ok {
		if u, exists := s.connAuth.Load(conn); exists {
			return u.(string)
		}
	}
	return ""
}

// getClientID returns a client identifier for a connection.
// Uses the authenticated username if available, otherwise uses the remote address.
func (s *Server) getClientID(w io.Writer) string {
	// First try to get the authenticated username
	if username := s.getConnUsername(w); username != "" {
		return username
	}
	// Fall back to remote address
	if conn, ok := w.(net.Conn); ok {
		return conn.RemoteAddr().String()
	}
	return "unknown"
}

// getConsumerGroup returns the consumer group for a connection.
// Returns "default" if no consumer group is associated with the connection.
func (s *Server) getConsumerGroup(w io.Writer) string {
	if conn, ok := w.(net.Conn); ok {
		if group, exists := s.connConsumerGroup.Load(conn); exists {
			return group.(string)
		}
	}
	return "default"
}

// checkProduceAuth checks if the connection is authorized to produce to a topic.
func (s *Server) checkProduceAuth(w io.Writer, topic string) error {
	if !s.authorizer.IsEnabled() {
		return nil
	}
	username := s.getConnUsername(w)
	return s.authorizer.CanProduce(username, topic)
}

// checkConsumeAuth checks if the connection is authorized to consume from a topic.
func (s *Server) checkConsumeAuth(w io.Writer, topic string) error {
	if !s.authorizer.IsEnabled() {
		return nil
	}
	username := s.getConnUsername(w)
	return s.authorizer.CanConsume(username, topic)
}

// checkAdminAuth checks if the connection is authorized for admin operations.
func (s *Server) checkAdminAuth(w io.Writer, topic string) error {
	if !s.authorizer.IsEnabled() {
		return nil
	}
	username := s.getConnUsername(w)
	return s.authorizer.CanAdmin(username, topic)
}

// checkUserManagementAuth checks if the connection can manage users.
func (s *Server) checkUserManagementAuth(w io.Writer) error {
	if !s.authorizer.IsEnabled() {
		return nil
	}
	username := s.getConnUsername(w)
	return s.authorizer.CanManageUsers(username)
}

// ============================================================================
// User Management Handlers
// ============================================================================

// handleUserCreate creates a new user.
func (s *Server) handleUserCreate(w io.Writer, payload []byte) error {
	// Get connection info for audit
	adminUser := s.getConnUsername(w)
	clientIP := ""
	if conn, ok := w.(net.Conn); ok {
		clientIP = getClientIP(conn)
	}

	if err := s.checkUserManagementAuth(w); err != nil {
		s.recordAudit(audit.EventAccessDenied, adminUser, clientIP, "", "user_create", "denied", nil)
		return s.sendErrorResponse(w, err)
	}

	req, err := protocol.DecodeBinaryUserRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid request: %w", err))
	}

	if req.Username == "" || req.Password == "" {
		return s.sendErrorResponse(w, fmt.Errorf("username and password are required"))
	}

	// In cluster mode, route through Raft consensus
	if s.broker.IsClusterMode() {
		// Hash password before replicating (password should never be sent in plaintext)
		passwordHash, err := auth.HashPassword(req.Password)
		if err != nil {
			return s.sendErrorResponse(w, fmt.Errorf("failed to hash password: %w", err))
		}
		if err := s.broker.ProposeCreateUser(req.Username, passwordHash, req.Roles); err != nil {
			s.recordAudit(audit.EventUserCreate, adminUser, clientIP, req.Username, "user_create", "failure", map[string]string{
				"error": err.Error(),
			})
			return s.sendErrorResponse(w, err)
		}
	} else {
		// Standalone mode - create directly
		if err := s.authorizer.UserStore().CreateUser(req.Username, req.Password, req.Roles); err != nil {
			s.recordAudit(audit.EventUserCreate, adminUser, clientIP, req.Username, "user_create", "failure", map[string]string{
				"error": err.Error(),
			})
			return s.sendErrorResponse(w, err)
		}
		// Persist changes
		if err := s.authorizer.UserStore().Save(); err != nil {
			s.logger.Warn("Failed to save user store", "error", err)
		}
	}

	s.securityLogger.LogUserCreated(req.Username, adminUser)

	// Record audit event for user creation
	s.recordAudit(audit.EventUserCreate, adminUser, clientIP, req.Username, "user_create", "success", map[string]string{
		"roles": fmt.Sprintf("%v", req.Roles),
	})

	resp := protocol.EncodeBinaryUserInfo(&protocol.BinaryUserInfo{
		Username: req.Username,
		Roles:    req.Roles,
		Enabled:  true,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpUserCreate, resp)
}

// handleUserDelete deletes a user.
func (s *Server) handleUserDelete(w io.Writer, payload []byte) error {
	// Get connection info for audit
	currentUser := s.getConnUsername(w)
	clientIP := ""
	if conn, ok := w.(net.Conn); ok {
		clientIP = getClientIP(conn)
	}

	if err := s.checkUserManagementAuth(w); err != nil {
		s.recordAudit(audit.EventAccessDenied, currentUser, clientIP, "", "user_delete", "denied", nil)
		return s.sendErrorResponse(w, err)
	}

	req, err := protocol.DecodeBinaryStringRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid request: %w", err))
	}
	username := req.Value

	// Prevent deleting yourself
	if username == currentUser {
		return s.sendErrorResponse(w, fmt.Errorf("cannot delete your own account"))
	}

	// In cluster mode, route through Raft consensus
	if s.broker.IsClusterMode() {
		if err := s.broker.ProposeDeleteUser(username); err != nil {
			s.recordAudit(audit.EventUserDelete, currentUser, clientIP, username, "user_delete", "failure", map[string]string{
				"error": err.Error(),
			})
			return s.sendErrorResponse(w, err)
		}
	} else {
		// Standalone mode - delete directly
		if err := s.authorizer.UserStore().DeleteUser(username); err != nil {
			s.recordAudit(audit.EventUserDelete, currentUser, clientIP, username, "user_delete", "failure", map[string]string{
				"error": err.Error(),
			})
			return s.sendErrorResponse(w, err)
		}
		if err := s.authorizer.UserStore().Save(); err != nil {
			s.logger.Warn("Failed to save user store", "error", err)
		}
	}

	s.securityLogger.LogUserDeleted(username, currentUser)

	// Record audit event for user deletion
	s.recordAudit(audit.EventUserDelete, currentUser, clientIP, username, "user_delete", "success", nil)

	resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{
		Success: true,
		Message: username,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpUserDelete, resp)
}

// handleUserUpdate updates a user's roles or enabled status.
func (s *Server) handleUserUpdate(w io.Writer, payload []byte) error {
	if err := s.checkUserManagementAuth(w); err != nil {
		return s.sendErrorResponse(w, err)
	}

	req, err := protocol.DecodeBinaryUserRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid request: %w", err))
	}

	// In cluster mode, route through Raft consensus
	if s.broker.IsClusterMode() {
		// No password change in this handler, so pass empty string
		var enabled *bool
		if req.Enabled {
			e := true
			enabled = &e
		}
		if err := s.broker.ProposeUpdateUser(req.Username, req.Roles, enabled, ""); err != nil {
			return s.sendErrorResponse(w, err)
		}
	} else {
		// Standalone mode - update directly
		if req.Roles != nil {
			if err := s.authorizer.UserStore().SetUserRoles(req.Username, req.Roles); err != nil {
				return s.sendErrorResponse(w, err)
			}
		}

		if err := s.authorizer.UserStore().SetUserEnabled(req.Username, req.Enabled); err != nil {
			return s.sendErrorResponse(w, err)
		}

		if err := s.authorizer.UserStore().Save(); err != nil {
			s.logger.Warn("Failed to save user store", "error", err)
		}
	}

	s.securityLogger.LogUserUpdated(req.Username, s.getConnUsername(w))

	resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{
		Success: true,
		Message: req.Username,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpUserUpdate, resp)
}

// handleUserList lists all users.
func (s *Server) handleUserList(w io.Writer) error {
	if err := s.checkUserManagementAuth(w); err != nil {
		return s.sendErrorResponse(w, err)
	}

	usernames := s.authorizer.UserStore().ListUsers()
	users := make([]protocol.BinaryUserInfo, 0, len(usernames))

	for _, username := range usernames {
		if user, exists := s.authorizer.UserStore().GetUser(username); exists {
			users = append(users, protocol.BinaryUserInfo{
				Username:  user.Username,
				Roles:     user.Roles,
				Enabled:   user.Enabled,
				CreatedAt: user.CreatedAt.Unix(),
				UpdatedAt: user.UpdatedAt.Unix(),
			})
		}
	}

	resp := protocol.EncodeBinaryUserListResponse(&protocol.BinaryUserListResponse{Users: users})
	return protocol.WriteBinaryMessage(w, protocol.OpUserList, resp)
}

// handleUserGet gets details for a specific user.
func (s *Server) handleUserGet(w io.Writer, payload []byte) error {
	if err := s.checkUserManagementAuth(w); err != nil {
		return s.sendErrorResponse(w, err)
	}

	req, err := protocol.DecodeBinaryStringRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid request: %w", err))
	}
	username := req.Value

	user, exists := s.authorizer.UserStore().GetUser(username)
	if !exists {
		return s.sendErrorResponse(w, fmt.Errorf("user %q not found", username))
	}

	perms := s.authorizer.UserStore().GetUserPermissions(username)
	permStrings := make([]string, len(perms))
	for i, p := range perms {
		permStrings[i] = string(p)
	}

	resp := protocol.EncodeBinaryUserInfo(&protocol.BinaryUserInfo{
		Username:    user.Username,
		Roles:       user.Roles,
		Permissions: permStrings,
		Enabled:     user.Enabled,
		CreatedAt:   user.CreatedAt.Unix(),
		UpdatedAt:   user.UpdatedAt.Unix(),
	})
	return protocol.WriteBinaryMessage(w, protocol.OpUserGet, resp)
}

// handlePasswordChange changes a user's password.
func (s *Server) handlePasswordChange(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryUserRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid request: %w", err))
	}

	currentUser := s.getConnUsername(w)

	// Users can change their own password with old password verification
	// Admins can change any user's password without old password
	if req.Username == currentUser {
		// Verify old password (OldPassword stored in Password field for this operation)
		if _, err := s.authorizer.Authenticate(currentUser, req.OldPassword); err != nil {
			return s.sendErrorResponse(w, fmt.Errorf("invalid current password"))
		}
	} else {
		// Must be admin to change other users' passwords
		if err := s.checkUserManagementAuth(w); err != nil {
			return s.sendErrorResponse(w, err)
		}
	}

	// In cluster mode, route through Raft consensus
	if s.broker.IsClusterMode() {
		// Hash password before replicating
		passwordHash, err := auth.HashPassword(req.Password)
		if err != nil {
			return s.sendErrorResponse(w, fmt.Errorf("failed to hash password: %w", err))
		}
		// Use ProposeUpdateUser with only password hash set
		if err := s.broker.ProposeUpdateUser(req.Username, nil, nil, passwordHash); err != nil {
			return s.sendErrorResponse(w, err)
		}
	} else {
		// Standalone mode - update directly
		if err := s.authorizer.UserStore().UpdatePassword(req.Username, req.Password); err != nil {
			return s.sendErrorResponse(w, err)
		}

		if err := s.authorizer.UserStore().Save(); err != nil {
			s.logger.Warn("Failed to save user store", "error", err)
		}
	}

	resp := protocol.EncodeBinarySuccessResponse(&protocol.BinarySuccessResponse{
		Success: true,
		Message: req.Username,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpPasswordChange, resp)
}

// ============================================================================
// ACL Management Handlers
// ============================================================================

// handleACLSet sets an ACL for a topic.
func (s *Server) handleACLSet(w io.Writer, payload []byte) error {
	if err := s.checkUserManagementAuth(w); err != nil {
		return s.sendErrorResponse(w, err)
	}

	req, err := protocol.DecodeBinaryACLRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid request: %w", err))
	}

	// Validate: if public is true, allow_anonymous must be enabled
	if req.Public && !s.authorizer.AllowAnonymous() {
		return s.sendErrorResponse(w, fmt.Errorf("cannot make topic public because anonymous access is disabled in server configuration"))
	}

	// In cluster mode, route through Raft consensus
	if s.broker.IsClusterMode() {
		if err := s.broker.ProposeSetACL(req.Topic, req.Public, req.AllowedUsers, req.AllowedRoles); err != nil {
			return s.sendErrorResponse(w, err)
		}
	} else {
		// Standalone mode - set directly
		acl := &auth.TopicACL{
			Topic:        req.Topic,
			Public:       req.Public,
			AllowedUsers: req.AllowedUsers,
			AllowedRoles: req.AllowedRoles,
		}
		s.authorizer.ACLStore().SetTopicACL(acl)

		if err := s.authorizer.ACLStore().Save(); err != nil {
			s.logger.Warn("Failed to save ACL store", "error", err)
		}
	}

	resp := protocol.EncodeBinaryACLResponse(&protocol.BinaryACLResponse{
		Success: true,
		Topic:   req.Topic,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpACLSet, resp)
}

// handleACLGet gets the ACL for a topic.
func (s *Server) handleACLGet(w io.Writer, payload []byte) error {
	if err := s.checkUserManagementAuth(w); err != nil {
		return s.sendErrorResponse(w, err)
	}

	req, err := protocol.DecodeBinaryStringRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid request: %w", err))
	}
	topic := req.Value

	acl, exists := s.authorizer.ACLStore().GetTopicACL(topic)
	if !exists {
		resp := protocol.EncodeBinaryACLInfo(&protocol.BinaryACLInfo{
			Topic:         topic,
			Exists:        false,
			DefaultPublic: s.authorizer.ACLStore().DefaultPublic,
		})
		return protocol.WriteBinaryMessage(w, protocol.OpACLGet, resp)
	}

	resp := protocol.EncodeBinaryACLInfo(&protocol.BinaryACLInfo{
		Topic:        acl.Topic,
		Exists:       true,
		Public:       acl.Public,
		AllowedUsers: acl.AllowedUsers,
		AllowedRoles: acl.AllowedRoles,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpACLGet, resp)
}

// handleACLDelete deletes the ACL for a topic.
func (s *Server) handleACLDelete(w io.Writer, payload []byte) error {
	if err := s.checkUserManagementAuth(w); err != nil {
		return s.sendErrorResponse(w, err)
	}

	req, err := protocol.DecodeBinaryStringRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid request: %w", err))
	}
	topic := req.Value

	// In cluster mode, route through Raft consensus
	if s.broker.IsClusterMode() {
		if err := s.broker.ProposeDeleteACL(topic); err != nil {
			return s.sendErrorResponse(w, err)
		}
	} else {
		// Standalone mode - delete directly
		s.authorizer.ACLStore().DeleteTopicACL(topic)

		if err := s.authorizer.ACLStore().Save(); err != nil {
			s.logger.Warn("Failed to save ACL store", "error", err)
		}
	}

	resp := protocol.EncodeBinaryACLResponse(&protocol.BinaryACLResponse{
		Success: true,
		Topic:   topic,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpACLDelete, resp)
}

// handleACLList lists all ACLs.
func (s *Server) handleACLList(w io.Writer) error {
	if err := s.checkUserManagementAuth(w); err != nil {
		return s.sendErrorResponse(w, err)
	}

	acls := s.authorizer.ACLStore().ListTopicACLs()
	aclList := make([]protocol.BinaryACLInfo, 0, len(acls))

	for _, acl := range acls {
		aclList = append(aclList, protocol.BinaryACLInfo{
			Topic:        acl.Topic,
			Exists:       true,
			Public:       acl.Public,
			AllowedUsers: acl.AllowedUsers,
			AllowedRoles: acl.AllowedRoles,
		})
	}

	resp := protocol.EncodeBinaryACLListResponse(&protocol.BinaryACLListResponse{
		ACLs:          aclList,
		DefaultPublic: s.authorizer.ACLStore().DefaultPublic,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpACLList, resp)
}

// handleRoleList lists all available roles.
func (s *Server) handleRoleList(w io.Writer) error {
	// Role listing is available to any authenticated user
	// (no admin check required - users need to know what roles exist)

	roles := s.authorizer.UserStore().ListRoles()
	roleList := make([]protocol.BinaryRoleInfo, 0, len(roles))

	for _, role := range roles {
		permissions := make([]string, len(role.Permissions))
		for i, p := range role.Permissions {
			permissions[i] = string(p)
		}
		roleList = append(roleList, protocol.BinaryRoleInfo{
			Name:        role.Name,
			Permissions: permissions,
			Description: role.Description,
		})
	}

	resp := protocol.EncodeBinaryRoleListResponse(&protocol.BinaryRoleListResponse{Roles: roleList})
	return protocol.WriteBinaryMessage(w, protocol.OpRoleList, resp)
}

// handleClusterMetadata returns partition-to-node mappings for smart client routing.
// This enables clients to route requests directly to partition leaders.
func (s *Server) handleClusterMetadata(w io.Writer, payload []byte) error {
	req, err := protocol.DecodeBinaryClusterMetadataRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid request: %w", err))
	}

	metadata, err := s.broker.GetClusterMetadata(req.Topic)
	if err != nil {
		return s.sendErrorResponse(w, err)
	}

	resp := protocol.EncodeBinaryClusterMetadataResponse(metadata)
	return protocol.WriteBinaryMessage(w, protocol.OpClusterMetadata, resp)
}

// ============================================================================
// Audit Trail Handlers
// ============================================================================

// handleAuditQuery handles audit event query requests.
// Only admin users can query audit events.
func (s *Server) handleAuditQuery(w io.Writer, payload []byte) error {
	// Check admin authorization
	if err := s.checkUserManagementAuth(w); err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("admin access required for audit queries"))
	}

	// Check if audit is enabled
	if s.auditStore == nil {
		return s.sendErrorResponse(w, fmt.Errorf("audit trail is not enabled"))
	}

	// Decode request
	req, err := protocol.DecodeBinaryAuditQueryRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid audit query request: %w", err))
	}

	// Build query filter
	filter := &audit.QueryFilter{
		User:     req.User,
		Resource: req.Resource,
		Result:   req.Result,
		Search:   req.Search,
		Limit:    int(req.Limit),
		Offset:   int(req.Offset),
	}

	// Set time range if provided
	if req.StartTime > 0 {
		t := time.Unix(req.StartTime, 0)
		filter.StartTime = &t
	}
	if req.EndTime > 0 {
		t := time.Unix(req.EndTime, 0)
		filter.EndTime = &t
	}

	// Convert event types
	for _, et := range req.EventTypes {
		filter.EventTypes = append(filter.EventTypes, audit.EventType(et))
	}

	// Execute query
	result, err := s.auditStore.Query(filter)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("audit query failed: %w", err))
	}

	// Convert to binary response
	binaryEvents := make([]protocol.BinaryAuditEvent, len(result.Events))
	for i, event := range result.Events {
		binaryEvents[i] = protocol.BinaryAuditEvent{
			ID:        event.ID,
			Timestamp: event.Timestamp.UnixMilli(),
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

	resp := protocol.EncodeBinaryAuditQueryResponse(&protocol.BinaryAuditQueryResponse{
		Events:     binaryEvents,
		TotalCount: int32(result.TotalCount),
		HasMore:    result.HasMore,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpAuditQuery, resp)
}

// handleAuditExport handles audit event export requests.
// Only admin users can export audit events.
func (s *Server) handleAuditExport(w io.Writer, payload []byte) error {
	// Check admin authorization
	if err := s.checkUserManagementAuth(w); err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("admin access required for audit export"))
	}

	// Check if audit is enabled
	if s.auditStore == nil {
		return s.sendErrorResponse(w, fmt.Errorf("audit trail is not enabled"))
	}

	// Decode request
	req, err := protocol.DecodeBinaryAuditExportRequest(payload)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("invalid audit export request: %w", err))
	}

	// Build query filter
	filter := &audit.QueryFilter{
		User:     req.Query.User,
		Resource: req.Query.Resource,
		Result:   req.Query.Result,
		Search:   req.Query.Search,
	}

	// Set time range if provided
	if req.Query.StartTime > 0 {
		t := time.Unix(req.Query.StartTime, 0)
		filter.StartTime = &t
	}
	if req.Query.EndTime > 0 {
		t := time.Unix(req.Query.EndTime, 0)
		filter.EndTime = &t
	}

	// Convert event types
	for _, et := range req.Query.EventTypes {
		filter.EventTypes = append(filter.EventTypes, audit.EventType(et))
	}

	// Determine export format
	format := audit.ExportJSON
	if req.Format == "csv" {
		format = audit.ExportCSV
	}

	// Execute export
	data, err := s.auditStore.Export(filter, format)
	if err != nil {
		return s.sendErrorResponse(w, fmt.Errorf("audit export failed: %w", err))
	}

	resp := protocol.EncodeBinaryAuditExportResponse(&protocol.BinaryAuditExportResponse{
		Format: req.Format,
		Data:   data,
	})
	return protocol.WriteBinaryMessage(w, protocol.OpAuditExport, resp)
}

// sendErrorResponse sends an error response to the client.
func (s *Server) sendErrorResponse(w io.Writer, err error) error {
	resp := protocol.EncodeBinaryErrorResponse(&protocol.BinaryErrorResponse{
		Code:    1,
		Message: err.Error(),
	})
	return protocol.WriteBinaryMessage(w, protocol.OpError, resp)
}

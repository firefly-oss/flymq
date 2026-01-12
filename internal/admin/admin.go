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
Package admin provides a REST API for FlyMQ cluster management.

ENDPOINTS:
==========
Cluster:

	GET  /api/v1/cluster       - Get cluster info
	GET  /api/v1/nodes         - List nodes

Topics:

	GET  /api/v1/topics        - List topics
	POST /api/v1/topics        - Create topic
	GET  /api/v1/topics/{name} - Get topic info
	DELETE /api/v1/topics/{name} - Delete topic

Consumer Groups:

	GET  /api/v1/consumers     - List consumer groups
	GET  /api/v1/consumers/{name} - Get consumer group
	DELETE /api/v1/consumers/{name} - Delete consumer group

Schemas:

	GET  /api/v1/schemas       - List schemas
	POST /api/v1/schemas       - Register schema
	GET  /api/v1/schemas/{name} - Get schema
	DELETE /api/v1/schemas/{name} - Delete schema

AUTHENTICATION:
===============
When enabled, requires Bearer token in Authorization header.
*/
package admin

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"flymq/internal/auth"
	"flymq/internal/config"
	"flymq/internal/logging"
)

// Server provides the Admin REST API.
type Server struct {
	config     *config.AdminConfig
	server     *http.Server
	logger     *logging.Logger
	handler    AdminHandler
	authorizer *auth.Authorizer
	authMw     *AuthMiddleware
}

// AdminHandler interface for admin operations.
type AdminHandler interface {
	// Cluster operations
	GetClusterInfo() (*ClusterInfo, error)
	GetNodes() ([]NodeInfo, error)

	// Topic operations
	ListTopics() ([]TopicInfo, error)
	CreateTopic(name string, opts TopicOptions) error
	DeleteTopic(name string) error
	GetTopicInfo(name string) (*TopicInfo, error)

	// Consumer group operations
	ListConsumerGroups() ([]ConsumerGroupInfo, error)
	GetConsumerGroup(name string) (*ConsumerGroupInfo, error)
	DeleteConsumerGroup(name string) error

	// Schema operations
	ListSchemas() ([]SchemaInfo, error)
	GetSchema(name string) (*SchemaInfo, error)
	RegisterSchema(name string, schema []byte) error
	DeleteSchema(name string) error

	// User operations
	ListUsers() ([]UserInfo, error)
	GetUser(username string) (*UserInfo, error)
	CreateUser(username, password string, roles []string) error
	UpdateUser(username string, roles []string, enabled *bool) error
	DeleteUser(username string) error
	ChangePassword(username, oldPassword, newPassword string) error

	// ACL operations
	ListACLs() ([]ACLInfo, error)
	GetACL(topic string) (*ACLInfo, error)
	SetACL(topic string, public bool, allowedUsers, allowedRoles []string) error
	DeleteACL(topic string) error

	// Role operations
	ListRoles() ([]RoleInfo, error)

	// DLQ operations
	GetDLQMessages(topic string, maxMessages int) ([]DLQMessage, error)
	ReplayDLQMessage(topic, messageID string) error
	PurgeDLQ(topic string) error

	// Metrics operations
	GetMetrics() (string, error)      // Prometheus format
	GetStats() (*StatsInfo, error)    // Rich JSON format
}

// ClusterInfo represents cluster information.
type ClusterInfo struct {
	ClusterID       string     `json:"cluster_id"`
	Version         string     `json:"version"`
	NodeCount       int        `json:"node_count"`
	LeaderID        string     `json:"leader_id"`
	RaftTerm        uint64     `json:"raft_term"`
	RaftCommitIndex uint64     `json:"raft_commit_index"`
	TopicCount      int        `json:"topic_count"`
	TotalMessages   int64      `json:"total_messages"`
	MessageRate     float64    `json:"message_rate"`
	Uptime          string     `json:"uptime"`
	Nodes           []NodeInfo `json:"nodes,omitempty"`
}

// NodeInfo represents detailed node information.
type NodeInfo struct {
	ID          string     `json:"id"`
	Address     string     `json:"address"`
	ClusterAddr string     `json:"cluster_addr"`
	AdminAddr   string     `json:"admin_addr,omitempty"`
	State       string     `json:"state"`
	RaftState   string     `json:"raft_state"`
	IsLeader    bool       `json:"is_leader"`
	LastSeen    string     `json:"last_seen"`
	Uptime      string     `json:"uptime,omitempty"`
	JoinedAt    string     `json:"joined_at,omitempty"`
	Stats       *NodeStats `json:"stats,omitempty"`
}

// NodeStats contains runtime statistics for a node.
type NodeStats struct {
	MemoryUsedMB     float64 `json:"memory_used_mb"`
	MemoryAllocMB    float64 `json:"memory_alloc_mb"`
	MemorySysMB      float64 `json:"memory_sys_mb"`
	Goroutines       int     `json:"goroutines"`
	NumGC            uint32  `json:"num_gc"`
	TopicCount       int     `json:"topic_count"`
	PartitionCount   int     `json:"partition_count"`
	MessagesReceived int64   `json:"messages_received"`
	MessagesSent     int64   `json:"messages_sent"`
	BytesReceived    int64   `json:"bytes_received"`
	BytesSent        int64   `json:"bytes_sent"`
}

// TopicInfo represents topic information.
type TopicInfo struct {
	Name           string `json:"name"`
	Partitions     int    `json:"partitions"`
	ReplicaFactor  int    `json:"replica_factor"`
	MessageCount   int64  `json:"message_count"`
	RetentionBytes int64  `json:"retention_bytes"`
	RetentionMs    int64  `json:"retention_ms"`
	CreatedAt      string `json:"created_at"`
}

// TopicOptions represents options for creating a topic.
type TopicOptions struct {
	Partitions     int   `json:"partitions"`
	ReplicaFactor  int   `json:"replica_factor"`
	RetentionBytes int64 `json:"retention_bytes"`
	RetentionMs    int64 `json:"retention_ms"`
}

// ConsumerGroupInfo represents consumer group information.
type ConsumerGroupInfo struct {
	Name       string         `json:"name"`
	Topic      string         `json:"topic"`
	Members    int            `json:"members"`
	State      string         `json:"state"`
	Lag        int64          `json:"lag"`
	Partitions []PartitionLag `json:"partitions,omitempty"`
}

// PartitionLag represents lag for a partition.
type PartitionLag struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
	Lag       int64 `json:"lag"`
}

// SchemaInfo represents schema information.
type SchemaInfo struct {
	Name      string `json:"name"`
	Version   int    `json:"version"`
	Type      string `json:"type"`
	Schema    string `json:"schema"`
	CreatedAt string `json:"created_at"`
}

// UserInfo represents user information.
type UserInfo struct {
	Username    string   `json:"username"`
	Roles       []string `json:"roles"`
	Permissions []string `json:"permissions,omitempty"`
	Enabled     bool     `json:"enabled"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at,omitempty"`
}

// CreateUserRequest represents a request to create a user.
type CreateUserRequest struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	Roles    []string `json:"roles"`
}

// UpdateUserRequest represents a request to update a user.
type UpdateUserRequest struct {
	Roles   []string `json:"roles,omitempty"`
	Enabled *bool    `json:"enabled,omitempty"`
}

// ChangePasswordRequest represents a request to change password.
type ChangePasswordRequest struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

// ACLInfo represents topic ACL information.
type ACLInfo struct {
	Topic        string   `json:"topic"`
	Public       bool     `json:"public"`
	AllowedUsers []string `json:"allowed_users"`
	AllowedRoles []string `json:"allowed_roles"`
}

// SetACLRequest represents a request to set topic ACL.
type SetACLRequest struct {
	Public       bool     `json:"public"`
	AllowedUsers []string `json:"allowed_users"`
	AllowedRoles []string `json:"allowed_roles"`
}

// RoleInfo represents role information.
type RoleInfo struct {
	Name        string   `json:"name"`
	Permissions []string `json:"permissions"`
	Description string   `json:"description"`
}

// DLQMessage represents a dead letter queue message.
type DLQMessage struct {
	ID        string `json:"id"`
	Topic     string `json:"topic"`
	Data      string `json:"data"` // base64 encoded
	Error     string `json:"error"`
	Retries   int    `json:"retries"`
	Timestamp string `json:"timestamp"`
}

// MetricsResponse represents metrics in Prometheus format.
type MetricsResponse struct {
	Metrics string `json:"metrics"`
}

// StatsInfo represents rich statistics for the Admin API.
type StatsInfo struct {
	// Cluster overview
	Cluster ClusterStats `json:"cluster"`

	// Per-node statistics
	Nodes []NodeStatsInfo `json:"nodes"`

	// Per-topic statistics
	Topics []TopicStatsInfo `json:"topics"`

	// Consumer group statistics
	ConsumerGroups []ConsumerGroupStats `json:"consumer_groups"`

	// System statistics
	System SystemStats `json:"system"`
}

// ClusterStats contains cluster-level statistics.
type ClusterStats struct {
	NodeCount          int     `json:"node_count"`
	HealthyNodes       int     `json:"healthy_nodes"`
	TopicCount         int     `json:"topic_count"`
	TotalPartitions    int     `json:"total_partitions"`
	TotalMessages      int64   `json:"total_messages"`
	MessagesPerSecond  float64 `json:"messages_per_second"`
	BytesPerSecond     float64 `json:"bytes_per_second"`
	ActiveConnections  int     `json:"active_connections"`
	ConsumerGroupCount int     `json:"consumer_group_count"`
}

// NodeStatsInfo contains statistics for a single node.
type NodeStatsInfo struct {
	NodeID           string  `json:"node_id"`
	Address          string  `json:"address"`
	State            string  `json:"state"`
	IsLeader         bool    `json:"is_leader"`
	CPUPercent       float64 `json:"cpu_percent"`
	MemoryUsedMB     float64 `json:"memory_used_mb"`
	MemoryTotalMB    float64 `json:"memory_total_mb"`
	Goroutines       int     `json:"goroutines"`
	MessagesReceived int64   `json:"messages_received"`
	MessagesSent     int64   `json:"messages_sent"`
	BytesReceived    int64   `json:"bytes_received"`
	BytesSent        int64   `json:"bytes_sent"`
	Uptime           string  `json:"uptime"`
}

// TopicStatsInfo contains statistics for a single topic.
type TopicStatsInfo struct {
	Name              string  `json:"name"`
	Partitions        int     `json:"partitions"`
	ReplicationFactor int     `json:"replication_factor"`
	MessageCount      int64   `json:"message_count"`
	SizeBytes         int64   `json:"size_bytes"`
	MessagesPerSecond float64 `json:"messages_per_second"`
	BytesPerSecond    float64 `json:"bytes_per_second"`
	OldestMessage     string  `json:"oldest_message,omitempty"`
	NewestMessage     string  `json:"newest_message,omitempty"`
}

// ConsumerGroupStats contains statistics for a consumer group.
type ConsumerGroupStats struct {
	GroupID        string `json:"group_id"`
	Topic          string `json:"topic"`
	Members        int    `json:"members"`
	State          string `json:"state"`
	TotalLag       int64  `json:"total_lag"`
	LastCommitTime string `json:"last_commit_time,omitempty"`
}

// SystemStats contains system-level statistics.
type SystemStats struct {
	UptimeSeconds   int64   `json:"uptime_seconds"`
	StartTime       string  `json:"start_time"`
	GoVersion       string  `json:"go_version"`
	NumCPU          int     `json:"num_cpu"`
	MemoryAllocMB   float64 `json:"memory_alloc_mb"`
	MemoryTotalMB   float64 `json:"memory_total_mb"`
	MemorySysMB     float64 `json:"memory_sys_mb"`
	Goroutines      int     `json:"goroutines"`
	NumGC           uint32  `json:"num_gc"`
	GCPauseMs       float64 `json:"gc_pause_ms"`
	OpenFiles       int     `json:"open_files"`
}

// NewServer creates a new admin API server.
func NewServer(cfg *config.AdminConfig, handler AdminHandler) *Server {
	return &Server{
		config:  cfg,
		handler: handler,
		logger:  logging.NewLogger("admin"),
	}
}

// SetAuthorizer sets the authorizer for authentication/authorization.
func (s *Server) SetAuthorizer(a *auth.Authorizer) {
	s.authorizer = a
	s.authMw = NewAuthMiddleware(a, s.config.AuthEnabled)
}

// Start starts the admin API server.
func (s *Server) Start() error {
	if !s.config.Enabled {
		s.logger.Info("Admin API server disabled")
		return nil
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.server = &http.Server{
		Addr:    s.config.Addr,
		Handler: mux,
	}

	// Configure TLS if enabled
	if s.config.TLSEnabled {
		certFile := s.config.TLSCertFile
		keyFile := s.config.TLSKeyFile

		// Auto-generate self-signed certificate if requested
		if s.config.TLSAutoGenerate {
			var err error
			certFile, keyFile, err = s.generateSelfSignedCert()
			if err != nil {
				return err
			}
			s.logger.Info("Generated self-signed certificate for Admin API",
				"cert", certFile, "key", keyFile)
		}

		go func() {
			s.logger.Info("Starting admin API server (HTTPS)", "addr", s.config.Addr)
			if err := s.server.ListenAndServeTLS(certFile, keyFile); err != nil && err != http.ErrServerClosed {
				s.logger.Error("Admin API server error", "error", err)
			}
		}()
	} else {
		go func() {
			s.logger.Info("Starting admin API server (HTTP)", "addr", s.config.Addr)
			if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.logger.Error("Admin API server error", "error", err)
			}
		}()
	}

	return nil
}

// generateSelfSignedCert generates a self-signed certificate for the admin API.
// Returns paths to the generated cert and key files.
func (s *Server) generateSelfSignedCert() (certFile, keyFile string, err error) {
	// Generate RSA key pair
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	// Create certificate template
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", "", err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"FlyMQ"},
			CommonName:   "FlyMQ Admin API",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // Valid for 1 year
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Add localhost and common IPs as SANs
	template.DNSNames = []string{"localhost", "flymq"}
	template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}

	// Create certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privKey.PublicKey, privKey)
	if err != nil {
		return "", "", err
	}

	// Determine output directory (use temp dir)
	tempDir := os.TempDir()
	certFile = filepath.Join(tempDir, "flymq-admin.crt")
	keyFile = filepath.Join(tempDir, "flymq-admin.key")

	// Write certificate
	certOut, err := os.Create(certFile)
	if err != nil {
		return "", "", err
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return "", "", err
	}

	// Write private key
	keyOut, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return "", "", err
	}
	defer keyOut.Close()
	privKeyBytes := x509.MarshalPKCS1PrivateKey(privKey)
	if err := pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: privKeyBytes}); err != nil {
		return "", "", err
	}

	return certFile, keyFile, nil
}

// Stop stops the admin API server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.logger.Info("Stopping admin API server")
	return s.server.Shutdown(ctx)
}

// registerRoutes registers all API routes with appropriate authentication.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	// Create a wrapper function if auth middleware is not set
	wrap := func(perm Permission, h http.HandlerFunc) http.HandlerFunc {
		if s.authMw == nil {
			return h
		}
		return s.authMw.RequirePermission(perm, h)
	}

	wrapAdminOrSelf := func(extractUsername func(r *http.Request) string, h http.HandlerFunc) http.HandlerFunc {
		if s.authMw == nil {
			return h
		}
		return s.authMw.RequireAdminOrSelf(extractUsername, h)
	}

	// Public endpoints - no auth required (basic health check)
	mux.HandleFunc("/api/v1/health", s.handleHealthCheck)

	// Swagger documentation - public (multiple paths for convenience)
	mux.HandleFunc("/api/v1/swagger.json", s.handleSwaggerJSON)
	mux.HandleFunc("/swagger.json", s.handleSwaggerJSON)
	mux.HandleFunc("/api/v1/swagger/", s.handleSwaggerUI)
	mux.HandleFunc("/swagger/", s.handleSwaggerUI)

	// Cluster info - read permission required when auth enabled
	mux.HandleFunc("/api/v1/cluster", wrap(PermissionRead, s.handleCluster))
	mux.HandleFunc("/api/v1/cluster/nodes", wrap(PermissionRead, s.handleNodes))

	// Metrics endpoints - admin permission required
	mux.HandleFunc("/api/v1/metrics", wrap(PermissionAdmin, s.handleMetrics))
	mux.HandleFunc("/api/v1/stats", wrap(PermissionAdmin, s.handleStats))

	// Topic routes - read for GET, admin for POST/DELETE
	mux.HandleFunc("/api/v1/topics", s.handleTopicsWithAuth)
	mux.HandleFunc("/api/v1/topics/", s.handleTopicWithAuth)

	// Consumer group routes - read for GET, admin for DELETE
	mux.HandleFunc("/api/v1/consumer-groups", wrap(PermissionRead, s.handleConsumerGroups))
	mux.HandleFunc("/api/v1/consumer-groups/", s.handleConsumerGroupWithAuth)

	// Schema routes - read for GET, admin for POST/DELETE
	mux.HandleFunc("/api/v1/schemas", s.handleSchemasWithAuth)
	mux.HandleFunc("/api/v1/schemas/", s.handleSchemaWithAuth)

	// User routes - admin only (except password change for self)
	mux.HandleFunc("/api/v1/users", wrap(PermissionAdmin, s.handleUsers))
	mux.HandleFunc("/api/v1/users/", s.handleUserWithAuth)

	// ACL routes - admin only
	mux.HandleFunc("/api/v1/acls", wrap(PermissionAdmin, s.handleACLs))
	mux.HandleFunc("/api/v1/acls/", wrap(PermissionAdmin, s.handleACL))

	// Role routes - admin only
	mux.HandleFunc("/api/v1/roles", wrap(PermissionAdmin, s.handleRoles))

	// DLQ routes - read for GET, admin for replay/purge
	mux.HandleFunc("/api/v1/dlq/", s.handleDLQWithAuth)

	// Keep the helper variable to silence unused warning
	_ = wrapAdminOrSelf
}

// handleCluster handles GET /api/v1/cluster
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	info, err := s.handler.GetClusterInfo()
	if err != nil {
		s.writeError(w, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, info)
}

// handleNodes handles GET /api/v1/cluster/nodes
func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodes, err := s.handler.GetNodes()
	if err != nil {
		s.writeError(w, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, nodes)
}

// handleTopics handles GET/POST /api/v1/topics
func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		topics, err := s.handler.ListTopics()
		if err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		s.writeJSON(w, topics)

	case http.MethodPost:
		var req struct {
			Name    string       `json:"name"`
			Options TopicOptions `json:"options"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, err, http.StatusBadRequest)
			return
		}
		if err := s.handler.CreateTopic(req.Name, req.Options); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleTopic handles GET/DELETE /api/v1/topics/{name}
func (s *Server) handleTopic(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/topics/")
	if name == "" {
		http.Error(w, "Topic name required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		info, err := s.handler.GetTopicInfo(name)
		if err != nil {
			s.writeError(w, err, http.StatusNotFound)
			return
		}
		s.writeJSON(w, info)

	case http.MethodDelete:
		if err := s.handler.DeleteTopic(name); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleConsumerGroups handles GET /api/v1/consumer-groups
func (s *Server) handleConsumerGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	groups, err := s.handler.ListConsumerGroups()
	if err != nil {
		s.writeError(w, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, groups)
}

// handleConsumerGroup handles GET/DELETE /api/v1/consumer-groups/{name}
func (s *Server) handleConsumerGroup(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/consumer-groups/")
	if name == "" {
		http.Error(w, "Consumer group name required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		info, err := s.handler.GetConsumerGroup(name)
		if err != nil {
			s.writeError(w, err, http.StatusNotFound)
			return
		}
		s.writeJSON(w, info)

	case http.MethodDelete:
		if err := s.handler.DeleteConsumerGroup(name); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleSchemas handles GET/POST /api/v1/schemas
func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		schemas, err := s.handler.ListSchemas()
		if err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		s.writeJSON(w, schemas)

	case http.MethodPost:
		var req struct {
			Name   string `json:"name"`
			Schema string `json:"schema"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, err, http.StatusBadRequest)
			return
		}
		if err := s.handler.RegisterSchema(req.Name, []byte(req.Schema)); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleSchema handles GET/DELETE /api/v1/schemas/{name}
func (s *Server) handleSchema(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/schemas/")
	if name == "" {
		http.Error(w, "Schema name required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		info, err := s.handler.GetSchema(name)
		if err != nil {
			s.writeError(w, err, http.StatusNotFound)
			return
		}
		s.writeJSON(w, info)

	case http.MethodDelete:
		if err := s.handler.DeleteSchema(name); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleUsers handles GET/POST /api/v1/users
func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		users, err := s.handler.ListUsers()
		if err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		s.writeJSON(w, users)

	case http.MethodPost:
		var req CreateUserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, err, http.StatusBadRequest)
			return
		}
		if err := s.handler.CreateUser(req.Username, req.Password, req.Roles); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleUser handles GET/PUT/DELETE /api/v1/users/{username}
func (s *Server) handleUser(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/users/")
	if path == "" {
		http.Error(w, "Username required", http.StatusBadRequest)
		return
	}

	// Check for password change endpoint: /api/v1/users/{username}/password
	if strings.HasSuffix(path, "/password") {
		username := strings.TrimSuffix(path, "/password")
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req ChangePasswordRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, err, http.StatusBadRequest)
			return
		}
		if err := s.handler.ChangePassword(username, req.OldPassword, req.NewPassword); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	username := path

	switch r.Method {
	case http.MethodGet:
		info, err := s.handler.GetUser(username)
		if err != nil {
			s.writeError(w, err, http.StatusNotFound)
			return
		}
		s.writeJSON(w, info)

	case http.MethodPut:
		var req UpdateUserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, err, http.StatusBadRequest)
			return
		}
		if err := s.handler.UpdateUser(username, req.Roles, req.Enabled); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		if err := s.handler.DeleteUser(username); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleACLs handles GET /api/v1/acls
func (s *Server) handleACLs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	acls, err := s.handler.ListACLs()
	if err != nil {
		s.writeError(w, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, acls)
}

// handleACL handles GET/PUT/DELETE /api/v1/acls/{topic}
func (s *Server) handleACL(w http.ResponseWriter, r *http.Request) {
	topic := strings.TrimPrefix(r.URL.Path, "/api/v1/acls/")
	if topic == "" {
		http.Error(w, "Topic name required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		info, err := s.handler.GetACL(topic)
		if err != nil {
			s.writeError(w, err, http.StatusNotFound)
			return
		}
		s.writeJSON(w, info)

	case http.MethodPut:
		var req SetACLRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeError(w, err, http.StatusBadRequest)
			return
		}
		if err := s.handler.SetACL(topic, req.Public, req.AllowedUsers, req.AllowedRoles); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		if err := s.handler.DeleteACL(topic); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleRoles handles GET /api/v1/roles
func (s *Server) handleRoles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	roles, err := s.handler.ListRoles()
	if err != nil {
		s.writeError(w, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, roles)
}

// handleDLQ handles GET/POST/DELETE /api/v1/dlq/{topic} and /api/v1/dlq/{topic}/replay/{messageId}
func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/dlq/")
	if path == "" {
		http.Error(w, "Topic name required", http.StatusBadRequest)
		return
	}

	// Check for replay endpoint: /api/v1/dlq/{topic}/replay/{messageId}
	if strings.Contains(path, "/replay/") {
		parts := strings.Split(path, "/replay/")
		if len(parts) != 2 {
			http.Error(w, "Invalid path", http.StatusBadRequest)
			return
		}
		topic := parts[0]
		messageID := parts[1]

		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err := s.handler.ReplayDLQMessage(topic, messageID); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	topic := path

	switch r.Method {
	case http.MethodGet:
		// Get max_messages from query param
		maxMessages := 100
		if max := r.URL.Query().Get("max_messages"); max != "" {
			if m, err := strconv.Atoi(max); err == nil && m > 0 {
				maxMessages = m
			}
		}
		msgs, err := s.handler.GetDLQMessages(topic, maxMessages)
		if err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		s.writeJSON(w, msgs)

	case http.MethodDelete:
		if err := s.handler.PurgeDLQ(topic); err != nil {
			s.writeError(w, err, http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleMetrics handles GET /api/v1/metrics (Prometheus format, requires admin)
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metrics, err := s.handler.GetMetrics()
	if err != nil {
		s.writeError(w, err, http.StatusInternalServerError)
		return
	}

	// Return metrics in Prometheus text format
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte(metrics))
}

// handleStats handles GET /api/v1/stats (rich JSON format, requires admin)
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats, err := s.handler.GetStats()
	if err != nil {
		s.writeError(w, err, http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, stats)
}

// handleHealthCheck handles GET /api/v1/health (public endpoint for basic health check)
func (s *Server) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Simple health response - public, no auth required
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"healthy"}`))
}

// writeJSON writes a JSON response.
func (s *Server) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

// writeError writes an error response.
func (s *Server) writeError(w http.ResponseWriter, err error, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

// ============================================================================
// Authenticated Handler Wrappers
// ============================================================================

// handleTopicsWithAuth handles topics with method-based permission checks.
func (s *Server) handleTopicsWithAuth(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// Read permission for GET
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionRead, s.handleTopics)(w, r)
		} else {
			s.handleTopics(w, r)
		}
	case http.MethodPost:
		// Admin permission for POST
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionAdmin, s.handleTopics)(w, r)
		} else {
			s.handleTopics(w, r)
		}
	default:
		s.handleTopics(w, r)
	}
}

// handleTopicWithAuth handles single topic with method-based permission checks.
func (s *Server) handleTopicWithAuth(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionRead, s.handleTopic)(w, r)
		} else {
			s.handleTopic(w, r)
		}
	case http.MethodDelete:
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionAdmin, s.handleTopic)(w, r)
		} else {
			s.handleTopic(w, r)
		}
	default:
		s.handleTopic(w, r)
	}
}

// handleConsumerGroupWithAuth handles consumer groups with method-based permission checks.
func (s *Server) handleConsumerGroupWithAuth(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionRead, s.handleConsumerGroup)(w, r)
		} else {
			s.handleConsumerGroup(w, r)
		}
	case http.MethodDelete:
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionAdmin, s.handleConsumerGroup)(w, r)
		} else {
			s.handleConsumerGroup(w, r)
		}
	default:
		s.handleConsumerGroup(w, r)
	}
}

// handleSchemasWithAuth handles schemas with method-based permission checks.
func (s *Server) handleSchemasWithAuth(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionRead, s.handleSchemas)(w, r)
		} else {
			s.handleSchemas(w, r)
		}
	case http.MethodPost:
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionAdmin, s.handleSchemas)(w, r)
		} else {
			s.handleSchemas(w, r)
		}
	default:
		s.handleSchemas(w, r)
	}
}

// handleSchemaWithAuth handles single schema with method-based permission checks.
func (s *Server) handleSchemaWithAuth(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionRead, s.handleSchema)(w, r)
		} else {
			s.handleSchema(w, r)
		}
	case http.MethodDelete:
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionAdmin, s.handleSchema)(w, r)
		} else {
			s.handleSchema(w, r)
		}
	default:
		s.handleSchema(w, r)
	}
}

// handleUserWithAuth handles user operations with appropriate permissions.
// Password change is allowed for self or admin; other operations require admin.
func (s *Server) handleUserWithAuth(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/users/")

	// Password change endpoint - allow self or admin
	if strings.HasSuffix(path, "/password") {
		extractUsername := func(r *http.Request) string {
			p := strings.TrimPrefix(r.URL.Path, "/api/v1/users/")
			return strings.TrimSuffix(p, "/password")
		}
		if s.authMw != nil {
			s.authMw.RequireAdminOrSelf(extractUsername, s.handleUser)(w, r)
		} else {
			s.handleUser(w, r)
		}
		return
	}

	// All other user operations require admin
	if s.authMw != nil {
		s.authMw.RequirePermission(PermissionAdmin, s.handleUser)(w, r)
	} else {
		s.handleUser(w, r)
	}
}

// handleDLQWithAuth handles DLQ with method-based permission checks.
func (s *Server) handleDLQWithAuth(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/dlq/")

	// Replay endpoint requires admin
	if strings.Contains(path, "/replay/") {
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionAdmin, s.handleDLQ)(w, r)
		} else {
			s.handleDLQ(w, r)
		}
		return
	}

	switch r.Method {
	case http.MethodGet:
		// Read permission for GET
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionRead, s.handleDLQ)(w, r)
		} else {
			s.handleDLQ(w, r)
		}
	case http.MethodDelete:
		// Admin permission for DELETE (purge)
		if s.authMw != nil {
			s.authMw.RequirePermission(PermissionAdmin, s.handleDLQ)(w, r)
		} else {
			s.handleDLQ(w, r)
		}
	default:
		s.handleDLQ(w, r)
	}
}

// ============================================================================
// Swagger Handlers
// ============================================================================

// handleSwaggerJSON serves the OpenAPI specification.
func (s *Server) handleSwaggerJSON(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(OpenAPISpec))
}

// handleSwaggerUI serves the Swagger UI.
func (s *Server) handleSwaggerUI(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Serve a simple HTML page that loads Swagger UI from CDN
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(swaggerUIHTML))
}

// swaggerUIHTML is the HTML template for Swagger UI.
const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FlyMQ Admin API - Swagger UI</title>
    <link rel="stylesheet" type="text/css" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
    <style>
        html { box-sizing: border-box; overflow-y: scroll; }
        *, *:before, *:after { box-sizing: inherit; }
        body { margin: 0; background: #fafafa; }
        .topbar { display: none; }
        .swagger-ui .info { margin: 20px 0; }
        .swagger-ui .info .title { font-size: 36px; }
    </style>
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" charset="UTF-8"></script>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
    <script>
        window.onload = function() {
            const ui = SwaggerUIBundle({
                url: window.location.origin + "/swagger.json",
                dom_id: '#swagger-ui',
                deepLinking: true,
                presets: [
                    SwaggerUIBundle.presets.apis,
                    SwaggerUIStandalonePreset
                ],
                plugins: [
                    SwaggerUIBundle.plugins.DownloadUrl
                ],
                layout: "StandaloneLayout"
            });
            window.ui = ui;
        };
    </script>
</body>
</html>`

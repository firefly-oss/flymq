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

package admin

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"flymq/internal/auth"
	"flymq/internal/config"
)

// mockHandler implements AdminHandler for testing
type mockHandler struct {
	topics         []TopicInfo
	consumerGroups []ConsumerGroupInfo
	schemas        []SchemaInfo
}

func (m *mockHandler) GetClusterInfo() (*ClusterInfo, error) {
	return &ClusterInfo{
		ClusterID:  "test-cluster",
		Version:    "1.0.0",
		NodeCount:  3,
		LeaderID:   "node-1",
		TopicCount: len(m.topics),
	}, nil
}

func (m *mockHandler) GetNodes() ([]NodeInfo, error) {
	return []NodeInfo{
		{ID: "node-1", Address: "localhost:9000", State: "active", IsLeader: true},
		{ID: "node-2", Address: "localhost:9001", State: "active", IsLeader: false},
	}, nil
}

func (m *mockHandler) ListTopics() ([]TopicInfo, error) {
	return m.topics, nil
}

func (m *mockHandler) CreateTopic(name string, opts TopicOptions) error {
	m.topics = append(m.topics, TopicInfo{Name: name, Partitions: opts.Partitions})
	return nil
}

func (m *mockHandler) DeleteTopic(name string) error {
	return nil
}

func (m *mockHandler) GetTopicInfo(name string) (*TopicInfo, error) {
	for _, t := range m.topics {
		if t.Name == name {
			return &t, nil
		}
	}
	return nil, nil
}

func (m *mockHandler) ListConsumerGroups() ([]ConsumerGroupInfo, error) {
	return m.consumerGroups, nil
}

func (m *mockHandler) GetConsumerGroup(name string) (*ConsumerGroupInfo, error) {
	for _, g := range m.consumerGroups {
		if g.Name == name {
			return &g, nil
		}
	}
	return nil, nil
}

func (m *mockHandler) DeleteConsumerGroup(name string) error {
	return nil
}

func (m *mockHandler) ListSchemas() ([]SchemaInfo, error) {
	return m.schemas, nil
}

func (m *mockHandler) GetSchema(name string) (*SchemaInfo, error) {
	for _, s := range m.schemas {
		if s.Name == name {
			return &s, nil
		}
	}
	return nil, nil
}

func (m *mockHandler) RegisterSchema(name string, schema []byte) error {
	m.schemas = append(m.schemas, SchemaInfo{Name: name, Schema: string(schema)})
	return nil
}

func (m *mockHandler) DeleteSchema(name string) error {
	return nil
}

// User management methods
func (m *mockHandler) ListUsers() ([]UserInfo, error) {
	return []UserInfo{}, nil
}

func (m *mockHandler) GetUser(username string) (*UserInfo, error) {
	return nil, nil
}

func (m *mockHandler) CreateUser(username, password string, roles []string) error {
	return nil
}

func (m *mockHandler) UpdateUser(username string, roles []string, enabled *bool) error {
	return nil
}

func (m *mockHandler) DeleteUser(username string) error {
	return nil
}

func (m *mockHandler) ChangePassword(username, oldPassword, newPassword string) error {
	return nil
}

// ACL management methods
func (m *mockHandler) ListACLs() ([]ACLInfo, error) {
	return []ACLInfo{}, nil
}

func (m *mockHandler) GetACL(topic string) (*ACLInfo, error) {
	return nil, nil
}

func (m *mockHandler) SetACL(topic string, public bool, allowedUsers, allowedRoles []string) error {
	return nil
}

func (m *mockHandler) DeleteACL(topic string) error {
	return nil
}

// Role management methods
func (m *mockHandler) ListRoles() ([]RoleInfo, error) {
	return []RoleInfo{}, nil
}

// DLQ management methods
func (m *mockHandler) GetDLQMessages(topic string, maxMessages int) ([]DLQMessage, error) {
	return []DLQMessage{}, nil
}

func (m *mockHandler) ReplayDLQMessage(topic, messageID string) error {
	return nil
}

func (m *mockHandler) PurgeDLQ(topic string) error {
	return nil
}

// Metrics methods
func (m *mockHandler) GetMetrics() (string, error) {
	return "", nil
}

func (m *mockHandler) GetStats() (*StatsInfo, error) {
	return &StatsInfo{
		Cluster: ClusterStats{
			NodeCount:          2,
			HealthyNodes:       2,
			TopicCount:         len(m.topics),
			ConsumerGroupCount: len(m.consumerGroups),
		},
	}, nil
}

// Partition management methods (horizontal scaling)
func (m *mockHandler) GetClusterMetadata(topic string) (*ClusterMetadataInfo, error) {
	return &ClusterMetadataInfo{
		ClusterID: "test-cluster",
		Topics:    []TopicPartitionMetadata{},
	}, nil
}

func (m *mockHandler) GetPartitionAssignments(topic string) ([]PartitionAssignmentInfo, error) {
	return []PartitionAssignmentInfo{}, nil
}

func (m *mockHandler) GetLeaderDistribution() (map[string]int, error) {
	return map[string]int{"node-1": 3, "node-2": 3}, nil
}

func (m *mockHandler) TriggerRebalance() (*RebalanceResult, error) {
	return &RebalanceResult{
		Success:    true,
		Message:    "Rebalance completed",
		Moves:      []PartitionMove{},
		OldLeaders: map[string]int{"node-1": 3, "node-2": 3},
		NewLeaders: map[string]int{"node-1": 3, "node-2": 3},
	}, nil
}

func (m *mockHandler) ReassignPartition(topic string, partition int, newLeader string, newReplicas []string) error {
	return nil
}

func TestNewServer(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true, Addr: ":8080"}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)

	if server == nil {
		t.Fatal("Expected non-nil server")
	}
}

func TestHandleCluster(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster", nil)
	w := httptest.NewRecorder()

	server.handleCluster(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var info ClusterInfo
	if err := json.NewDecoder(w.Body).Decode(&info); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if info.ClusterID != "test-cluster" {
		t.Errorf("Expected cluster ID 'test-cluster', got %s", info.ClusterID)
	}
}

func TestHandleNodes(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cluster/nodes", nil)
	w := httptest.NewRecorder()

	server.handleNodes(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var nodes []NodeInfo
	if err := json.NewDecoder(w.Body).Decode(&nodes); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}
}

func TestHandleTopics(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true}
	handler := &mockHandler{
		topics: []TopicInfo{
			{Name: "orders", Partitions: 3},
			{Name: "events", Partitions: 1},
		},
	}
	server := NewServer(cfg, handler)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/topics", nil)
	w := httptest.NewRecorder()

	server.handleTopics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var topics []TopicInfo
	if err := json.NewDecoder(w.Body).Decode(&topics); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(topics))
	}
}

func TestHandleTopicsCreate(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)

	body := `{"name": "new-topic", "options": {"partitions": 5}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topics", strings.NewReader(body))
	w := httptest.NewRecorder()

	server.handleTopics(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	if len(handler.topics) != 1 {
		t.Errorf("Expected 1 topic, got %d", len(handler.topics))
	}
}

func TestHandleConsumerGroups(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true}
	handler := &mockHandler{
		consumerGroups: []ConsumerGroupInfo{
			{Name: "group-1", Topic: "orders", Members: 3},
		},
	}
	server := NewServer(cfg, handler)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/consumer-groups", nil)
	w := httptest.NewRecorder()

	server.handleConsumerGroups(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var groups []ConsumerGroupInfo
	if err := json.NewDecoder(w.Body).Decode(&groups); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(groups) != 1 {
		t.Errorf("Expected 1 group, got %d", len(groups))
	}
}

func TestHandleSchemas(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true}
	handler := &mockHandler{
		schemas: []SchemaInfo{
			{Name: "order-schema", Version: 1, Type: "json"},
		},
	}
	server := NewServer(cfg, handler)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/schemas", nil)
	w := httptest.NewRecorder()

	server.handleSchemas(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var schemas []SchemaInfo
	if err := json.NewDecoder(w.Body).Decode(&schemas); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(schemas) != 1 {
		t.Errorf("Expected 1 schema, got %d", len(schemas))
	}
}

func TestHandleSchemasRegister(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)

	body := `{"name": "new-schema", "schema": "{\"type\": \"object\"}"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/schemas", strings.NewReader(body))
	w := httptest.NewRecorder()

	server.handleSchemas(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	if len(handler.schemas) != 1 {
		t.Errorf("Expected 1 schema, got %d", len(handler.schemas))
	}
}

func TestMethodNotAllowed(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)

	req := httptest.NewRequest(http.MethodPut, "/api/v1/cluster", nil)
	w := httptest.NewRecorder()

	server.handleCluster(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

// ============================================================================
// Authentication and RBAC Tests
// ============================================================================

// setupAuthTest creates a test environment with authentication enabled.
func setupAuthTest(t *testing.T) (*Server, *auth.Authorizer, *auth.UserStore) {
	t.Helper()

	userStore := auth.NewUserStore("")
	aclStore := auth.NewACLStore("")
	authorizer := auth.NewAuthorizer(userStore, aclStore, true)

	cfg := &config.AdminConfig{Enabled: true, AuthEnabled: true}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)
	server.SetAuthorizer(authorizer)

	return server, authorizer, userStore
}

// basicAuth creates a Basic Authorization header value.
func basicAuth(username, password string) string {
	creds := username + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(creds))
}

// TestExtractBasicAuth tests the Basic Auth header parsing.
func TestExtractBasicAuth(t *testing.T) {
	tests := []struct {
		name         string
		authHeader   string
		wantUser     string
		wantPassword string
		wantOK       bool
	}{
		{
			name:         "valid credentials",
			authHeader:   basicAuth("admin", "secret123"),
			wantUser:     "admin",
			wantPassword: "secret123",
			wantOK:       true,
		},
		{
			name:         "empty header",
			authHeader:   "",
			wantUser:     "",
			wantPassword: "",
			wantOK:       false,
		},
		{
			name:         "bearer token (wrong type)",
			authHeader:   "Bearer some-token",
			wantUser:     "",
			wantPassword: "",
			wantOK:       false,
		},
		{
			name:         "invalid base64",
			authHeader:   "Basic not-valid-base64!!!",
			wantUser:     "",
			wantPassword: "",
			wantOK:       false,
		},
		{
			name:         "no colon separator",
			authHeader:   "Basic " + base64.StdEncoding.EncodeToString([]byte("nocolon")),
			wantUser:     "",
			wantPassword: "",
			wantOK:       false,
		},
		{
			name:         "password with colon",
			authHeader:   basicAuth("user", "pass:word:with:colons"),
			wantUser:     "user",
			wantPassword: "pass:word:with:colons",
			wantOK:       true,
		},
		{
			name:         "empty password",
			authHeader:   basicAuth("user", ""),
			wantUser:     "user",
			wantPassword: "",
			wantOK:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			user, password, ok := extractBasicAuth(req)
			if ok != tt.wantOK {
				t.Errorf("extractBasicAuth() ok = %v, want %v", ok, tt.wantOK)
			}
			if user != tt.wantUser {
				t.Errorf("extractBasicAuth() user = %q, want %q", user, tt.wantUser)
			}
			if password != tt.wantPassword {
				t.Errorf("extractBasicAuth() password = %q, want %q", password, tt.wantPassword)
			}
		})
	}
}

// TestAuthMiddlewareRequireAuth tests the RequireAuth middleware.
func TestAuthMiddlewareRequireAuth(t *testing.T) {
	server, _, userStore := setupAuthTest(t)

	// Create a test user
	if err := userStore.CreateUser("testuser", "testpass", []string{"consumer"}); err != nil {
		t.Fatalf("Failed to create test user: %v", err)
	}

	tests := []struct {
		name           string
		authHeader     string
		wantStatus     int
		wantBodySubstr string
	}{
		{
			name:           "no auth header",
			authHeader:     "",
			wantStatus:     http.StatusUnauthorized,
			wantBodySubstr: "Authentication required",
		},
		{
			name:           "valid credentials",
			authHeader:     basicAuth("testuser", "testpass"),
			wantStatus:     http.StatusOK,
			wantBodySubstr: "",
		},
		{
			name:           "wrong password",
			authHeader:     basicAuth("testuser", "wrongpass"),
			wantStatus:     http.StatusUnauthorized,
			wantBodySubstr: "Invalid credentials",
		},
		{
			name:           "non-existent user",
			authHeader:     basicAuth("nobody", "password"),
			wantStatus:     http.StatusUnauthorized,
			wantBodySubstr: "Invalid credentials",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := server.authMw.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("success"))
			})

			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			w := httptest.NewRecorder()

			handler(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Expected status %d, got %d", tt.wantStatus, w.Code)
			}
			if tt.wantBodySubstr != "" && !strings.Contains(w.Body.String(), tt.wantBodySubstr) {
				t.Errorf("Expected body to contain %q, got %q", tt.wantBodySubstr, w.Body.String())
			}
		})
	}
}

// TestAuthMiddlewareRequirePermission tests permission-based access control.
func TestAuthMiddlewareRequirePermission(t *testing.T) {
	server, _, userStore := setupAuthTest(t)

	// Create users with different roles
	if err := userStore.CreateUser("admin_user", "adminpass", []string{"admin"}); err != nil {
		t.Fatalf("Failed to create admin user: %v", err)
	}
	if err := userStore.CreateUser("consumer_user", "consumerpass", []string{"consumer"}); err != nil {
		t.Fatalf("Failed to create consumer user: %v", err)
	}
	if err := userStore.CreateUser("producer_user", "producerpass", []string{"producer"}); err != nil {
		t.Fatalf("Failed to create producer user: %v", err)
	}
	if err := userStore.CreateUser("guest_user", "guestpass", []string{"guest"}); err != nil {
		t.Fatalf("Failed to create guest user: %v", err)
	}

	tests := []struct {
		name       string
		permission Permission
		authHeader string
		wantStatus int
	}{
		// PermissionNone - public endpoint
		{"public - no auth", PermissionNone, "", http.StatusOK},
		{"public - with auth", PermissionNone, basicAuth("guest_user", "guestpass"), http.StatusOK},

		// PermissionRead
		{"read - no auth", PermissionRead, "", http.StatusUnauthorized},
		{"read - admin user", PermissionRead, basicAuth("admin_user", "adminpass"), http.StatusOK},
		{"read - consumer user", PermissionRead, basicAuth("consumer_user", "consumerpass"), http.StatusOK},
		{"read - producer user (no read)", PermissionRead, basicAuth("producer_user", "producerpass"), http.StatusForbidden},
		{"read - guest user (no perms)", PermissionRead, basicAuth("guest_user", "guestpass"), http.StatusForbidden},

		// PermissionWrite
		{"write - no auth", PermissionWrite, "", http.StatusUnauthorized},
		{"write - admin user", PermissionWrite, basicAuth("admin_user", "adminpass"), http.StatusOK},
		{"write - producer user", PermissionWrite, basicAuth("producer_user", "producerpass"), http.StatusOK},
		{"write - consumer user (no write)", PermissionWrite, basicAuth("consumer_user", "consumerpass"), http.StatusForbidden},
		{"write - guest user (no perms)", PermissionWrite, basicAuth("guest_user", "guestpass"), http.StatusForbidden},

		// PermissionAdmin
		{"admin - no auth", PermissionAdmin, "", http.StatusUnauthorized},
		{"admin - admin user", PermissionAdmin, basicAuth("admin_user", "adminpass"), http.StatusOK},
		{"admin - consumer user", PermissionAdmin, basicAuth("consumer_user", "consumerpass"), http.StatusForbidden},
		{"admin - producer user", PermissionAdmin, basicAuth("producer_user", "producerpass"), http.StatusForbidden},
		{"admin - guest user", PermissionAdmin, basicAuth("guest_user", "guestpass"), http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := server.authMw.RequirePermission(tt.permission, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("success"))
			})

			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			w := httptest.NewRecorder()

			handler(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Expected status %d, got %d (body: %s)", tt.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}

// TestAuthMiddlewareRequireAdminOrSelf tests the admin-or-self middleware.
func TestAuthMiddlewareRequireAdminOrSelf(t *testing.T) {
	server, _, userStore := setupAuthTest(t)

	// Create users
	if err := userStore.CreateUser("admin_user", "adminpass", []string{"admin"}); err != nil {
		t.Fatalf("Failed to create admin user: %v", err)
	}
	if err := userStore.CreateUser("alice", "alicepass", []string{"consumer"}); err != nil {
		t.Fatalf("Failed to create alice: %v", err)
	}
	if err := userStore.CreateUser("bob", "bobpass", []string{"consumer"}); err != nil {
		t.Fatalf("Failed to create bob: %v", err)
	}

	// extractUsername function that gets target from URL path
	extractUsername := func(r *http.Request) string {
		return strings.TrimPrefix(r.URL.Path, "/api/v1/users/")
	}

	tests := []struct {
		name       string
		path       string // target user path
		authHeader string // current user credentials
		wantStatus int
	}{
		{"no auth", "/api/v1/users/alice", "", http.StatusUnauthorized},
		{"alice accessing alice (self)", "/api/v1/users/alice", basicAuth("alice", "alicepass"), http.StatusOK},
		{"alice accessing bob (other)", "/api/v1/users/bob", basicAuth("alice", "alicepass"), http.StatusForbidden},
		{"admin accessing alice", "/api/v1/users/alice", basicAuth("admin_user", "adminpass"), http.StatusOK},
		{"admin accessing bob", "/api/v1/users/bob", basicAuth("admin_user", "adminpass"), http.StatusOK},
		{"bob accessing alice (other)", "/api/v1/users/alice", basicAuth("bob", "bobpass"), http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := server.authMw.RequireAdminOrSelf(extractUsername, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("success"))
			})

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			w := httptest.NewRecorder()

			handler(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Expected status %d, got %d (body: %s)", tt.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}

// TestAuthDisabledPassesThrough tests that auth disabled allows all requests.
func TestAuthDisabledPassesThrough(t *testing.T) {
	// Create server with auth disabled
	userStore := auth.NewUserStore("")
	aclStore := auth.NewACLStore("")
	authorizer := auth.NewAuthorizer(userStore, aclStore, false) // auth disabled

	cfg := &config.AdminConfig{Enabled: true, AuthEnabled: false}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)
	server.SetAuthorizer(authorizer)

	// Even admin permission should pass without auth when disabled
	wrapped := server.authMw.RequirePermission(PermissionAdmin, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrapped(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 when auth disabled, got %d", w.Code)
	}
}

// TestDisabledUserCannotAuthenticate tests that disabled users are rejected.
func TestDisabledUserCannotAuthenticate(t *testing.T) {
	server, _, userStore := setupAuthTest(t)

	// Create and disable a user
	if err := userStore.CreateUser("disabled_user", "password", []string{"consumer"}); err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}
	if err := userStore.SetUserEnabled("disabled_user", false); err != nil {
		t.Fatalf("Failed to disable user: %v", err)
	}

	handler := server.authMw.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", basicAuth("disabled_user", "password"))
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401 for disabled user, got %d", w.Code)
	}
}

// TestContextHelpers tests GetUser and GetUsername context helpers.
func TestContextHelpers(t *testing.T) {
	server, _, userStore := setupAuthTest(t)

	if err := userStore.CreateUser("contextuser", "password", []string{"consumer"}); err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	var gotUser *auth.User
	var gotUsername string

	handler := server.authMw.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		gotUser = GetUser(r)
		gotUsername = GetUsername(r)
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", basicAuth("contextuser", "password"))
	w := httptest.NewRecorder()

	handler(w, req)

	if gotUser == nil {
		t.Error("Expected user in context, got nil")
	} else if gotUser.Username != "contextuser" {
		t.Errorf("Expected username 'contextuser', got %q", gotUser.Username)
	}

	if gotUsername != "contextuser" {
		t.Errorf("Expected username 'contextuser', got %q", gotUsername)
	}
}

// TestWWWAuthenticateHeader tests that 401 responses include WWW-Authenticate header.
func TestWWWAuthenticateHeader(t *testing.T) {
	server, _, _ := setupAuthTest(t)

	handler := server.authMw.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("Expected status 401, got %d", w.Code)
	}

	wwwAuth := w.Header().Get("WWW-Authenticate")
	if wwwAuth == "" {
		t.Error("Expected WWW-Authenticate header to be set")
	}
	if !strings.Contains(wwwAuth, "Basic") {
		t.Errorf("Expected WWW-Authenticate header to contain 'Basic', got %q", wwwAuth)
	}
}

// ============================================================================
// Integration Tests: Protected Endpoint Access
// ============================================================================

// TestPublicEndpointsAccessible tests that public endpoints are accessible without auth.
func TestPublicEndpointsAccessible(t *testing.T) {
	server, _, _ := setupAuthTest(t)

	publicEndpoints := []struct {
		path   string
		method string
	}{
		{"/api/v1/cluster", http.MethodGet},
		{"/api/v1/cluster/nodes", http.MethodGet},
		{"/api/v1/metrics", http.MethodGet},
		{"/api/v1/swagger.json", http.MethodGet},
		{"/api/v1/swagger/", http.MethodGet},
	}

	for _, ep := range publicEndpoints {
		t.Run(ep.path, func(t *testing.T) {
			req := httptest.NewRequest(ep.method, ep.path, nil)
			w := httptest.NewRecorder()

			// Get appropriate handler
			switch {
			case ep.path == "/api/v1/cluster":
				server.handleCluster(w, req)
			case ep.path == "/api/v1/cluster/nodes":
				server.handleNodes(w, req)
			case ep.path == "/api/v1/metrics":
				server.handleMetrics(w, req)
			case ep.path == "/api/v1/swagger.json":
				server.handleSwaggerJSON(w, req)
			case strings.HasPrefix(ep.path, "/api/v1/swagger/"):
				server.handleSwaggerUI(w, req)
			}

			// Public endpoints should return 200 (or 405 for wrong method, but not 401/403)
			if w.Code == http.StatusUnauthorized || w.Code == http.StatusForbidden {
				t.Errorf("%s %s returned auth error %d, expected public access", ep.method, ep.path, w.Code)
			}
		})
	}
}

// TestProtectedEndpointsRequireAuth tests that protected endpoints require authentication.
func TestProtectedEndpointsRequireAuth(t *testing.T) {
	server, _, _ := setupAuthTest(t)

	protectedEndpoints := []struct {
		path    string
		method  string
		handler http.HandlerFunc
	}{
		{"/api/v1/topics", http.MethodGet, server.handleTopicsWithAuth},
		{"/api/v1/topics", http.MethodPost, server.handleTopicsWithAuth},
		{"/api/v1/topics/test-topic", http.MethodGet, server.handleTopicWithAuth},
		{"/api/v1/topics/test-topic", http.MethodDelete, server.handleTopicWithAuth},
		{"/api/v1/schemas", http.MethodGet, server.handleSchemasWithAuth},
		{"/api/v1/schemas", http.MethodPost, server.handleSchemasWithAuth},
		{"/api/v1/users/testuser", http.MethodGet, server.handleUserWithAuth},
	}

	for _, ep := range protectedEndpoints {
		t.Run(ep.method+" "+ep.path, func(t *testing.T) {
			req := httptest.NewRequest(ep.method, ep.path, nil)
			w := httptest.NewRecorder()

			ep.handler(w, req)

			if w.Code != http.StatusUnauthorized {
				t.Errorf("%s %s returned %d, expected 401 Unauthorized", ep.method, ep.path, w.Code)
			}
		})
	}
}

// TestTopicsEndpointPermissions tests RBAC for topic endpoints.
func TestTopicsEndpointPermissions(t *testing.T) {
	server, _, userStore := setupAuthTest(t)

	// Create users
	userStore.CreateUser("admin_user", "adminpass", []string{"admin"})
	userStore.CreateUser("consumer_user", "consumerpass", []string{"consumer"})
	userStore.CreateUser("producer_user", "producerpass", []string{"producer"})

	tests := []struct {
		name       string
		method     string
		path       string
		authHeader string
		wantStatus int
	}{
		// GET topics - requires read permission
		{"GET topics - admin", http.MethodGet, "/api/v1/topics", basicAuth("admin_user", "adminpass"), http.StatusOK},
		{"GET topics - consumer", http.MethodGet, "/api/v1/topics", basicAuth("consumer_user", "consumerpass"), http.StatusOK},
		{"GET topics - producer (no read)", http.MethodGet, "/api/v1/topics", basicAuth("producer_user", "producerpass"), http.StatusForbidden},

		// POST topics - requires admin permission
		{"POST topics - admin", http.MethodPost, "/api/v1/topics", basicAuth("admin_user", "adminpass"), http.StatusBadRequest}, // bad request because no body
		{"POST topics - consumer", http.MethodPost, "/api/v1/topics", basicAuth("consumer_user", "consumerpass"), http.StatusForbidden},

		// DELETE topic - requires admin permission
		{"DELETE topic - admin", http.MethodDelete, "/api/v1/topics/test", basicAuth("admin_user", "adminpass"), http.StatusNoContent},
		{"DELETE topic - consumer", http.MethodDelete, "/api/v1/topics/test", basicAuth("consumer_user", "consumerpass"), http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set("Authorization", tt.authHeader)
			w := httptest.NewRecorder()

			if strings.HasPrefix(tt.path, "/api/v1/topics/") {
				server.handleTopicWithAuth(w, req)
			} else {
				server.handleTopicsWithAuth(w, req)
			}

			if w.Code != tt.wantStatus {
				t.Errorf("Expected status %d, got %d (body: %s)", tt.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}

// TestUserPasswordChangeSelfService tests that users can change their own passwords.
func TestUserPasswordChangeSelfService(t *testing.T) {
	server, _, userStore := setupAuthTest(t)

	// Create users
	userStore.CreateUser("alice", "alicepass", []string{"consumer"})
	userStore.CreateUser("admin_user", "adminpass", []string{"admin"})

	tests := []struct {
		name       string
		path       string
		authHeader string
		wantStatus int
	}{
		{"alice changes own password", "/api/v1/users/alice/password", basicAuth("alice", "alicepass"), http.StatusOK},
		{"alice tries to change bob's password", "/api/v1/users/bob/password", basicAuth("alice", "alicepass"), http.StatusForbidden},
		{"admin changes alice's password", "/api/v1/users/alice/password", basicAuth("admin_user", "adminpass"), http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := `{"old_password":"alicepass","new_password":"newpass"}`
			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(body))
			req.Header.Set("Authorization", tt.authHeader)
			w := httptest.NewRecorder()

			server.handleUserWithAuth(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Expected status %d, got %d (body: %s)", tt.wantStatus, w.Code, w.Body.String())
			}
		})
	}
}

// TestSwaggerEndpoint tests the Swagger endpoints are accessible.
func TestSwaggerEndpoint(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)

	t.Run("swagger.json returns OpenAPI spec", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/swagger.json", nil)
		w := httptest.NewRecorder()

		server.handleSwaggerJSON(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		contentType := w.Header().Get("Content-Type")
		if !strings.Contains(contentType, "application/json") {
			t.Errorf("Expected Content-Type application/json, got %s", contentType)
		}

		// Verify it's valid JSON with OpenAPI fields
		var spec map[string]interface{}
		if err := json.NewDecoder(w.Body).Decode(&spec); err != nil {
			t.Fatalf("Failed to decode JSON: %v", err)
		}

		if _, ok := spec["openapi"]; !ok {
			t.Error("Expected 'openapi' field in spec")
		}
		if _, ok := spec["paths"]; !ok {
			t.Error("Expected 'paths' field in spec")
		}
	})

	t.Run("swagger UI returns HTML", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/swagger/", nil)
		w := httptest.NewRecorder()

		server.handleSwaggerUI(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d", w.Code)
		}

		contentType := w.Header().Get("Content-Type")
		if !strings.Contains(contentType, "text/html") {
			t.Errorf("Expected Content-Type text/html, got %s", contentType)
		}

		if !strings.Contains(w.Body.String(), "swagger-ui") {
			t.Error("Expected HTML to contain swagger-ui")
		}
	})
}

// TestSwaggerRoutesRegistered tests that all Swagger routes are properly registered in the mux.
func TestSwaggerRoutesRegistered(t *testing.T) {
	cfg := &config.AdminConfig{Enabled: true, Addr: ":9096"}
	handler := &mockHandler{}
	server := NewServer(cfg, handler)

	// Create a test mux and register routes
	mux := http.NewServeMux()
	server.registerRoutes(mux)

	// Test all Swagger paths
	swaggerPaths := []struct {
		path        string
		contentType string
		contains    string
	}{
		{"/api/v1/swagger.json", "application/json", "openapi"},
		{"/swagger.json", "application/json", "openapi"},
		{"/api/v1/swagger/", "text/html", "swagger-ui"},
		{"/swagger/", "text/html", "swagger-ui"},
		{"/swagger/index.html", "text/html", "swagger-ui"},
	}

	for _, sp := range swaggerPaths {
		t.Run(sp.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, sp.path, nil)
			w := httptest.NewRecorder()

			mux.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("%s: expected status 200, got %d (body: %s)", sp.path, w.Code, w.Body.String())
			}

			contentType := w.Header().Get("Content-Type")
			if !strings.Contains(contentType, sp.contentType) {
				t.Errorf("%s: expected Content-Type %s, got %s", sp.path, sp.contentType, contentType)
			}

			if !strings.Contains(w.Body.String(), sp.contains) {
				t.Errorf("%s: expected body to contain %q", sp.path, sp.contains)
			}
		})
	}
}

// TestMultipleRoles tests users with multiple roles get combined permissions.
func TestMultipleRoles(t *testing.T) {
	server, _, userStore := setupAuthTest(t)

	// Create user with both consumer and producer roles
	if err := userStore.CreateUser("multi_role", "password", []string{"consumer", "producer"}); err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// This user should have both read and write permissions
	tests := []struct {
		name       string
		permission Permission
		wantStatus int
	}{
		{"read permission", PermissionRead, http.StatusOK},
		{"write permission", PermissionWrite, http.StatusOK},
		{"admin permission", PermissionAdmin, http.StatusForbidden}, // still no admin
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := server.authMw.RequirePermission(tt.permission, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			req.Header.Set("Authorization", basicAuth("multi_role", "password"))
			w := httptest.NewRecorder()

			handler(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("Expected status %d, got %d", tt.wantStatus, w.Code)
			}
		})
	}
}

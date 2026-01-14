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

package integration

import (
	"net"
	"strings"
	"testing"
	"time"

	"flymq/internal/broker"
	"flymq/internal/config"
	"flymq/internal/server"
	"flymq/pkg/client"
)

// authTestServer wraps a server with authentication enabled
type authTestServer struct {
	server *server.Server
	broker *broker.Broker
	addr   string
}

// newAuthTestServer creates a new test server with authentication enabled.
// It creates an admin user and optionally a consumer user.
func newAuthTestServer(t *testing.T, defaultPublic bool) *authTestServer {
	t.Helper()

	// Find an available port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	dataDir := t.TempDir()

	cfg := &config.Config{
		BindAddr:     addr,
		DataDir:      dataDir,
		SegmentBytes: 1024 * 1024,
		Auth: config.AuthConfig{
			Enabled:        true,
			RBACEnabled:    true,
			AllowAnonymous: false,
			DefaultPublic:  defaultPublic,
			UserFile:       dataDir + "/users.json",
			ACLFile:        dataDir + "/acls.json",
			AdminUsername:  "admin",
			AdminPassword:  "adminpass",
		},
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	srv := server.NewServer(cfg, b)
	if err := srv.Start(); err != nil {
		b.Close()
		t.Fatalf("Failed to start server: %v", err)
	}

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	ts := &authTestServer{
		server: srv,
		broker: b,
		addr:   addr,
	}

	t.Cleanup(func() {
		srv.Stop()
		b.Close()
	})

	return ts
}

// createTestUser creates an additional user for testing using the client library.
func createTestUser(t *testing.T, addr, adminUser, adminPass, newUser, newPass string, roles []string) {
	t.Helper()

	c, err := client.NewClientWithOptions(addr, client.ClientOptions{
		Username: adminUser,
		Password: adminPass,
	})
	if err != nil {
		t.Fatalf("Failed to connect as admin: %v", err)
	}
	defer c.Close()

	if err := c.CreateUser(newUser, newPass, roles); err != nil {
		t.Fatalf("Failed to create user %s: %v", newUser, err)
	}
}

// ============================================================================
// Authentication Tests
// ============================================================================

func TestAuthenticationRequired(t *testing.T) {
	ts := newAuthTestServer(t, false) // defaultPublic=false means topics require auth

	// Connect without credentials
	c, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Try to produce without authentication - should fail
	_, err = c.Produce("private-topic", []byte("test message"))
	if err == nil {
		t.Error("Expected error when producing without authentication")
	}
	if !strings.Contains(err.Error(), "auth") && !strings.Contains(err.Error(), "unauthorized") {
		t.Errorf("Expected auth-related error, got: %v", err)
	}
}

func TestAuthenticationWithValidCredentials(t *testing.T) {
	ts := newAuthTestServer(t, false)

	// Connect with valid credentials
	c, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create authenticated client: %v", err)
	}
	defer c.Close()

	// Should be able to produce
	offset, err := c.Produce("test-topic", []byte("test message"))
	if err != nil {
		t.Fatalf("Failed to produce with valid credentials: %v", err)
	}
	if offset != 0 {
		t.Errorf("Expected offset 0, got %d", offset)
	}
}

func TestAuthenticationWithInvalidCredentials(t *testing.T) {
	ts := newAuthTestServer(t, false)

	// Connect with invalid password
	_, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "wrongpassword",
	})
	if err == nil {
		t.Error("Expected error when connecting with invalid credentials")
	}
	if !strings.Contains(err.Error(), "authentication") && !strings.Contains(err.Error(), "invalid") {
		t.Errorf("Expected authentication failure error, got: %v", err)
	}
}

func TestAuthenticationWithNonExistentUser(t *testing.T) {
	ts := newAuthTestServer(t, false)

	// Connect with non-existent user
	_, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "nonexistent",
		Password: "somepassword",
	})
	if err == nil {
		t.Error("Expected error when connecting with non-existent user")
	}
}

// ============================================================================
// RBAC Permission Tests
// ============================================================================

func TestRBACAdminCanDoEverything(t *testing.T) {
	ts := newAuthTestServer(t, false)

	// Connect as admin
	c, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	// Admin can create topics
	if err := c.CreateTopic("admin-topic", 1); err != nil {
		t.Errorf("Admin should be able to create topics: %v", err)
	}

	// Admin can produce
	if _, err := c.Produce("admin-topic", []byte("admin message")); err != nil {
		t.Errorf("Admin should be able to produce: %v", err)
	}

	// Admin can consume
	if _, _, err := c.Fetch("admin-topic", 0, 0, 10); err != nil {
		t.Errorf("Admin should be able to consume: %v", err)
	}

	// Admin can delete topics
	if err := c.DeleteTopic("admin-topic"); err != nil {
		t.Errorf("Admin should be able to delete topics: %v", err)
	}
}

func TestRBACConsumerCanOnlyRead(t *testing.T) {
	ts := newAuthTestServer(t, true) // defaultPublic=true so we can test

	// Create a consumer user
	createTestUser(t, ts.addr, "admin", "adminpass", "consumer", "consumerpass", []string{"consumer"})

	// Create topic and produce a message as admin first
	adminClient, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	if err := adminClient.CreateTopic("consumer-test-topic", 1); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	if _, err := adminClient.Produce("consumer-test-topic", []byte("test message")); err != nil {
		t.Fatalf("Failed to produce: %v", err)
	}
	adminClient.Close()

	// Connect as consumer
	c, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "consumer",
		Password: "consumerpass",
	})
	if err != nil {
		t.Fatalf("Failed to create consumer client: %v", err)
	}
	defer c.Close()

	// Consumer can read
	msgs, _, err := c.Fetch("consumer-test-topic", 0, 0, 10)
	if err != nil {
		t.Errorf("Consumer should be able to read: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("Expected 1 message, got %d", len(msgs))
	}

	// Consumer cannot write to private topics (when we make it private)
	// For now just verify they can't create topics
	if err := c.CreateTopic("consumer-new-topic", 1); err == nil {
		t.Error("Consumer should not be able to create topics")
	}
}

func TestRBACProducerCanOnlyWrite(t *testing.T) {
	ts := newAuthTestServer(t, true) // defaultPublic=true

	// Create a producer user
	createTestUser(t, ts.addr, "admin", "adminpass", "producer", "producerpass", []string{"producer"})

	// Create topic as admin
	adminClient, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	if err := adminClient.CreateTopic("producer-test-topic", 1); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	adminClient.Close()

	// Connect as producer
	c, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "producer",
		Password: "producerpass",
	})
	if err != nil {
		t.Fatalf("Failed to create producer client: %v", err)
	}
	defer c.Close()

	// Producer can write
	_, err = c.Produce("producer-test-topic", []byte("producer message"))
	if err != nil {
		t.Errorf("Producer should be able to write: %v", err)
	}

	// Producer cannot create topics (admin only)
	if err := c.CreateTopic("producer-new-topic", 1); err == nil {
		t.Error("Producer should not be able to create topics")
	}
}

// ============================================================================
// Public Topic Tests
// ============================================================================

func TestPublicTopicsAccessibleWithoutAuth(t *testing.T) {
	ts := newAuthTestServer(t, true) // defaultPublic=true

	// Create topic and produce as admin
	adminClient, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	if err := adminClient.CreateTopic("public-topic", 1); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	if _, err := adminClient.Produce("public-topic", []byte("public message")); err != nil {
		t.Fatalf("Failed to produce: %v", err)
	}
	adminClient.Close()

	// Connect without auth and try to read (should work for public topic)
	c, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create unauthenticated client: %v", err)
	}
	defer c.Close()

	// Reading from public topic should work
	msgs, _, err := c.Fetch("public-topic", 0, 0, 10)
	if err != nil {
		t.Errorf("Should be able to read public topic without auth: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("Expected 1 message, got %d", len(msgs))
	}
}

// ============================================================================
// User Management Tests via Binary Protocol
// ============================================================================

func TestUserManagementViaClient(t *testing.T) {
	ts := newAuthTestServer(t, true)

	// Connect as admin
	c, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer c.Close()

	// Create a new user
	newUser := "testuser"
	newPass := "testpass"
	if err := c.CreateUser(newUser, newPass, []string{"consumer"}); err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// List users and verify
	users, err := c.ListUsers()
	if err != nil {
		t.Fatalf("Failed to list users: %v", err)
	}

	found := false
	for _, u := range users {
		if u.Username == newUser {
			found = true
			if len(u.Roles) != 1 || u.Roles[0] != "consumer" {
				t.Errorf("Expected roles [consumer], got %v", u.Roles)
			}
			break
		}
	}
	if !found {
		t.Errorf("Created user %s not found in user list", newUser)
	}

	// Verify new user can authenticate
	newClient, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: newUser,
		Password: newPass,
	})
	if err != nil {
		t.Errorf("New user should be able to authenticate: %v", err)
	} else {
		newClient.Close()
	}

	// Delete the user
	if err := c.DeleteUser(newUser); err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}

	// Verify deleted user cannot authenticate
	_, err = client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: newUser,
		Password: newPass,
	})
	if err == nil {
		t.Error("Deleted user should not be able to authenticate")
	}
}

// ============================================================================
// WhoAmI Tests
// ============================================================================

func TestWhoAmIAuthenticated(t *testing.T) {
	ts := newAuthTestServer(t, true)

	c, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	resp, err := c.WhoAmI()
	if err != nil {
		t.Fatalf("WhoAmI failed: %v", err)
	}

	if !resp.Authenticated {
		t.Error("Expected Authenticated=true")
	}
	if resp.Username != "admin" {
		t.Errorf("Expected Username='admin', got '%s'", resp.Username)
	}
}

func TestWhoAmIUnauthenticated(t *testing.T) {
	ts := newAuthTestServer(t, true)

	c, err := client.NewClient(ts.addr)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	resp, err := c.WhoAmI()
	if err != nil {
		t.Fatalf("WhoAmI failed: %v", err)
	}

	if resp.Authenticated {
		t.Error("Expected Authenticated=false for unauthenticated client")
	}
}

// ============================================================================
// ACL Management Tests
// ============================================================================

func TestACLManagement(t *testing.T) {
	ts := newAuthTestServer(t, true)

	// Connect as admin
	c, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer c.Close()

	// Create a topic
	topicName := "acl-test-topic"
	if err := c.CreateTopic(topicName, 1); err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Set ACL to make topic private with specific user
	createTestUser(t, ts.addr, "admin", "adminpass", "acluser", "aclpass", []string{"consumer"})

	if err := c.SetACL(topicName, false, []string{"acluser"}, nil); err != nil {
		t.Fatalf("Failed to set ACL: %v", err)
	}

	// Verify ACL
	acl, err := c.GetACL(topicName)
	if err != nil {
		t.Fatalf("Failed to get ACL: %v", err)
	}
	if acl.Public {
		t.Error("Expected topic to be private")
	}

	// List ACLs
	acls, _, err := c.ListACLs()
	if err != nil {
		t.Fatalf("Failed to list ACLs: %v", err)
	}
	found := false
	for _, a := range acls {
		if a.Topic == topicName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("ACL for %s not found in list", topicName)
	}

	// Delete ACL
	if err := c.DeleteACL(topicName); err != nil {
		t.Fatalf("Failed to delete ACL: %v", err)
	}
}

// ============================================================================
// Role Listing Tests
// ============================================================================

func TestListRoles(t *testing.T) {
	ts := newAuthTestServer(t, true)

	c, err := client.NewClientWithOptions(ts.addr, client.ClientOptions{
		Username: "admin",
		Password: "adminpass",
	})
	if err != nil {
		t.Fatalf("Failed to create admin client: %v", err)
	}
	defer c.Close()

	roles, err := c.ListRoles()
	if err != nil {
		t.Fatalf("Failed to list roles: %v", err)
	}

	// Should have default roles: admin, producer, consumer, guest
	expectedRoles := map[string]bool{
		"admin":    false,
		"producer": false,
		"consumer": false,
		"guest":    false,
	}

	for _, r := range roles {
		if _, ok := expectedRoles[r.Name]; ok {
			expectedRoles[r.Name] = true
		}
	}

	for role, found := range expectedRoles {
		if !found {
			t.Errorf("Expected to find role '%s'", role)
		}
	}
}

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

package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestUserStore_CreateAndAuthenticate(t *testing.T) {
	store := NewUserStore("")

	// Create a user
	err := store.CreateUser("testuser", "password123", []string{"admin"})
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Authenticate with correct password
	user, err := store.Authenticate("testuser", "password123")
	if err != nil {
		t.Fatalf("Failed to authenticate: %v", err)
	}
	if user.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", user.Username)
	}

	// Authenticate with wrong password
	_, err = store.Authenticate("testuser", "wrongpassword")
	if err != ErrInvalidCredentials {
		t.Errorf("Expected ErrInvalidCredentials, got %v", err)
	}

	// Authenticate with non-existent user
	_, err = store.Authenticate("nonexistent", "password123")
	if err != ErrInvalidCredentials {
		t.Errorf("Expected ErrInvalidCredentials, got %v", err)
	}
}

func TestUserStore_DuplicateUser(t *testing.T) {
	store := NewUserStore("")

	err := store.CreateUser("testuser", "password123", []string{"admin"})
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Try to create duplicate user
	err = store.CreateUser("testuser", "password456", []string{"consumer"})
	if err == nil {
		t.Error("Expected error when creating duplicate user")
	}
}

func TestUserStore_DisabledUser(t *testing.T) {
	store := NewUserStore("")

	err := store.CreateUser("testuser", "password123", []string{"admin"})
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Disable the user
	err = store.SetUserEnabled("testuser", false)
	if err != nil {
		t.Fatalf("Failed to disable user: %v", err)
	}

	// Try to authenticate disabled user
	_, err = store.Authenticate("testuser", "password123")
	if err != ErrUserDisabled {
		t.Errorf("Expected ErrUserDisabled, got %v", err)
	}
}

func TestUserStore_Permissions(t *testing.T) {
	store := NewUserStore("")

	err := store.CreateUser("admin", "password", []string{"admin"})
	if err != nil {
		t.Fatalf("Failed to create admin user: %v", err)
	}

	err = store.CreateUser("producer", "password", []string{"producer"})
	if err != nil {
		t.Fatalf("Failed to create producer user: %v", err)
	}

	err = store.CreateUser("consumer", "password", []string{"consumer"})
	if err != nil {
		t.Fatalf("Failed to create consumer user: %v", err)
	}

	// Admin should have all permissions
	if !store.HasPermission("admin", PermissionRead) {
		t.Error("Admin should have read permission")
	}
	if !store.HasPermission("admin", PermissionWrite) {
		t.Error("Admin should have write permission")
	}
	if !store.HasPermission("admin", PermissionAdmin) {
		t.Error("Admin should have admin permission")
	}

	// Producer should only have write permission
	if store.HasPermission("producer", PermissionRead) {
		t.Error("Producer should not have read permission")
	}
	if !store.HasPermission("producer", PermissionWrite) {
		t.Error("Producer should have write permission")
	}

	// Consumer should only have read permission
	if !store.HasPermission("consumer", PermissionRead) {
		t.Error("Consumer should have read permission")
	}
	if store.HasPermission("consumer", PermissionWrite) {
		t.Error("Consumer should not have write permission")
	}
}

func TestUserStore_ListRoles(t *testing.T) {
	store := NewUserStore("")

	roles := store.ListRoles()
	if len(roles) != 4 {
		t.Errorf("Expected 4 default roles, got %d", len(roles))
	}

	// Verify all default roles are present
	roleNames := make(map[string]bool)
	for _, r := range roles {
		roleNames[r.Name] = true
		// Verify each role has a description
		if r.Description == "" {
			t.Errorf("Role %s should have a description", r.Name)
		}
	}

	expectedRoles := []string{"admin", "producer", "consumer", "guest"}
	for _, name := range expectedRoles {
		if !roleNames[name] {
			t.Errorf("Expected role %s not found", name)
		}
	}

	// Verify admin role has all permissions
	adminRole, exists := store.GetRole("admin")
	if !exists {
		t.Fatal("Admin role should exist")
	}
	if len(adminRole.Permissions) != 3 {
		t.Errorf("Admin role should have 3 permissions, got %d", len(adminRole.Permissions))
	}

	// Verify guest role has no permissions
	guestRole, exists := store.GetRole("guest")
	if !exists {
		t.Fatal("Guest role should exist")
	}
	if len(guestRole.Permissions) != 0 {
		t.Errorf("Guest role should have 0 permissions, got %d", len(guestRole.Permissions))
	}
}

func TestUserStore_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "users.json")

	// Create store and add user
	store := NewUserStore(filePath)
	err := store.CreateUser("testuser", "password123", []string{"admin"})
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Save to file
	err = store.Save()
	if err != nil {
		t.Fatalf("Failed to save: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatal("User file was not created")
	}
}

func TestAuthorizer_Disabled(t *testing.T) {
	// When auth is disabled, all operations should be allowed
	authorizer := NewAuthorizer(nil, nil, false)

	if authorizer.IsEnabled() {
		t.Error("Authorizer should be disabled")
	}

	// All operations should succeed when disabled
	if err := authorizer.CanProduce("", "any-topic"); err != nil {
		t.Errorf("CanProduce should succeed when disabled: %v", err)
	}
	if err := authorizer.CanConsume("", "any-topic"); err != nil {
		t.Errorf("CanConsume should succeed when disabled: %v", err)
	}
	if err := authorizer.CanAdmin("", "any-topic"); err != nil {
		t.Errorf("CanAdmin should succeed when disabled: %v", err)
	}
}

func TestAuthorizer_PublicTopic(t *testing.T) {
	userStore := NewUserStore("")
	aclStore := NewACLStore("")
	aclStore.DefaultPublic = false // Topics are private by default

	// Mark a topic as public
	aclStore.SetTopicPublic("public-topic", true)

	authorizer := NewAuthorizer(userStore, aclStore, true)

	// Anonymous user should be able to access public topic
	if err := authorizer.CanProduce("", "public-topic"); err != nil {
		t.Errorf("Anonymous should access public topic: %v", err)
	}

	// Anonymous user should NOT be able to access private topic
	if err := authorizer.CanProduce("", "private-topic"); err != ErrAuthRequired {
		t.Errorf("Expected ErrAuthRequired for private topic, got: %v", err)
	}
}

func TestAuthorizer_RoleBasedAccess(t *testing.T) {
	userStore := NewUserStore("")
	aclStore := NewACLStore("")
	aclStore.DefaultPublic = false

	// Create users with different roles
	userStore.CreateUser("admin", "password", []string{"admin"})
	userStore.CreateUser("producer", "password", []string{"producer"})
	userStore.CreateUser("consumer", "password", []string{"consumer"})

	authorizer := NewAuthorizer(userStore, aclStore, true)

	// Admin should have all access
	if err := authorizer.CanProduce("admin", "any-topic"); err != nil {
		t.Errorf("Admin should be able to produce: %v", err)
	}
	if err := authorizer.CanConsume("admin", "any-topic"); err != nil {
		t.Errorf("Admin should be able to consume: %v", err)
	}
	if err := authorizer.CanAdmin("admin", "any-topic"); err != nil {
		t.Errorf("Admin should have admin access: %v", err)
	}

	// Producer should only be able to produce
	if err := authorizer.CanProduce("producer", "any-topic"); err != nil {
		t.Errorf("Producer should be able to produce: %v", err)
	}
	if err := authorizer.CanConsume("producer", "any-topic"); err != ErrUnauthorized {
		t.Errorf("Producer should not be able to consume: %v", err)
	}

	// Consumer should only be able to consume
	if err := authorizer.CanConsume("consumer", "any-topic"); err != nil {
		t.Errorf("Consumer should be able to consume: %v", err)
	}
	if err := authorizer.CanProduce("consumer", "any-topic"); err != ErrUnauthorized {
		t.Errorf("Consumer should not be able to produce: %v", err)
	}
}

func TestACLStore_TopicSpecificAccess(t *testing.T) {
	userStore := NewUserStore("")
	aclStore := NewACLStore("")
	aclStore.DefaultPublic = false

	// Create a user with no roles
	userStore.CreateUser("specialuser", "password", []string{})

	// Grant specific access to a topic
	aclStore.SetTopicACL(&TopicACL{
		Topic:        "special-topic",
		AllowedUsers: []string{"specialuser"},
	})

	authorizer := NewAuthorizer(userStore, aclStore, true)

	// User should have access to the specific topic
	if err := authorizer.CanProduce("specialuser", "special-topic"); err != nil {
		t.Errorf("User should have access to special-topic: %v", err)
	}

	// User should NOT have access to other topics
	if err := authorizer.CanProduce("specialuser", "other-topic"); err != ErrUnauthorized {
		t.Errorf("User should not have access to other-topic: %v", err)
	}
}

// ============================================================================
// Cluster Replication Apply Method Tests
// ============================================================================

func TestUserStore_ApplyCreateUser(t *testing.T) {
	store := NewUserStore("")

	// Hash a password manually (simulating what the leader does)
	passwordHash, err := HashPassword("testpassword")
	if err != nil {
		t.Fatalf("Failed to hash password: %v", err)
	}

	// Apply create user (as if from Raft log)
	err = store.ApplyCreateUser("replicateduser", passwordHash, []string{"viewer"})
	if err != nil {
		t.Fatalf("ApplyCreateUser failed: %v", err)
	}

	// Verify user can authenticate
	user, err := store.Authenticate("replicateduser", "testpassword")
	if err != nil {
		t.Fatalf("Failed to authenticate replicated user: %v", err)
	}
	if user.Username != "replicateduser" {
		t.Errorf("Expected username 'replicateduser', got '%s'", user.Username)
	}
	if len(user.Roles) != 1 || user.Roles[0] != "viewer" {
		t.Errorf("Expected roles [viewer], got %v", user.Roles)
	}

	// Duplicate user should fail
	err = store.ApplyCreateUser("replicateduser", passwordHash, []string{"admin"})
	if err == nil {
		t.Error("Expected error for duplicate user")
	}
}

func TestUserStore_ApplyDeleteUser(t *testing.T) {
	store := NewUserStore("")

	// Create a user first
	err := store.CreateUser("todelete", "password", []string{"user"})
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Apply delete (as if from Raft log)
	err = store.ApplyDeleteUser("todelete")
	if err != nil {
		t.Fatalf("ApplyDeleteUser failed: %v", err)
	}

	// User should no longer exist
	_, err = store.Authenticate("todelete", "password")
	if err != ErrInvalidCredentials {
		t.Errorf("Expected ErrInvalidCredentials, got %v", err)
	}

	// Deleting non-existent user should fail
	err = store.ApplyDeleteUser("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent user")
	}
}

func TestUserStore_ApplyUpdateUser(t *testing.T) {
	store := NewUserStore("")

	// Create a user first
	err := store.CreateUser("toupdate", "password", []string{"user"})
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Update roles
	err = store.ApplyUpdateUser("toupdate", []string{"admin", "user"}, nil, "")
	if err != nil {
		t.Fatalf("ApplyUpdateUser (roles) failed: %v", err)
	}

	user, _ := store.GetUser("toupdate")
	if len(user.Roles) != 2 {
		t.Errorf("Expected 2 roles, got %d", len(user.Roles))
	}

	// Update enabled status
	enabled := false
	err = store.ApplyUpdateUser("toupdate", nil, &enabled, "")
	if err != nil {
		t.Fatalf("ApplyUpdateUser (enabled) failed: %v", err)
	}

	user, _ = store.GetUser("toupdate")
	if user.Enabled {
		t.Error("Expected user to be disabled")
	}

	// Update password hash
	newHash, _ := HashPassword("newpassword")
	err = store.ApplyUpdateUser("toupdate", nil, nil, newHash)
	if err != nil {
		t.Fatalf("ApplyUpdateUser (password) failed: %v", err)
	}

	// Re-enable and verify new password works
	enabled = true
	store.ApplyUpdateUser("toupdate", nil, &enabled, "")
	_, err = store.Authenticate("toupdate", "newpassword")
	if err != nil {
		t.Errorf("Failed to authenticate with new password: %v", err)
	}
}

func TestACLStore_ApplySetACL(t *testing.T) {
	store := NewACLStore("")

	// Apply set ACL (as if from Raft log)
	store.ApplySetACL("replicated-topic", false, []string{"user1", "user2"}, []string{"admin"})

	// Verify ACL was set
	acl, exists := store.GetTopicACL("replicated-topic")
	if !exists {
		t.Fatal("ACL should exist")
	}
	if acl.Public {
		t.Error("ACL should not be public")
	}
	if len(acl.AllowedUsers) != 2 {
		t.Errorf("Expected 2 allowed users, got %d", len(acl.AllowedUsers))
	}
	if len(acl.AllowedRoles) != 1 {
		t.Errorf("Expected 1 allowed role, got %d", len(acl.AllowedRoles))
	}

	// Update existing ACL
	store.ApplySetACL("replicated-topic", true, nil, nil)
	acl, _ = store.GetTopicACL("replicated-topic")
	if !acl.Public {
		t.Error("ACL should now be public")
	}
}

func TestACLStore_ApplyDeleteACL(t *testing.T) {
	store := NewACLStore("")

	// Set an ACL first
	store.SetTopicACL(&TopicACL{
		Topic:  "todelete-topic",
		Public: false,
	})

	// Apply delete (as if from Raft log)
	store.ApplyDeleteACL("todelete-topic")

	// Verify ACL was deleted
	_, exists := store.GetTopicACL("todelete-topic")
	if exists {
		t.Error("ACL should have been deleted")
	}

	// Deleting non-existent ACL should not panic
	store.ApplyDeleteACL("nonexistent-topic") // Should not panic
}

func TestAuthorizer_ApplyMethods(t *testing.T) {
	userStore := NewUserStore("")
	aclStore := NewACLStore("")
	authorizer := NewAuthorizer(userStore, aclStore, true)

	// Test ApplyCreateUser through Authorizer
	passwordHash, _ := HashPassword("testpass")
	err := authorizer.ApplyCreateUser("clusteruser", passwordHash, []string{"admin"})
	if err != nil {
		t.Fatalf("ApplyCreateUser failed: %v", err)
	}

	// Verify user exists and can authenticate
	user, err := authorizer.Authenticate("clusteruser", "testpass")
	if err != nil {
		t.Fatalf("Failed to authenticate: %v", err)
	}
	if user.Username != "clusteruser" {
		t.Errorf("Expected username 'clusteruser', got '%s'", user.Username)
	}

	// Test ApplySetACL through Authorizer
	authorizer.ApplySetACL("cluster-topic", false, []string{"clusteruser"}, nil)

	// Verify ACL works
	if err := authorizer.CanProduce("clusteruser", "cluster-topic"); err != nil {
		t.Errorf("User should have access to cluster-topic: %v", err)
	}

	// Test ApplyDeleteACL through Authorizer
	authorizer.ApplyDeleteACL("cluster-topic")

	// Topic should now use default (public)
	if err := authorizer.CanProduce("clusteruser", "cluster-topic"); err != nil {
		t.Errorf("Topic should be accessible after ACL deletion (default public): %v", err)
	}

	// Test ApplyUpdateUser through Authorizer
	enabled := false
	err = authorizer.ApplyUpdateUser("clusteruser", nil, &enabled, "")
	if err != nil {
		t.Fatalf("ApplyUpdateUser failed: %v", err)
	}

	// User should be disabled
	_, err = authorizer.Authenticate("clusteruser", "testpass")
	if err != ErrUserDisabled {
		t.Errorf("Expected ErrUserDisabled, got %v", err)
	}

	// Test ApplyDeleteUser through Authorizer
	err = authorizer.ApplyDeleteUser("clusteruser")
	if err != nil {
		t.Fatalf("ApplyDeleteUser failed: %v", err)
	}

	// User should no longer exist
	_, err = authorizer.Authenticate("clusteruser", "testpass")
	if err != ErrInvalidCredentials {
		t.Errorf("Expected ErrInvalidCredentials, got %v", err)
	}
}

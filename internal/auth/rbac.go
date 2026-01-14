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
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// TopicACL defines access control for a specific topic.
type TopicACL struct {
	Topic        string       `json:"topic"`
	Public       bool         `json:"public"`                  // If true, no auth required
	AllowedUsers []string     `json:"allowed_users,omitempty"` // Specific users with access
	AllowedRoles []string     `json:"allowed_roles,omitempty"` // Roles with access
	Permissions  []Permission `json:"permissions,omitempty"`   // Required permissions
}

// ACLStore manages topic-level access control lists.
type ACLStore struct {
	mu       sync.RWMutex
	acls     map[string]*TopicACL // topic -> ACL
	filePath string
	// Default behavior for topics without explicit ACL
	DefaultPublic bool
}

// NewACLStore creates a new ACL store.
func NewACLStore(filePath string) *ACLStore {
	return &ACLStore{
		acls:          make(map[string]*TopicACL),
		filePath:      filePath,
		DefaultPublic: true, // Backward compatible: topics are public by default
	}
}

// Load loads ACLs from the configured file path.
func (s *ACLStore) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.filePath == "" {
		return nil
	}

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read ACL store: %w", err)
	}

	var storeData struct {
		ACLs          map[string]*TopicACL `json:"acls"`
		DefaultPublic bool                 `json:"default_public"`
	}
	if err := json.Unmarshal(data, &storeData); err != nil {
		return fmt.Errorf("failed to parse ACL store: %w", err)
	}

	s.acls = storeData.ACLs
	if s.acls == nil {
		s.acls = make(map[string]*TopicACL)
	}
	s.DefaultPublic = storeData.DefaultPublic
	return nil
}

// Save persists ACLs to the configured file path.
func (s *ACLStore) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.filePath == "" {
		return nil
	}

	storeData := struct {
		ACLs          map[string]*TopicACL `json:"acls"`
		DefaultPublic bool                 `json:"default_public"`
	}{
		ACLs:          s.acls,
		DefaultPublic: s.DefaultPublic,
	}

	data, err := json.MarshalIndent(storeData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal ACL store: %w", err)
	}

	return os.WriteFile(s.filePath, data, 0600)
}

// SetTopicACL sets the ACL for a topic.
func (s *ACLStore) SetTopicACL(acl *TopicACL) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acls[acl.Topic] = acl
}

// GetTopicACL returns the ACL for a topic.
func (s *ACLStore) GetTopicACL(topic string) (*TopicACL, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	acl, exists := s.acls[topic]
	return acl, exists
}

// DeleteTopicACL removes the ACL for a topic.
func (s *ACLStore) DeleteTopicACL(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.acls, topic)
}

// ListTopicACLs returns all topic ACLs.
func (s *ACLStore) ListTopicACLs() []*TopicACL {
	s.mu.RLock()
	defer s.mu.RUnlock()

	acls := make([]*TopicACL, 0, len(s.acls))
	for _, acl := range s.acls {
		acls = append(acls, acl)
	}
	return acls
}

// SetTopicPublic marks a topic as public (no auth required).
func (s *ACLStore) SetTopicPublic(topic string, public bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	acl, exists := s.acls[topic]
	if !exists {
		acl = &TopicACL{Topic: topic}
		s.acls[topic] = acl
	}
	acl.Public = public
}

// ApplySetACL sets an ACL for a topic.
// This is used for cluster replication.
func (s *ACLStore) ApplySetACL(topic string, public bool, allowedUsers, allowedRoles []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acls[topic] = &TopicACL{
		Topic:        topic,
		Public:       public,
		AllowedUsers: allowedUsers,
		AllowedRoles: allowedRoles,
	}
}

// ApplyDeleteACL removes an ACL for a topic.
// This is used for cluster replication.
func (s *ACLStore) ApplyDeleteACL(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.acls, topic)
}

// Authorizer combines user and ACL stores for authorization decisions.
type Authorizer struct {
	userStore      *UserStore
	aclStore       *ACLStore
	enabled        bool
	allowAnonymous bool
}

// NewAuthorizer creates a new authorizer.
func NewAuthorizer(userStore *UserStore, aclStore *ACLStore, enabled bool) *Authorizer {
	return &Authorizer{
		userStore:      userStore,
		aclStore:       aclStore,
		enabled:        enabled,
		allowAnonymous: false, // Secure default: require authentication
	}
}

// SetAllowAnonymous sets whether anonymous (unauthenticated) connections are allowed.
// When enabled, anonymous users get read-only access to public topics.
func (a *Authorizer) SetAllowAnonymous(allow bool) {
	a.allowAnonymous = allow
}

// AllowAnonymous returns whether anonymous connections are allowed.
func (a *Authorizer) AllowAnonymous() bool {
	return a.allowAnonymous
}

// IsEnabled returns whether authentication is enabled.
func (a *Authorizer) IsEnabled() bool {
	return a.enabled
}

// Authenticate verifies credentials and returns the user.
func (a *Authorizer) Authenticate(username, password string) (*User, error) {
	if !a.enabled {
		return nil, nil // Auth disabled, allow all
	}
	return a.userStore.Authenticate(username, password)
}

// AuthorizeTopicAccess checks if a user can access a topic with the given permission.
func (a *Authorizer) AuthorizeTopicAccess(username string, topic string, perm Permission) error {
	if !a.enabled {
		return nil // Auth disabled, allow all
	}

	// Check if topic is public
	acl, hasACL := a.aclStore.GetTopicACL(topic)
	isPublicTopic := (hasACL && acl.Public) || (!hasACL && a.aclStore.DefaultPublic)

	// Public topics are accessible to everyone (including anonymous users)
	if isPublicTopic {
		return nil
	}

	// Non-public topic requires authentication
	if username == "" {
		return ErrAuthRequired
	}

	// Check user permissions
	user, exists := a.userStore.GetUser(username)
	if !exists {
		return ErrUnauthorized
	}

	if !user.Enabled {
		return ErrUserDisabled
	}

	// Check if user has the required permission via roles
	if a.userStore.HasPermission(username, perm) {
		return nil
	}

	// Check topic-specific ACL
	if hasACL {
		// Check allowed users
		for _, allowedUser := range acl.AllowedUsers {
			if allowedUser == username {
				return nil
			}
		}

		// Check allowed roles
		for _, allowedRole := range acl.AllowedRoles {
			for _, userRole := range user.Roles {
				if userRole == allowedRole {
					return nil
				}
			}
		}
	}

	return ErrUnauthorized
}

// CanProduce checks if a user can produce to a topic.
func (a *Authorizer) CanProduce(username, topic string) error {
	return a.AuthorizeTopicAccess(username, topic, PermissionWrite)
}

// CanConsume checks if a user can consume from a topic.
func (a *Authorizer) CanConsume(username, topic string) error {
	return a.AuthorizeTopicAccess(username, topic, PermissionRead)
}

// CanAdmin checks if a user can perform admin operations on a topic.
// Unlike read/write operations, admin operations are never allowed
// just because a topic is public - they always require admin permission.
func (a *Authorizer) CanAdmin(username, topic string) error {
	if !a.enabled {
		return nil // Auth disabled, allow all
	}

	// Admin operations always require authentication
	if username == "" {
		return ErrAuthRequired
	}

	// Check user exists and is enabled
	user, exists := a.userStore.GetUser(username)
	if !exists {
		return ErrUnauthorized
	}
	if !user.Enabled {
		return ErrUserDisabled
	}

	// Admin operations require admin permission - public topic status is ignored
	if a.userStore.HasPermission(username, PermissionAdmin) {
		return nil
	}

	return ErrUnauthorized
}

// CanManageUsers checks if a user can manage other users.
func (a *Authorizer) CanManageUsers(username string) error {
	if !a.enabled {
		return nil
	}
	if username == "" {
		return ErrAuthRequired
	}
	if !a.userStore.HasPermission(username, PermissionAdmin) {
		return ErrUnauthorized
	}
	return nil
}

// UserStore returns the underlying user store.
func (a *Authorizer) UserStore() *UserStore {
	return a.userStore
}

// ACLStore returns the underlying ACL store.
func (a *Authorizer) ACLStore() *ACLStore {
	return a.aclStore
}

// ApplyCreateUser creates a user with an already-hashed password.
// This implements the cluster.AuthApplier interface.
func (a *Authorizer) ApplyCreateUser(username, passwordHash string, roles []string) error {
	return a.userStore.ApplyCreateUser(username, passwordHash, roles)
}

// ApplyDeleteUser removes a user.
// This implements the cluster.AuthApplier interface.
func (a *Authorizer) ApplyDeleteUser(username string) error {
	return a.userStore.ApplyDeleteUser(username)
}

// ApplyUpdateUser updates a user's roles, enabled status, and/or password hash.
// This implements the cluster.AuthApplier interface.
func (a *Authorizer) ApplyUpdateUser(username string, roles []string, enabled *bool, passwordHash string) error {
	return a.userStore.ApplyUpdateUser(username, roles, enabled, passwordHash)
}

// ApplySetACL sets an ACL for a topic.
// This implements the cluster.AuthApplier interface.
func (a *Authorizer) ApplySetACL(topic string, public bool, allowedUsers, allowedRoles []string) {
	a.aclStore.ApplySetACL(topic, public, allowedUsers, allowedRoles)
}

// ApplyDeleteACL removes an ACL for a topic.
// This implements the cluster.AuthApplier interface.
func (a *Authorizer) ApplyDeleteACL(topic string) {
	a.aclStore.ApplyDeleteACL(topic)
}

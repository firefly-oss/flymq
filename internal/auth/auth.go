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

// Package auth provides authentication and authorization for FlyMQ.
package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Permission represents an access permission type.
type Permission string

const (
	PermissionRead  Permission = "read"
	PermissionWrite Permission = "write"
	PermissionAdmin Permission = "admin"
)

// Role represents a user role with associated permissions.
type Role struct {
	Name        string       `json:"name"`
	Permissions []Permission `json:"permissions"`
	Description string       `json:"description,omitempty"`
}

// DefaultRoles defines the built-in roles.
var DefaultRoles = map[string]*Role{
	"admin": {
		Name:        "admin",
		Permissions: []Permission{PermissionRead, PermissionWrite, PermissionAdmin},
		Description: "Full access to all operations",
	},
	"producer": {
		Name:        "producer",
		Permissions: []Permission{PermissionWrite},
		Description: "Write-only access (produce messages)",
	},
	"consumer": {
		Name:        "consumer",
		Permissions: []Permission{PermissionRead},
		Description: "Read-only access (consume messages)",
	},
	"guest": {
		Name:        "guest",
		Permissions: []Permission{},
		Description: "Access to public topics only",
	},
}

// User represents an authenticated user.
type User struct {
	Username     string    `json:"username"`
	PasswordHash string    `json:"password_hash"`
	Roles        []string  `json:"roles"`
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// UserStore manages user credentials and authentication.
type UserStore struct {
	mu       sync.RWMutex
	users    map[string]*User
	roles    map[string]*Role
	filePath string
}

// NewUserStore creates a new user store.
func NewUserStore(filePath string) *UserStore {
	store := &UserStore{
		users:    make(map[string]*User),
		roles:    make(map[string]*Role),
		filePath: filePath,
	}
	// Initialize with default roles
	for name, role := range DefaultRoles {
		store.roles[name] = role
	}
	return store
}

// Load loads users from the configured file path.
func (s *UserStore) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.filePath == "" {
		return nil
	}

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file yet, start fresh
		}
		return fmt.Errorf("failed to read user store: %w", err)
	}

	var storeData struct {
		Users map[string]*User `json:"users"`
		Roles map[string]*Role `json:"roles,omitempty"`
	}
	if err := json.Unmarshal(data, &storeData); err != nil {
		return fmt.Errorf("failed to parse user store: %w", err)
	}

	s.users = storeData.Users
	if s.users == nil {
		s.users = make(map[string]*User)
	}
	// Merge custom roles with defaults
	for name, role := range storeData.Roles {
		s.roles[name] = role
	}
	return nil
}

// Save persists users to the configured file path.
func (s *UserStore) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.filePath == "" {
		return nil
	}

	storeData := struct {
		Users map[string]*User `json:"users"`
		Roles map[string]*Role `json:"roles,omitempty"`
	}{
		Users: s.users,
		Roles: s.roles,
	}

	data, err := json.MarshalIndent(storeData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal user store: %w", err)
	}

	return os.WriteFile(s.filePath, data, 0600)
}

// CreateUser creates a new user with the given password.
func (s *UserStore) CreateUser(username, password string, roles []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; exists {
		return fmt.Errorf("user %q already exists", username)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	now := time.Now()
	s.users[username] = &User{
		Username:     username,
		PasswordHash: string(hash),
		Roles:        roles,
		Enabled:      true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	return nil
}

// Authenticate verifies username and password, returning the user if valid.
func (s *UserStore) Authenticate(username, password string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, exists := s.users[username]
	if !exists {
		return nil, ErrInvalidCredentials
	}

	if !user.Enabled {
		return nil, ErrUserDisabled
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, ErrInvalidCredentials
	}

	return user, nil
}

// GetUser returns a user by username.
func (s *UserStore) GetUser(username string) (*User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	user, exists := s.users[username]
	return user, exists
}

// DeleteUser removes a user.
func (s *UserStore) DeleteUser(username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; !exists {
		return fmt.Errorf("user %q not found", username)
	}
	delete(s.users, username)
	return nil
}

// UpdatePassword updates a user's password.
func (s *UserStore) UpdatePassword(username, newPassword string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	user, exists := s.users[username]
	if !exists {
		return fmt.Errorf("user %q not found", username)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	user.PasswordHash = string(hash)
	user.UpdatedAt = time.Now()
	return nil
}

// SetUserRoles updates a user's roles.
func (s *UserStore) SetUserRoles(username string, roles []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	user, exists := s.users[username]
	if !exists {
		return fmt.Errorf("user %q not found", username)
	}

	user.Roles = roles
	user.UpdatedAt = time.Now()
	return nil
}

// SetUserEnabled enables or disables a user.
func (s *UserStore) SetUserEnabled(username string, enabled bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	user, exists := s.users[username]
	if !exists {
		return fmt.Errorf("user %q not found", username)
	}

	user.Enabled = enabled
	user.UpdatedAt = time.Now()
	return nil
}

// ListUsers returns all usernames.
func (s *UserStore) ListUsers() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	users := make([]string, 0, len(s.users))
	for username := range s.users {
		users = append(users, username)
	}
	return users
}

// GetRole returns a role by name.
func (s *UserStore) GetRole(name string) (*Role, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	role, exists := s.roles[name]
	return role, exists
}

// ListRoles returns all available roles.
func (s *UserStore) ListRoles() []*Role {
	s.mu.RLock()
	defer s.mu.RUnlock()

	roles := make([]*Role, 0, len(s.roles))
	for _, role := range s.roles {
		roles = append(roles, role)
	}
	return roles
}

// GetUserPermissions returns all permissions for a user based on their roles.
func (s *UserStore) GetUserPermissions(username string) []Permission {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, exists := s.users[username]
	if !exists {
		return nil
	}

	permSet := make(map[Permission]bool)
	for _, roleName := range user.Roles {
		if role, ok := s.roles[roleName]; ok {
			for _, perm := range role.Permissions {
				permSet[perm] = true
			}
		}
	}

	perms := make([]Permission, 0, len(permSet))
	for perm := range permSet {
		perms = append(perms, perm)
	}
	return perms
}

// HasPermission checks if a user has a specific permission.
func (s *UserStore) HasPermission(username string, perm Permission) bool {
	perms := s.GetUserPermissions(username)
	for _, p := range perms {
		if p == perm {
			return true
		}
	}
	return false
}

// HashPassword hashes a password using bcrypt.
// This is exposed for use by cluster replication to hash passwords before replicating.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("failed to hash password: %w", err)
	}
	return string(hash), nil
}

// ApplyCreateUser creates a user with an already-hashed password.
// This is used for cluster replication where the password is hashed on the leader
// and replicated to followers.
func (s *UserStore) ApplyCreateUser(username, passwordHash string, roles []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; exists {
		return fmt.Errorf("user %q already exists", username)
	}

	now := time.Now()
	s.users[username] = &User{
		Username:     username,
		PasswordHash: passwordHash,
		Roles:        roles,
		Enabled:      true,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	return nil
}

// ApplyDeleteUser removes a user.
// This is used for cluster replication.
func (s *UserStore) ApplyDeleteUser(username string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; !exists {
		return fmt.Errorf("user %q not found", username)
	}
	delete(s.users, username)
	return nil
}

// ApplyUpdateUser updates a user's roles, enabled status, and/or password hash.
// This is used for cluster replication.
func (s *UserStore) ApplyUpdateUser(username string, roles []string, enabled *bool, passwordHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	user, exists := s.users[username]
	if !exists {
		return fmt.Errorf("user %q not found", username)
	}

	if roles != nil {
		user.Roles = roles
	}
	if enabled != nil {
		user.Enabled = *enabled
	}
	if passwordHash != "" {
		user.PasswordHash = passwordHash
	}
	user.UpdatedAt = time.Now()
	return nil
}

// Common errors
var (
	ErrInvalidCredentials = errors.New("invalid username or password")
	ErrUserDisabled       = errors.New("user account is disabled")
	ErrUnauthorized       = errors.New("unauthorized")
	ErrAuthRequired       = errors.New("authentication required")
)

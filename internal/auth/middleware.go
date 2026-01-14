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
	"context"
)

// contextKey is a type for context keys to avoid collisions.
type contextKey string

const (
	// UserContextKey is the context key for the authenticated user.
	UserContextKey contextKey = "auth_user"
	// UsernameContextKey is the context key for the username string.
	UsernameContextKey contextKey = "auth_username"
)

// AuthRequest represents an authentication request payload.
type AuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// AuthResponse represents an authentication response payload.
type AuthResponse struct {
	Success     bool     `json:"success"`
	Username    string   `json:"username,omitempty"`
	Roles       []string `json:"roles,omitempty"`
	Error       string   `json:"error,omitempty"`
	Permissions []string `json:"permissions,omitempty"`
}

// NOTE: Authentication over the wire protocol uses binary encoding.
// See protocol.DecodeBinaryAuthRequest and protocol.EncodeBinaryAuthResponse.
// The JSON tags on AuthRequest/AuthResponse are for HTTP admin API only.

// NewSuccessResponse creates a successful authentication response.
func NewSuccessResponse(user *User, perms []Permission) *AuthResponse {
	permStrs := make([]string, len(perms))
	for i, p := range perms {
		permStrs[i] = string(p)
	}
	return &AuthResponse{
		Success:     true,
		Username:    user.Username,
		Roles:       user.Roles,
		Permissions: permStrs,
	}
}

// NewErrorResponse creates an error authentication response.
func NewErrorResponse(err error) *AuthResponse {
	return &AuthResponse{
		Success: false,
		Error:   err.Error(),
	}
}

// WithUser adds the authenticated user to the context.
func WithUser(ctx context.Context, user *User) context.Context {
	ctx = context.WithValue(ctx, UserContextKey, user)
	if user != nil {
		ctx = context.WithValue(ctx, UsernameContextKey, user.Username)
	}
	return ctx
}

// UserFromContext retrieves the authenticated user from the context.
func UserFromContext(ctx context.Context) (*User, bool) {
	user, ok := ctx.Value(UserContextKey).(*User)
	return user, ok
}

// UsernameFromContext retrieves the username from the context.
func UsernameFromContext(ctx context.Context) string {
	username, ok := ctx.Value(UsernameContextKey).(string)
	if !ok {
		return ""
	}
	return username
}

// WhoAmIResponse represents the response for the WhoAmI operation.
type WhoAmIResponse struct {
	Authenticated bool     `json:"authenticated"`
	Username      string   `json:"username,omitempty"`
	Roles         []string `json:"roles,omitempty"`
	Permissions   []string `json:"permissions,omitempty"`
}

// NOTE: WhoAmI over the wire protocol uses binary encoding.
// See protocol.EncodeBinaryAuthResponse for wire protocol encoding.

// NewWhoAmIResponse creates a WhoAmI response for an authenticated user.
func NewWhoAmIResponse(user *User, perms []Permission) *WhoAmIResponse {
	if user == nil {
		return &WhoAmIResponse{
			Authenticated: false,
		}
	}
	permStrs := make([]string, len(perms))
	for i, p := range perms {
		permStrs[i] = string(p)
	}
	return &WhoAmIResponse{
		Authenticated: true,
		Username:      user.Username,
		Roles:         user.Roles,
		Permissions:   permStrs,
	}
}

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
	"context"
	"encoding/base64"
	"net/http"
	"strings"

	"flymq/internal/auth"
)

// contextKey is a custom type for context keys to avoid collisions.
type contextKey string

const (
	// userContextKey is the key for storing the authenticated user in context.
	userContextKey contextKey = "user"
	// usernameContextKey is the key for storing the username in context.
	usernameContextKey contextKey = "username"
)

// Permission levels for endpoint protection.
type Permission int

const (
	// PermissionNone - endpoint is public, no auth required
	PermissionNone Permission = iota
	// PermissionRead - requires read permission
	PermissionRead
	// PermissionWrite - requires write permission
	PermissionWrite
	// PermissionAdmin - requires admin permission
	PermissionAdmin
)

// AuthMiddleware provides authentication and authorization for HTTP handlers.
type AuthMiddleware struct {
	authorizer  *auth.Authorizer
	authEnabled bool
}

// NewAuthMiddleware creates a new authentication middleware.
func NewAuthMiddleware(authorizer *auth.Authorizer, authEnabled bool) *AuthMiddleware {
	return &AuthMiddleware{
		authorizer:  authorizer,
		authEnabled: authEnabled,
	}
}

// Authenticate extracts credentials and validates them, adding user to context.
// Returns the wrapped handler that performs authentication.
func (m *AuthMiddleware) Authenticate(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// If auth is not enabled, pass through
		if !m.authEnabled || m.authorizer == nil || !m.authorizer.IsEnabled() {
			next(w, r)
			return
		}

		// Try to extract credentials
		username, password, ok := extractBasicAuth(r)
		if !ok {
			// No credentials provided - set empty context and continue
			// Handler will check if endpoint requires auth
			next(w, r)
			return
		}

		// Validate credentials
		user, err := m.authorizer.Authenticate(username, password)
		if err != nil {
			writeAuthError(w, "Invalid credentials", http.StatusUnauthorized)
			return
		}

		// Add user to context
		ctx := context.WithValue(r.Context(), userContextKey, user)
		ctx = context.WithValue(ctx, usernameContextKey, username)
		next(w, r.WithContext(ctx))
	}
}

// RequireAuth wraps a handler to require authentication.
func (m *AuthMiddleware) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return m.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		// If auth is not enabled, pass through
		if !m.authEnabled || m.authorizer == nil || !m.authorizer.IsEnabled() {
			next(w, r)
			return
		}

		// Check if user is authenticated
		if GetUsername(r) == "" {
			writeAuthError(w, "Authentication required", http.StatusUnauthorized)
			return
		}

		next(w, r)
	})
}

// RequirePermission wraps a handler to require a specific permission.
func (m *AuthMiddleware) RequirePermission(perm Permission, next http.HandlerFunc) http.HandlerFunc {
	return m.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		// If auth is not enabled, pass through
		if !m.authEnabled || m.authorizer == nil || !m.authorizer.IsEnabled() {
			next(w, r)
			return
		}

		// Public endpoints don't require auth
		if perm == PermissionNone {
			next(w, r)
			return
		}

		// Check if user is authenticated
		username := GetUsername(r)
		if username == "" {
			writeAuthError(w, "Authentication required", http.StatusUnauthorized)
			return
		}

		// Check permission
		var hasPermission bool
		userStore := m.authorizer.UserStore()
		switch perm {
		case PermissionRead:
			hasPermission = userStore.HasPermission(username, auth.PermissionRead) ||
				userStore.HasPermission(username, auth.PermissionAdmin)
		case PermissionWrite:
			hasPermission = userStore.HasPermission(username, auth.PermissionWrite) ||
				userStore.HasPermission(username, auth.PermissionAdmin)
		case PermissionAdmin:
			hasPermission = userStore.HasPermission(username, auth.PermissionAdmin)
		}

		if !hasPermission {
			writeAuthError(w, "Insufficient permissions", http.StatusForbidden)
			return
		}

		next(w, r)
	})
}

// RequireAdminOrSelf wraps a handler that requires admin permission OR the user to be operating on themselves.
// The usernameParam should be the URL path parameter containing the target username.
func (m *AuthMiddleware) RequireAdminOrSelf(extractUsername func(r *http.Request) string, next http.HandlerFunc) http.HandlerFunc {
	return m.Authenticate(func(w http.ResponseWriter, r *http.Request) {
		// If auth is not enabled, pass through
		if !m.authEnabled || m.authorizer == nil || !m.authorizer.IsEnabled() {
			next(w, r)
			return
		}

		// Check if user is authenticated
		currentUser := GetUsername(r)
		if currentUser == "" {
			writeAuthError(w, "Authentication required", http.StatusUnauthorized)
			return
		}

		// Extract target username from request
		targetUser := extractUsername(r)

		// Allow if user is operating on themselves
		if currentUser == targetUser {
			next(w, r)
			return
		}

		// Otherwise require admin permission
		if !m.authorizer.UserStore().HasPermission(currentUser, auth.PermissionAdmin) {
			writeAuthError(w, "Insufficient permissions", http.StatusForbidden)
			return
		}

		next(w, r)
	})
}

// extractBasicAuth extracts username and password from HTTP Basic Auth header.
func extractBasicAuth(r *http.Request) (username, password string, ok bool) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", "", false
	}

	// Check for Basic auth
	if !strings.HasPrefix(authHeader, "Basic ") {
		return "", "", false
	}

	// Decode base64 credentials
	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", "", false
	}

	// Split username:password
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}

	return parts[0], parts[1], true
}

// GetUser returns the authenticated user from the request context.
func GetUser(r *http.Request) *auth.User {
	if user, ok := r.Context().Value(userContextKey).(*auth.User); ok {
		return user
	}
	return nil
}

// GetUsername returns the authenticated username from the request context.
func GetUsername(r *http.Request) string {
	if username, ok := r.Context().Value(usernameContextKey).(string); ok {
		return username
	}
	return ""
}

// writeAuthError writes an authentication/authorization error response.
func writeAuthError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("WWW-Authenticate", `Basic realm="FlyMQ Admin API"`)
	w.WriteHeader(status)
	w.Write([]byte(`{"error":"` + message + `"}`))
}

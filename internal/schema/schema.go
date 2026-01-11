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
Package schema provides message schema validation for FlyMQ.

OVERVIEW:
=========
Schema validation ensures messages conform to a defined structure.
This prevents malformed data from entering the system.

SUPPORTED SCHEMA TYPES:
=======================
- JSON Schema: Full validation support
- Avro: Basic support (placeholder)
- Protobuf: Basic support (placeholder)

SCHEMA EVOLUTION:
=================
Schemas can evolve over time with compatibility modes:
- NONE: No compatibility checking
- BACKWARD: New schema can read old data
- FORWARD: Old schema can read new data
- FULL: Both backward and forward compatible

VERSIONING:
===========
Each topic can have multiple schema versions. Messages can specify
which version to validate against, or use the latest.

USAGE:
======

	registry, _ := schema.NewRegistry("/var/lib/flymq/schemas")
	schema, _ := registry.Register("orders", schema.SchemaTypeJSON, jsonSchema, schema.CompatibilityBackward)

	validator := schema.NewValidator(registry)
	result := validator.Validate("orders", messageData)
	if !result.Valid {
	    // Handle validation errors
	}
*/
package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"flymq/internal/logging"
)

// SchemaType represents the type of schema.
type SchemaType string

const (
	SchemaTypeJSON  SchemaType = "json"
	SchemaTypeAvro  SchemaType = "avro"
	SchemaTypeProto SchemaType = "protobuf"
)

// CompatibilityMode defines schema evolution compatibility rules.
type CompatibilityMode string

const (
	CompatibilityNone     CompatibilityMode = "none"     // No compatibility checking
	CompatibilityBackward CompatibilityMode = "backward" // New schema can read old data
	CompatibilityForward  CompatibilityMode = "forward"  // Old schema can read new data
	CompatibilityFull     CompatibilityMode = "full"     // Both backward and forward
)

// Schema represents a message schema.
type Schema struct {
	ID            string            `json:"id"`
	Topic         string            `json:"topic"`
	Version       int               `json:"version"`
	Type          SchemaType        `json:"type"`
	Definition    string            `json:"definition"`
	Compatibility CompatibilityMode `json:"compatibility"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

// ValidationResult contains the result of schema validation.
type ValidationResult struct {
	Valid  bool     `json:"valid"`
	Errors []string `json:"errors,omitempty"`
}

// Registry manages schemas for topics.
type Registry struct {
	mu      sync.RWMutex
	schemas map[string]map[int]*Schema // topic -> version -> schema
	latest  map[string]int             // topic -> latest version
	dir     string
	logger  *logging.Logger
}

// NewRegistry creates a new schema registry.
func NewRegistry(dir string) (*Registry, error) {
	r := &Registry{
		schemas: make(map[string]map[int]*Schema),
		latest:  make(map[string]int),
		dir:     dir,
		logger:  logging.NewLogger("schema"),
	}

	if dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create schema directory: %w", err)
		}
		if err := r.loadSchemas(); err != nil {
			return nil, err
		}
	}

	return r, nil
}

// Register registers a new schema for a topic.
func (r *Registry) Register(topic string, schemaType SchemaType, definition string, compat CompatibilityMode) (*Schema, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Validate the schema definition
	if err := r.validateDefinition(schemaType, definition); err != nil {
		return nil, fmt.Errorf("invalid schema definition: %w", err)
	}

	// Get next version
	version := 1
	if v, exists := r.latest[topic]; exists {
		version = v + 1

		// Check compatibility with previous version
		if compat != CompatibilityNone {
			prevSchema := r.schemas[topic][v]
			if err := r.checkCompatibility(prevSchema, definition, compat); err != nil {
				return nil, fmt.Errorf("schema compatibility check failed: %w", err)
			}
		}
	}

	// Create schema
	schema := &Schema{
		ID:            fmt.Sprintf("%s-v%d", topic, version),
		Topic:         topic,
		Version:       version,
		Type:          schemaType,
		Definition:    definition,
		Compatibility: compat,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	// Store schema
	if r.schemas[topic] == nil {
		r.schemas[topic] = make(map[int]*Schema)
	}
	r.schemas[topic][version] = schema
	r.latest[topic] = version

	// Persist schema
	if r.dir != "" {
		if err := r.saveSchema(schema); err != nil {
			return nil, err
		}
	}

	r.logger.Info("Registered schema", "topic", topic, "version", version, "type", schemaType)
	return schema, nil
}

// Get retrieves a schema by topic and version.
func (r *Registry) Get(topic string, version int) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if versions, exists := r.schemas[topic]; exists {
		if schema, exists := versions[version]; exists {
			return schema, nil
		}
	}
	return nil, fmt.Errorf("schema not found: %s v%d", topic, version)
}

// GetLatest retrieves the latest schema for a topic.
func (r *Registry) GetLatest(topic string) (*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if version, exists := r.latest[topic]; exists {
		return r.schemas[topic][version], nil
	}
	return nil, fmt.Errorf("no schema found for topic: %s", topic)
}

// List returns all schemas for a topic, or all schemas if topic is empty.
func (r *Registry) List(topic string) ([]*Schema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// If topic is empty, return all schemas
	if topic == "" {
		var allSchemas []*Schema
		for _, versions := range r.schemas {
			for _, schema := range versions {
				allSchemas = append(allSchemas, schema)
			}
		}
		return allSchemas, nil
	}

	versions, exists := r.schemas[topic]
	if !exists {
		return nil, nil
	}

	schemas := make([]*Schema, 0, len(versions))
	for _, schema := range versions {
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

// ListTopics returns all topics with schemas.
func (r *Registry) ListTopics() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	topics := make([]string, 0, len(r.schemas))
	for topic := range r.schemas {
		topics = append(topics, topic)
	}
	return topics
}

// Delete removes a schema version.
func (r *Registry) Delete(topic string, version int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if versions, exists := r.schemas[topic]; exists {
		if _, exists := versions[version]; exists {
			delete(versions, version)
			if len(versions) == 0 {
				delete(r.schemas, topic)
				delete(r.latest, topic)
			}
			r.logger.Info("Deleted schema", "topic", topic, "version", version)
			return nil
		}
	}
	return fmt.Errorf("schema not found: %s v%d", topic, version)
}

// validateDefinition validates a schema definition.
func (r *Registry) validateDefinition(schemaType SchemaType, definition string) error {
	switch schemaType {
	case SchemaTypeJSON:
		var js map[string]interface{}
		if err := json.Unmarshal([]byte(definition), &js); err != nil {
			return fmt.Errorf("invalid JSON schema: %w", err)
		}
		return nil
	case SchemaTypeAvro, SchemaTypeProto:
		// Basic validation - just check it's not empty
		if definition == "" {
			return errors.New("schema definition cannot be empty")
		}
		return nil
	default:
		return fmt.Errorf("unsupported schema type: %s", schemaType)
	}
}

// checkCompatibility checks if a new schema is compatible with the previous version.
func (r *Registry) checkCompatibility(prev *Schema, newDef string, mode CompatibilityMode) error {
	// For JSON schemas, we do basic field compatibility checking
	if prev.Type != SchemaTypeJSON {
		return nil // Skip compatibility check for non-JSON schemas
	}

	var prevSchema, newSchema map[string]interface{}
	if err := json.Unmarshal([]byte(prev.Definition), &prevSchema); err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(newDef), &newSchema); err != nil {
		return err
	}

	switch mode {
	case CompatibilityBackward:
		// New schema should be able to read old data
		// All required fields in new schema should exist in old schema
		return r.checkBackwardCompatibility(prevSchema, newSchema)
	case CompatibilityForward:
		// Old schema should be able to read new data
		return r.checkBackwardCompatibility(newSchema, prevSchema)
	case CompatibilityFull:
		if err := r.checkBackwardCompatibility(prevSchema, newSchema); err != nil {
			return err
		}
		return r.checkBackwardCompatibility(newSchema, prevSchema)
	}
	return nil
}

func (r *Registry) checkBackwardCompatibility(oldSchema, newSchema map[string]interface{}) error {
	// Check if required fields in new schema exist in old schema
	newRequired, _ := newSchema["required"].([]interface{})
	oldProps, _ := oldSchema["properties"].(map[string]interface{})

	for _, req := range newRequired {
		fieldName, ok := req.(string)
		if !ok {
			continue
		}
		if _, exists := oldProps[fieldName]; !exists {
			return fmt.Errorf("new required field '%s' not in old schema", fieldName)
		}
	}
	return nil
}

// saveSchema persists a schema to disk.
func (r *Registry) saveSchema(schema *Schema) error {
	filename := filepath.Join(r.dir, fmt.Sprintf("%s-v%d.json", schema.Topic, schema.Version))
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

// loadSchemas loads all schemas from disk.
func (r *Registry) loadSchemas() error {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		data, err := os.ReadFile(filepath.Join(r.dir, entry.Name()))
		if err != nil {
			r.logger.Warn("Failed to read schema file", "file", entry.Name(), "error", err)
			continue
		}

		var schema Schema
		if err := json.Unmarshal(data, &schema); err != nil {
			r.logger.Warn("Failed to parse schema file", "file", entry.Name(), "error", err)
			continue
		}

		if r.schemas[schema.Topic] == nil {
			r.schemas[schema.Topic] = make(map[int]*Schema)
		}
		r.schemas[schema.Topic][schema.Version] = &schema

		if schema.Version > r.latest[schema.Topic] {
			r.latest[schema.Topic] = schema.Version
		}
	}

	r.logger.Info("Loaded schemas", "count", len(r.schemas))
	return nil
}

// ApplyRegister implements the SchemaRegistryApplier interface.
// This allows the registry to be used by the broker for cluster replication.
func (r *Registry) ApplyRegister(name, schemaType, definition, compatibility string) error {
	// Convert string types to our internal types
	var st SchemaType
	switch schemaType {
	case "json":
		st = SchemaTypeJSON
	case "avro":
		st = SchemaTypeAvro
	case "protobuf", "proto":
		st = SchemaTypeProto
	default:
		return fmt.Errorf("unsupported schema type: %s", schemaType)
	}

	var compat CompatibilityMode
	switch compatibility {
	case "none", "":
		compat = CompatibilityNone
	case "backward":
		compat = CompatibilityBackward
	case "forward":
		compat = CompatibilityForward
	case "full":
		compat = CompatibilityFull
	default:
		compat = CompatibilityBackward // Default to backward
	}

	_, err := r.Register(name, st, definition, compat)
	return err
}

// ApplyDelete implements the SchemaRegistryApplier interface.
// This allows the registry to be used by the broker for cluster replication.
func (r *Registry) ApplyDelete(name string, version int) error {
	return r.Delete(name, version)
}

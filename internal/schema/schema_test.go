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

package schema

import (
	"testing"
)

func TestNewRegistry(t *testing.T) {
	dir := t.TempDir()
	registry, err := NewRegistry(dir)
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}
	if registry == nil {
		t.Fatal("Expected non-nil registry")
	}
}

func TestNewRegistryInMemory(t *testing.T) {
	registry, err := NewRegistry("")
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}
	if registry == nil {
		t.Fatal("Expected non-nil registry")
	}
}

func TestRegisterSchema(t *testing.T) {
	registry, _ := NewRegistry("")

	schemaDef := `{"type": "object", "properties": {"name": {"type": "string"}}}`
	schema, err := registry.Register("test-topic", SchemaTypeJSON, schemaDef, CompatibilityNone)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	if schema.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", schema.Topic)
	}
	if schema.Version != 1 {
		t.Errorf("Expected version 1, got %d", schema.Version)
	}
	if schema.Type != SchemaTypeJSON {
		t.Errorf("Expected type JSON, got %s", schema.Type)
	}
}

func TestRegisterInvalidSchema(t *testing.T) {
	registry, _ := NewRegistry("")

	_, err := registry.Register("test-topic", SchemaTypeJSON, "invalid json", CompatibilityNone)
	if err == nil {
		t.Error("Expected error for invalid JSON schema")
	}
}

func TestGetSchema(t *testing.T) {
	registry, _ := NewRegistry("")

	schemaDef := `{"type": "object"}`
	registry.Register("test-topic", SchemaTypeJSON, schemaDef, CompatibilityNone)

	schema, err := registry.Get("test-topic", 1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if schema.Version != 1 {
		t.Errorf("Expected version 1, got %d", schema.Version)
	}
}

func TestGetNonExistentSchema(t *testing.T) {
	registry, _ := NewRegistry("")

	_, err := registry.Get("non-existent", 1)
	if err == nil {
		t.Error("Expected error for non-existent schema")
	}
}

func TestGetLatestSchema(t *testing.T) {
	registry, _ := NewRegistry("")

	registry.Register("test-topic", SchemaTypeJSON, `{"type": "object"}`, CompatibilityNone)
	registry.Register("test-topic", SchemaTypeJSON, `{"type": "object", "properties": {}}`, CompatibilityNone)

	schema, err := registry.GetLatest("test-topic")
	if err != nil {
		t.Fatalf("GetLatest failed: %v", err)
	}
	if schema.Version != 2 {
		t.Errorf("Expected version 2, got %d", schema.Version)
	}
}

func TestListSchemas(t *testing.T) {
	registry, _ := NewRegistry("")

	registry.Register("test-topic", SchemaTypeJSON, `{"type": "object"}`, CompatibilityNone)
	registry.Register("test-topic", SchemaTypeJSON, `{"type": "object", "properties": {}}`, CompatibilityNone)

	schemas, err := registry.List("test-topic")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(schemas) != 2 {
		t.Errorf("Expected 2 schemas, got %d", len(schemas))
	}
}

func TestListTopics(t *testing.T) {
	registry, _ := NewRegistry("")

	registry.Register("topic-a", SchemaTypeJSON, `{"type": "object"}`, CompatibilityNone)
	registry.Register("topic-b", SchemaTypeJSON, `{"type": "object"}`, CompatibilityNone)

	topics := registry.ListTopics()
	if len(topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(topics))
	}
}

func TestDeleteSchema(t *testing.T) {
	registry, _ := NewRegistry("")

	registry.Register("test-topic", SchemaTypeJSON, `{"type": "object"}`, CompatibilityNone)

	err := registry.Delete("test-topic", 1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = registry.Get("test-topic", 1)
	if err == nil {
		t.Error("Expected error after deletion")
	}
}

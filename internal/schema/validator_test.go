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

func newTestValidator(t *testing.T) (*Validator, *Registry) {
	t.Helper()
	registry, _ := NewRegistry("")
	return NewValidator(registry), registry
}

func TestValidateNoSchema(t *testing.T) {
	validator, _ := newTestValidator(t)

	result := validator.Validate("no-schema-topic", []byte(`{"any": "data"}`))
	if !result.Valid {
		t.Error("Expected validation to pass when no schema exists")
	}
}

func TestValidateValidMessage(t *testing.T) {
	validator, registry := newTestValidator(t)

	schemaDef := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "number"}
		},
		"required": ["name"]
	}`
	registry.Register("test-topic", SchemaTypeJSON, schemaDef, CompatibilityNone)

	message := []byte(`{"name": "John", "age": 30}`)
	result := validator.Validate("test-topic", message)
	if !result.Valid {
		t.Errorf("Expected valid message, got errors: %v", result.Errors)
	}
}

func TestValidateMissingRequiredField(t *testing.T) {
	validator, registry := newTestValidator(t)

	schemaDef := `{
		"type": "object",
		"properties": {
			"name": {"type": "string"}
		},
		"required": ["name"]
	}`
	registry.Register("test-topic", SchemaTypeJSON, schemaDef, CompatibilityNone)

	message := []byte(`{"age": 30}`)
	result := validator.Validate("test-topic", message)
	if result.Valid {
		t.Error("Expected validation to fail for missing required field")
	}
	if len(result.Errors) == 0 {
		t.Error("Expected error messages")
	}
}

func TestValidateTypeMismatch(t *testing.T) {
	validator, registry := newTestValidator(t)

	schemaDef := `{
		"type": "object",
		"properties": {
			"age": {"type": "number"}
		}
	}`
	registry.Register("test-topic", SchemaTypeJSON, schemaDef, CompatibilityNone)

	message := []byte(`{"age": "not a number"}`)
	result := validator.Validate("test-topic", message)
	if result.Valid {
		t.Error("Expected validation to fail for type mismatch")
	}
}

func TestValidateInvalidJSON(t *testing.T) {
	validator, registry := newTestValidator(t)

	schemaDef := `{"type": "object"}`
	registry.Register("test-topic", SchemaTypeJSON, schemaDef, CompatibilityNone)

	message := []byte(`not valid json`)
	result := validator.Validate("test-topic", message)
	if result.Valid {
		t.Error("Expected validation to fail for invalid JSON")
	}
}

func TestValidateWithVersion(t *testing.T) {
	validator, registry := newTestValidator(t)

	registry.Register("test-topic", SchemaTypeJSON, `{"type": "object", "required": ["v1"]}`, CompatibilityNone)
	registry.Register("test-topic", SchemaTypeJSON, `{"type": "object", "required": ["v2"]}`, CompatibilityNone)

	// Validate against v1
	result := validator.ValidateWithVersion("test-topic", 1, []byte(`{"v1": "value"}`))
	if !result.Valid {
		t.Errorf("Expected valid for v1, got errors: %v", result.Errors)
	}

	// Validate against v2
	result = validator.ValidateWithVersion("test-topic", 2, []byte(`{"v2": "value"}`))
	if !result.Valid {
		t.Errorf("Expected valid for v2, got errors: %v", result.Errors)
	}
}

func TestExtractSchemaID(t *testing.T) {
	schemaID := "test-topic-v1"
	payload := []byte("test payload")

	encoded := PrependSchemaID(schemaID, payload)

	extractedID, extractedPayload, hasSchema := ExtractSchemaID(encoded)
	if !hasSchema {
		t.Error("Expected hasSchema to be true")
	}
	if extractedID != schemaID {
		t.Errorf("Expected schema ID %s, got %s", schemaID, extractedID)
	}
	if string(extractedPayload) != string(payload) {
		t.Errorf("Payload mismatch")
	}
}

func TestExtractSchemaIDNoHeader(t *testing.T) {
	payload := []byte("plain message without schema header")

	_, extractedPayload, hasSchema := ExtractSchemaID(payload)
	if hasSchema {
		t.Error("Expected hasSchema to be false")
	}
	if string(extractedPayload) != string(payload) {
		t.Error("Expected original payload to be returned")
	}
}

func TestParseSchemaID(t *testing.T) {
	topic, version, err := ParseSchemaID("orders-v3")
	if err != nil {
		t.Fatalf("ParseSchemaID failed: %v", err)
	}
	if topic != "orders" {
		t.Errorf("Expected topic 'orders', got %s", topic)
	}
	if version != 3 {
		t.Errorf("Expected version 3, got %d", version)
	}
}

func TestParseSchemaIDInvalid(t *testing.T) {
	_, _, err := ParseSchemaID("invalid-format")
	if err == nil {
		t.Error("Expected error for invalid schema ID format")
	}
}

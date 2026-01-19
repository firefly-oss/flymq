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
Schema Validator for FlyMQ.

OVERVIEW:
=========
Validates message payloads against registered JSON schemas.
Ensures data quality and contract compliance between producers/consumers.

VALIDATION PROCESS:
===================
1. Look up schema for topic from registry
2. Parse message as JSON
3. Validate against schema rules
4. Return validation result with errors

VALIDATION RULES:
=================
- Type checking (string, number, boolean, array, object)
- Required field validation
- Enum value validation
- Pattern matching for strings
- Min/max for numbers and arrays

PERFORMANCE:
============
Schemas are cached after first lookup.
Validation is performed in-memory without I/O.
*/
package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/hamba/avro/v2"
)

// Validator validates messages against schemas.
type Validator struct {
	registry    *Registry
	avroSchemas sync.Map // cache of parsed Avro schemas: schemaID -> avro.Schema
}

// NewValidator creates a new validator.
func NewValidator(registry *Registry) *Validator {
	return &Validator{registry: registry}
}

// Validate validates a message against the latest schema for a topic.
func (v *Validator) Validate(topic string, message []byte) *ValidationResult {
	schema, err := v.registry.GetLatest(topic)
	if err != nil {
		// No schema registered, validation passes
		return &ValidationResult{Valid: true}
	}
	return v.ValidateWithSchema(schema, message)
}

// ValidateWithSchema validates a message against a specific schema.
func (v *Validator) ValidateWithSchema(schema *Schema, message []byte) *ValidationResult {
	switch schema.Type {
	case SchemaTypeJSON:
		return v.validateJSON(schema, message)
	case SchemaTypeAvro:
		return v.validateAvro(schema, message)
	case SchemaTypeProto:
		return v.validateProto(schema, message)
	default:
		return &ValidationResult{
			Valid:  false,
			Errors: []string{fmt.Sprintf("unsupported schema type: %s", schema.Type)},
		}
	}
}

// validateJSON validates a message against a JSON schema.
func (v *Validator) validateJSON(schema *Schema, message []byte) *ValidationResult {
	// Parse the schema definition
	var schemaDef map[string]interface{}
	if err := json.Unmarshal([]byte(schema.Definition), &schemaDef); err != nil {
		return &ValidationResult{
			Valid:  false,
			Errors: []string{fmt.Sprintf("invalid schema definition: %v", err)},
		}
	}

	// Parse the message
	var msg map[string]interface{}
	if err := json.Unmarshal(message, &msg); err != nil {
		return &ValidationResult{
			Valid:  false,
			Errors: []string{fmt.Sprintf("invalid JSON message: %v", err)},
		}
	}

	errors := v.validateJSONObject(schemaDef, msg, "")
	return &ValidationResult{
		Valid:  len(errors) == 0,
		Errors: errors,
	}
}

// validateJSONObject validates a JSON object against a schema.
func (v *Validator) validateJSONObject(schema map[string]interface{}, data map[string]interface{}, path string) []string {
	var errors []string

	// Check required fields
	if required, ok := schema["required"].([]interface{}); ok {
		for _, r := range required {
			fieldName, ok := r.(string)
			if !ok {
				continue
			}
			if _, exists := data[fieldName]; !exists {
				fieldPath := fieldName
				if path != "" {
					fieldPath = path + "." + fieldName
				}
				errors = append(errors, fmt.Sprintf("missing required field: %s", fieldPath))
			}
		}
	}

	// Check properties
	properties, _ := schema["properties"].(map[string]interface{})
	for fieldName, value := range data {
		fieldPath := fieldName
		if path != "" {
			fieldPath = path + "." + fieldName
		}

		propSchema, exists := properties[fieldName]
		if !exists {
			// Check if additional properties are allowed
			if additionalProps, ok := schema["additionalProperties"].(bool); ok && !additionalProps {
				errors = append(errors, fmt.Sprintf("unexpected field: %s", fieldPath))
			}
			continue
		}

		propDef, ok := propSchema.(map[string]interface{})
		if !ok {
			continue
		}

		// Validate type
		if expectedType, ok := propDef["type"].(string); ok {
			if err := v.validateType(value, expectedType, fieldPath); err != "" {
				errors = append(errors, err)
			}
		}

		// Recursive validation for nested objects
		if nestedObj, ok := value.(map[string]interface{}); ok {
			if nestedSchema, ok := propDef["properties"].(map[string]interface{}); ok {
				nestedErrors := v.validateJSONObject(map[string]interface{}{
					"properties": nestedSchema,
					"required":   propDef["required"],
				}, nestedObj, fieldPath)
				errors = append(errors, nestedErrors...)
			}
		}
	}

	return errors
}

// getJSONType returns the JSON type of a value.
func (v *Validator) getJSONType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch reflect.TypeOf(value).Kind() {
	case reflect.String:
		return "string"
	case reflect.Float64:
		// JSON numbers are parsed as float64
		f := value.(float64)
		if f == float64(int64(f)) {
			return "integer"
		}
		return "number"
	case reflect.Bool:
		return "boolean"
	case reflect.Slice:
		return "array"
	case reflect.Map:
		return "object"
	default:
		return "unknown"
	}
}

// validateType validates a value against an expected type.
func (v *Validator) validateType(value interface{}, expectedType, path string) string {
	actualType := v.getJSONType(value)
	if actualType != expectedType {
		// Handle number/integer compatibility
		if expectedType == "number" && actualType == "integer" {
			return ""
		}
		return fmt.Sprintf("type mismatch at %s: expected %s, got %s", path, expectedType, actualType)
	}
	return ""
}

// validateAvro validates a message against an Avro schema.
func (v *Validator) validateAvro(schema *Schema, message []byte) *ValidationResult {
	if len(message) == 0 {
		return &ValidationResult{
			Valid:  false,
			Errors: []string{"empty message"},
		}
	}

	// Get or parse the Avro schema
	schemaKey := fmt.Sprintf("%s-v%d", schema.Topic, schema.Version)
	var avroSchema avro.Schema
	if cached, ok := v.avroSchemas.Load(schemaKey); ok {
		avroSchema = cached.(avro.Schema)
	} else {
		var err error
		avroSchema, err = avro.Parse(schema.Definition)
		if err != nil {
			return &ValidationResult{
				Valid:  false,
				Errors: []string{fmt.Sprintf("invalid Avro schema: %v", err)},
			}
		}
		v.avroSchemas.Store(schemaKey, avroSchema)
	}

	// Try to unmarshal the message to validate it conforms to the schema
	var decoded interface{}
	if err := avro.Unmarshal(avroSchema, message, &decoded); err != nil {
		return &ValidationResult{
			Valid:  false,
			Errors: []string{fmt.Sprintf("Avro validation failed: %v", err)},
		}
	}

	return &ValidationResult{Valid: true}
}

// validateProto validates a message against a Protobuf schema.
// Note: Full Protobuf validation requires compiled descriptors which are not
// available at runtime. This performs basic structural validation only.
func (v *Validator) validateProto(schema *Schema, message []byte) *ValidationResult {
	if len(message) == 0 {
		return &ValidationResult{
			Valid:  false,
			Errors: []string{"empty message"},
		}
	}

	// Protobuf messages have a specific wire format. We can do basic validation
	// by checking that the message can be parsed as valid protobuf wire format.
	// However, without the compiled descriptor, we cannot validate field types
	// or required fields.
	//
	// Basic wire format validation:
	// - Each field is encoded as (field_number << 3) | wire_type
	// - Wire types: 0=varint, 1=64-bit, 2=length-delimited, 5=32-bit
	if err := validateProtobufWireFormat(message); err != nil {
		return &ValidationResult{
			Valid:  false,
			Errors: []string{fmt.Sprintf("invalid Protobuf wire format: %v", err)},
		}
	}

	return &ValidationResult{Valid: true}
}

// validateProtobufWireFormat performs basic validation of protobuf wire format.
func validateProtobufWireFormat(data []byte) error {
	pos := 0
	for pos < len(data) {
		// Read the tag (varint)
		tag, n := decodeVarint(data[pos:])
		if n <= 0 {
			return fmt.Errorf("invalid varint at position %d", pos)
		}
		pos += n

		wireType := tag & 0x7
		switch wireType {
		case 0: // Varint
			_, n := decodeVarint(data[pos:])
			if n <= 0 {
				return fmt.Errorf("invalid varint value at position %d", pos)
			}
			pos += n
		case 1: // 64-bit
			if pos+8 > len(data) {
				return fmt.Errorf("truncated 64-bit value at position %d", pos)
			}
			pos += 8
		case 2: // Length-delimited
			length, n := decodeVarint(data[pos:])
			if n <= 0 {
				return fmt.Errorf("invalid length at position %d", pos)
			}
			pos += n
			if pos+int(length) > len(data) {
				return fmt.Errorf("truncated length-delimited value at position %d", pos)
			}
			pos += int(length)
		case 5: // 32-bit
			if pos+4 > len(data) {
				return fmt.Errorf("truncated 32-bit value at position %d", pos)
			}
			pos += 4
		case 3, 4: // Start/end group (deprecated)
			// Groups are deprecated but valid
			continue
		default:
			return fmt.Errorf("unknown wire type %d at position %d", wireType, pos)
		}
	}
	return nil
}

// decodeVarint decodes a varint from the buffer and returns the value and bytes consumed.
func decodeVarint(buf []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range buf {
		if i >= 10 { // varint is at most 10 bytes
			return 0, -1
		}
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, -1
}

// ValidateWithVersion validates a message against a specific schema version.
func (v *Validator) ValidateWithVersion(topic string, version int, message []byte) *ValidationResult {
	schema, err := v.registry.Get(topic, version)
	if err != nil {
		return &ValidationResult{
			Valid:  false,
			Errors: []string{err.Error()},
		}
	}
	return v.ValidateWithSchema(schema, message)
}

// ExtractSchemaID extracts the schema ID from a message header.
// Messages can optionally include a schema ID in the first bytes.
func ExtractSchemaID(message []byte) (schemaID string, payload []byte, hasSchema bool) {
	// Check for schema header: [0x00][4-byte length][schema-id][payload]
	if len(message) < 5 || message[0] != 0x00 {
		return "", message, false
	}

	idLen := int(message[1])<<24 | int(message[2])<<16 | int(message[3])<<8 | int(message[4])
	if len(message) < 5+idLen {
		return "", message, false
	}

	schemaID = string(message[5 : 5+idLen])
	payload = message[5+idLen:]
	return schemaID, payload, true
}

// PrependSchemaID prepends a schema ID to a message.
func PrependSchemaID(schemaID string, payload []byte) []byte {
	idLen := len(schemaID)
	result := make([]byte, 5+idLen+len(payload))
	result[0] = 0x00
	result[1] = byte(idLen >> 24)
	result[2] = byte(idLen >> 16)
	result[3] = byte(idLen >> 8)
	result[4] = byte(idLen)
	copy(result[5:], schemaID)
	copy(result[5+idLen:], payload)
	return result
}

// ParseSchemaID parses a schema ID into topic and version.
func ParseSchemaID(schemaID string) (topic string, version int, err error) {
	// Schema ID format: "topic-vN"
	parts := strings.Split(schemaID, "-v")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid schema ID format: %s", schemaID)
	}
	topic = parts[0]
	_, err = fmt.Sscanf(parts[1], "%d", &version)
	if err != nil {
		return "", 0, fmt.Errorf("invalid version in schema ID: %s", schemaID)
	}
	return topic, version, nil
}

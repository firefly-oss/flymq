package client

import (
	"testing"
)

func TestAvroSerDe(t *testing.T) {
	schema := `{"type":"record","name":"test","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`
	err := SetAvroSchema(schema)
	if err != nil {
		t.Fatalf("Failed to set schema: %v", err)
	}

	data := map[string]interface{}{
		"a": 1,
		"b": "hello",
	}

	encoded, err := AvroSerde.Encode(data)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	var decoded map[string]interface{}
	err = AvroSerde.Decode(encoded, &decoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded["a"].(int) != 1 || decoded["b"].(string) != "hello" {
		t.Errorf("Decoded data mismatch: %v", decoded)
	}
}

package client

import (
	"encoding/json"
	"fmt"
)

// Encoder defines the interface for message payload encoding.
type Encoder interface {
	Encode(v interface{}) ([]byte, error)
	Name() string
}

// Decoder defines the interface for message payload decoding.
type Decoder interface {
	Decode(data []byte, v interface{}) error
	Name() string
}

// Default encoders and decoders.
var (
	BinarySerde = &binarySerde{}
	JSONSerde   = &jsonSerde{}
	StringSerde = &stringSerde{}
)

type binarySerde struct{}

func (s *binarySerde) Encode(v interface{}) ([]byte, error) {
	if data, ok := v.([]byte); ok {
		return data, nil
	}
	return nil, fmt.Errorf("binary encoder expects []byte, got %T", v)
}

func (s *binarySerde) Decode(data []byte, v interface{}) error {
	if target, ok := v.(*[]byte); ok {
		*target = data
		return nil
	}
	return fmt.Errorf("binary decoder expects *[]byte, got %T", v)
}

func (s *binarySerde) Name() string { return "binary" }

type jsonSerde struct{}

func (s *jsonSerde) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (s *jsonSerde) Decode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (s *jsonSerde) Name() string { return "json" }

type stringSerde struct{}

func (s *stringSerde) Encode(v interface{}) ([]byte, error) {
	if str, ok := v.(string); ok {
		return []byte(str), nil
	}
	if strer, ok := v.(fmt.Stringer); ok {
		return []byte(strer.String()), nil
	}
	return []byte(fmt.Sprint(v)), nil
}

func (s *stringSerde) Decode(data []byte, v interface{}) error {
	if target, ok := v.(*string); ok {
		*target = string(data)
		return nil
	}
	return fmt.Errorf("string decoder expects *string, got %T", v)
}

func (s *stringSerde) Name() string { return "string" }

// GetSerde returns a Serde by name.
func GetSerde(name string) (Encoder, Decoder, error) {
	switch name {
	case "binary":
		return BinarySerde, BinarySerde, nil
	case "json":
		return JSONSerde, JSONSerde, nil
	case "string":
		return StringSerde, StringSerde, nil
	default:
		return nil, nil, fmt.Errorf("unknown serde: %s", name)
	}
}

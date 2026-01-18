package client

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/proto"
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

// SerdeRegistry manages encoders and decoders.
type SerdeRegistry struct {
	encoders map[string]Encoder
	decoders map[string]Decoder
	mu       sync.RWMutex
}

var globalRegistry = &SerdeRegistry{
	encoders: make(map[string]Encoder),
	decoders: make(map[string]Decoder),
}

func init() {
	RegisterSerde(BinarySerde, BinarySerde)
	RegisterSerde(JSONSerde, JSONSerde)
	RegisterSerde(StringSerde, StringSerde)
	RegisterSerde(AvroSerde, AvroSerde)
	RegisterSerde(ProtoSerde, ProtoSerde)
}

type avroSerde struct {
	mu     sync.RWMutex
	schema avro.Schema
}

func (s *avroSerde) SetSchema(schemaStr string) error {
	sch, err := avro.Parse(schemaStr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schema = sch
	return nil
}

func (s *avroSerde) Encode(v interface{}) ([]byte, error) {
	if encoder, ok := v.(func() ([]byte, error)); ok {
		return encoder()
	}

	s.mu.RLock()
	sch := s.schema
	s.mu.RUnlock()

	if sch == nil {
		return nil, fmt.Errorf("avro encoding requires a schema; use SetAvroSchema(schema) or provide a custom encoder")
	}

	return avro.Marshal(sch, v)
}

func (s *avroSerde) Decode(data []byte, v interface{}) error {
	if decoder, ok := v.(func([]byte) error); ok {
		return decoder(data)
	}

	s.mu.RLock()
	sch := s.schema
	s.mu.RUnlock()

	if sch == nil {
		return fmt.Errorf("avro decoding requires a schema; use SetAvroSchema(schema) or provide a custom decoder")
	}

	return avro.Unmarshal(sch, data, v)
}

func (s *avroSerde) Name() string { return "avro" }

type protoSerde struct{}

func (s *protoSerde) Encode(v interface{}) ([]byte, error) {
	if encoder, ok := v.(func() ([]byte, error)); ok {
		return encoder()
	}

	msg, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("protobuf encoder expects proto.Message, got %T", v)
	}

	return proto.Marshal(msg)
}

func (s *protoSerde) Decode(data []byte, v interface{}) error {
	if decoder, ok := v.(func([]byte) error); ok {
		return decoder(data)
	}

	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("protobuf decoder expects proto.Message, got %T", v)
	}

	return proto.Unmarshal(data, msg)
}

func (s *protoSerde) Name() string { return "protobuf" }

var (
	AvroSerde  = &avroSerde{}
	ProtoSerde = &protoSerde{}
)

// SetAvroSchema sets the schema for the global Avro SerDe.
func SetAvroSchema(schema string) error {
	return AvroSerde.SetSchema(schema)
}

// SetProtoDescriptor is a placeholder for future dynamic Protobuf support.
func SetProtoDescriptor(descriptor []byte) {
	// In a real implementation, we would load the descriptor to allow dynamic encoding/decoding
}

// RegisterSerde registers an encoder and decoder.
func RegisterSerde(e Encoder, d Decoder) {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.encoders[e.Name()] = e
	globalRegistry.decoders[d.Name()] = d
}

// GetSerde returns a Serde by name.
func GetSerde(name string) (Encoder, Decoder, error) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	e, okE := globalRegistry.encoders[name]
	d, okD := globalRegistry.decoders[name]
	if !okE || !okD {
		return nil, nil, fmt.Errorf("unknown serde: %s", name)
	}
	return e, d, nil
}

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
Binary protocol encoding for high-performance message passing.

PERFORMANCE OPTIMIZATION:
=========================
JSON encoding adds significant overhead:
- Base64 encoding for binary data (33% size increase)
- JSON parsing overhead (~10% of latency)

Binary encoding eliminates this overhead by using a compact binary format.

BINARY PRODUCE REQUEST FORMAT:
==============================
  [2 bytes] topic length (uint16, big-endian)
  [N bytes] topic name (UTF-8)
  [4 bytes] key length (uint32, big-endian, 0 = no key)
  [K bytes] key data
  [4 bytes] value length (uint32, big-endian)
  [V bytes] value data
  [4 bytes] partition (int32, big-endian, -1 = auto)

BINARY PRODUCE RESPONSE FORMAT:
===============================
  [8 bytes] offset (uint64, big-endian)
  [4 bytes] partition (int32, big-endian)

FLAG USAGE:
===========
The Flags byte in the header indicates binary mode:
  0x01 = Binary payload (vs JSON)
  0x02 = Compressed (future)
*/
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Flag constants for the header Flags field.
const (
	FlagBinary     byte = 0x01 // Payload is binary encoded (not JSON)
	FlagCompressed byte = 0x02 // Payload is compressed (future)
)

// IsBinaryPayload checks if the message uses binary encoding.
func IsBinaryPayload(flags byte) bool {
	return flags&FlagBinary != 0
}

// BinaryProduceRequest represents a binary-encoded produce request.
type BinaryProduceRequest struct {
	Topic     string
	Key       []byte
	Value     []byte
	Partition int32
}

// RecordMetadata represents complete metadata for a produced record.
// Similar to Kafka's RecordMetadata, this contains all information about
// where and when the message was stored.
//
// Binary format: [2B topic_len][topic][4B partition][8B offset][8B timestamp][4B key_size][4B value_size]
type RecordMetadata struct {
	Topic       string // Topic name
	Partition   int32  // Partition the record was sent to
	Offset      uint64 // Offset of the record in the partition
	Timestamp   int64  // Timestamp in milliseconds (Unix epoch)
	KeySize     int32  // Size of the key in bytes (-1 if no key)
	ValueSize   int32  // Size of the value in bytes
}

var (
	ErrInvalidBinaryFormat = errors.New("invalid binary format")
	ErrBufferTooSmall      = errors.New("buffer too small")
)

// EncodeBinaryProduceRequest encodes a produce request to binary format.
func EncodeBinaryProduceRequest(req *BinaryProduceRequest) []byte {
	topicLen := len(req.Topic)
	keyLen := len(req.Key)
	valueLen := len(req.Value)

	// Calculate total size
	size := 2 + topicLen + 4 + keyLen + 4 + valueLen + 4
	buf := make([]byte, size)
	offset := 0

	// Topic
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen

	// Key
	binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
	offset += 4
	if keyLen > 0 {
		copy(buf[offset:], req.Key)
		offset += keyLen
	}

	// Value
	binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
	offset += 4
	if valueLen > 0 {
		copy(buf[offset:], req.Value)
		offset += valueLen
	}

	// Partition
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Partition))

	return buf
}

// DecodeBinaryProduceRequest decodes a binary produce request.
func DecodeBinaryProduceRequest(data []byte) (*BinaryProduceRequest, error) {
	if len(data) < 14 { // minimum: 2+0+4+0+4+0+4
		return nil, ErrBufferTooSmall
	}

	offset := 0
	req := &BinaryProduceRequest{}

	// Topic
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	req.Topic = string(data[offset : offset+topicLen])
	offset += topicLen

	// Key
	if offset+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	keyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if keyLen > 0 {
		if offset+keyLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		req.Key = make([]byte, keyLen)
		copy(req.Key, data[offset:offset+keyLen])
		offset += keyLen
	}

	// Value
	if offset+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	valueLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if valueLen > 0 {
		if offset+valueLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		req.Value = make([]byte, valueLen)
		copy(req.Value, data[offset:offset+valueLen])
		offset += valueLen
	}

	// Partition
	if offset+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	req.Partition = int32(binary.BigEndian.Uint32(data[offset:]))

	return req, nil
}

// EncodeRecordMetadata encodes complete record metadata to binary format.
// Format: [2B topic_len][topic][4B partition][8B offset][8B timestamp][4B key_size][4B value_size]
func EncodeRecordMetadata(meta *RecordMetadata) []byte {
	topicBytes := []byte(meta.Topic)
	topicLen := len(topicBytes)

	// Total size: 2 + topic_len + 4 + 8 + 8 + 4 + 4 = 30 + topic_len
	size := 2 + topicLen + 4 + 8 + 8 + 4 + 4
	buf := make([]byte, size)
	offset := 0

	// Topic
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], topicBytes)
	offset += topicLen

	// Partition
	binary.BigEndian.PutUint32(buf[offset:], uint32(meta.Partition))
	offset += 4

	// Offset
	binary.BigEndian.PutUint64(buf[offset:], meta.Offset)
	offset += 8

	// Timestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(meta.Timestamp))
	offset += 8

	// Key size
	binary.BigEndian.PutUint32(buf[offset:], uint32(meta.KeySize))
	offset += 4

	// Value size
	binary.BigEndian.PutUint32(buf[offset:], uint32(meta.ValueSize))

	return buf
}

// DecodeRecordMetadata decodes complete record metadata from binary format.
func DecodeRecordMetadata(data []byte) (*RecordMetadata, error) {
	if len(data) < 2 {
		return nil, ErrBufferTooSmall
	}

	offset := 0

	// Topic
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2

	if len(data) < offset+topicLen+28 { // 4+8+8+4+4 = 28
		return nil, ErrBufferTooSmall
	}

	topic := string(data[offset : offset+topicLen])
	offset += topicLen

	// Partition
	partition := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Offset
	recordOffset := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Timestamp
	timestamp := int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Key size
	keySize := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Value size
	valueSize := int32(binary.BigEndian.Uint32(data[offset:]))

	return &RecordMetadata{
		Topic:     topic,
		Partition: partition,
		Offset:    recordOffset,
		Timestamp: timestamp,
		KeySize:   keySize,
		ValueSize: valueSize,
	}, nil
}

// BinaryConsumeRequest represents a binary-encoded consume request.
type BinaryConsumeRequest struct {
	Topic     string
	Partition int32
	Offset    uint64
}

// EncodeBinaryConsumeRequest encodes a consume request to binary format.
func EncodeBinaryConsumeRequest(req *BinaryConsumeRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen+4+8)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Partition))
	offset += 4
	binary.BigEndian.PutUint64(buf[offset:], req.Offset)

	return buf
}

// DecodeBinaryConsumeRequest decodes a binary consume request.
func DecodeBinaryConsumeRequest(data []byte) (*BinaryConsumeRequest, error) {
	if len(data) < 14 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+12 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}

	return &BinaryConsumeRequest{
		Topic:     string(data[offset : offset+topicLen]),
		Partition: int32(binary.BigEndian.Uint32(data[offset+topicLen:])),
		Offset:    binary.BigEndian.Uint64(data[offset+topicLen+4:]),
	}, nil
}

// BinaryConsumeResponse represents a binary-encoded consume response.
type BinaryConsumeResponse struct {
	Key   []byte
	Value []byte
}

// EncodeBinaryConsumeResponse encodes a consume response to binary format.
func EncodeBinaryConsumeResponse(resp *BinaryConsumeResponse) []byte {
	keyLen := len(resp.Key)
	valueLen := len(resp.Value)
	buf := make([]byte, 4+keyLen+4+valueLen)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
	offset += 4
	if keyLen > 0 {
		copy(buf[offset:], resp.Key)
		offset += keyLen
	}
	binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
	offset += 4
	if valueLen > 0 {
		copy(buf[offset:], resp.Value)
	}

	return buf
}

// DecodeBinaryConsumeResponse decodes a binary consume response.
func DecodeBinaryConsumeResponse(data []byte) (*BinaryConsumeResponse, error) {
	if len(data) < 8 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	keyLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+keyLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}

	resp := &BinaryConsumeResponse{}
	if keyLen > 0 {
		resp.Key = make([]byte, keyLen)
		copy(resp.Key, data[offset:offset+keyLen])
		offset += keyLen
	}

	valueLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if valueLen > 0 {
		if offset+valueLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		resp.Value = make([]byte, valueLen)
		copy(resp.Value, data[offset:offset+valueLen])
	}

	return resp, nil
}

// WriteBinaryMessage writes a message with the binary flag set.
func WriteBinaryMessage(w io.Writer, op OpCode, payload []byte) error {
	h := Header{
		Magic:   MagicByte,
		Version: ProtocolVersion,
		Op:      op,
		Flags:   FlagBinary,
		Length:  uint32(len(payload)),
	}

	if err := WriteHeader(w, h); err != nil {
		return err
	}

	if len(payload) > 0 {
		_, err := w.Write(payload)
		return err
	}
	return nil
}

// FastEncoder provides zero-allocation encoding for high-throughput scenarios.
type FastEncoder struct {
	buf []byte
}

// NewFastEncoder creates a new fast encoder with the given buffer capacity.
func NewFastEncoder(capacity int) *FastEncoder {
	return &FastEncoder{
		buf: make([]byte, 0, capacity),
	}
}

// Reset resets the encoder for reuse.
func (e *FastEncoder) Reset() {
	e.buf = e.buf[:0]
}

// Bytes returns the encoded bytes.
func (e *FastEncoder) Bytes() []byte {
	return e.buf
}

// WriteUint16 writes a uint16 in big-endian format.
func (e *FastEncoder) WriteUint16(v uint16) {
	e.buf = append(e.buf, byte(v>>8), byte(v))
}

// WriteUint32 writes a uint32 in big-endian format.
func (e *FastEncoder) WriteUint32(v uint32) {
	e.buf = append(e.buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// WriteUint64 writes a uint64 in big-endian format.
func (e *FastEncoder) WriteUint64(v uint64) {
	e.buf = append(e.buf,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// WriteBytes writes a length-prefixed byte slice.
func (e *FastEncoder) WriteBytes(data []byte) {
	e.WriteUint32(uint32(len(data)))
	e.buf = append(e.buf, data...)
}

// WriteString writes a length-prefixed string.
func (e *FastEncoder) WriteString(s string) {
	e.WriteUint16(uint16(len(s)))
	e.buf = append(e.buf, s...)
}

// EncodeBinaryProduceRequestFast encodes a produce request with zero allocations.
func EncodeBinaryProduceRequestFast(enc *FastEncoder, req *BinaryProduceRequest) {
	enc.WriteString(req.Topic)
	enc.WriteBytes(req.Key)
	enc.WriteBytes(req.Value)
	enc.WriteUint32(uint32(req.Partition))
}

// BatchProduceRequest represents a batch of produce requests.
type BatchProduceRequest struct {
	Topic    string
	Messages []BatchMessage
}

// BatchMessage represents a single message in a batch.
type BatchMessage struct {
	Key       []byte
	Value     []byte
	Partition int32
}

// EncodeBatchProduceRequest encodes a batch of messages for high throughput.
func EncodeBatchProduceRequest(req *BatchProduceRequest) []byte {
	// Calculate size
	topicLen := len(req.Topic)
	size := 2 + topicLen + 4 // topic + message count
	for _, msg := range req.Messages {
		size += 4 + len(msg.Key) + 4 + len(msg.Value) + 4 // key + value + partition
	}

	buf := make([]byte, size)
	offset := 0

	// Topic
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen

	// Message count
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(req.Messages)))
	offset += 4

	// Messages
	for _, msg := range req.Messages {
		// Key
		keyLen := len(msg.Key)
		binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
		offset += 4
		if keyLen > 0 {
			copy(buf[offset:], msg.Key)
			offset += keyLen
		}

		// Value
		valueLen := len(msg.Value)
		binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
		offset += 4
		if valueLen > 0 {
			copy(buf[offset:], msg.Value)
			offset += valueLen
		}

		// Partition
		binary.BigEndian.PutUint32(buf[offset:], uint32(msg.Partition))
		offset += 4
	}

	return buf
}

// DecodeBatchProduceRequest decodes a batch produce request.
func DecodeBatchProduceRequest(data []byte) (*BatchProduceRequest, error) {
	if len(data) < 6 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}

	req := &BatchProduceRequest{
		Topic: string(data[offset : offset+topicLen]),
	}
	offset += topicLen

	msgCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	req.Messages = make([]BatchMessage, 0, msgCount)
	for i := 0; i < msgCount; i++ {
		if offset+12 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}

		msg := BatchMessage{}

		// Key
		keyLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if keyLen > 0 {
			if offset+keyLen > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			msg.Key = data[offset : offset+keyLen]
			offset += keyLen
		}

		// Value
		if offset+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		valueLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if valueLen > 0 {
			if offset+valueLen > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			msg.Value = data[offset : offset+valueLen]
			offset += valueLen
		}

		// Partition
		if offset+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		msg.Partition = int32(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		req.Messages = append(req.Messages, msg)
	}

	return req, nil
}

// BinaryFetchRequest represents a binary-encoded fetch request.
type BinaryFetchRequest struct {
	Topic       string
	Partition   int32
	Offset      uint64
	MaxMessages int32
}

// EncodeBinaryFetchRequest encodes a fetch request to binary format.
func EncodeBinaryFetchRequest(req *BinaryFetchRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen+4+8+4)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Partition))
	offset += 4
	binary.BigEndian.PutUint64(buf[offset:], req.Offset)
	offset += 8
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.MaxMessages))

	return buf
}

// DecodeBinaryFetchRequest decodes a binary fetch request.
func DecodeBinaryFetchRequest(data []byte) (*BinaryFetchRequest, error) {
	if len(data) < 18 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+16 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}

	return &BinaryFetchRequest{
		Topic:       string(data[offset : offset+topicLen]),
		Partition:   int32(binary.BigEndian.Uint32(data[offset+topicLen:])),
		Offset:      binary.BigEndian.Uint64(data[offset+topicLen+4:]),
		MaxMessages: int32(binary.BigEndian.Uint32(data[offset+topicLen+12:])),
	}, nil
}

// BinaryFetchMessage represents a single message in a binary fetch response.
type BinaryFetchMessage struct {
	Offset uint64
	Key    []byte
	Value  []byte
}

// BinaryFetchResponse represents a binary-encoded fetch response.
type BinaryFetchResponse struct {
	Messages   []BinaryFetchMessage
	NextOffset uint64
}

// EncodeBinaryFetchResponse encodes a fetch response to binary format.
// Format: [4 bytes count][8 bytes nextOffset][messages...]
// Each message: [8 bytes offset][4 bytes keyLen][key][4 bytes valueLen][value]
func EncodeBinaryFetchResponse(resp *BinaryFetchResponse) []byte {
	// Calculate size
	size := 4 + 8 // count + nextOffset
	for _, msg := range resp.Messages {
		size += 8 + 4 + len(msg.Key) + 4 + len(msg.Value)
	}

	buf := make([]byte, size)
	offset := 0

	// Message count
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Messages)))
	offset += 4

	// Next offset
	binary.BigEndian.PutUint64(buf[offset:], resp.NextOffset)
	offset += 8

	// Messages
	for _, msg := range resp.Messages {
		// Offset
		binary.BigEndian.PutUint64(buf[offset:], msg.Offset)
		offset += 8

		// Key
		keyLen := len(msg.Key)
		binary.BigEndian.PutUint32(buf[offset:], uint32(keyLen))
		offset += 4
		if keyLen > 0 {
			copy(buf[offset:], msg.Key)
			offset += keyLen
		}

		// Value
		valueLen := len(msg.Value)
		binary.BigEndian.PutUint32(buf[offset:], uint32(valueLen))
		offset += 4
		if valueLen > 0 {
			copy(buf[offset:], msg.Value)
			offset += valueLen
		}
	}

	return buf
}

// DecodeBinaryFetchResponse decodes a binary fetch response.
func DecodeBinaryFetchResponse(data []byte) (*BinaryFetchResponse, error) {
	if len(data) < 12 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	msgCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	nextOffset := binary.BigEndian.Uint64(data[offset:])
	offset += 8

	resp := &BinaryFetchResponse{
		Messages:   make([]BinaryFetchMessage, 0, msgCount),
		NextOffset: nextOffset,
	}

	for i := 0; i < msgCount; i++ {
		if offset+16 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}

		msg := BinaryFetchMessage{}

		// Offset
		msg.Offset = binary.BigEndian.Uint64(data[offset:])
		offset += 8

		// Key
		keyLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if keyLen > 0 {
			if offset+keyLen > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			msg.Key = data[offset : offset+keyLen]
			offset += keyLen
		}

		// Value
		if offset+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		valueLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if valueLen > 0 {
			if offset+valueLen > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			msg.Value = data[offset : offset+valueLen]
			offset += valueLen
		}

		resp.Messages = append(resp.Messages, msg)
	}

	return resp, nil
}

// ============================================================================
// CreateTopic Binary Protocol
// ============================================================================

// BinaryCreateTopicRequest represents a binary-encoded create topic request.
type BinaryCreateTopicRequest struct {
	Topic      string
	Partitions int32
}

// EncodeBinaryCreateTopicRequest encodes a create topic request.
func EncodeBinaryCreateTopicRequest(req *BinaryCreateTopicRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen+4)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Partitions))

	return buf
}

// DecodeBinaryCreateTopicRequest decodes a binary create topic request.
func DecodeBinaryCreateTopicRequest(data []byte) (*BinaryCreateTopicRequest, error) {
	if len(data) < 6 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}

	return &BinaryCreateTopicRequest{
		Topic:      string(data[offset : offset+topicLen]),
		Partitions: int32(binary.BigEndian.Uint32(data[offset+topicLen:])),
	}, nil
}

// BinaryCreateTopicResponse represents a binary create topic response.
type BinaryCreateTopicResponse struct {
	Success bool
}

// EncodeBinaryCreateTopicResponse encodes a create topic response.
func EncodeBinaryCreateTopicResponse(resp *BinaryCreateTopicResponse) []byte {
	if resp.Success {
		return []byte{1}
	}
	return []byte{0}
}

// DecodeBinaryCreateTopicResponse decodes a binary create topic response.
func DecodeBinaryCreateTopicResponse(data []byte) (*BinaryCreateTopicResponse, error) {
	if len(data) < 1 {
		return nil, ErrBufferTooSmall
	}
	return &BinaryCreateTopicResponse{Success: data[0] == 1}, nil
}

// ============================================================================
// Subscribe Binary Protocol
// ============================================================================

// BinarySubscribeRequest represents a binary-encoded subscribe request.
type BinarySubscribeRequest struct {
	Topic     string
	GroupID   string
	Partition int32
	Mode      string // "earliest", "latest", "commit"
}

// EncodeBinarySubscribeRequest encodes a subscribe request.
func EncodeBinarySubscribeRequest(req *BinarySubscribeRequest) []byte {
	topicLen := len(req.Topic)
	groupLen := len(req.GroupID)
	modeLen := len(req.Mode)
	buf := make([]byte, 2+topicLen+2+groupLen+4+1+modeLen)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen

	binary.BigEndian.PutUint16(buf[offset:], uint16(groupLen))
	offset += 2
	copy(buf[offset:], req.GroupID)
	offset += groupLen

	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Partition))
	offset += 4

	buf[offset] = byte(modeLen)
	offset++
	copy(buf[offset:], req.Mode)

	return buf
}

// DecodeBinarySubscribeRequest decodes a binary subscribe request.
func DecodeBinarySubscribeRequest(data []byte) (*BinarySubscribeRequest, error) {
	if len(data) < 9 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	req := &BinarySubscribeRequest{}

	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+6 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	req.Topic = string(data[offset : offset+topicLen])
	offset += topicLen

	groupLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+groupLen+5 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	req.GroupID = string(data[offset : offset+groupLen])
	offset += groupLen

	req.Partition = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	modeLen := int(data[offset])
	offset++
	if offset+modeLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	req.Mode = string(data[offset : offset+modeLen])

	return req, nil
}

// BinarySubscribeResponse represents a binary subscribe response.
type BinarySubscribeResponse struct {
	Offset uint64
}

// EncodeBinarySubscribeResponse encodes a subscribe response.
func EncodeBinarySubscribeResponse(resp *BinarySubscribeResponse) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, resp.Offset)
	return buf
}

// DecodeBinarySubscribeResponse decodes a binary subscribe response.
func DecodeBinarySubscribeResponse(data []byte) (*BinarySubscribeResponse, error) {
	if len(data) < 8 {
		return nil, ErrBufferTooSmall
	}
	return &BinarySubscribeResponse{Offset: binary.BigEndian.Uint64(data)}, nil
}

// ============================================================================
// Commit Binary Protocol
// ============================================================================

// BinaryCommitRequest represents a binary-encoded commit request.
type BinaryCommitRequest struct {
	Topic     string
	GroupID   string
	Partition int32
	Offset    uint64
}

// EncodeBinaryCommitRequest encodes a commit request.
func EncodeBinaryCommitRequest(req *BinaryCommitRequest) []byte {
	topicLen := len(req.Topic)
	groupLen := len(req.GroupID)
	buf := make([]byte, 2+topicLen+2+groupLen+4+8)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen

	binary.BigEndian.PutUint16(buf[offset:], uint16(groupLen))
	offset += 2
	copy(buf[offset:], req.GroupID)
	offset += groupLen

	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Partition))
	offset += 4

	binary.BigEndian.PutUint64(buf[offset:], req.Offset)

	return buf
}

// DecodeBinaryCommitRequest decodes a binary commit request.
func DecodeBinaryCommitRequest(data []byte) (*BinaryCommitRequest, error) {
	if len(data) < 16 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	req := &BinaryCommitRequest{}

	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+14 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	req.Topic = string(data[offset : offset+topicLen])
	offset += topicLen

	groupLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+groupLen+12 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	req.GroupID = string(data[offset : offset+groupLen])
	offset += groupLen

	req.Partition = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	req.Offset = binary.BigEndian.Uint64(data[offset:])

	return req, nil
}

// BinaryCommitResponse represents a binary commit response.
type BinaryCommitResponse struct {
	Success bool
}

// EncodeBinaryCommitResponse encodes a commit response.
func EncodeBinaryCommitResponse(resp *BinaryCommitResponse) []byte {
	if resp.Success {
		return []byte{1}
	}
	return []byte{0}
}

// DecodeBinaryCommitResponse decodes a binary commit response.
func DecodeBinaryCommitResponse(data []byte) (*BinaryCommitResponse, error) {
	if len(data) < 1 {
		return nil, ErrBufferTooSmall
	}
	return &BinaryCommitResponse{Success: data[0] == 1}, nil
}

// ============================================================================
// ListTopics Binary Protocol
// ============================================================================

// BinaryListTopicsResponse represents a binary list topics response.
type BinaryListTopicsResponse struct {
	Topics []string
}

// EncodeBinaryListTopicsResponse encodes a list topics response.
func EncodeBinaryListTopicsResponse(resp *BinaryListTopicsResponse) []byte {
	size := 4 // topic count
	for _, topic := range resp.Topics {
		size += 2 + len(topic)
	}

	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Topics)))
	offset += 4

	for _, topic := range resp.Topics {
		topicLen := len(topic)
		binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
		offset += 2
		copy(buf[offset:], topic)
		offset += topicLen
	}

	return buf
}

// DecodeBinaryListTopicsResponse decodes a binary list topics response.
func DecodeBinaryListTopicsResponse(data []byte) (*BinaryListTopicsResponse, error) {
	if len(data) < 4 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	topicCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	resp := &BinaryListTopicsResponse{
		Topics: make([]string, 0, topicCount),
	}

	for i := 0; i < topicCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+topicLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		resp.Topics = append(resp.Topics, string(data[offset:offset+topicLen]))
		offset += topicLen
	}

	return resp, nil
}

// ============================================================================
// DeleteTopic Binary Protocol
// ============================================================================

// BinaryDeleteTopicRequest represents a binary delete topic request.
type BinaryDeleteTopicRequest struct {
	Topic string
}

// EncodeBinaryDeleteTopicRequest encodes a delete topic request.
func EncodeBinaryDeleteTopicRequest(req *BinaryDeleteTopicRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen)
	binary.BigEndian.PutUint16(buf, uint16(topicLen))
	copy(buf[2:], req.Topic)
	return buf
}

// DecodeBinaryDeleteTopicRequest decodes a binary delete topic request.
func DecodeBinaryDeleteTopicRequest(data []byte) (*BinaryDeleteTopicRequest, error) {
	if len(data) < 2 {
		return nil, ErrBufferTooSmall
	}
	topicLen := int(binary.BigEndian.Uint16(data))
	if 2+topicLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	return &BinaryDeleteTopicRequest{Topic: string(data[2 : 2+topicLen])}, nil
}

// BinaryDeleteTopicResponse represents a binary delete topic response.
type BinaryDeleteTopicResponse struct {
	Success bool
}

// EncodeBinaryDeleteTopicResponse encodes a delete topic response.
func EncodeBinaryDeleteTopicResponse(resp *BinaryDeleteTopicResponse) []byte {
	if resp.Success {
		return []byte{1}
	}
	return []byte{0}
}

// DecodeBinaryDeleteTopicResponse decodes a binary delete topic response.
func DecodeBinaryDeleteTopicResponse(data []byte) (*BinaryDeleteTopicResponse, error) {
	if len(data) < 1 {
		return nil, ErrBufferTooSmall
	}
	return &BinaryDeleteTopicResponse{Success: data[0] == 1}, nil
}

// ============================================================================
// Metadata Binary Protocol
// ============================================================================

// BinaryMetadataRequest represents a binary metadata request.
type BinaryMetadataRequest struct {
	Topic string
}

// EncodeBinaryMetadataRequest encodes a metadata request.
func EncodeBinaryMetadataRequest(req *BinaryMetadataRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen)
	binary.BigEndian.PutUint16(buf, uint16(topicLen))
	copy(buf[2:], req.Topic)
	return buf
}

// DecodeBinaryMetadataRequest decodes a binary metadata request.
func DecodeBinaryMetadataRequest(data []byte) (*BinaryMetadataRequest, error) {
	if len(data) < 2 {
		return nil, ErrBufferTooSmall
	}
	topicLen := int(binary.BigEndian.Uint16(data))
	if 2+topicLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	return &BinaryMetadataRequest{Topic: string(data[2 : 2+topicLen])}, nil
}

// BinaryPartitionInfo represents partition metadata.
type BinaryPartitionInfo struct {
	ID            int32
	LeaderID      int32
	OldestOffset  uint64
	NewestOffset  uint64
}

// BinaryMetadataResponse represents a binary metadata response.
type BinaryMetadataResponse struct {
	Topic       string
	Partitions  []BinaryPartitionInfo
}

// EncodeBinaryMetadataResponse encodes a metadata response.
func EncodeBinaryMetadataResponse(resp *BinaryMetadataResponse) []byte {
	topicLen := len(resp.Topic)
	size := 2 + topicLen + 4 + len(resp.Partitions)*24
	buf := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], resp.Topic)
	offset += topicLen

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Partitions)))
	offset += 4

	for _, p := range resp.Partitions {
		binary.BigEndian.PutUint32(buf[offset:], uint32(p.ID))
		offset += 4
		binary.BigEndian.PutUint32(buf[offset:], uint32(p.LeaderID))
		offset += 4
		binary.BigEndian.PutUint64(buf[offset:], p.OldestOffset)
		offset += 8
		binary.BigEndian.PutUint64(buf[offset:], p.NewestOffset)
		offset += 8
	}

	return buf
}

// DecodeBinaryMetadataResponse decodes a binary metadata response.
func DecodeBinaryMetadataResponse(data []byte) (*BinaryMetadataResponse, error) {
	if len(data) < 6 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}

	resp := &BinaryMetadataResponse{
		Topic: string(data[offset : offset+topicLen]),
	}
	offset += topicLen

	partitionCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	resp.Partitions = make([]BinaryPartitionInfo, partitionCount)
	for i := 0; i < partitionCount; i++ {
		if offset+24 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		resp.Partitions[i] = BinaryPartitionInfo{
			ID:           int32(binary.BigEndian.Uint32(data[offset:])),
			LeaderID:     int32(binary.BigEndian.Uint32(data[offset+4:])),
			OldestOffset: binary.BigEndian.Uint64(data[offset+8:]),
			NewestOffset: binary.BigEndian.Uint64(data[offset+16:]),
		}
		offset += 24
	}

	return resp, nil
}

// ============================================================================
// Auth Binary Protocol
// ============================================================================

// BinaryAuthRequest represents a binary auth request.
type BinaryAuthRequest struct {
	Username string
	Password string
}

// EncodeBinaryAuthRequest encodes an auth request.
func EncodeBinaryAuthRequest(req *BinaryAuthRequest) []byte {
	userLen := len(req.Username)
	passLen := len(req.Password)
	buf := make([]byte, 2+userLen+2+passLen)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], uint16(userLen))
	offset += 2
	copy(buf[offset:], req.Username)
	offset += userLen

	binary.BigEndian.PutUint16(buf[offset:], uint16(passLen))
	offset += 2
	copy(buf[offset:], req.Password)

	return buf
}

// DecodeBinaryAuthRequest decodes a binary auth request.
func DecodeBinaryAuthRequest(data []byte) (*BinaryAuthRequest, error) {
	if len(data) < 4 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	userLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+userLen+2 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	username := string(data[offset : offset+userLen])
	offset += userLen

	passLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+passLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	password := string(data[offset : offset+passLen])

	return &BinaryAuthRequest{Username: username, Password: password}, nil
}

// BinaryAuthResponse represents a binary auth response.
type BinaryAuthResponse struct {
	Success  bool
	Username string
	Roles    []string
	Error    string
}

// EncodeBinaryAuthResponse encodes an auth response.
func EncodeBinaryAuthResponse(resp *BinaryAuthResponse) []byte {
	userLen := len(resp.Username)
	errorLen := len(resp.Error)
	rolesSize := 4 // role count
	for _, role := range resp.Roles {
		rolesSize += 1 + len(role)
	}

	buf := make([]byte, 1+2+userLen+rolesSize+2+errorLen)
	offset := 0

	// Success flag
	if resp.Success {
		buf[offset] = 1
	}
	offset++

	// Username
	binary.BigEndian.PutUint16(buf[offset:], uint16(userLen))
	offset += 2
	copy(buf[offset:], resp.Username)
	offset += userLen

	// Roles
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Roles)))
	offset += 4
	for _, role := range resp.Roles {
		buf[offset] = byte(len(role))
		offset++
		copy(buf[offset:], role)
		offset += len(role)
	}

	// Error
	binary.BigEndian.PutUint16(buf[offset:], uint16(errorLen))
	offset += 2
	copy(buf[offset:], resp.Error)

	return buf
}

// DecodeBinaryAuthResponse decodes a binary auth response.
func DecodeBinaryAuthResponse(data []byte) (*BinaryAuthResponse, error) {
	if len(data) < 9 {
		return nil, ErrBufferTooSmall
	}

	offset := 0
	resp := &BinaryAuthResponse{}

	resp.Success = data[offset] == 1
	offset++

	userLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+userLen+6 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	resp.Username = string(data[offset : offset+userLen])
	offset += userLen

	roleCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	resp.Roles = make([]string, 0, roleCount)
	for i := 0; i < roleCount; i++ {
		if offset >= len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roleLen := int(data[offset])
		offset++
		if offset+roleLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		resp.Roles = append(resp.Roles, string(data[offset:offset+roleLen]))
		offset += roleLen
	}

	if offset+2 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	errorLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+errorLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	resp.Error = string(data[offset : offset+errorLen])

	return resp, nil
}

// ============================================================================
// Error Binary Protocol
// ============================================================================

// BinaryErrorResponse represents a binary error response.
type BinaryErrorResponse struct {
	Code    int32
	Message string
}

// EncodeBinaryErrorResponse encodes an error response.
func EncodeBinaryErrorResponse(resp *BinaryErrorResponse) []byte {
	msgLen := len(resp.Message)
	buf := make([]byte, 4+2+msgLen)
	offset := 0

	binary.BigEndian.PutUint32(buf[offset:], uint32(resp.Code))
	offset += 4
	binary.BigEndian.PutUint16(buf[offset:], uint16(msgLen))
	offset += 2
	copy(buf[offset:], resp.Message)

	return buf
}

// DecodeBinaryErrorResponse decodes a binary error response.
func DecodeBinaryErrorResponse(data []byte) (*BinaryErrorResponse, error) {
	if len(data) < 6 {
		return nil, ErrBufferTooSmall
	}

	code := int32(binary.BigEndian.Uint32(data))
	msgLen := int(binary.BigEndian.Uint16(data[4:]))
	if 6+msgLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}

	return &BinaryErrorResponse{
		Code:    code,
		Message: string(data[6 : 6+msgLen]),
	}, nil
}

// ============================================================================
// Advanced Operations Binary Protocol
// ============================================================================

// BinaryProduceDelayedRequest represents a binary produce delayed request.
type BinaryProduceDelayedRequest struct {
	Topic   string
	Data    []byte
	DelayMs int64
}

// DecodeBinaryProduceDelayedRequest decodes a binary produce delayed request.
// Format: [2B topic_len][topic][4B data_len][data][8B delay_ms]
func DecodeBinaryProduceDelayedRequest(data []byte) (*BinaryProduceDelayedRequest, error) {
	if len(data) < 14 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+12 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	dataLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+dataLen+8 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	msgData := make([]byte, dataLen)
	copy(msgData, data[offset:offset+dataLen])
	offset += dataLen
	delayMs := int64(binary.BigEndian.Uint64(data[offset:]))
	return &BinaryProduceDelayedRequest{Topic: topic, Data: msgData, DelayMs: delayMs}, nil
}

// BinaryProduceWithTTLRequest represents a binary produce with TTL request.
type BinaryProduceWithTTLRequest struct {
	Topic string
	Data  []byte
	TTLMs int64
}

// DecodeBinaryProduceWithTTLRequest decodes a binary produce with TTL request.
// Format: [2B topic_len][topic][4B data_len][data][8B ttl_ms]
func DecodeBinaryProduceWithTTLRequest(data []byte) (*BinaryProduceWithTTLRequest, error) {
	if len(data) < 14 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+12 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	dataLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+dataLen+8 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	msgData := make([]byte, dataLen)
	copy(msgData, data[offset:offset+dataLen])
	offset += dataLen
	ttlMs := int64(binary.BigEndian.Uint64(data[offset:]))
	return &BinaryProduceWithTTLRequest{Topic: topic, Data: msgData, TTLMs: ttlMs}, nil
}

// BinaryProduceWithSchemaRequest represents a binary produce with schema request.
type BinaryProduceWithSchemaRequest struct {
	Topic      string
	SchemaName string
	Data       []byte
}

// DecodeBinaryProduceWithSchemaRequest decodes a binary produce with schema request.
// Format: [2B topic_len][topic][2B schema_len][schema][4B data_len][data]
func DecodeBinaryProduceWithSchemaRequest(data []byte) (*BinaryProduceWithSchemaRequest, error) {
	if len(data) < 8 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+6 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	schemaLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+schemaLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	schemaName := string(data[offset : offset+schemaLen])
	offset += schemaLen
	dataLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+dataLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	msgData := make([]byte, dataLen)
	copy(msgData, data[offset:offset+dataLen])
	return &BinaryProduceWithSchemaRequest{Topic: topic, SchemaName: schemaName, Data: msgData}, nil
}

// BinaryRegisterSchemaRequest represents a binary register schema request.
type BinaryRegisterSchemaRequest struct {
	Name   string
	Type   string
	Schema []byte
}

// DecodeBinaryRegisterSchemaRequest decodes a binary register schema request.
// Format: [2B name_len][name][1B type_len][type][4B schema_len][schema]
func DecodeBinaryRegisterSchemaRequest(data []byte) (*BinaryRegisterSchemaRequest, error) {
	if len(data) < 7 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	nameLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+nameLen+5 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	name := string(data[offset : offset+nameLen])
	offset += nameLen
	typeLen := int(data[offset])
	offset++
	if offset+typeLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	schemaType := string(data[offset : offset+typeLen])
	offset += typeLen
	schemaLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+schemaLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	schemaData := make([]byte, schemaLen)
	copy(schemaData, data[offset:offset+schemaLen])
	return &BinaryRegisterSchemaRequest{Name: name, Type: schemaType, Schema: schemaData}, nil
}

// BinarySchemaResponse represents a binary schema response.
type BinarySchemaResponse struct {
	Name    string
	Type    string
	Version uint32
}

// EncodeBinarySchemaResponse encodes a binary schema response.
func EncodeBinarySchemaResponse(resp *BinarySchemaResponse) []byte {
	nameLen := len(resp.Name)
	typeLen := len(resp.Type)
	buf := make([]byte, 2+nameLen+1+typeLen+4)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(nameLen))
	offset += 2
	copy(buf[offset:], resp.Name)
	offset += nameLen
	buf[offset] = byte(typeLen)
	offset++
	copy(buf[offset:], resp.Type)
	offset += typeLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(resp.Version))
	return buf
}

// BinaryListSchemasRequest represents a binary list schemas request.
type BinaryListSchemasRequest struct {
	Topic string
}

// DecodeBinaryListSchemasRequest decodes a binary list schemas request.
// Format: [2B topic_len][topic] (topic can be empty)
func DecodeBinaryListSchemasRequest(data []byte) (*BinaryListSchemasRequest, error) {
	if len(data) < 2 {
		return &BinaryListSchemasRequest{Topic: ""}, nil
	}
	topicLen := int(binary.BigEndian.Uint16(data))
	if 2+topicLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	return &BinaryListSchemasRequest{Topic: string(data[2 : 2+topicLen])}, nil
}

// BinarySchemaInfo represents schema info in list response.
type BinarySchemaInfo struct {
	Name      string
	Type      string
	Version   uint32
	CreatedAt int64
}

// BinaryListSchemasResponse represents a binary list schemas response.
type BinaryListSchemasResponse struct {
	Schemas []BinarySchemaInfo
}

// EncodeBinaryListSchemasResponse encodes a binary list schemas response.
func EncodeBinaryListSchemasResponse(resp *BinaryListSchemasResponse) []byte {
	size := 4 // count
	for _, s := range resp.Schemas {
		size += 2 + len(s.Name) + 1 + len(s.Type) + 4
	}
	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Schemas)))
	offset += 4
	for _, s := range resp.Schemas {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(s.Name)))
		offset += 2
		copy(buf[offset:], s.Name)
		offset += len(s.Name)
		buf[offset] = byte(len(s.Type))
		offset++
		copy(buf[offset:], s.Type)
		offset += len(s.Type)
		binary.BigEndian.PutUint32(buf[offset:], uint32(s.Version))
		offset += 4
	}
	return buf
}

// BinaryValidateSchemaRequest represents a binary validate schema request.
type BinaryValidateSchemaRequest struct {
	Name    string
	Message []byte
}

// DecodeBinaryValidateSchemaRequest decodes a binary validate schema request.
// Format: [2B name_len][name][4B message_len][message]
func DecodeBinaryValidateSchemaRequest(data []byte) (*BinaryValidateSchemaRequest, error) {
	if len(data) < 6 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	nameLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+nameLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	name := string(data[offset : offset+nameLen])
	offset += nameLen
	msgLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+msgLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	msg := make([]byte, msgLen)
	copy(msg, data[offset:offset+msgLen])
	return &BinaryValidateSchemaRequest{Name: name, Message: msg}, nil
}

// BinaryValidateSchemaResponse represents a binary validate schema response.
type BinaryValidateSchemaResponse struct {
	Valid  bool
	Errors []string
}

// EncodeBinaryValidateSchemaResponse encodes a binary validate schema response.
func EncodeBinaryValidateSchemaResponse(resp *BinaryValidateSchemaResponse) []byte {
	size := 1 + 4 // valid + error count
	for _, e := range resp.Errors {
		size += 2 + len(e)
	}
	buf := make([]byte, size)
	offset := 0
	if resp.Valid {
		buf[offset] = 1
	}
	offset++
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Errors)))
	offset += 4
	for _, e := range resp.Errors {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(e)))
		offset += 2
		copy(buf[offset:], e)
		offset += len(e)
	}
	return buf
}

// BinaryGetSchemaRequest represents a binary get schema request.
type BinaryGetSchemaRequest struct {
	Topic   string
	Version uint32
}

// DecodeBinaryGetSchemaRequest decodes a binary get schema request.
// Format: [2B topic_len][topic][4B version]
func DecodeBinaryGetSchemaRequest(data []byte) (*BinaryGetSchemaRequest, error) {
	if len(data) < 6 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	version := binary.BigEndian.Uint32(data[offset:])
	return &BinaryGetSchemaRequest{Topic: topic, Version: version}, nil
}

// BinaryGetSchemaResponse represents a binary get schema response.
type BinaryGetSchemaResponse struct {
	ID            string
	Topic         string
	Version       uint32
	Type          string
	Definition    string
	Compatibility string
	CreatedAt     int64
}

// EncodeBinaryGetSchemaResponse encodes a binary get schema response.
func EncodeBinaryGetSchemaResponse(resp *BinaryGetSchemaResponse) []byte {
	idLen := len(resp.ID)
	topicLen := len(resp.Topic)
	typeLen := len(resp.Type)
	defLen := len(resp.Definition)
	compatLen := len(resp.Compatibility)
	buf := make([]byte, 2+idLen+2+topicLen+4+1+typeLen+4+defLen+1+compatLen)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(idLen))
	offset += 2
	copy(buf[offset:], resp.ID)
	offset += idLen
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], resp.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(resp.Version))
	offset += 4
	buf[offset] = byte(typeLen)
	offset++
	copy(buf[offset:], resp.Type)
	offset += typeLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(defLen))
	offset += 4
	copy(buf[offset:], resp.Definition)
	offset += defLen
	buf[offset] = byte(compatLen)
	offset++
	copy(buf[offset:], resp.Compatibility)
	return buf
}

// BinaryDeleteSchemaRequest represents a binary delete schema request.
type BinaryDeleteSchemaRequest struct {
	Topic   string
	Version uint32
}

// DecodeBinaryDeleteSchemaRequest decodes a binary delete schema request.
// Format: [2B topic_len][topic][4B version]
func DecodeBinaryDeleteSchemaRequest(data []byte) (*BinaryDeleteSchemaRequest, error) {
	if len(data) < 6 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	version := binary.BigEndian.Uint32(data[offset:])
	return &BinaryDeleteSchemaRequest{Topic: topic, Version: version}, nil
}

// BinaryDLQRequest represents a binary DLQ request.
type BinaryDLQRequest struct {
	Topic       string
	MaxMessages int32
}

// DecodeBinaryDLQRequest decodes a binary DLQ request.
// Format: [2B topic_len][topic][4B max_messages]
func DecodeBinaryDLQRequest(data []byte) (*BinaryDLQRequest, error) {
	if len(data) < 6 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	maxMsgs := int32(binary.BigEndian.Uint32(data[offset:]))
	return &BinaryDLQRequest{Topic: topic, MaxMessages: maxMsgs}, nil
}

// BinaryDLQMessage represents a DLQ message.
type BinaryDLQMessage struct {
	ID      string
	Data    []byte
	Error   string
	Retries int32
}

// BinaryDLQResponse represents a binary DLQ response.
type BinaryDLQResponse struct {
	Messages []BinaryDLQMessage
}

// EncodeBinaryDLQResponse encodes a binary DLQ response.
func EncodeBinaryDLQResponse(resp *BinaryDLQResponse) []byte {
	size := 4 // count
	for _, m := range resp.Messages {
		size += 2 + len(m.ID) + 4 + len(m.Data) + 2 + len(m.Error) + 4
	}
	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Messages)))
	offset += 4
	for _, m := range resp.Messages {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(m.ID)))
		offset += 2
		copy(buf[offset:], m.ID)
		offset += len(m.ID)
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(m.Data)))
		offset += 4
		copy(buf[offset:], m.Data)
		offset += len(m.Data)
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(m.Error)))
		offset += 2
		copy(buf[offset:], m.Error)
		offset += len(m.Error)
		binary.BigEndian.PutUint32(buf[offset:], uint32(m.Retries))
		offset += 4
	}
	return buf
}

// BinaryReplayDLQRequest represents a binary replay DLQ request.
type BinaryReplayDLQRequest struct {
	Topic     string
	MessageID string
}

// DecodeBinaryReplayDLQRequest decodes a binary replay DLQ request.
// Format: [2B topic_len][topic][2B msg_id_len][msg_id]
func DecodeBinaryReplayDLQRequest(data []byte) (*BinaryReplayDLQRequest, error) {
	if len(data) < 4 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+2 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	msgIDLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+msgIDLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	msgID := string(data[offset : offset+msgIDLen])
	return &BinaryReplayDLQRequest{Topic: topic, MessageID: msgID}, nil
}

// BinaryPurgeDLQRequest represents a binary purge DLQ request.
type BinaryPurgeDLQRequest struct {
	Topic string
}

// DecodeBinaryPurgeDLQRequest decodes a binary purge DLQ request.
// Format: [2B topic_len][topic]
func DecodeBinaryPurgeDLQRequest(data []byte) (*BinaryPurgeDLQRequest, error) {
	if len(data) < 2 {
		return nil, ErrBufferTooSmall
	}
	topicLen := int(binary.BigEndian.Uint16(data))
	if 2+topicLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	return &BinaryPurgeDLQRequest{Topic: string(data[2 : 2+topicLen])}, nil
}

// BinaryTxnResponse represents a binary transaction response.
type BinaryTxnResponse struct {
	TxnID   string
	Success bool
}

// EncodeBinaryTxnResponse encodes a binary transaction response.
// Format: [2B txn_id_len][txn_id][1B success]
func EncodeBinaryTxnResponse(resp *BinaryTxnResponse) []byte {
	txnIDLen := len(resp.TxnID)
	buf := make([]byte, 2+txnIDLen+1)
	binary.BigEndian.PutUint16(buf, uint16(txnIDLen))
	copy(buf[2:], resp.TxnID)
	if resp.Success {
		buf[2+txnIDLen] = 1
	}
	return buf
}

// BinaryTxnRequest represents a binary transaction request.
type BinaryTxnRequest struct {
	TxnID string
}

// DecodeBinaryTxnRequest decodes a binary transaction request.
// Format: [2B txn_id_len][txn_id]
func DecodeBinaryTxnRequest(data []byte) (*BinaryTxnRequest, error) {
	if len(data) < 2 {
		return nil, ErrBufferTooSmall
	}
	txnIDLen := int(binary.BigEndian.Uint16(data))
	if 2+txnIDLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	return &BinaryTxnRequest{TxnID: string(data[2 : 2+txnIDLen])}, nil
}

// BinaryTxnProduceRequest represents a binary transaction produce request.
type BinaryTxnProduceRequest struct {
	TxnID string
	Topic string
	Data  []byte
}

// DecodeBinaryTxnProduceRequest decodes a binary transaction produce request.
// Format: [2B txn_id_len][txn_id][2B topic_len][topic][4B data_len][data]
func DecodeBinaryTxnProduceRequest(data []byte) (*BinaryTxnProduceRequest, error) {
	if len(data) < 8 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	txnIDLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+txnIDLen+6 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	txnID := string(data[offset : offset+txnIDLen])
	offset += txnIDLen
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	dataLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+dataLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	msgData := make([]byte, dataLen)
	copy(msgData, data[offset:offset+dataLen])
	return &BinaryTxnProduceRequest{TxnID: txnID, Topic: topic, Data: msgData}, nil
}

// BinarySuccessResponse represents a simple success response with optional message.
type BinarySuccessResponse struct {
	Success bool
	Message string
}

// EncodeBinarySuccessResponse encodes a success response.
func EncodeBinarySuccessResponse(resp *BinarySuccessResponse) []byte {
	msgLen := len(resp.Message)
	buf := make([]byte, 1+2+msgLen)
	if resp.Success {
		buf[0] = 1
	}
	binary.BigEndian.PutUint16(buf[1:], uint16(msgLen))
	copy(buf[3:], resp.Message)
	return buf
}

// BinaryStringRequest represents a simple string request.
type BinaryStringRequest struct {
	Value string
}

// DecodeBinaryStringRequest decodes a binary string request.
// Format: [2B len][string]
func DecodeBinaryStringRequest(data []byte) (*BinaryStringRequest, error) {
	if len(data) < 2 {
		return nil, ErrBufferTooSmall
	}
	strLen := int(binary.BigEndian.Uint16(data))
	if 2+strLen > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	return &BinaryStringRequest{Value: string(data[2 : 2+strLen])}, nil
}

// BinaryUserRequest represents a binary user management request.
type BinaryUserRequest struct {
	Username    string
	Password    string
	OldPassword string
	Roles       []string
	Enabled     bool
}

// DecodeBinaryUserRequest decodes a binary user request.
// Format: [2B user_len][user][2B pass_len][pass][2B old_pass_len][old_pass][4B role_count][roles...][1B enabled]
func DecodeBinaryUserRequest(data []byte) (*BinaryUserRequest, error) {
	if len(data) < 10 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	userLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+userLen+8 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	username := string(data[offset : offset+userLen])
	offset += userLen
	passLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+passLen+6 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	password := string(data[offset : offset+passLen])
	offset += passLen
	oldPassLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+oldPassLen+5 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	oldPassword := string(data[offset : offset+oldPassLen])
	offset += oldPassLen
	roleCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	roles := make([]string, 0, roleCount)
	for i := 0; i < roleCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roleLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+roleLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roles = append(roles, string(data[offset:offset+roleLen]))
		offset += roleLen
	}
	enabled := false
	if offset < len(data) {
		enabled = data[offset] == 1
	}
	return &BinaryUserRequest{Username: username, Password: password, OldPassword: oldPassword, Roles: roles, Enabled: enabled}, nil
}

// BinaryUserInfo represents user info.
type BinaryUserInfo struct {
	Username    string
	Roles       []string
	Permissions []string
	Enabled     bool
	CreatedAt   int64
	UpdatedAt   int64
}

// BinaryUserListResponse represents a binary user list response.
type BinaryUserListResponse struct {
	Users []BinaryUserInfo
}

// EncodeBinaryUserListResponse encodes a binary user list response.
func EncodeBinaryUserListResponse(resp *BinaryUserListResponse) []byte {
	size := 4 // count
	for _, u := range resp.Users {
		size += 2 + len(u.Username) + 4
		for _, r := range u.Roles {
			size += 2 + len(r)
		}
		size += 4 // permissions count
		for _, p := range u.Permissions {
			size += 2 + len(p)
		}
		size += 1 + 8 + 8 // enabled + createdAt + updatedAt
	}
	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Users)))
	offset += 4
	for _, u := range resp.Users {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(u.Username)))
		offset += 2
		copy(buf[offset:], u.Username)
		offset += len(u.Username)
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(u.Roles)))
		offset += 4
		for _, r := range u.Roles {
			binary.BigEndian.PutUint16(buf[offset:], uint16(len(r)))
			offset += 2
			copy(buf[offset:], r)
			offset += len(r)
		}
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(u.Permissions)))
		offset += 4
		for _, p := range u.Permissions {
			binary.BigEndian.PutUint16(buf[offset:], uint16(len(p)))
			offset += 2
			copy(buf[offset:], p)
			offset += len(p)
		}
		if u.Enabled {
			buf[offset] = 1
		}
		offset++
		binary.BigEndian.PutUint64(buf[offset:], uint64(u.CreatedAt))
		offset += 8
		binary.BigEndian.PutUint64(buf[offset:], uint64(u.UpdatedAt))
		offset += 8
	}
	return buf
}

// EncodeBinaryUserInfo encodes a single user info.
func EncodeBinaryUserInfo(info *BinaryUserInfo) []byte {
	size := 2 + len(info.Username) + 4
	for _, r := range info.Roles {
		size += 2 + len(r)
	}
	size += 4
	for _, p := range info.Permissions {
		size += 2 + len(p)
	}
	size += 1 + 8 + 8
	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(info.Username)))
	offset += 2
	copy(buf[offset:], info.Username)
	offset += len(info.Username)
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(info.Roles)))
	offset += 4
	for _, r := range info.Roles {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(r)))
		offset += 2
		copy(buf[offset:], r)
		offset += len(r)
	}
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(info.Permissions)))
	offset += 4
	for _, p := range info.Permissions {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(p)))
		offset += 2
		copy(buf[offset:], p)
		offset += len(p)
	}
	if info.Enabled {
		buf[offset] = 1
	}
	offset++
	binary.BigEndian.PutUint64(buf[offset:], uint64(info.CreatedAt))
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], uint64(info.UpdatedAt))
	return buf
}

// BinaryACLRequest represents a binary ACL request.
type BinaryACLRequest struct {
	Topic        string
	Public       bool
	AllowedUsers []string
	AllowedRoles []string
}

// DecodeBinaryACLRequest decodes a binary ACL request.
// Format: [2B topic_len][topic][1B public][4B user_count][users...][4B role_count][roles...]
func DecodeBinaryACLRequest(data []byte) (*BinaryACLRequest, error) {
	if len(data) < 11 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+9 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	public := data[offset] == 1
	offset++
	userCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	users := make([]string, 0, userCount)
	for i := 0; i < userCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		userLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+userLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		users = append(users, string(data[offset:offset+userLen]))
		offset += userLen
	}
	if offset+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	roleCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	roles := make([]string, 0, roleCount)
	for i := 0; i < roleCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roleLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+roleLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roles = append(roles, string(data[offset:offset+roleLen]))
		offset += roleLen
	}
	return &BinaryACLRequest{Topic: topic, Public: public, AllowedUsers: users, AllowedRoles: roles}, nil
}

// BinaryACLInfo represents ACL info.
type BinaryACLInfo struct {
	Topic         string
	Exists        bool
	Public        bool
	DefaultPublic bool
	AllowedUsers  []string
	AllowedRoles  []string
}

// BinaryACLResponse represents a simple ACL response (for set/delete).
type BinaryACLResponse struct {
	Success bool
	Topic   string
}

// EncodeBinaryACLResponse encodes a simple ACL response.
func EncodeBinaryACLResponse(resp *BinaryACLResponse) []byte {
	topicLen := len(resp.Topic)
	buf := make([]byte, 1+2+topicLen)
	if resp.Success {
		buf[0] = 1
	}
	binary.BigEndian.PutUint16(buf[1:], uint16(topicLen))
	copy(buf[3:], resp.Topic)
	return buf
}

// EncodeBinaryACLInfo encodes a binary ACL info response.
func EncodeBinaryACLInfo(resp *BinaryACLInfo) []byte {
	size := 2 + len(resp.Topic) + 3 + 4
	for _, u := range resp.AllowedUsers {
		size += 2 + len(u)
	}
	size += 4
	for _, r := range resp.AllowedRoles {
		size += 2 + len(r)
	}
	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(resp.Topic)))
	offset += 2
	copy(buf[offset:], resp.Topic)
	offset += len(resp.Topic)
	if resp.Exists {
		buf[offset] = 1
	}
	offset++
	if resp.Public {
		buf[offset] = 1
	}
	offset++
	if resp.DefaultPublic {
		buf[offset] = 1
	}
	offset++
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.AllowedUsers)))
	offset += 4
	for _, u := range resp.AllowedUsers {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(u)))
		offset += 2
		copy(buf[offset:], u)
		offset += len(u)
	}
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.AllowedRoles)))
	offset += 4
	for _, r := range resp.AllowedRoles {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(r)))
		offset += 2
		copy(buf[offset:], r)
		offset += len(r)
	}
	return buf
}

// BinaryACLListResponse represents a binary ACL list response.
type BinaryACLListResponse struct {
	ACLs          []BinaryACLInfo
	DefaultPublic bool
}

// EncodeBinaryACLListResponse encodes a binary ACL list response.
func EncodeBinaryACLListResponse(resp *BinaryACLListResponse) []byte {
	size := 4 + 1 // count + default_public
	for _, acl := range resp.ACLs {
		size += 2 + len(acl.Topic) + 2 + 4
		for _, u := range acl.AllowedUsers {
			size += 2 + len(u)
		}
		size += 4
		for _, r := range acl.AllowedRoles {
			size += 2 + len(r)
		}
	}
	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.ACLs)))
	offset += 4
	for _, acl := range resp.ACLs {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(acl.Topic)))
		offset += 2
		copy(buf[offset:], acl.Topic)
		offset += len(acl.Topic)
		if acl.Exists {
			buf[offset] = 1
		}
		offset++
		if acl.Public {
			buf[offset] = 1
		}
		offset++
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(acl.AllowedUsers)))
		offset += 4
		for _, u := range acl.AllowedUsers {
			binary.BigEndian.PutUint16(buf[offset:], uint16(len(u)))
			offset += 2
			copy(buf[offset:], u)
			offset += len(u)
		}
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(acl.AllowedRoles)))
		offset += 4
		for _, r := range acl.AllowedRoles {
			binary.BigEndian.PutUint16(buf[offset:], uint16(len(r)))
			offset += 2
			copy(buf[offset:], r)
			offset += len(r)
		}
	}
	if resp.DefaultPublic {
		buf[offset] = 1
	}
	return buf
}

// BinaryRoleInfo represents role info.
type BinaryRoleInfo struct {
	Name        string
	Permissions []string
	Description string
}

// BinaryRoleListResponse represents a binary role list response.
type BinaryRoleListResponse struct {
	Roles []BinaryRoleInfo
}

// EncodeBinaryRoleListResponse encodes a binary role list response.
func EncodeBinaryRoleListResponse(resp *BinaryRoleListResponse) []byte {
	size := 4 // count
	for _, r := range resp.Roles {
		size += 2 + len(r.Name) + 4
		for _, p := range r.Permissions {
			size += 2 + len(p)
		}
		size += 2 + len(r.Description)
	}
	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Roles)))
	offset += 4
	for _, r := range resp.Roles {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.Name)))
		offset += 2
		copy(buf[offset:], r.Name)
		offset += len(r.Name)
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(r.Permissions)))
		offset += 4
		for _, p := range r.Permissions {
			binary.BigEndian.PutUint16(buf[offset:], uint16(len(p)))
			offset += 2
			copy(buf[offset:], p)
			offset += len(p)
		}
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(r.Description)))
		offset += 2
		copy(buf[offset:], r.Description)
		offset += len(r.Description)
	}
	return buf
}

// ============================================================================
// Consumer Group Binary Protocol
// ============================================================================

// BinaryGetOffsetRequest represents a request to get a committed offset.
type BinaryGetOffsetRequest struct {
    Topic     string
    GroupID   string
    Partition int32
}

// EncodeBinaryGetOffsetRequest encodes a get-offset request.
func EncodeBinaryGetOffsetRequest(req *BinaryGetOffsetRequest) []byte {
    topicLen := len(req.Topic)
    groupLen := len(req.GroupID)
    buf := make([]byte, 2+topicLen+2+groupLen+4)
    off := 0
    binary.BigEndian.PutUint16(buf[off:], uint16(topicLen))
    off += 2
    copy(buf[off:], req.Topic)
    off += topicLen
    binary.BigEndian.PutUint16(buf[off:], uint16(groupLen))
    off += 2
    copy(buf[off:], req.GroupID)
    off += groupLen
    binary.BigEndian.PutUint32(buf[off:], uint32(req.Partition))
    return buf
}

// DecodeBinaryGetOffsetRequest decodes a get-offset request.
func DecodeBinaryGetOffsetRequest(data []byte) (*BinaryGetOffsetRequest, error) {
    if len(data) < 8 {
        return nil, ErrBufferTooSmall
    }
    off := 0
    tlen := int(binary.BigEndian.Uint16(data[off:]))
    off += 2
    if off+tlen+6 > len(data) {
        return nil, ErrInvalidBinaryFormat
    }
    topic := string(data[off : off+tlen])
    off += tlen
    glen := int(binary.BigEndian.Uint16(data[off:]))
    off += 2
    if off+glen+4 > len(data) {
        return nil, ErrInvalidBinaryFormat
    }
    group := string(data[off : off+glen])
    off += glen
    part := int32(binary.BigEndian.Uint32(data[off:]))
    return &BinaryGetOffsetRequest{Topic: topic, GroupID: group, Partition: part}, nil
}

// BinaryGetOffsetResponse represents a committed offset response.
type BinaryGetOffsetResponse struct {
    Offset uint64
}

// EncodeBinaryGetOffsetResponse encodes a get-offset response.
func EncodeBinaryGetOffsetResponse(resp *BinaryGetOffsetResponse) []byte {
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, resp.Offset)
    return buf
}

// DecodeBinaryGetOffsetResponse decodes a get-offset response.
func DecodeBinaryGetOffsetResponse(data []byte) (*BinaryGetOffsetResponse, error) {
    if len(data) < 8 {
        return nil, ErrBufferTooSmall
    }
    return &BinaryGetOffsetResponse{Offset: binary.BigEndian.Uint64(data)}, nil
}

// BinaryResetOffsetRequest represents a request to reset offset.
// Mode: "earliest", "latest", or "offset". If mode=="offset", Offset is used.
type BinaryResetOffsetRequest struct {
    Topic     string
    GroupID   string
    Partition int32
    Mode      string
    Offset    uint64
}

// DecodeBinaryResetOffsetRequest decodes a reset-offset request.
// Format: [2B topic][topic][2B group][group][4B partition][1B mode_len][mode][optional 8B offset]
func DecodeBinaryResetOffsetRequest(data []byte) (*BinaryResetOffsetRequest, error) {
    if len(data) < 9 {
        return nil, ErrBufferTooSmall
    }
    off := 0
    tlen := int(binary.BigEndian.Uint16(data[off:]))
    off += 2
    if off+tlen+7 > len(data) {
        return nil, ErrInvalidBinaryFormat
    }
    topic := string(data[off : off+tlen])
    off += tlen
    glen := int(binary.BigEndian.Uint16(data[off:]))
    off += 2
    if off+glen+5 > len(data) {
        return nil, ErrInvalidBinaryFormat
    }
    group := string(data[off : off+glen])
    off += glen
    part := int32(binary.BigEndian.Uint32(data[off:]))
    off += 4
    mlen := int(data[off])
    off++
    if off+mlen > len(data) {
        return nil, ErrInvalidBinaryFormat
    }
    mode := string(data[off : off+mlen])
    off += mlen
    var explicit uint64
    if mode == "offset" {
        if off+8 > len(data) {
            return nil, ErrInvalidBinaryFormat
        }
        explicit = binary.BigEndian.Uint64(data[off:])
    }
    return &BinaryResetOffsetRequest{Topic: topic, GroupID: group, Partition: part, Mode: mode, Offset: explicit}, nil
}

// BinarySimpleBoolResponse is a generic success boolean response.
type BinarySimpleBoolResponse struct{ Success bool }

// EncodeBinarySimpleBoolResponse encodes a simple boolean response.
func EncodeBinarySimpleBoolResponse(resp *BinarySimpleBoolResponse) []byte {
    if resp.Success { return []byte{1} }
    return []byte{0}
}

// BinaryListGroupsResponse lists consumer groups.
// For each group: topic, group_id, members, offsets map.
// Format per group: [2B topic][topic][2B group][group][4B members][4B count][count*(4B partition + 8B offset)]
// Preceded by [4B group_count]

type BinaryGroupOffsets struct{ Partition int32; Offset uint64 }

type BinaryListGroupsResponse struct {
    Groups []struct{
        Topic   string
        GroupID string
        Members uint32
        Offsets []BinaryGroupOffsets
    }
}

func EncodeBinaryListGroupsResponse(resp *BinaryListGroupsResponse) []byte {
    // Compute size
    size := 4
    for _, g := range resp.Groups {
        size += 2 + len(g.Topic) + 2 + len(g.GroupID) + 4 + 4 + len(g.Offsets)*(4+8)
    }
    buf := make([]byte, size)
    off := 0
    binary.BigEndian.PutUint32(buf[off:], uint32(len(resp.Groups)))
    off += 4
    for _, g := range resp.Groups {
        binary.BigEndian.PutUint16(buf[off:], uint16(len(g.Topic)))
        off += 2
        copy(buf[off:], g.Topic)
        off += len(g.Topic)
        binary.BigEndian.PutUint16(buf[off:], uint16(len(g.GroupID)))
        off += 2
        copy(buf[off:], g.GroupID)
        off += len(g.GroupID)
        binary.BigEndian.PutUint32(buf[off:], g.Members)
        off += 4
        binary.BigEndian.PutUint32(buf[off:], uint32(len(g.Offsets)))
        off += 4
        for _, o := range g.Offsets {
            binary.BigEndian.PutUint32(buf[off:], uint32(o.Partition))
            off += 4
            binary.BigEndian.PutUint64(buf[off:], o.Offset)
            off += 8
        }
    }
    return buf
}

// BinaryDescribeGroupRequest requests info for a single group
// Format: [2B topic][topic][2B group][group]

type BinaryDescribeGroupRequest struct{ Topic, GroupID string }

func DecodeBinaryDescribeGroupRequest(data []byte) (*BinaryDescribeGroupRequest, error) {
    if len(data) < 4 { return nil, ErrBufferTooSmall }
    off := 0
    tlen := int(binary.BigEndian.Uint16(data[off:]))
    off += 2
    if off+tlen+2 > len(data) { return nil, ErrInvalidBinaryFormat }
    topic := string(data[off:off+tlen])
    off += tlen
    glen := int(binary.BigEndian.Uint16(data[off:]))
    off += 2
    if off+glen > len(data) { return nil, ErrInvalidBinaryFormat }
    group := string(data[off:off+glen])
    return &BinaryDescribeGroupRequest{Topic: topic, GroupID: group}, nil
}

// BinaryDescribeGroupResponse mirrors a single group's info

type BinaryDescribeGroupResponse struct {
    Topic   string
    GroupID string
    Members uint32
    Offsets []BinaryGroupOffsets
}

func EncodeBinaryDescribeGroupResponse(resp *BinaryDescribeGroupResponse) []byte {
    // Size: topic+group strings + members + count + offsets
    size := 2 + len(resp.Topic) + 2 + len(resp.GroupID) + 4 + 4 + len(resp.Offsets)*(4+8)
    buf := make([]byte, size)
    off := 0
    binary.BigEndian.PutUint16(buf[off:], uint16(len(resp.Topic)))
    off += 2
    copy(buf[off:], resp.Topic)
    off += len(resp.Topic)
    binary.BigEndian.PutUint16(buf[off:], uint16(len(resp.GroupID)))
    off += 2
    copy(buf[off:], resp.GroupID)
    off += len(resp.GroupID)
    binary.BigEndian.PutUint32(buf[off:], resp.Members)
    off += 4
    binary.BigEndian.PutUint32(buf[off:], uint32(len(resp.Offsets)))
    off += 4
    for _, o := range resp.Offsets {
        binary.BigEndian.PutUint32(buf[off:], uint32(o.Partition))
        off += 4
        binary.BigEndian.PutUint64(buf[off:], o.Offset)
        off += 8
    }
    return buf
}

// BinaryGetLagRequest and Response

type BinaryGetLagRequest struct{ Topic string; GroupID string; Partition int32 }

type BinaryGetLagResponse struct {
    CurrentOffset   uint64 // next to consume (committed)
    CommittedOffset uint64 // same as CurrentOffset for clarity
    LatestOffset    uint64 // next offset after highest message
    Lag             uint64 // LatestOffset - CurrentOffset
}

func DecodeBinaryGetLagRequest(data []byte) (*BinaryGetLagRequest, error) {
    r, err := DecodeBinaryGetOffsetRequest(data)
    if err != nil { return nil, err }
    return &BinaryGetLagRequest{Topic: r.Topic, GroupID: r.GroupID, Partition: r.Partition}, nil
}

func EncodeBinaryGetLagResponse(resp *BinaryGetLagResponse) []byte {
    buf := make([]byte, 8*4)
    binary.BigEndian.PutUint64(buf[0:], resp.CurrentOffset)
    binary.BigEndian.PutUint64(buf[8:], resp.CommittedOffset)
    binary.BigEndian.PutUint64(buf[16:], resp.LatestOffset)
    binary.BigEndian.PutUint64(buf[24:], resp.Lag)
    return buf
}

// BinaryDeleteGroupRequest/Response

type BinaryDeleteGroupRequest struct{ Topic, GroupID string }

func DecodeBinaryDeleteGroupRequest(data []byte) (*BinaryDeleteGroupRequest, error) {
    if len(data) < 4 { return nil, ErrBufferTooSmall }
    off := 0
    tlen := int(binary.BigEndian.Uint16(data[off:]))
    off += 2
    if off+tlen+2 > len(data) { return nil, ErrInvalidBinaryFormat }
    topic := string(data[off:off+tlen])
    off += tlen
    glen := int(binary.BigEndian.Uint16(data[off:]))
    off += 2
    if off+glen > len(data) { return nil, ErrInvalidBinaryFormat }
    group := string(data[off:off+glen])
    return &BinaryDeleteGroupRequest{Topic: topic, GroupID: group}, nil
}

// ============================================================================
// Client-Side Encoder Functions
// ============================================================================

// EncodeBinaryProduceDelayedRequest encodes a produce delayed request for client use.
func EncodeBinaryProduceDelayedRequest(req *BinaryProduceDelayedRequest) []byte {
	topicLen := len(req.Topic)
	dataLen := len(req.Data)
	buf := make([]byte, 2+topicLen+4+dataLen+8)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(dataLen))
	offset += 4
	copy(buf[offset:], req.Data)
	offset += dataLen
	binary.BigEndian.PutUint64(buf[offset:], uint64(req.DelayMs))
	return buf
}

// EncodeBinaryProduceWithTTLRequest encodes a produce with TTL request for client use.
func EncodeBinaryProduceWithTTLRequest(req *BinaryProduceWithTTLRequest) []byte {
	topicLen := len(req.Topic)
	dataLen := len(req.Data)
	buf := make([]byte, 2+topicLen+4+dataLen+8)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(dataLen))
	offset += 4
	copy(buf[offset:], req.Data)
	offset += dataLen
	binary.BigEndian.PutUint64(buf[offset:], uint64(req.TTLMs))
	return buf
}

// EncodeBinaryProduceWithSchemaRequest encodes a produce with schema request for client use.
func EncodeBinaryProduceWithSchemaRequest(req *BinaryProduceWithSchemaRequest) []byte {
	topicLen := len(req.Topic)
	schemaLen := len(req.SchemaName)
	dataLen := len(req.Data)
	buf := make([]byte, 2+topicLen+2+schemaLen+4+dataLen)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint16(buf[offset:], uint16(schemaLen))
	offset += 2
	copy(buf[offset:], req.SchemaName)
	offset += schemaLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(dataLen))
	offset += 4
	copy(buf[offset:], req.Data)
	return buf
}

// EncodeBinaryRegisterSchemaRequest encodes a register schema request for client use.
func EncodeBinaryRegisterSchemaRequest(req *BinaryRegisterSchemaRequest) []byte {
	nameLen := len(req.Name)
	typeLen := len(req.Type)
	schemaLen := len(req.Schema)
	buf := make([]byte, 2+nameLen+1+typeLen+4+schemaLen)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(nameLen))
	offset += 2
	copy(buf[offset:], req.Name)
	offset += nameLen
	buf[offset] = byte(typeLen)
	offset++
	copy(buf[offset:], req.Type)
	offset += typeLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(schemaLen))
	offset += 4
	copy(buf[offset:], req.Schema)
	return buf
}

// EncodeBinaryListSchemasRequest encodes a list schemas request for client use.
func EncodeBinaryListSchemasRequest(req *BinaryListSchemasRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen)
	binary.BigEndian.PutUint16(buf, uint16(topicLen))
	copy(buf[2:], req.Topic)
	return buf
}

// EncodeBinaryValidateSchemaRequest encodes a validate schema request for client use.
func EncodeBinaryValidateSchemaRequest(req *BinaryValidateSchemaRequest) []byte {
	nameLen := len(req.Name)
	msgLen := len(req.Message)
	buf := make([]byte, 2+nameLen+4+msgLen)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(nameLen))
	offset += 2
	copy(buf[offset:], req.Name)
	offset += nameLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(msgLen))
	offset += 4
	copy(buf[offset:], req.Message)
	return buf
}

// EncodeBinaryGetSchemaRequest encodes a get schema request for client use.
func EncodeBinaryGetSchemaRequest(req *BinaryGetSchemaRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen+4)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Version))
	return buf
}

// EncodeBinaryDeleteSchemaRequest encodes a delete schema request for client use.
func EncodeBinaryDeleteSchemaRequest(req *BinaryDeleteSchemaRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen+4)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Version))
	return buf
}

// EncodeBinaryDLQRequest encodes a DLQ request for client use.
func EncodeBinaryDLQRequest(req *BinaryDLQRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen+4)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.MaxMessages))
	return buf
}

// EncodeBinaryReplayDLQRequest encodes a replay DLQ request for client use.
func EncodeBinaryReplayDLQRequest(req *BinaryReplayDLQRequest) []byte {
	topicLen := len(req.Topic)
	msgIDLen := len(req.MessageID)
	buf := make([]byte, 2+topicLen+2+msgIDLen)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint16(buf[offset:], uint16(msgIDLen))
	offset += 2
	copy(buf[offset:], req.MessageID)
	return buf
}

// EncodeBinaryPurgeDLQRequest encodes a purge DLQ request for client use.
func EncodeBinaryPurgeDLQRequest(req *BinaryPurgeDLQRequest) []byte {
	topicLen := len(req.Topic)
	buf := make([]byte, 2+topicLen)
	binary.BigEndian.PutUint16(buf, uint16(topicLen))
	copy(buf[2:], req.Topic)
	return buf
}

// EncodeBinaryTxnRequest encodes a transaction request for client use.
func EncodeBinaryTxnRequest(req *BinaryTxnRequest) []byte {
	txnIDLen := len(req.TxnID)
	buf := make([]byte, 2+txnIDLen)
	binary.BigEndian.PutUint16(buf, uint16(txnIDLen))
	copy(buf[2:], req.TxnID)
	return buf
}

// EncodeBinaryTxnProduceRequest encodes a transaction produce request for client use.
func EncodeBinaryTxnProduceRequest(req *BinaryTxnProduceRequest) []byte {
	txnIDLen := len(req.TxnID)
	topicLen := len(req.Topic)
	dataLen := len(req.Data)
	buf := make([]byte, 2+txnIDLen+2+topicLen+4+dataLen)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(txnIDLen))
	offset += 2
	copy(buf[offset:], req.TxnID)
	offset += txnIDLen
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(dataLen))
	offset += 4
	copy(buf[offset:], req.Data)
	return buf
}

// EncodeBinaryUserRequest encodes a user request for client use.
func EncodeBinaryUserRequest(req *BinaryUserRequest) []byte {
	userLen := len(req.Username)
	passLen := len(req.Password)
	oldPassLen := len(req.OldPassword)
	size := 2 + userLen + 2 + passLen + 2 + oldPassLen + 4
	for _, r := range req.Roles {
		size += 2 + len(r)
	}
	size += 1 // enabled
	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(userLen))
	offset += 2
	copy(buf[offset:], req.Username)
	offset += userLen
	binary.BigEndian.PutUint16(buf[offset:], uint16(passLen))
	offset += 2
	copy(buf[offset:], req.Password)
	offset += passLen
	binary.BigEndian.PutUint16(buf[offset:], uint16(oldPassLen))
	offset += 2
	copy(buf[offset:], req.OldPassword)
	offset += oldPassLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(req.Roles)))
	offset += 4
	for _, r := range req.Roles {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(r)))
		offset += 2
		copy(buf[offset:], r)
		offset += len(r)
	}
	if req.Enabled {
		buf[offset] = 1
	}
	return buf
}

// EncodeBinaryStringRequest encodes a simple string request for client use.
func EncodeBinaryStringRequest(value string) []byte {
	valLen := len(value)
	buf := make([]byte, 2+valLen)
	binary.BigEndian.PutUint16(buf, uint16(valLen))
	copy(buf[2:], value)
	return buf
}

// EncodeBinaryACLRequest encodes an ACL request for client use.
func EncodeBinaryACLRequest(req *BinaryACLRequest) []byte {
	topicLen := len(req.Topic)
	size := 2 + topicLen + 1 + 4
	for _, u := range req.AllowedUsers {
		size += 2 + len(u)
	}
	size += 4
	for _, r := range req.AllowedRoles {
		size += 2 + len(r)
	}
	buf := make([]byte, size)
	offset := 0
	binary.BigEndian.PutUint16(buf[offset:], uint16(topicLen))
	offset += 2
	copy(buf[offset:], req.Topic)
	offset += topicLen
	if req.Public {
		buf[offset] = 1
	}
	offset++
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(req.AllowedUsers)))
	offset += 4
	for _, u := range req.AllowedUsers {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(u)))
		offset += 2
		copy(buf[offset:], u)
		offset += len(u)
	}
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(req.AllowedRoles)))
	offset += 4
	for _, r := range req.AllowedRoles {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(r)))
		offset += 2
		copy(buf[offset:], r)
		offset += len(r)
	}
	return buf
}

// DecodeBinaryTxnResponse decodes a binary transaction response.
func DecodeBinaryTxnResponse(data []byte) (*BinaryTxnResponse, error) {
	if len(data) < 2 {
		return nil, ErrBufferTooSmall
	}
	txnIDLen := int(binary.BigEndian.Uint16(data))
	if 2+txnIDLen+1 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	txnID := string(data[2 : 2+txnIDLen])
	success := false
	if 2+txnIDLen < len(data) {
		success = data[2+txnIDLen] == 1
	}
	return &BinaryTxnResponse{TxnID: txnID, Success: success}, nil
}

// DecodeBinaryUserListResponse decodes a binary user list response.
func DecodeBinaryUserListResponse(data []byte) (*BinaryUserListResponse, error) {
	if len(data) < 4 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	userCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	users := make([]BinaryUserInfo, 0, userCount)
	for i := 0; i < userCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		userLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+userLen+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		username := string(data[offset : offset+userLen])
		offset += userLen
		roleCount := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		roles := make([]string, 0, roleCount)
		for j := 0; j < roleCount; j++ {
			if offset+2 > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			roleLen := int(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if offset+roleLen > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			roles = append(roles, string(data[offset:offset+roleLen]))
			offset += roleLen
		}
		// Skip permissions (4B count + strings)
		if offset+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		permCount := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		perms := make([]string, 0, permCount)
		for j := 0; j < permCount; j++ {
			if offset+2 > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			permLen := int(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if offset+permLen > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			perms = append(perms, string(data[offset:offset+permLen]))
			offset += permLen
		}
		// enabled + createdAt + updatedAt
		if offset+1+8+8 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		enabled := data[offset] == 1
		offset++
		createdAt := int64(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		updatedAt := int64(binary.BigEndian.Uint64(data[offset:]))
		offset += 8
		users = append(users, BinaryUserInfo{
			Username:    username,
			Roles:       roles,
			Permissions: perms,
			Enabled:     enabled,
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt,
		})
	}
	return &BinaryUserListResponse{Users: users}, nil
}

// DecodeBinarySuccessResponse decodes a binary success response.
func DecodeBinarySuccessResponse(data []byte) (*BinarySuccessResponse, error) {
	if len(data) < 1 {
		return nil, ErrBufferTooSmall
	}
	success := data[0] == 1
	msg := ""
	if len(data) >= 3 {
		msgLen := int(binary.BigEndian.Uint16(data[1:]))
		if 3+msgLen <= len(data) {
			msg = string(data[3 : 3+msgLen])
		}
	}
	return &BinarySuccessResponse{Success: success, Message: msg}, nil
}

// DecodeBinaryDLQResponse decodes a binary DLQ response.
func DecodeBinaryDLQResponse(data []byte) (*BinaryDLQResponse, error) {
	if len(data) < 4 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	msgCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	messages := make([]BinaryDLQMessage, 0, msgCount)
	for i := 0; i < msgCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		idLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+idLen+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		id := string(data[offset : offset+idLen])
		offset += idLen
		dataLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if offset+dataLen+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		msgData := make([]byte, dataLen)
		copy(msgData, data[offset:offset+dataLen])
		offset += dataLen
		errLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+errLen+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		errMsg := string(data[offset : offset+errLen])
		offset += errLen
		retries := int32(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		messages = append(messages, BinaryDLQMessage{
			ID:      id,
			Data:    msgData,
			Error:   errMsg,
			Retries: retries,
		})
	}
	return &BinaryDLQResponse{Messages: messages}, nil
}

// DecodeBinaryListSchemasResponse decodes a binary list schemas response.
func DecodeBinaryListSchemasResponse(data []byte) (*BinaryListSchemasResponse, error) {
	if len(data) < 4 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	count := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	schemas := make([]BinarySchemaInfo, 0, count)
	for i := 0; i < count; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		nameLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+nameLen+1 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		typeLen := int(data[offset])
		offset++
		if offset+typeLen+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		schemaType := string(data[offset : offset+typeLen])
		offset += typeLen
		version := binary.BigEndian.Uint32(data[offset:])
		offset += 4
		schemas = append(schemas, BinarySchemaInfo{
			Name:    name,
			Type:    schemaType,
			Version: version,
		})
	}
	return &BinaryListSchemasResponse{Schemas: schemas}, nil
}

// DecodeBinaryValidateSchemaResponse decodes a binary validate schema response.
func DecodeBinaryValidateSchemaResponse(data []byte) (*BinaryValidateSchemaResponse, error) {
	if len(data) < 5 {
		return nil, ErrBufferTooSmall
	}
	valid := data[0] == 1
	errCount := int(binary.BigEndian.Uint32(data[1:]))
	offset := 5
	errors := make([]string, 0, errCount)
	for i := 0; i < errCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		errLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+errLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		errors = append(errors, string(data[offset:offset+errLen]))
		offset += errLen
	}
	return &BinaryValidateSchemaResponse{Valid: valid, Errors: errors}, nil
}

// ============================================================================
// Client-Side Consumer Group Encoders/Decoders
// ============================================================================

// EncodeBinaryResetOffsetRequest encodes a reset offset request for client use.
func EncodeBinaryResetOffsetRequest(req *BinaryResetOffsetRequest) []byte {
	topicLen := len(req.Topic)
	groupLen := len(req.GroupID)
	modeLen := len(req.Mode)
	// Base size: topic + group + partition + mode length byte + mode
	size := 2 + topicLen + 2 + groupLen + 4 + 1 + modeLen
	// Add offset if mode is "offset"
	if req.Mode == "offset" {
		size += 8
	}
	buf := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint16(buf[off:], uint16(topicLen))
	off += 2
	copy(buf[off:], req.Topic)
	off += topicLen
	binary.BigEndian.PutUint16(buf[off:], uint16(groupLen))
	off += 2
	copy(buf[off:], req.GroupID)
	off += groupLen
	binary.BigEndian.PutUint32(buf[off:], uint32(req.Partition))
	off += 4
	buf[off] = byte(modeLen)
	off++
	copy(buf[off:], req.Mode)
	off += modeLen
	if req.Mode == "offset" {
		binary.BigEndian.PutUint64(buf[off:], req.Offset)
	}
	return buf
}

// EncodeBinaryGetLagRequest encodes a get-lag request for client use.
func EncodeBinaryGetLagRequest(req *BinaryGetLagRequest) []byte {
	// Same format as GetOffset request
	return EncodeBinaryGetOffsetRequest(&BinaryGetOffsetRequest{
		Topic:     req.Topic,
		GroupID:   req.GroupID,
		Partition: req.Partition,
	})
}

// DecodeBinaryGetLagResponse decodes a get-lag response for client use.
func DecodeBinaryGetLagResponse(data []byte) (*BinaryGetLagResponse, error) {
	if len(data) < 32 {
		return nil, ErrBufferTooSmall
	}
	return &BinaryGetLagResponse{
		CurrentOffset:   binary.BigEndian.Uint64(data[0:]),
		CommittedOffset: binary.BigEndian.Uint64(data[8:]),
		LatestOffset:    binary.BigEndian.Uint64(data[16:]),
		Lag:             binary.BigEndian.Uint64(data[24:]),
	}, nil
}

// DecodeBinaryListGroupsResponse decodes a list-groups response for client use.
func DecodeBinaryListGroupsResponse(data []byte) (*BinaryListGroupsResponse, error) {
	if len(data) < 4 {
		return nil, ErrBufferTooSmall
	}
	off := 0
	count := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	resp := &BinaryListGroupsResponse{
		Groups: make([]struct {
			Topic   string
			GroupID string
			Members uint32
			Offsets []BinaryGroupOffsets
		}, count),
	}
	for i := 0; i < count; i++ {
		if off+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		tlen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if off+tlen+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		topic := string(data[off : off+tlen])
		off += tlen
		glen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if off+glen+8 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		groupID := string(data[off : off+glen])
		off += glen
		members := binary.BigEndian.Uint32(data[off:])
		off += 4
		offsetCount := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		offsets := make([]BinaryGroupOffsets, offsetCount)
		for j := 0; j < offsetCount; j++ {
			if off+12 > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			offsets[j].Partition = int32(binary.BigEndian.Uint32(data[off:]))
			off += 4
			offsets[j].Offset = binary.BigEndian.Uint64(data[off:])
			off += 8
		}
		resp.Groups[i] = struct {
			Topic   string
			GroupID string
			Members uint32
			Offsets []BinaryGroupOffsets
		}{Topic: topic, GroupID: groupID, Members: members, Offsets: offsets}
	}
	return resp, nil
}

// EncodeBinaryDescribeGroupRequest encodes a describe-group request for client use.
func EncodeBinaryDescribeGroupRequest(req *BinaryDescribeGroupRequest) []byte {
	topicLen := len(req.Topic)
	groupLen := len(req.GroupID)
	buf := make([]byte, 2+topicLen+2+groupLen)
	off := 0
	binary.BigEndian.PutUint16(buf[off:], uint16(topicLen))
	off += 2
	copy(buf[off:], req.Topic)
	off += topicLen
	binary.BigEndian.PutUint16(buf[off:], uint16(groupLen))
	off += 2
	copy(buf[off:], req.GroupID)
	return buf
}

// DecodeBinaryDescribeGroupResponse decodes a describe-group response for client use.
func DecodeBinaryDescribeGroupResponse(data []byte) (*BinaryDescribeGroupResponse, error) {
	if len(data) < 8 {
		return nil, ErrBufferTooSmall
	}
	off := 0
	tlen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	if off+tlen+2 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[off : off+tlen])
	off += tlen
	glen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	if off+glen+8 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	groupID := string(data[off : off+glen])
	off += glen
	members := binary.BigEndian.Uint32(data[off:])
	off += 4
	offsetCount := int(binary.BigEndian.Uint32(data[off:]))
	off += 4
	offsets := make([]BinaryGroupOffsets, offsetCount)
	for i := 0; i < offsetCount; i++ {
		if off+12 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		offsets[i].Partition = int32(binary.BigEndian.Uint32(data[off:]))
		off += 4
		offsets[i].Offset = binary.BigEndian.Uint64(data[off:])
		off += 8
	}
	return &BinaryDescribeGroupResponse{
		Topic:   topic,
		GroupID: groupID,
		Members: members,
		Offsets: offsets,
	}, nil
}

// EncodeBinaryDeleteGroupRequest encodes a delete-group request for client use.
func EncodeBinaryDeleteGroupRequest(req *BinaryDeleteGroupRequest) []byte {
	// Same format as DescribeGroupRequest
	return EncodeBinaryDescribeGroupRequest(&BinaryDescribeGroupRequest{
		Topic:   req.Topic,
		GroupID: req.GroupID,
	})
}

// DecodeBinaryUserInfo decodes a single user info response.
func DecodeBinaryUserInfo(data []byte) (*BinaryUserInfo, error) {
	if len(data) < 2 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	userLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+userLen+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	username := string(data[offset : offset+userLen])
	offset += userLen
	roleCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	roles := make([]string, 0, roleCount)
	for i := 0; i < roleCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roleLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+roleLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roles = append(roles, string(data[offset:offset+roleLen]))
		offset += roleLen
	}
	if offset+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	permCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	perms := make([]string, 0, permCount)
	for i := 0; i < permCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		permLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+permLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		perms = append(perms, string(data[offset:offset+permLen]))
		offset += permLen
	}
	if offset+1+8+8 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	enabled := data[offset] == 1
	offset++
	createdAt := int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8
	updatedAt := int64(binary.BigEndian.Uint64(data[offset:]))
	return &BinaryUserInfo{
		Username:    username,
		Roles:       roles,
		Permissions: perms,
		Enabled:     enabled,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}, nil
}

// DecodeBinaryACLInfo decodes a single ACL info response.
func DecodeBinaryACLInfo(data []byte) (*BinaryACLInfo, error) {
	if len(data) < 2 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	topicLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+topicLen+3 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	topic := string(data[offset : offset+topicLen])
	offset += topicLen
	exists := data[offset] == 1
	offset++
	public := data[offset] == 1
	offset++
	defaultPublic := data[offset] == 1
	offset++
	if offset+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	userCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	users := make([]string, 0, userCount)
	for i := 0; i < userCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		userLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+userLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		users = append(users, string(data[offset:offset+userLen]))
		offset += userLen
	}
	if offset+4 > len(data) {
		return nil, ErrInvalidBinaryFormat
	}
	roleCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	roles := make([]string, 0, roleCount)
	for i := 0; i < roleCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roleLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+roleLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roles = append(roles, string(data[offset:offset+roleLen]))
		offset += roleLen
	}
	return &BinaryACLInfo{
		Topic:         topic,
		Exists:        exists,
		Public:        public,
		DefaultPublic: defaultPublic,
		AllowedUsers:  users,
		AllowedRoles:  roles,
	}, nil
}

// DecodeBinaryACLListResponse decodes a binary ACL list response.
func DecodeBinaryACLListResponse(data []byte) (*BinaryACLListResponse, error) {
	if len(data) < 5 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	count := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	acls := make([]BinaryACLInfo, 0, count)
	for i := 0; i < count; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+topicLen+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		topic := string(data[offset : offset+topicLen])
		offset += topicLen
		exists := data[offset] == 1
		offset++
		public := data[offset] == 1
		offset++
		if offset+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		userCount := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		users := make([]string, 0, userCount)
		for j := 0; j < userCount; j++ {
			if offset+2 > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			userLen := int(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if offset+userLen > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			users = append(users, string(data[offset:offset+userLen]))
			offset += userLen
		}
		if offset+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		roleCount := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		roles := make([]string, 0, roleCount)
		for j := 0; j < roleCount; j++ {
			if offset+2 > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			roleLen := int(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if offset+roleLen > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			roles = append(roles, string(data[offset:offset+roleLen]))
			offset += roleLen
		}
		acls = append(acls, BinaryACLInfo{
			Topic:        topic,
			Exists:       exists,
			Public:       public,
			AllowedUsers: users,
			AllowedRoles: roles,
		})
	}
	defaultPublic := false
	if offset < len(data) {
		defaultPublic = data[offset] == 1
	}
	return &BinaryACLListResponse{ACLs: acls, DefaultPublic: defaultPublic}, nil
}

// DecodeBinaryRoleListResponse decodes a binary role list response.
func DecodeBinaryRoleListResponse(data []byte) (*BinaryRoleListResponse, error) {
	if len(data) < 4 {
		return nil, ErrBufferTooSmall
	}
	offset := 0
	count := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	roles := make([]BinaryRoleInfo, 0, count)
	for i := 0; i < count; i++ {
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		nameLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+nameLen+4 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		permCount := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		perms := make([]string, 0, permCount)
		for j := 0; j < permCount; j++ {
			if offset+2 > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			permLen := int(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if offset+permLen > len(data) {
				return nil, ErrInvalidBinaryFormat
			}
			perms = append(perms, string(data[offset:offset+permLen]))
			offset += permLen
		}
		if offset+2 > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		descLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+descLen > len(data) {
			return nil, ErrInvalidBinaryFormat
		}
		desc := string(data[offset : offset+descLen])
		offset += descLen
		roles = append(roles, BinaryRoleInfo{
			Name:        name,
			Permissions: perms,
			Description: desc,
		})
	}
	return &BinaryRoleListResponse{Roles: roles}, nil
}

// BinaryClusterJoinRequest represents a binary cluster join request.
type BinaryClusterJoinRequest struct {
	Peer string
}

// EncodeBinaryClusterJoinRequest encodes a cluster join request for client use.
func EncodeBinaryClusterJoinRequest(req *BinaryClusterJoinRequest) []byte {
	return EncodeBinaryStringRequest(req.Peer)
}

// DecodeBinaryClusterJoinRequest decodes a binary cluster join request.
func DecodeBinaryClusterJoinRequest(data []byte) (*BinaryClusterJoinRequest, error) {
	req, err := DecodeBinaryStringRequest(data)
	if err != nil {
		return nil, err
	}
	return &BinaryClusterJoinRequest{Peer: req.Value}, nil
}

// ============================================================================
// Cluster Metadata Protocol for Partition Routing (Horizontal Scaling)
// ============================================================================

// BinaryClusterMetadataRequest requests partition-to-node mappings.
// If Topic is empty, returns metadata for all topics.
type BinaryClusterMetadataRequest struct {
	Topic string // Optional: specific topic, or empty for all topics
}

// PartitionMetadata contains routing info for a single partition.
type PartitionMetadata struct {
	Partition  int32    // Partition number
	LeaderID   string   // Node ID of the partition leader
	LeaderAddr string   // Client-facing address of the leader (host:port)
	Epoch      uint64   // Leader epoch for fencing stale requests
	State      string   // Partition state: "online", "offline", "reassigning", "syncing"
	Replicas   []string // Node IDs of all replicas (including leader)
	ISR        []string // In-Sync Replicas (node IDs)
}

// TopicMetadata contains partition metadata for a topic.
type TopicMetadata struct {
	Topic      string
	Partitions []PartitionMetadata
}

// BinaryClusterMetadataResponse contains partition-to-node mappings.
type BinaryClusterMetadataResponse struct {
	ClusterID string          // Cluster identifier
	Topics    []TopicMetadata // Partition metadata per topic
}

// EncodeBinaryClusterMetadataRequest encodes a cluster metadata request.
func EncodeBinaryClusterMetadataRequest(req *BinaryClusterMetadataRequest) []byte {
	return EncodeBinaryStringRequest(req.Topic)
}

// DecodeBinaryClusterMetadataRequest decodes a cluster metadata request.
func DecodeBinaryClusterMetadataRequest(data []byte) (*BinaryClusterMetadataRequest, error) {
	// Empty data means request all topics
	if len(data) == 0 {
		return &BinaryClusterMetadataRequest{Topic: ""}, nil
	}
	req, err := DecodeBinaryStringRequest(data)
	if err != nil {
		return nil, err
	}
	return &BinaryClusterMetadataRequest{Topic: req.Value}, nil
}

// EncodeBinaryClusterMetadataResponse encodes a cluster metadata response.
// Format:
//
//	[2B cluster_id_len][cluster_id]
//	[4B topic_count]
//	  [2B topic_len][topic]
//	  [4B partition_count]
//	    [4B partition][2B leader_id_len][leader_id][2B leader_addr_len][leader_addr][8B epoch]
//	    [2B state_len][state][4B replica_count][replicas...][4B isr_count][isr...]
func EncodeBinaryClusterMetadataResponse(resp *BinaryClusterMetadataResponse) []byte {
	// Calculate size
	size := 2 + len(resp.ClusterID) + 4 // cluster_id + topic_count
	for _, topic := range resp.Topics {
		size += 2 + len(topic.Topic) + 4 // topic_name + partition_count
		for _, p := range topic.Partitions {
			size += 4 + 2 + len(p.LeaderID) + 2 + len(p.LeaderAddr) + 8 // partition, leader_id, leader_addr, epoch
			size += 2 + len(p.State)                                    // state
			size += 4                                                   // replica_count
			for _, r := range p.Replicas {
				size += 2 + len(r) // replica_id
			}
			size += 4 // isr_count
			for _, i := range p.ISR {
				size += 2 + len(i) // isr_id
			}
		}
	}

	buf := make([]byte, size)
	offset := 0

	// Cluster ID
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(resp.ClusterID)))
	offset += 2
	copy(buf[offset:], resp.ClusterID)
	offset += len(resp.ClusterID)

	// Topic count
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Topics)))
	offset += 4

	// Topics
	for _, topic := range resp.Topics {
		// Topic name
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(topic.Topic)))
		offset += 2
		copy(buf[offset:], topic.Topic)
		offset += len(topic.Topic)

		// Partition count
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(topic.Partitions)))
		offset += 4

		// Partitions
		for _, p := range topic.Partitions {
			binary.BigEndian.PutUint32(buf[offset:], uint32(p.Partition))
			offset += 4

			binary.BigEndian.PutUint16(buf[offset:], uint16(len(p.LeaderID)))
			offset += 2
			copy(buf[offset:], p.LeaderID)
			offset += len(p.LeaderID)

			binary.BigEndian.PutUint16(buf[offset:], uint16(len(p.LeaderAddr)))
			offset += 2
			copy(buf[offset:], p.LeaderAddr)
			offset += len(p.LeaderAddr)

			binary.BigEndian.PutUint64(buf[offset:], p.Epoch)
			offset += 8

			// State
			binary.BigEndian.PutUint16(buf[offset:], uint16(len(p.State)))
			offset += 2
			copy(buf[offset:], p.State)
			offset += len(p.State)

			// Replicas
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(p.Replicas)))
			offset += 4
			for _, r := range p.Replicas {
				binary.BigEndian.PutUint16(buf[offset:], uint16(len(r)))
				offset += 2
				copy(buf[offset:], r)
				offset += len(r)
			}

			// ISR
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(p.ISR)))
			offset += 4
			for _, i := range p.ISR {
				binary.BigEndian.PutUint16(buf[offset:], uint16(len(i)))
				offset += 2
				copy(buf[offset:], i)
				offset += len(i)
			}
		}
	}

	return buf
}

// DecodeBinaryClusterMetadataResponse decodes a cluster metadata response.
func DecodeBinaryClusterMetadataResponse(data []byte) (*BinaryClusterMetadataResponse, error) {
	if len(data) < 6 {
		return nil, errors.New("cluster metadata response too short")
	}

	offset := 0

	// Cluster ID
	clusterIDLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+clusterIDLen > len(data) {
		return nil, errors.New("invalid cluster ID length")
	}
	clusterID := string(data[offset : offset+clusterIDLen])
	offset += clusterIDLen

	// Topic count
	if offset+4 > len(data) {
		return nil, errors.New("missing topic count")
	}
	topicCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	topics := make([]TopicMetadata, 0, topicCount)
	for i := 0; i < topicCount; i++ {
		// Topic name
		if offset+2 > len(data) {
			return nil, errors.New("missing topic name length")
		}
		topicLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+topicLen > len(data) {
			return nil, errors.New("invalid topic name length")
		}
		topicName := string(data[offset : offset+topicLen])
		offset += topicLen

		// Partition count
		if offset+4 > len(data) {
			return nil, errors.New("missing partition count")
		}
		partitionCount := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		partitions := make([]PartitionMetadata, 0, partitionCount)
		for j := 0; j < partitionCount; j++ {
			if offset+4 > len(data) {
				return nil, errors.New("missing partition number")
			}
			partition := int32(binary.BigEndian.Uint32(data[offset:]))
			offset += 4

			// Leader ID
			if offset+2 > len(data) {
				return nil, errors.New("missing leader ID length")
			}
			leaderIDLen := int(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if offset+leaderIDLen > len(data) {
				return nil, errors.New("invalid leader ID length")
			}
			leaderID := string(data[offset : offset+leaderIDLen])
			offset += leaderIDLen

			// Leader address
			if offset+2 > len(data) {
				return nil, errors.New("missing leader address length")
			}
			leaderAddrLen := int(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if offset+leaderAddrLen > len(data) {
				return nil, errors.New("invalid leader address length")
			}
			leaderAddr := string(data[offset : offset+leaderAddrLen])
			offset += leaderAddrLen

			// Epoch
			if offset+8 > len(data) {
				return nil, errors.New("missing epoch")
			}
			epoch := binary.BigEndian.Uint64(data[offset:])
			offset += 8

			// State
			if offset+2 > len(data) {
				return nil, errors.New("missing state length")
			}
			stateLen := int(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if offset+stateLen > len(data) {
				return nil, errors.New("invalid state length")
			}
			state := string(data[offset : offset+stateLen])
			offset += stateLen

			// Replicas
			if offset+4 > len(data) {
				return nil, errors.New("missing replica count")
			}
			replicaCount := int(binary.BigEndian.Uint32(data[offset:]))
			offset += 4
			replicas := make([]string, 0, replicaCount)
			for k := 0; k < replicaCount; k++ {
				if offset+2 > len(data) {
					return nil, errors.New("missing replica ID length")
				}
				replicaLen := int(binary.BigEndian.Uint16(data[offset:]))
				offset += 2
				if offset+replicaLen > len(data) {
					return nil, errors.New("invalid replica ID length")
				}
				replicas = append(replicas, string(data[offset:offset+replicaLen]))
				offset += replicaLen
			}

			// ISR
			if offset+4 > len(data) {
				return nil, errors.New("missing ISR count")
			}
			isrCount := int(binary.BigEndian.Uint32(data[offset:]))
			offset += 4
			isr := make([]string, 0, isrCount)
			for k := 0; k < isrCount; k++ {
				if offset+2 > len(data) {
					return nil, errors.New("missing ISR ID length")
				}
				isrLen := int(binary.BigEndian.Uint16(data[offset:]))
				offset += 2
				if offset+isrLen > len(data) {
					return nil, errors.New("invalid ISR ID length")
				}
				isr = append(isr, string(data[offset:offset+isrLen]))
				offset += isrLen
			}

			partitions = append(partitions, PartitionMetadata{
				Partition:  partition,
				LeaderID:   leaderID,
				LeaderAddr: leaderAddr,
				Epoch:      epoch,
				State:      state,
				Replicas:   replicas,
				ISR:        isr,
			})
		}

		topics = append(topics, TopicMetadata{
			Topic:      topicName,
			Partitions: partitions,
		})
	}

	return &BinaryClusterMetadataResponse{
		ClusterID: clusterID,
		Topics:    topics,
	}, nil
}

// ============================================================================
// Audit Trail Operations
// ============================================================================

// BinaryAuditQueryRequest represents a request to query audit events.
// Format: [8B start_time][8B end_time][4B type_count][types...][2B user_len][user]
//         [2B resource_len][resource][2B result_len][result][2B search_len][search]
//         [4B limit][4B offset]
type BinaryAuditQueryRequest struct {
	StartTime  int64    // Unix timestamp (0 = no filter)
	EndTime    int64    // Unix timestamp (0 = no filter)
	EventTypes []string // Event types to filter
	User       string   // Username filter
	Resource   string   // Resource filter
	Result     string   // Result filter (success, failure, denied)
	Search     string   // Full-text search
	Limit      int32    // Max events to return
	Offset     int32    // Pagination offset
}

// BinaryAuditEvent represents a single audit event in binary format.
type BinaryAuditEvent struct {
	ID        string            // Event ID
	Timestamp int64             // Unix timestamp (milliseconds)
	Type      string            // Event type
	User      string            // Username
	ClientIP  string            // Client IP
	Resource  string            // Resource affected
	Action    string            // Action performed
	Result    string            // Result (success, failure, denied)
	Details   map[string]string // Additional details
	NodeID    string            // Cluster node ID
}

// BinaryAuditQueryResponse represents the response to an audit query.
type BinaryAuditQueryResponse struct {
	Events     []BinaryAuditEvent
	TotalCount int32
	HasMore    bool
}

// BinaryAuditExportRequest represents a request to export audit events.
// Format: [query_request][2B format_len][format]
type BinaryAuditExportRequest struct {
	Query  BinaryAuditQueryRequest
	Format string // "json" or "csv"
}

// BinaryAuditExportResponse represents the response to an audit export.
type BinaryAuditExportResponse struct {
	Format string // Format of the data
	Data   []byte // Exported data
}

// EncodeBinaryAuditQueryRequest encodes an audit query request.
func EncodeBinaryAuditQueryRequest(req *BinaryAuditQueryRequest) []byte {
	// Calculate size
	size := 8 + 8 + 4 // start_time + end_time + type_count
	for _, t := range req.EventTypes {
		size += 2 + len(t)
	}
	size += 2 + len(req.User)
	size += 2 + len(req.Resource)
	size += 2 + len(req.Result)
	size += 2 + len(req.Search)
	size += 4 + 4 // limit + offset

	buf := make([]byte, size)
	offset := 0

	// Start time
	binary.BigEndian.PutUint64(buf[offset:], uint64(req.StartTime))
	offset += 8

	// End time
	binary.BigEndian.PutUint64(buf[offset:], uint64(req.EndTime))
	offset += 8

	// Event types
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(req.EventTypes)))
	offset += 4
	for _, t := range req.EventTypes {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(t)))
		offset += 2
		copy(buf[offset:], t)
		offset += len(t)
	}

	// User
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(req.User)))
	offset += 2
	copy(buf[offset:], req.User)
	offset += len(req.User)

	// Resource
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(req.Resource)))
	offset += 2
	copy(buf[offset:], req.Resource)
	offset += len(req.Resource)

	// Result
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(req.Result)))
	offset += 2
	copy(buf[offset:], req.Result)
	offset += len(req.Result)

	// Search
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(req.Search)))
	offset += 2
	copy(buf[offset:], req.Search)
	offset += len(req.Search)

	// Limit
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Limit))
	offset += 4

	// Offset
	binary.BigEndian.PutUint32(buf[offset:], uint32(req.Offset))

	return buf
}

// DecodeBinaryAuditQueryRequest decodes an audit query request.
func DecodeBinaryAuditQueryRequest(data []byte) (*BinaryAuditQueryRequest, error) {
	if len(data) < 28 { // minimum size
		return nil, errors.New("audit query request too short")
	}

	offset := 0
	req := &BinaryAuditQueryRequest{}

	// Start time
	req.StartTime = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// End time
	req.EndTime = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Event types
	typeCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	req.EventTypes = make([]string, 0, typeCount)
	for i := 0; i < typeCount; i++ {
		if offset+2 > len(data) {
			return nil, errors.New("invalid event type length")
		}
		typeLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+typeLen > len(data) {
			return nil, errors.New("invalid event type")
		}
		req.EventTypes = append(req.EventTypes, string(data[offset:offset+typeLen]))
		offset += typeLen
	}

	// User
	if offset+2 > len(data) {
		return nil, errors.New("missing user length")
	}
	userLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+userLen > len(data) {
		return nil, errors.New("invalid user")
	}
	req.User = string(data[offset : offset+userLen])
	offset += userLen

	// Resource
	if offset+2 > len(data) {
		return nil, errors.New("missing resource length")
	}
	resourceLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+resourceLen > len(data) {
		return nil, errors.New("invalid resource")
	}
	req.Resource = string(data[offset : offset+resourceLen])
	offset += resourceLen

	// Result
	if offset+2 > len(data) {
		return nil, errors.New("missing result length")
	}
	resultLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+resultLen > len(data) {
		return nil, errors.New("invalid result")
	}
	req.Result = string(data[offset : offset+resultLen])
	offset += resultLen

	// Search
	if offset+2 > len(data) {
		return nil, errors.New("missing search length")
	}
	searchLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+searchLen > len(data) {
		return nil, errors.New("invalid search")
	}
	req.Search = string(data[offset : offset+searchLen])
	offset += searchLen

	// Limit
	if offset+4 > len(data) {
		return nil, errors.New("missing limit")
	}
	req.Limit = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Offset
	if offset+4 > len(data) {
		return nil, errors.New("missing offset")
	}
	req.Offset = int32(binary.BigEndian.Uint32(data[offset:]))

	return req, nil
}



// EncodeBinaryAuditEvent encodes a single audit event.
func EncodeBinaryAuditEvent(event *BinaryAuditEvent) []byte {
	// Calculate size
	size := 2 + len(event.ID)
	size += 8 // timestamp
	size += 2 + len(event.Type)
	size += 2 + len(event.User)
	size += 2 + len(event.ClientIP)
	size += 2 + len(event.Resource)
	size += 2 + len(event.Action)
	size += 2 + len(event.Result)
	size += 2 + len(event.NodeID)
	size += 4 // details count
	for k, v := range event.Details {
		size += 2 + len(k) + 2 + len(v)
	}

	buf := make([]byte, size)
	offset := 0

	// ID
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(event.ID)))
	offset += 2
	copy(buf[offset:], event.ID)
	offset += len(event.ID)

	// Timestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(event.Timestamp))
	offset += 8

	// Type
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(event.Type)))
	offset += 2
	copy(buf[offset:], event.Type)
	offset += len(event.Type)

	// User
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(event.User)))
	offset += 2
	copy(buf[offset:], event.User)
	offset += len(event.User)

	// ClientIP
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(event.ClientIP)))
	offset += 2
	copy(buf[offset:], event.ClientIP)
	offset += len(event.ClientIP)

	// Resource
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(event.Resource)))
	offset += 2
	copy(buf[offset:], event.Resource)
	offset += len(event.Resource)

	// Action
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(event.Action)))
	offset += 2
	copy(buf[offset:], event.Action)
	offset += len(event.Action)

	// Result
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(event.Result)))
	offset += 2
	copy(buf[offset:], event.Result)
	offset += len(event.Result)

	// NodeID
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(event.NodeID)))
	offset += 2
	copy(buf[offset:], event.NodeID)
	offset += len(event.NodeID)

	// Details
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(event.Details)))
	offset += 4
	for k, v := range event.Details {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(k)))
		offset += 2
		copy(buf[offset:], k)
		offset += len(k)
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(v)))
		offset += 2
		copy(buf[offset:], v)
		offset += len(v)
	}

	return buf
}

// EncodeBinaryAuditQueryResponse encodes an audit query response.
func EncodeBinaryAuditQueryResponse(resp *BinaryAuditQueryResponse) []byte {
	// First encode all events
	var eventData [][]byte
	totalEventSize := 0
	for _, event := range resp.Events {
		data := EncodeBinaryAuditEvent(&event)
		eventData = append(eventData, data)
		totalEventSize += 4 + len(data) // 4 bytes for length prefix
	}

	// Calculate total size: event_count(4) + events + total_count(4) + has_more(1)
	size := 4 + totalEventSize + 4 + 1
	buf := make([]byte, size)
	offset := 0

	// Event count
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Events)))
	offset += 4

	// Events
	for _, data := range eventData {
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(data)))
		offset += 4
		copy(buf[offset:], data)
		offset += len(data)
	}

	// Total count
	binary.BigEndian.PutUint32(buf[offset:], uint32(resp.TotalCount))
	offset += 4

	// Has more
	if resp.HasMore {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}

	return buf
}

// DecodeBinaryAuditEvent decodes a single audit event.
func DecodeBinaryAuditEvent(data []byte) (*BinaryAuditEvent, int, error) {
	if len(data) < 2 {
		return nil, 0, errors.New("audit event too short")
	}

	offset := 0
	event := &BinaryAuditEvent{}

	// ID
	idLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+idLen > len(data) {
		return nil, 0, errors.New("invalid event ID")
	}
	event.ID = string(data[offset : offset+idLen])
	offset += idLen

	// Timestamp
	if offset+8 > len(data) {
		return nil, 0, errors.New("missing timestamp")
	}
	event.Timestamp = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Type
	if offset+2 > len(data) {
		return nil, 0, errors.New("missing type length")
	}
	typeLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+typeLen > len(data) {
		return nil, 0, errors.New("invalid type")
	}
	event.Type = string(data[offset : offset+typeLen])
	offset += typeLen

	// User
	if offset+2 > len(data) {
		return nil, 0, errors.New("missing user length")
	}
	userLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+userLen > len(data) {
		return nil, 0, errors.New("invalid user")
	}
	event.User = string(data[offset : offset+userLen])
	offset += userLen

	// ClientIP
	if offset+2 > len(data) {
		return nil, 0, errors.New("missing client IP length")
	}
	clientIPLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+clientIPLen > len(data) {
		return nil, 0, errors.New("invalid client IP")
	}
	event.ClientIP = string(data[offset : offset+clientIPLen])
	offset += clientIPLen

	// Resource
	if offset+2 > len(data) {
		return nil, 0, errors.New("missing resource length")
	}
	resourceLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+resourceLen > len(data) {
		return nil, 0, errors.New("invalid resource")
	}
	event.Resource = string(data[offset : offset+resourceLen])
	offset += resourceLen

	// Action
	if offset+2 > len(data) {
		return nil, 0, errors.New("missing action length")
	}
	actionLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+actionLen > len(data) {
		return nil, 0, errors.New("invalid action")
	}
	event.Action = string(data[offset : offset+actionLen])
	offset += actionLen

	// Result
	if offset+2 > len(data) {
		return nil, 0, errors.New("missing result length")
	}
	resultLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+resultLen > len(data) {
		return nil, 0, errors.New("invalid result")
	}
	event.Result = string(data[offset : offset+resultLen])
	offset += resultLen

	// NodeID
	if offset+2 > len(data) {
		return nil, 0, errors.New("missing node ID length")
	}
	nodeIDLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+nodeIDLen > len(data) {
		return nil, 0, errors.New("invalid node ID")
	}
	event.NodeID = string(data[offset : offset+nodeIDLen])
	offset += nodeIDLen

	// Details
	if offset+4 > len(data) {
		return nil, 0, errors.New("missing details count")
	}
	detailsCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	event.Details = make(map[string]string, detailsCount)
	for i := 0; i < detailsCount; i++ {
		if offset+2 > len(data) {
			return nil, 0, errors.New("missing detail key length")
		}
		keyLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+keyLen > len(data) {
			return nil, 0, errors.New("invalid detail key")
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		if offset+2 > len(data) {
			return nil, 0, errors.New("missing detail value length")
		}
		valLen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+valLen > len(data) {
			return nil, 0, errors.New("invalid detail value")
		}
		event.Details[key] = string(data[offset : offset+valLen])
		offset += valLen
	}

	return event, offset, nil
}

// DecodeBinaryAuditQueryResponse decodes an audit query response.
func DecodeBinaryAuditQueryResponse(data []byte) (*BinaryAuditQueryResponse, error) {
	if len(data) < 9 { // event_count(4) + total_count(4) + has_more(1)
		return nil, errors.New("audit query response too short")
	}

	offset := 0
	resp := &BinaryAuditQueryResponse{}

	// Event count
	eventCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Events
	resp.Events = make([]BinaryAuditEvent, 0, eventCount)
	for i := 0; i < eventCount; i++ {
		if offset+4 > len(data) {
			return nil, errors.New("missing event length")
		}
		eventLen := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if offset+eventLen > len(data) {
			return nil, errors.New("invalid event data")
		}
		event, _, err := DecodeBinaryAuditEvent(data[offset : offset+eventLen])
		if err != nil {
			return nil, fmt.Errorf("failed to decode event: %w", err)
		}
		resp.Events = append(resp.Events, *event)
		offset += eventLen
	}

	// Total count
	if offset+4 > len(data) {
		return nil, errors.New("missing total count")
	}
	resp.TotalCount = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Has more
	if offset >= len(data) {
		return nil, errors.New("missing has_more flag")
	}
	resp.HasMore = data[offset] == 1

	return resp, nil
}

// EncodeBinaryAuditExportRequest encodes an audit export request.
func EncodeBinaryAuditExportRequest(req *BinaryAuditExportRequest) []byte {
	queryData := EncodeBinaryAuditQueryRequest(&req.Query)
	size := 4 + len(queryData) + 2 + len(req.Format)
	buf := make([]byte, size)
	offset := 0

	// Query data length
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(queryData)))
	offset += 4
	copy(buf[offset:], queryData)
	offset += len(queryData)

	// Format
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(req.Format)))
	offset += 2
	copy(buf[offset:], req.Format)

	return buf
}

// DecodeBinaryAuditExportRequest decodes an audit export request.
func DecodeBinaryAuditExportRequest(data []byte) (*BinaryAuditExportRequest, error) {
	if len(data) < 6 {
		return nil, errors.New("audit export request too short")
	}

	offset := 0
	req := &BinaryAuditExportRequest{}

	// Query data length
	queryLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+queryLen > len(data) {
		return nil, errors.New("invalid query data")
	}
	query, err := DecodeBinaryAuditQueryRequest(data[offset : offset+queryLen])
	if err != nil {
		return nil, fmt.Errorf("failed to decode query: %w", err)
	}
	req.Query = *query
	offset += queryLen

	// Format
	if offset+2 > len(data) {
		return nil, errors.New("missing format length")
	}
	formatLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+formatLen > len(data) {
		return nil, errors.New("invalid format")
	}
	req.Format = string(data[offset : offset+formatLen])

	return req, nil
}

// EncodeBinaryAuditExportResponse encodes an audit export response.
func EncodeBinaryAuditExportResponse(resp *BinaryAuditExportResponse) []byte {
	size := 2 + len(resp.Format) + 4 + len(resp.Data)
	buf := make([]byte, size)
	offset := 0

	// Format
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(resp.Format)))
	offset += 2
	copy(buf[offset:], resp.Format)
	offset += len(resp.Format)

	// Data
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(resp.Data)))
	offset += 4
	copy(buf[offset:], resp.Data)

	return buf
}

// DecodeBinaryAuditExportResponse decodes an audit export response.
func DecodeBinaryAuditExportResponse(data []byte) (*BinaryAuditExportResponse, error) {
	if len(data) < 6 {
		return nil, errors.New("audit export response too short")
	}

	offset := 0
	resp := &BinaryAuditExportResponse{}

	// Format
	formatLen := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+formatLen > len(data) {
		return nil, errors.New("invalid format")
	}
	resp.Format = string(data[offset : offset+formatLen])
	offset += formatLen

	// Data
	if offset+4 > len(data) {
		return nil, errors.New("missing data length")
	}
	dataLen := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if offset+dataLen > len(data) {
		return nil, errors.New("invalid data")
	}
	resp.Data = make([]byte, dataLen)
	copy(resp.Data, data[offset:offset+dataLen])

	return resp, nil
}
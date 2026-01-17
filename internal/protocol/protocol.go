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
Package protocol defines the FlyMQ binary wire protocol.

PROTOCOL OVERVIEW:
==================
FlyMQ uses a custom BINARY-ONLY protocol for client-server communication.
The protocol is designed to be:
- Simple: Easy to implement in any language
- Efficient: Minimal overhead, compact binary encoding
- Extensible: Opcodes and flags allow adding new features
- Safe: Magic bytes and version checking prevent protocol mismatches
- Fast: No JSON parsing overhead (~30% less overhead than JSON)

IMPORTANT: All payloads use binary encoding. There is NO JSON support.
See binary.go for the encoding/decoding implementations.

MESSAGE FORMAT:
===============
Every message consists of a fixed 8-byte header followed by a binary payload:

	+-------+-------+-------+-------+-------+-------+-------+-------+
	| Magic | Ver   | Op    | Flags | Length (4 bytes, big-endian) |
	+-------+-------+-------+-------+-------+-------+-------+-------+
	|                  Binary Payload (Length bytes)                |
	+---------------------------------------------------------------+

HEADER FIELDS:
==============
- Magic (1 byte): 0xAF - Identifies this as a FlyMQ message
- Version (1 byte): Protocol version (currently 0x01)
- Op (1 byte): Operation code (see OpCode constants)
- Flags (1 byte): 0x01=Binary (always set), 0x02=Compressed (future)
- Length (4 bytes): Payload length in bytes (big-endian)

Total header size: 8 bytes (HeaderSize constant)

OPCODE RANGES:
==============
- 0x01-0x0F: Core operations (produce, consume, topics)
- 0x10-0x1F: Schema operations
- 0x20-0x2F: Dead letter queue operations
- 0x30-0x3F: Delayed message and TTL operations
- 0x40-0x4F: Transaction operations
- 0x50-0x5F: Cluster operations
- 0x60-0x6F: Consumer group operations
- 0x70-0x7F: Authentication and user management operations
- 0xFF: Error response

BINARY PAYLOAD ENCODING:
========================
All payloads use length-prefixed binary encoding:

  Strings:     [uint16 length][UTF-8 bytes]
  Byte slices: [uint32 length][raw bytes]
  Integers:    Big-endian encoding (int32=4 bytes, int64/uint64=8 bytes)
  Arrays:      [uint32 count][elements...]
  Booleans:    Single byte (0x00=false, 0x01=true)

EXAMPLE: PRODUCE REQUEST (OpProduce=0x01)
=========================================
Produce "hello" to topic "test":

  Header (8 bytes):
    AF 01 01 01 00 00 00 13
    │  │  │  │  └──────────── Length: 19 bytes
    │  │  │  └─────────────── Flags: 0x01 (binary)
    │  │  └────────────────── Op: 0x01 (produce)
    │  └───────────────────── Version: 0x01
    └──────────────────────── Magic: 0xAF

  Payload (19 bytes):
    00 04 74 65 73 74           Topic: "test" (len=4)
    00 00 00 00                 Key: empty (len=0)
    00 00 00 05 68 65 6C 6C 6F  Value: "hello" (len=5)
    FF FF FF FF                 Partition: -1 (auto)

VERSIONING:
===========
The protocol version allows for future incompatible changes. Clients and
servers MUST reject messages with unknown versions or invalid magic bytes.

IMPLEMENTING A CLIENT:
======================
1. Open TCP connection (optionally with TLS)
2. For each request:
   a. Encode payload using binary format (see binary.go)
   b. Write 8-byte header with opcode, flags=0x01, and length
   c. Write binary payload
   d. Read 8-byte response header
   e. Read binary response payload
   f. Decode binary response

See docs/protocol.md for complete protocol specification.
See binary.go for BinaryXxxRequest/Response structs and EncodeXxx/DecodeXxx functions.
*/
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

// Protocol constants define the wire format parameters.
const (
	// MagicByte identifies FlyMQ protocol messages.
	// 0xAF was chosen as a distinctive value unlikely to appear in random data.
	// This helps detect protocol mismatches and connection issues.
	MagicByte byte = 0xAF

	// ProtocolVersion is the current protocol version.
	// Increment this when making incompatible protocol changes.
	ProtocolVersion byte = 0x01

	// MaxMessageSize limits payload size to prevent memory exhaustion.
	// 32MB is generous for most use cases while preventing abuse.
	MaxMessageSize = 32 * 1024 * 1024 // 32MB

	// HeaderSize is the fixed size of the message header in bytes.
	HeaderSize = 8
)

// OpCode represents the operation type in a protocol message.
// Each operation has a unique code that tells the server what action to perform.
type OpCode byte

// Operation codes organized by category.
// Each range is reserved for a specific feature area.
const (
	// ========== Core Operations (0x01-0x0F) ==========
	// These are the fundamental message queue operations.

	OpProduce        OpCode = 0x01 // Write a message to a topic
	OpConsume        OpCode = 0x02 // Read a single message from a topic
	OpCreateTopic    OpCode = 0x03 // Create a new topic
	OpMetadata       OpCode = 0x04 // Get topic metadata
	OpSubscribe      OpCode = 0x05 // Subscribe to a topic with consumer group
	OpCommit         OpCode = 0x06 // Commit consumer offset (checkpoint progress)
	OpFetch          OpCode = 0x07 // Fetch multiple messages (batch consume)
	OpListTopics     OpCode = 0x08 // List all topics
	OpDeleteTopic    OpCode = 0x09 // Delete a topic and its data
	OpConsumeZeroCopy OpCode = 0x0A // Zero-copy consume using sendfile (large messages)
	OpFetchBinary    OpCode = 0x0B // Binary-encoded batch fetch (high performance)

	// ========== Schema Operations (0x10-0x1F) ==========
	// Schema registry for message validation.

	OpRegisterSchema    OpCode = 0x10 // Register a new schema (JSON, Avro, Protobuf)
	OpGetSchema         OpCode = 0x11 // Get schema by ID or version
	OpListSchemas       OpCode = 0x12 // List schemas for a topic
	OpValidateMessage   OpCode = 0x13 // Validate message against schema
	OpValidateSchema    OpCode = 0x13 // Alias for OpValidateMessage
	OpProduceWithSchema OpCode = 0x14 // Produce with automatic schema validation
	OpDeleteSchema      OpCode = 0x15 // Delete a schema version

	// ========== Dead Letter Queue Operations (0x20-0x2F) ==========
	// DLQ handles messages that failed processing.

	OpGetDLQMessages OpCode = 0x20 // Get messages from DLQ for inspection
	OpFetchDLQ       OpCode = 0x20 // Alias for OpGetDLQMessages
	OpReplayDLQ      OpCode = 0x21 // Replay a DLQ message back to the main topic
	OpPurgeDLQ       OpCode = 0x22 // Delete all messages from DLQ

	// ========== Delayed Message Operations (0x30-0x3F) ==========
	// Support for scheduled/delayed message delivery.

	OpProduceDelayed OpCode = 0x30 // Produce a message with delayed delivery
	OpCancelDelayed  OpCode = 0x31 // Cancel a pending delayed message

	// ========== TTL Operations (0x35-0x3F) ==========
	// Time-to-live for automatic message expiration.

	OpProduceWithTTL OpCode = 0x35 // Produce with automatic expiration

	// ========== Transaction Operations (0x40-0x4F) ==========
	// ACID transactions for atomic multi-message operations.

	OpBeginTx     OpCode = 0x40 // Begin a new transaction
	OpTxnBegin    OpCode = 0x40 // Alias for OpBeginTx
	OpCommitTx    OpCode = 0x41 // Commit transaction (make changes permanent)
	OpTxnCommit   OpCode = 0x41 // Alias for OpCommitTx
	OpAbortTx     OpCode = 0x42 // Abort transaction (rollback changes)
	OpTxnRollback OpCode = 0x42 // Alias for OpAbortTx
	OpProduceTx   OpCode = 0x43 // Produce within a transaction
	OpTxnProduce  OpCode = 0x43 // Alias for OpProduceTx

	// ========== Cluster Operations (0x50-0x5F) ==========
	// Distributed cluster management.

	OpClusterJoin     OpCode = 0x50 // Join a cluster as a new node
	OpClusterLeave    OpCode = 0x51 // Leave the cluster gracefully
	OpClusterStatus   OpCode = 0x52 // Get cluster membership and health
	OpClusterMetadata OpCode = 0x53 // Get partition-to-node mappings for smart routing

	// ========== Consumer Group Operations (0x60-0x6F) ==========
	// Consumer group management and offset operations.

	OpGetOffset       OpCode = 0x60 // Get committed offset for a consumer group
	OpResetOffset     OpCode = 0x61 // Reset consumer group offset to a specific position
	OpListGroups      OpCode = 0x62 // List all consumer groups
	OpDescribeGroup   OpCode = 0x63 // Get detailed info about a consumer group
	OpGetLag          OpCode = 0x64 // Get consumer lag (difference between latest and committed offset)
	OpDeleteGroup     OpCode = 0x65 // Delete a consumer group
	OpSeekToTimestamp OpCode = 0x66 // Seek to offset by timestamp

	// ========== Authentication Operations (0x70-0x7F) ==========
	// Authentication and authorization operations.

	OpAuth         OpCode = 0x70 // Authenticate with username/password
	OpAuthResponse OpCode = 0x71 // Authentication response
	OpWhoAmI       OpCode = 0x72 // Get current authenticated user info

	// ========== User Management Operations (0x73-0x7F) ==========
	// Admin operations for managing users, roles, and ACLs.

	OpUserCreate     OpCode = 0x73 // Create a new user
	OpUserDelete     OpCode = 0x74 // Delete a user
	OpUserUpdate     OpCode = 0x75 // Update user (roles, enabled status)
	OpUserList       OpCode = 0x76 // List all users
	OpUserGet        OpCode = 0x77 // Get user details
	OpACLSet         OpCode = 0x78 // Set topic ACL
	OpACLGet         OpCode = 0x79 // Get topic ACL
	OpACLDelete      OpCode = 0x7A // Delete topic ACL
	OpACLList        OpCode = 0x7B // List all ACLs
	OpPasswordChange OpCode = 0x7C // Change user password
	OpRoleList       OpCode = 0x7D // List all available roles

	// ========== Error Response (0xFF) ==========
	OpError OpCode = 0xFF // Error response from server
)

// SubscribeMode defines where a consumer should start reading messages.
// This is specified when subscribing to a topic.
type SubscribeMode string

const (
	// SubscribeFromEarliest starts from the oldest available message.
	// Use this to process all historical messages.
	SubscribeFromEarliest SubscribeMode = "earliest"

	// SubscribeFromLatest starts from new messages only.
	// Use this when you only care about future messages.
	SubscribeFromLatest SubscribeMode = "latest"

	// SubscribeFromCommit resumes from the last committed offset.
	// Use this for reliable processing with at-least-once semantics.
	SubscribeFromCommit SubscribeMode = "commit"
)

// Header is the fixed-size header that precedes every message.
// It contains metadata needed to parse and route the message.
type Header struct {
	Magic   byte   // Must be MagicByte (0xAF)
	Version byte   // Protocol version (currently 0x01)
	Op      OpCode // Operation code
	Flags   byte   // Reserved for future use (compression, etc.)
	Length  uint32 // Payload length in bytes
}

// Message represents a complete protocol message (header + payload).
type Message struct {
	Header  Header // Fixed-size header
	Payload []byte // Variable-length binary payload (see binary.go for encoding)
}

// Protocol errors returned during message parsing.
var (
	// ErrInvalidMagic indicates the magic byte doesn't match.
	// This usually means the connection is not speaking FlyMQ protocol.
	ErrInvalidMagic = errors.New("invalid magic byte")

	// ErrInvalidVersion indicates an unsupported protocol version.
	// The client and server may need to be upgraded to match.
	ErrInvalidVersion = errors.New("invalid protocol version")

	// ErrMessageTooLarge indicates the payload exceeds MaxMessageSize.
	// This protects against memory exhaustion attacks.
	ErrMessageTooLarge = errors.New("message too large")
)

// ReadHeader reads and validates a message header from the reader.
//
// VALIDATION:
// - Magic byte must match MagicByte (0xAF)
// - Version must match ProtocolVersion (0x01)
// - Length must not exceed MaxMessageSize (32MB)
//
// RETURNS:
// - Parsed Header struct
// - ErrInvalidMagic if magic byte doesn't match
// - ErrInvalidVersion if version is unsupported
// - ErrMessageTooLarge if payload would be too large
// - io.EOF or other I/O errors
func ReadHeader(r io.Reader) (Header, error) {
	buf := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Header{}, err
	}

	// Parse header fields from buffer
	h := Header{
		Magic:   buf[0],
		Version: buf[1],
		Op:      OpCode(buf[2]),
		Flags:   buf[3],
		Length:  binary.BigEndian.Uint32(buf[4:]),
	}

	// Validate magic byte
	if h.Magic != MagicByte {
		return Header{}, ErrInvalidMagic
	}

	// Validate protocol version
	if h.Version != ProtocolVersion {
		return Header{}, ErrInvalidVersion
	}

	// Validate message size
	if h.Length > MaxMessageSize {
		return Header{}, ErrMessageTooLarge
	}

	return h, nil
}

// WriteHeader writes a message header to the writer.
// The header is serialized as 8 bytes in big-endian format.
func WriteHeader(w io.Writer, h Header) error {
	buf := make([]byte, HeaderSize)
	buf[0] = h.Magic
	buf[1] = h.Version
	buf[2] = byte(h.Op)
	buf[3] = h.Flags
	binary.BigEndian.PutUint32(buf[4:], h.Length)
	_, err := w.Write(buf)
	return err
}

// ReadMessage reads a complete message (header + payload) from the reader.
//
// PROCESS:
// 1. Read and validate the header
// 2. Allocate buffer for payload (if any)
// 3. Read the payload
//
// This is the primary function for receiving messages from clients.
func ReadMessage(r io.Reader) (*Message, error) {
	h, err := ReadHeader(r)
	if err != nil {
		return nil, err
	}

	msg := &Message{Header: h}

	// Read payload if present
	if h.Length > 0 {
		msg.Payload = make([]byte, h.Length)
		if _, err := io.ReadFull(r, msg.Payload); err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// WriteMessage writes a complete message (header + payload) to the writer.
//
// This is a convenience function that constructs the header automatically.
// The magic byte and version are set to the current protocol values.
//
// PARAMETERS:
// - w: Writer to send the message to
// - op: Operation code for this message
// - payload: Message payload (binary-encoded, see binary.go)
func WriteMessage(w io.Writer, op OpCode, payload []byte) error {
	h := Header{
		Magic:   MagicByte,
		Version: ProtocolVersion,
		Op:      op,
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

// WriteError sends an error response to the client.
// The error message is sent as the payload with OpError opcode.
func WriteError(w io.Writer, err error) error {
	return WriteMessage(w, OpError, []byte(err.Error()))
}

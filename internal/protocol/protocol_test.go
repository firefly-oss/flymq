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

package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestReadHeader(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    Header
		wantErr error
	}{
		{
			name: "valid header",
			input: []byte{
				MagicByte, ProtocolVersion, byte(OpProduce), 0x00,
				0x00, 0x00, 0x00, 0x10, // length = 16
			},
			want: Header{
				Magic:   MagicByte,
				Version: ProtocolVersion,
				Op:      OpProduce,
				Flags:   0,
				Length:  16,
			},
			wantErr: nil,
		},
		{
			name: "invalid magic byte",
			input: []byte{
				0x00, ProtocolVersion, byte(OpProduce), 0x00,
				0x00, 0x00, 0x00, 0x10,
			},
			wantErr: ErrInvalidMagic,
		},
		{
			name: "invalid version",
			input: []byte{
				MagicByte, 0xFF, byte(OpProduce), 0x00,
				0x00, 0x00, 0x00, 0x10,
			},
			wantErr: ErrInvalidVersion,
		},
		{
			name: "message too large",
			input: []byte{
				MagicByte, ProtocolVersion, byte(OpProduce), 0x00,
				0x10, 0x00, 0x00, 0x00, // 256MB > MaxMessageSize
			},
			wantErr: ErrMessageTooLarge,
		},
		{
			name:    "short read",
			input:   []byte{MagicByte, ProtocolVersion},
			wantErr: io.ErrUnexpectedEOF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader(tt.input)
			got, err := ReadHeader(r)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("ReadHeader() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Errorf("ReadHeader() unexpected error: %v", err)
				return
			}

			if got != tt.want {
				t.Errorf("ReadHeader() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestWriteHeader(t *testing.T) {
	h := Header{
		Magic:   MagicByte,
		Version: ProtocolVersion,
		Op:      OpConsume,
		Flags:   0x01,
		Length:  100,
	}

	var buf bytes.Buffer
	err := WriteHeader(&buf, h)
	if err != nil {
		t.Fatalf("WriteHeader() error: %v", err)
	}

	if buf.Len() != HeaderSize {
		t.Errorf("Expected %d bytes, got %d", HeaderSize, buf.Len())
	}

	data := buf.Bytes()
	if data[0] != MagicByte {
		t.Errorf("Magic byte mismatch")
	}
	if data[1] != ProtocolVersion {
		t.Errorf("Version mismatch")
	}
	if OpCode(data[2]) != OpConsume {
		t.Errorf("OpCode mismatch")
	}
	if data[3] != 0x01 {
		t.Errorf("Flags mismatch")
	}
	length := binary.BigEndian.Uint32(data[4:])
	if length != 100 {
		t.Errorf("Length mismatch: got %d, want 100", length)
	}
}

func TestReadMessage(t *testing.T) {
	payload := []byte(`{"topic":"test","data":"hello"}`)

	// Build a valid message
	var buf bytes.Buffer
	header := []byte{
		MagicByte, ProtocolVersion, byte(OpProduce), 0x00,
	}
	buf.Write(header)
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(payload)))
	buf.Write(lenBytes)
	buf.Write(payload)

	msg, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage() error: %v", err)
	}

	if msg.Header.Op != OpProduce {
		t.Errorf("Expected OpProduce, got %v", msg.Header.Op)
	}
	if !bytes.Equal(msg.Payload, payload) {
		t.Errorf("Payload mismatch")
	}
}

func TestReadMessageEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	header := []byte{
		MagicByte, ProtocolVersion, byte(OpMetadata), 0x00,
		0x00, 0x00, 0x00, 0x00, // length = 0
	}
	buf.Write(header)

	msg, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage() error: %v", err)
	}

	if msg.Header.Length != 0 {
		t.Errorf("Expected length 0, got %d", msg.Header.Length)
	}
	if len(msg.Payload) != 0 {
		t.Errorf("Expected empty payload, got %d bytes", len(msg.Payload))
	}
}

func TestWriteMessage(t *testing.T) {
	payload := []byte(`{"topic":"test"}`)
	var buf bytes.Buffer

	err := WriteMessage(&buf, OpCreateTopic, payload, 0)
	if err != nil {
		t.Fatalf("WriteMessage() error: %v", err)
	}

	// Read it back
	msg, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage() error: %v", err)
	}

	if msg.Header.Op != OpCreateTopic {
		t.Errorf("Expected OpCreateTopic, got %v", msg.Header.Op)
	}
	if !bytes.Equal(msg.Payload, payload) {
		t.Errorf("Payload mismatch")
	}
}

func TestWriteMessageEmptyPayload(t *testing.T) {
	var buf bytes.Buffer

	err := WriteMessage(&buf, OpListTopics, nil, 0)
	if err != nil {
		t.Fatalf("WriteMessage() error: %v", err)
	}

	msg, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage() error: %v", err)
	}

	if msg.Header.Length != 0 {
		t.Errorf("Expected length 0, got %d", msg.Header.Length)
	}
}

func TestWriteError(t *testing.T) {
	var buf bytes.Buffer
	testErr := errors.New("test error message")

	err := WriteError(&buf, testErr)
	if err != nil {
		t.Fatalf("WriteError() error: %v", err)
	}

	msg, err := ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage() error: %v", err)
	}

	if msg.Header.Op != OpError {
		t.Errorf("Expected OpError, got %v", msg.Header.Op)
	}
	if string(msg.Payload) != testErr.Error() {
		t.Errorf("Expected error message %q, got %q", testErr.Error(), string(msg.Payload))
	}
}

func TestOpCodeConstants(t *testing.T) {
	// Verify opcode ranges are correct
	tests := []struct {
		name string
		op   OpCode
		min  byte
		max  byte
	}{
		{"OpProduce", OpProduce, 0x01, 0x0F},
		{"OpConsume", OpConsume, 0x01, 0x0F},
		{"OpCreateTopic", OpCreateTopic, 0x01, 0x0F},
		{"OpRegisterSchema", OpRegisterSchema, 0x10, 0x1F},
		{"OpGetDLQMessages", OpGetDLQMessages, 0x20, 0x2F},
		{"OpProduceDelayed", OpProduceDelayed, 0x30, 0x3F},
		{"OpBeginTx", OpBeginTx, 0x40, 0x4F},
		{"OpClusterJoin", OpClusterJoin, 0x50, 0x5F},
		{"OpError", OpError, 0xFF, 0xFF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if byte(tt.op) < tt.min || byte(tt.op) > tt.max {
				t.Errorf("%s (0x%02X) not in expected range [0x%02X, 0x%02X]",
					tt.name, byte(tt.op), tt.min, tt.max)
			}
		})
	}
}

func TestSubscribeModeConstants(t *testing.T) {
	if SubscribeFromEarliest != "earliest" {
		t.Errorf("SubscribeFromEarliest = %q, want %q", SubscribeFromEarliest, "earliest")
	}
	if SubscribeFromLatest != "latest" {
		t.Errorf("SubscribeFromLatest = %q, want %q", SubscribeFromLatest, "latest")
	}
	if SubscribeFromCommit != "commit" {
		t.Errorf("SubscribeFromCommit = %q, want %q", SubscribeFromCommit, "commit")
	}
}

func TestRoundTrip(t *testing.T) {
	// Test multiple messages in sequence
	messages := []struct {
		op      OpCode
		payload []byte
	}{
		{OpProduce, []byte(`{"topic":"test1"}`)},
		{OpConsume, []byte(`{"topic":"test2","offset":0}`)},
		{OpMetadata, nil},
		{OpCreateTopic, []byte(`{"name":"newtopic","partitions":3}`)},
	}

	var buf bytes.Buffer

	// Write all messages
	for _, m := range messages {
		if err := WriteMessage(&buf, m.op, m.payload, 0); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}
	}

	// Read all messages back
	for i, m := range messages {
		msg, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("ReadMessage failed for message %d: %v", i, err)
		}
		if msg.Header.Op != m.op {
			t.Errorf("Message %d: expected op %v, got %v", i, m.op, msg.Header.Op)
		}
		if !bytes.Equal(msg.Payload, m.payload) {
			t.Errorf("Message %d: payload mismatch", i)
		}
	}
}

// ============================================================================
// Cluster Metadata Protocol Tests
// ============================================================================

func TestClusterMetadataRequestEncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		topic string
	}{
		{"empty topic", ""},
		{"specific topic", "my-topic"},
		{"topic with special chars", "my-topic-123_test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := BinaryClusterMetadataRequest{Topic: tt.topic}
			encoded := EncodeBinaryClusterMetadataRequest(&req)

			decoded, err := DecodeBinaryClusterMetadataRequest(encoded)
			if err != nil {
				t.Fatalf("Failed to decode: %v", err)
			}

			if decoded.Topic != tt.topic {
				t.Errorf("Topic = %s, want %s", decoded.Topic, tt.topic)
			}
		})
	}
}

func TestClusterMetadataResponseEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		resp BinaryClusterMetadataResponse
	}{
		{
			name: "empty response",
			resp: BinaryClusterMetadataResponse{
				ClusterID: "cluster-1",
				Topics:    []TopicMetadata{},
			},
		},
		{
			name: "single topic single partition",
			resp: BinaryClusterMetadataResponse{
				ClusterID: "cluster-1",
				Topics: []TopicMetadata{
					{
						Topic: "topic1",
						Partitions: []PartitionMetadata{
							{
								Partition:  0,
								LeaderID:   "node-1",
								LeaderAddr: "localhost:9092",
								Epoch:      1,
							},
						},
					},
				},
			},
		},
		{
			name: "multiple topics multiple partitions",
			resp: BinaryClusterMetadataResponse{
				ClusterID: "cluster-abc",
				Topics: []TopicMetadata{
					{
						Topic: "topic1",
						Partitions: []PartitionMetadata{
							{Partition: 0, LeaderID: "node-1", LeaderAddr: "host1:9092", Epoch: 1},
							{Partition: 1, LeaderID: "node-2", LeaderAddr: "host2:9092", Epoch: 2},
							{Partition: 2, LeaderID: "node-3", LeaderAddr: "host3:9092", Epoch: 1},
						},
					},
					{
						Topic: "topic2",
						Partitions: []PartitionMetadata{
							{Partition: 0, LeaderID: "node-2", LeaderAddr: "host2:9092", Epoch: 5},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeBinaryClusterMetadataResponse(&tt.resp)

			decoded, err := DecodeBinaryClusterMetadataResponse(encoded)
			if err != nil {
				t.Fatalf("Failed to decode: %v", err)
			}

			if decoded.ClusterID != tt.resp.ClusterID {
				t.Errorf("ClusterID = %s, want %s", decoded.ClusterID, tt.resp.ClusterID)
			}

			if len(decoded.Topics) != len(tt.resp.Topics) {
				t.Fatalf("Topics count = %d, want %d", len(decoded.Topics), len(tt.resp.Topics))
			}

			for i, topic := range decoded.Topics {
				expected := tt.resp.Topics[i]
				if topic.Topic != expected.Topic {
					t.Errorf("Topic[%d].Topic = %s, want %s", i, topic.Topic, expected.Topic)
				}

				if len(topic.Partitions) != len(expected.Partitions) {
					t.Fatalf("Topic[%d].Partitions count = %d, want %d", i, len(topic.Partitions), len(expected.Partitions))
				}

				for j, part := range topic.Partitions {
					exp := expected.Partitions[j]
					if part.Partition != exp.Partition {
						t.Errorf("Partition = %d, want %d", part.Partition, exp.Partition)
					}
					if part.LeaderID != exp.LeaderID {
						t.Errorf("LeaderID = %s, want %s", part.LeaderID, exp.LeaderID)
					}
					if part.LeaderAddr != exp.LeaderAddr {
						t.Errorf("LeaderAddr = %s, want %s", part.LeaderAddr, exp.LeaderAddr)
					}
					if part.Epoch != exp.Epoch {
						t.Errorf("Epoch = %d, want %d", part.Epoch, exp.Epoch)
					}
				}
			}
		})
	}
}

# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
FlyMQ Binary Protocol Encoding/Decoding.

This module provides binary encode/decode functions for high-performance
communication with FlyMQ servers. Binary protocol eliminates JSON/base64
overhead for maximum throughput.

Binary Format Conventions:
- All multi-byte integers are big-endian
- Strings are length-prefixed: [2 bytes len][N bytes UTF-8]
- Byte arrays are length-prefixed: [4 bytes len][N bytes data]
- Booleans are 1 byte (0x00 = False, 0x01 = True)
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


# Flag constant for binary payload
FLAG_BINARY: int = 0x01


# =============================================================================
# Produce Request/Response
# =============================================================================

@dataclass
class BinaryProduceRequest:
    """Binary produce request."""
    topic: str
    key: bytes
    value: bytes
    partition: int = -1  # -1 = auto


def encode_produce_request(req: BinaryProduceRequest) -> bytes:
    """
    Encode produce request to binary.
    
    Format:
        [2B topic_len][topic][4B key_len][key][4B value_len][value][4B partition]
    """
    topic_bytes = req.topic.encode("utf-8")
    parts = [
        struct.pack(">H", len(topic_bytes)),
        topic_bytes,
        struct.pack(">I", len(req.key)),
        req.key,
        struct.pack(">I", len(req.value)),
        req.value,
        struct.pack(">i", req.partition),
    ]
    return b"".join(parts)


@dataclass
class RecordMetadata:
    """
    Metadata for a produced record (Kafka-like).

    Similar to Kafka's RecordMetadata, this contains all information about
    where and when the message was stored.

    Attributes:
        topic: Topic name
        partition: Partition the record was sent to
        offset: Offset of the record in the partition
        timestamp: Timestamp in milliseconds (Unix epoch)
        key_size: Size of the key in bytes (-1 if no key)
        value_size: Size of the value in bytes
    """
    topic: str
    partition: int
    offset: int
    timestamp: int  # milliseconds since epoch
    key_size: int   # -1 if no key
    value_size: int

    @property
    def timestamp_datetime(self):
        """Get timestamp as datetime object."""
        from datetime import datetime
        return datetime.fromtimestamp(self.timestamp / 1000.0)


def decode_record_metadata(data: bytes) -> RecordMetadata:
    """
    Decode RecordMetadata from binary format.

    Format: [2B topic_len][topic][4B partition][8B offset][8B timestamp][4B key_size][4B value_size]
    """
    if len(data) < 2:
        raise ValueError(f"Buffer too small: {len(data)} < 2")

    pos = 0

    # Topic
    topic_len = struct.unpack(">H", data[pos:pos+2])[0]
    pos += 2

    if len(data) < pos + topic_len + 28:  # 4+8+8+4+4 = 28
        raise ValueError(f"Buffer too small for RecordMetadata")

    topic = data[pos:pos+topic_len].decode("utf-8")
    pos += topic_len

    # Partition
    partition = struct.unpack(">i", data[pos:pos+4])[0]
    pos += 4

    # Offset
    offset = struct.unpack(">Q", data[pos:pos+8])[0]
    pos += 8

    # Timestamp
    timestamp = struct.unpack(">q", data[pos:pos+8])[0]
    pos += 8

    # Key size
    key_size = struct.unpack(">i", data[pos:pos+4])[0]
    pos += 4

    # Value size
    value_size = struct.unpack(">i", data[pos:pos+4])[0]

    return RecordMetadata(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        key_size=key_size,
        value_size=value_size,
    )


# Legacy alias for backward compatibility during transition
BinaryProduceResponse = RecordMetadata


def decode_produce_response(data: bytes) -> RecordMetadata:
    """
    Decode produce response (now returns RecordMetadata).

    Format: [2B topic_len][topic][4B partition][8B offset][8B timestamp][4B key_size][4B value_size]
    """
    return decode_record_metadata(data)


# =============================================================================
# Consume Request/Response
# =============================================================================

@dataclass
class BinaryConsumeRequest:
    """Binary consume request."""
    topic: str
    partition: int
    offset: int


def encode_consume_request(req: BinaryConsumeRequest) -> bytes:
    """
    Encode consume request to binary.
    
    Format: [2B topic_len][topic][4B partition][8B offset]
    """
    topic_bytes = req.topic.encode("utf-8")
    parts = [
        struct.pack(">H", len(topic_bytes)),
        topic_bytes,
        struct.pack(">i", req.partition),
        struct.pack(">Q", req.offset),
    ]
    return b"".join(parts)


@dataclass
class BinaryConsumeResponse:
    """Binary consume response."""
    key: bytes
    value: bytes


def decode_consume_response(data: bytes) -> BinaryConsumeResponse:
    """
    Decode binary consume response.
    
    Format: [4B key_len][key][4B value_len][value]
    """
    if len(data) < 8:
        raise ValueError(f"Buffer too small: {len(data)} < 8")
    
    offset = 0
    key_len = struct.unpack(">I", data[offset:offset+4])[0]
    offset += 4
    
    key = data[offset:offset+key_len] if key_len > 0 else b""
    offset += key_len
    
    if offset + 4 > len(data):
        raise ValueError("Invalid binary format: truncated value length")
    
    value_len = struct.unpack(">I", data[offset:offset+4])[0]
    offset += 4
    
    value = data[offset:offset+value_len] if value_len > 0 else b""
    
    return BinaryConsumeResponse(key=key, value=value)


# =============================================================================
# Fetch Request/Response
# =============================================================================

@dataclass
class BinaryFetchRequest:
    """Binary fetch request."""
    topic: str
    partition: int
    offset: int
    max_messages: int
    filter: str = ""  # Optional regex filter


def encode_fetch_request(req: BinaryFetchRequest) -> bytes:
    """
    Encode fetch request to binary.
    
    Format: [2B topic_len][topic][4B partition][8B offset][4B max_messages][2B filter_len][filter]
    """
    topic_bytes = req.topic.encode("utf-8")
    filter_bytes = req.filter.encode("utf-8") if req.filter else b""
    parts = [
        struct.pack(">H", len(topic_bytes)),
        topic_bytes,
        struct.pack(">i", req.partition),
        struct.pack(">Q", req.offset),
        struct.pack(">i", req.max_messages),
        struct.pack(">H", len(filter_bytes)),
        filter_bytes,
    ]
    return b"".join(parts)


@dataclass
class BinaryFetchMessage:
    """A single message in a fetch response."""
    offset: int
    key: bytes
    value: bytes


@dataclass
class BinaryFetchResponse:
    """Binary fetch response."""
    messages: list[BinaryFetchMessage]
    next_offset: int


def decode_fetch_response(data: bytes) -> BinaryFetchResponse:
    """
    Decode binary fetch response.
    
    Format: [4B count][8B next_offset][messages...]
    Each message: [8B offset][4B key_len][key][4B value_len][value]
    """
    if len(data) < 12:
        raise ValueError(f"Buffer too small: {len(data)} < 12")
    
    pos = 0
    msg_count = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    
    next_offset = struct.unpack(">Q", data[pos:pos+8])[0]
    pos += 8
    
    messages = []
    for _ in range(msg_count):
        if pos + 16 > len(data):
            raise ValueError("Invalid binary format: truncated message")
        
        msg_offset = struct.unpack(">Q", data[pos:pos+8])[0]
        pos += 8
        
        key_len = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4
        key = data[pos:pos+key_len] if key_len > 0 else b""
        pos += key_len
        
        if pos + 4 > len(data):
            raise ValueError("Invalid binary format: truncated value length")
        
        value_len = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4
        value = data[pos:pos+value_len] if value_len > 0 else b""
        pos += value_len
        
        messages.append(BinaryFetchMessage(offset=msg_offset, key=key, value=value))
    
    return BinaryFetchResponse(messages=messages, next_offset=next_offset)


# =============================================================================
# Create Topic Request/Response
# =============================================================================

@dataclass
class BinaryCreateTopicRequest:
    """Binary create topic request."""
    topic: str
    partitions: int


def encode_create_topic_request(req: BinaryCreateTopicRequest) -> bytes:
    """
    Encode create topic request to binary.
    
    Format: [2B topic_len][topic][4B partitions]
    """
    topic_bytes = req.topic.encode("utf-8")
    parts = [
        struct.pack(">H", len(topic_bytes)),
        topic_bytes,
        struct.pack(">i", req.partitions),
    ]
    return b"".join(parts)


@dataclass
class BinaryCreateTopicResponse:
    """Binary create topic response."""
    success: bool


def decode_create_topic_response(data: bytes) -> BinaryCreateTopicResponse:
    """Decode binary create topic response. Format: [1B success]"""
    if len(data) < 1:
        raise ValueError("Buffer too small")
    return BinaryCreateTopicResponse(success=data[0] == 1)


# =============================================================================
# Delete Topic Request/Response
# =============================================================================

@dataclass
class BinaryDeleteTopicRequest:
    """Binary delete topic request."""
    topic: str


def encode_delete_topic_request(req: BinaryDeleteTopicRequest) -> bytes:
    """Encode delete topic request. Format: [2B topic_len][topic]"""
    topic_bytes = req.topic.encode("utf-8")
    return struct.pack(">H", len(topic_bytes)) + topic_bytes


@dataclass
class BinaryDeleteTopicResponse:
    """Binary delete topic response."""
    success: bool


def decode_delete_topic_response(data: bytes) -> BinaryDeleteTopicResponse:
    """Decode binary delete topic response. Format: [1B success]"""
    if len(data) < 1:
        raise ValueError("Buffer too small")
    return BinaryDeleteTopicResponse(success=data[0] == 1)


# =============================================================================
# List Topics Response
# =============================================================================

@dataclass
class BinaryListTopicsResponse:
    """Binary list topics response."""
    topics: list[str]


def decode_list_topics_response(data: bytes) -> BinaryListTopicsResponse:
    """
    Decode binary list topics response.
    
    Format: [4B count][topics...] where each topic is [2B len][topic]
    """
    if len(data) < 4:
        raise ValueError("Buffer too small")
    
    pos = 0
    topic_count = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    
    topics = []
    for _ in range(topic_count):
        if pos + 2 > len(data):
            raise ValueError("Invalid binary format: truncated topic length")
        
        topic_len = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        
        if pos + topic_len > len(data):
            raise ValueError("Invalid binary format: truncated topic")
        
        topics.append(data[pos:pos+topic_len].decode("utf-8"))
        pos += topic_len
    
    return BinaryListTopicsResponse(topics=topics)


# =============================================================================
# Subscribe Request/Response
# =============================================================================

@dataclass
class BinarySubscribeRequest:
    """Binary subscribe request."""
    topic: str
    group_id: str
    partition: int
    mode: str  # "earliest", "latest", "commit"


def encode_subscribe_request(req: BinarySubscribeRequest) -> bytes:
    """
    Encode subscribe request to binary.
    
    Format: [2B topic_len][topic][2B group_len][group][4B partition][1B mode_len][mode]
    """
    topic_bytes = req.topic.encode("utf-8")
    group_bytes = req.group_id.encode("utf-8")
    mode_bytes = req.mode.encode("utf-8")
    
    parts = [
        struct.pack(">H", len(topic_bytes)),
        topic_bytes,
        struct.pack(">H", len(group_bytes)),
        group_bytes,
        struct.pack(">i", req.partition),
        struct.pack("B", len(mode_bytes)),
        mode_bytes,
    ]
    return b"".join(parts)


@dataclass
class BinarySubscribeResponse:
    """Binary subscribe response."""
    offset: int


def decode_subscribe_response(data: bytes) -> BinarySubscribeResponse:
    """Decode binary subscribe response. Format: [8B offset]"""
    if len(data) < 8:
        raise ValueError("Buffer too small")
    offset = struct.unpack(">Q", data[0:8])[0]
    return BinarySubscribeResponse(offset=offset)


# =============================================================================
# Commit Request/Response
# =============================================================================

@dataclass
class BinaryCommitRequest:
    """Binary commit request."""
    topic: str
    group_id: str
    partition: int
    offset: int


def encode_commit_request(req: BinaryCommitRequest) -> bytes:
    """
    Encode commit request to binary.
    
    Format: [2B topic_len][topic][2B group_len][group][4B partition][8B offset]
    """
    topic_bytes = req.topic.encode("utf-8")
    group_bytes = req.group_id.encode("utf-8")
    
    parts = [
        struct.pack(">H", len(topic_bytes)),
        topic_bytes,
        struct.pack(">H", len(group_bytes)),
        group_bytes,
        struct.pack(">i", req.partition),
        struct.pack(">Q", req.offset),
    ]
    return b"".join(parts)


@dataclass
class BinaryCommitResponse:
    """Binary commit response."""
    success: bool


def decode_commit_response(data: bytes) -> BinaryCommitResponse:
    """Decode binary commit response. Format: [1B success]"""
    if len(data) < 1:
        raise ValueError("Buffer too small")
    return BinaryCommitResponse(success=data[0] == 1)


# =============================================================================
# Consumer Group: Get Offset Request/Response
# =============================================================================

@dataclass
class BinaryGetOffsetRequest:
    """Binary get offset request."""
    topic: str
    group_id: str
    partition: int


def encode_get_offset_request(req: BinaryGetOffsetRequest) -> bytes:
    """
    Encode get offset request to binary.
    
    Format: [2B topic_len][topic][2B group_len][group][4B partition]
    """
    topic_bytes = req.topic.encode("utf-8")
    group_bytes = req.group_id.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">H", len(group_bytes)) + group_bytes +
        struct.pack(">i", req.partition)
    )


@dataclass
class BinaryGetOffsetResponse:
    """Binary get offset response."""
    offset: int


def decode_get_offset_response(data: bytes) -> BinaryGetOffsetResponse:
    """Decode binary get offset response. Format: [8B offset]"""
    if len(data) < 8:
        raise ValueError("Buffer too small")
    return BinaryGetOffsetResponse(offset=struct.unpack(">Q", data[0:8])[0])


# =============================================================================
# Consumer Group: Reset Offset Request/Response
# =============================================================================

@dataclass
class BinaryResetOffsetRequest:
    """Binary reset offset request."""
    topic: str
    group_id: str
    partition: int
    mode: str  # "earliest", "latest", "offset"
    offset: int = 0


def encode_reset_offset_request(req: BinaryResetOffsetRequest) -> bytes:
    """
    Encode reset offset request to binary.
    
    Format: [2B topic_len][topic][2B group_len][group][4B partition][1B mode_len][mode][8B offset if mode=offset]
    """
    topic_bytes = req.topic.encode("utf-8")
    group_bytes = req.group_id.encode("utf-8")
    mode_bytes = req.mode.encode("utf-8")
    parts = [
        struct.pack(">H", len(topic_bytes)), topic_bytes,
        struct.pack(">H", len(group_bytes)), group_bytes,
        struct.pack(">i", req.partition),
        struct.pack("B", len(mode_bytes)), mode_bytes,
    ]
    if req.mode == "offset":
        parts.append(struct.pack(">Q", req.offset))
    return b"".join(parts)


@dataclass
class BinarySimpleBoolResponse:
    """Simple boolean response."""
    success: bool


def decode_simple_bool_response(data: bytes) -> BinarySimpleBoolResponse:
    """Decode simple boolean response. Format: [1B success]"""
    if len(data) < 1:
        raise ValueError("Buffer too small")
    return BinarySimpleBoolResponse(success=data[0] == 1)


# =============================================================================
# Consumer Group: List Groups Response
# =============================================================================

@dataclass
class BinaryGroupOffset:
    """Offset for a partition in a consumer group."""
    partition: int
    offset: int


@dataclass
class BinaryGroupInfo:
    """Information about a consumer group."""
    topic: str
    group_id: str
    members: int
    offsets: list[BinaryGroupOffset]


@dataclass
class BinaryListGroupsResponse:
    """Binary list groups response."""
    groups: list[BinaryGroupInfo]


def decode_list_groups_response(data: bytes) -> BinaryListGroupsResponse:
    """
    Decode binary list groups response.
    
    Format: [4B count][groups...]
    Each group: [2B topic_len][topic][2B group_len][group][4B members][4B offset_count][offsets...]
    Each offset: [4B partition][8B offset]
    """
    if len(data) < 4:
        raise ValueError("Buffer too small")
    pos = 0
    count = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    groups: list[BinaryGroupInfo] = []
    for _ in range(count):
        if pos + 2 > len(data):
            raise ValueError("Invalid binary format")
        tlen = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        if pos + tlen + 2 > len(data):
            raise ValueError("Invalid binary format")
        topic = data[pos:pos+tlen].decode("utf-8")
        pos += tlen
        glen = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        if pos + glen + 8 > len(data):
            raise ValueError("Invalid binary format")
        gid = data[pos:pos+glen].decode("utf-8")
        pos += glen
        members = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4
        ocnt = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4
        offs: list[BinaryGroupOffset] = []
        for _ in range(ocnt):
            if pos + 12 > len(data):
                raise ValueError("Invalid binary format")
            part = struct.unpack(">I", data[pos:pos+4])[0]
            pos += 4
            off = struct.unpack(">Q", data[pos:pos+8])[0]
            pos += 8
            offs.append(BinaryGroupOffset(part, off))
        groups.append(BinaryGroupInfo(topic, gid, members, offs))
    return BinaryListGroupsResponse(groups)


# =============================================================================
# Consumer Group: Describe Group Request/Response
# =============================================================================

@dataclass
class BinaryDescribeGroupRequest:
    """Binary describe group request."""
    topic: str
    group_id: str


def encode_describe_group_request(req: BinaryDescribeGroupRequest) -> bytes:
    """Encode describe group request. Format: [2B topic_len][topic][2B group_len][group]"""
    topic_bytes = req.topic.encode("utf-8")
    group_bytes = req.group_id.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">H", len(group_bytes)) + group_bytes
    )


@dataclass
class BinaryDescribeGroupResponse:
    """Binary describe group response."""
    topic: str
    group_id: str
    members: int
    offsets: list[BinaryGroupOffset]


def decode_describe_group_response(data: bytes) -> BinaryDescribeGroupResponse:
    """
    Decode binary describe group response.
    
    Format: [2B topic_len][topic][2B group_len][group][4B members][4B offset_count][offsets...]
    """
    if len(data) < 8:
        raise ValueError("Buffer too small")
    pos = 0
    tlen = struct.unpack(">H", data[pos:pos+2])[0]
    pos += 2
    if pos + tlen + 2 > len(data):
        raise ValueError("Invalid binary format")
    topic = data[pos:pos+tlen].decode("utf-8")
    pos += tlen
    glen = struct.unpack(">H", data[pos:pos+2])[0]
    pos += 2
    if pos + glen + 8 > len(data):
        raise ValueError("Invalid binary format")
    gid = data[pos:pos+glen].decode("utf-8")
    pos += glen
    members = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    ocnt = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    offs: list[BinaryGroupOffset] = []
    for _ in range(ocnt):
        if pos + 12 > len(data):
            raise ValueError("Invalid binary format")
        part = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4
        off = struct.unpack(">Q", data[pos:pos+8])[0]
        pos += 8
        offs.append(BinaryGroupOffset(part, off))
    return BinaryDescribeGroupResponse(topic, gid, members, offs)


# =============================================================================
# Consumer Group: Get Lag Request/Response
# =============================================================================

@dataclass
class BinaryGetLagRequest:
    """Binary get lag request."""
    topic: str
    group_id: str
    partition: int


def encode_get_lag_request(req: BinaryGetLagRequest) -> bytes:
    """Encode get lag request (same format as get offset)."""
    return encode_get_offset_request(
        BinaryGetOffsetRequest(req.topic, req.group_id, req.partition)
    )


@dataclass
class BinaryGetLagResponse:
    """Binary get lag response."""
    current_offset: int
    committed_offset: int
    latest_offset: int
    lag: int


def decode_get_lag_response(data: bytes) -> BinaryGetLagResponse:
    """
    Decode binary get lag response.
    
    Format: [8B current_offset][8B committed_offset][8B latest_offset][8B lag]
    """
    if len(data) < 32:
        raise ValueError("Buffer too small")
    co, cm, lo, lag = struct.unpack(">QQQQ", data[0:32])
    return BinaryGetLagResponse(co, cm, lo, lag)


# =============================================================================
# Consumer Group: Delete Group Request
# =============================================================================

@dataclass
class BinaryDeleteGroupRequest:
    """Binary delete group request."""
    topic: str
    group_id: str


def encode_delete_group_request(req: BinaryDeleteGroupRequest) -> bytes:
    """Encode delete group request. Format: [2B topic_len][topic][2B group_len][group]"""
    topic_bytes = req.topic.encode("utf-8")
    group_bytes = req.group_id.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">H", len(group_bytes)) + group_bytes
    )


# =============================================================================
# Error Response
# =============================================================================

@dataclass
class BinaryErrorResponse:
    """Binary error response from server."""
    code: int
    message: str


def decode_error_response(data: bytes) -> BinaryErrorResponse:
    """Decode a binary error response."""
    if len(data) < 6:
        # Fallback for old servers sending plain text
        return BinaryErrorResponse(code=1, message=data.decode("utf-8", errors="replace"))

    code = struct.unpack(">I", data[:4])[0]
    msg_len = struct.unpack(">H", data[4:6])[0]

    if len(data) < 6 + msg_len:
        return BinaryErrorResponse(code=code, message=data[6:].decode("utf-8", errors="replace"))

    message = data[6 : 6 + msg_len].decode("utf-8")
    return BinaryErrorResponse(code=code, message=message)


# =============================================================================
# Advanced Produce: Delayed / TTL
# =============================================================================

@dataclass
class BinaryProduceDelayedRequest:
    """Binary produce delayed request."""
    topic: str
    data: bytes
    delay_ms: int


def encode_produce_delayed_request(req: BinaryProduceDelayedRequest) -> bytes:
    """
    Encode produce delayed request.
    
    Format: [2B topic_len][topic][4B data_len][data][8B delay_ms]
    """
    topic_bytes = req.topic.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">I", len(req.data)) + req.data +
        struct.pack(">Q", req.delay_ms)
    )


@dataclass
class BinaryProduceWithTTLRequest:
    """Binary produce with TTL request."""
    topic: str
    data: bytes
    ttl_ms: int


def encode_produce_with_ttl_request(req: BinaryProduceWithTTLRequest) -> bytes:
    """
    Encode produce with TTL request.
    
    Format: [2B topic_len][topic][4B data_len][data][8B ttl_ms]
    """
    topic_bytes = req.topic.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">I", len(req.data)) + req.data +
        struct.pack(">Q", req.ttl_ms)
    )


# =============================================================================
# Advanced Produce: Schema
# =============================================================================

@dataclass
class BinaryProduceWithSchemaRequest:
    """Binary produce with schema request."""
    topic: str
    data: bytes
    schema_name: str


def encode_produce_with_schema_request(req: BinaryProduceWithSchemaRequest) -> bytes:
    """
    Encode produce with schema request.
    
    Format: [2B topic_len][topic][4B data_len][data][2B schema_len][schema]
    """
    topic_bytes = req.topic.encode("utf-8")
    schema_bytes = req.schema_name.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">I", len(req.data)) + req.data +
        struct.pack(">H", len(schema_bytes)) + schema_bytes
    )


# =============================================================================
# Transaction Operations
# =============================================================================

@dataclass
class BinaryTxnRequest:
    """Binary transaction ID request (commit/abort)."""
    txn_id: str


def encode_txn_request(req: BinaryTxnRequest) -> bytes:
    """Encode transaction request. Format: [2B txn_id_len][txn_id]"""
    txn_bytes = req.txn_id.encode("utf-8")
    return struct.pack(">H", len(txn_bytes)) + txn_bytes


@dataclass
class BinaryBeginTxnResponse:
    """Binary begin transaction response."""
    txn_id: str


def decode_begin_txn_response(data: bytes) -> BinaryBeginTxnResponse:
    """Decode begin transaction response. Format: [2B txn_id_len][txn_id]"""
    if len(data) < 2:
        raise ValueError("Buffer too small")
    txn_len = struct.unpack(">H", data[0:2])[0]
    txn_id = data[2:2+txn_len].decode("utf-8")
    return BinaryBeginTxnResponse(txn_id=txn_id)


@dataclass
class BinaryTxnProduceRequest:
    """Binary transaction produce request."""
    txn_id: str
    topic: str
    data: bytes


def encode_txn_produce_request(req: BinaryTxnProduceRequest) -> bytes:
    """
    Encode transaction produce request.
    
    Format: [2B txn_id_len][txn_id][2B topic_len][topic][4B data_len][data]
    """
    txn_bytes = req.txn_id.encode("utf-8")
    topic_bytes = req.topic.encode("utf-8")
    return (
        struct.pack(">H", len(txn_bytes)) + txn_bytes +
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">I", len(req.data)) + req.data
    )


# =============================================================================
# Schema Registry Operations
# =============================================================================

@dataclass
class BinaryRegisterSchemaRequest:
    """Binary register schema request."""
    name: str
    schema_type: str
    schema: bytes


def encode_register_schema_request(req: BinaryRegisterSchemaRequest) -> bytes:
    """
    Encode register schema request.
    
    Format: [2B name_len][name][1B type_len][type][4B schema_len][schema]
    """
    name_bytes = req.name.encode("utf-8")
    type_bytes = req.schema_type.encode("utf-8")
    return (
        struct.pack(">H", len(name_bytes)) + name_bytes +
        struct.pack("B", len(type_bytes)) + type_bytes +
        struct.pack(">I", len(req.schema)) + req.schema
    )


@dataclass
class BinaryListSchemasRequest:
    """Binary list schemas request."""
    topic: str


def encode_list_schemas_request(req: BinaryListSchemasRequest) -> bytes:
    """Encode list schemas request. Format: [2B topic_len][topic]"""
    topic_bytes = req.topic.encode("utf-8")
    return struct.pack(">H", len(topic_bytes)) + topic_bytes


@dataclass
class BinarySchemaInfo:
    """Schema info from list schemas response."""
    name: str
    schema_type: str
    version: int


@dataclass
class BinaryListSchemasResponse:
    """Binary list schemas response."""
    schemas: list[BinarySchemaInfo]


def decode_list_schemas_response(data: bytes) -> BinaryListSchemasResponse:
    """
    Decode list schemas response.
    
    Format: [4B count][schemas...]
    Each schema: [2B name_len][name][1B type_len][type][4B version]
    """
    if len(data) < 4:
        raise ValueError("Buffer too small")
    pos = 0
    count = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    schemas: list[BinarySchemaInfo] = []
    for _ in range(count):
        name_len = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        name = data[pos:pos+name_len].decode("utf-8")
        pos += name_len
        type_len = data[pos]
        pos += 1
        schema_type = data[pos:pos+type_len].decode("utf-8")
        pos += type_len
        version = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4
        schemas.append(BinarySchemaInfo(name, schema_type, version))
    return BinaryListSchemasResponse(schemas=schemas)


@dataclass
class BinaryGetSchemaRequest:
    """Binary get schema request."""
    topic: str
    version: int


def encode_get_schema_request(req: BinaryGetSchemaRequest) -> bytes:
    """Encode get schema request. Format: [2B topic_len][topic][4B version]"""
    topic_bytes = req.topic.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">I", req.version)
    )


@dataclass
class BinaryValidateSchemaRequest:
    """Binary validate schema request."""
    name: str
    message: bytes


def encode_validate_schema_request(req: BinaryValidateSchemaRequest) -> bytes:
    """Encode validate schema request. Format: [2B name_len][name][4B msg_len][msg]"""
    name_bytes = req.name.encode("utf-8")
    return (
        struct.pack(">H", len(name_bytes)) + name_bytes +
        struct.pack(">I", len(req.message)) + req.message
    )


@dataclass
class BinaryValidateSchemaResponse:
    """Binary validate schema response."""
    valid: bool


def decode_validate_schema_response(data: bytes) -> BinaryValidateSchemaResponse:
    """Decode validate schema response. Format: [1B valid]"""
    if len(data) < 1:
        raise ValueError("Buffer too small")
    return BinaryValidateSchemaResponse(valid=data[0] == 1)


@dataclass
class BinaryDeleteSchemaRequest:
    """Binary delete schema request."""
    topic: str
    version: int


def encode_delete_schema_request(req: BinaryDeleteSchemaRequest) -> bytes:
    """Encode delete schema request. Format: [2B topic_len][topic][4B version]"""
    topic_bytes = req.topic.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">I", req.version)
    )


# =============================================================================
# Dead Letter Queue Operations
# =============================================================================

@dataclass
class BinaryFetchDLQRequest:
    """Binary fetch DLQ request."""
    topic: str
    max_messages: int


def encode_fetch_dlq_request(req: BinaryFetchDLQRequest) -> bytes:
    """Encode fetch DLQ request. Format: [2B topic_len][topic][4B max_messages]"""
    topic_bytes = req.topic.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">I", req.max_messages)
    )


@dataclass
class BinaryDLQMessage:
    """A single DLQ message."""
    id: str
    data: bytes
    error: str
    retries: int


@dataclass
class BinaryFetchDLQResponse:
    """Binary fetch DLQ response."""
    messages: list[BinaryDLQMessage]


def decode_fetch_dlq_response(data: bytes) -> BinaryFetchDLQResponse:
    """
    Decode fetch DLQ response.
    
    Format: [4B count][messages...]
    Each message: [2B id_len][id][4B data_len][data][2B error_len][error][4B retries]
    """
    if len(data) < 4:
        raise ValueError("Buffer too small")
    pos = 0
    count = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    messages: list[BinaryDLQMessage] = []
    for _ in range(count):
        id_len = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        msg_id = data[pos:pos+id_len].decode("utf-8")
        pos += id_len
        data_len = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4
        msg_data = data[pos:pos+data_len]
        pos += data_len
        error_len = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        error = data[pos:pos+error_len].decode("utf-8")
        pos += error_len
        retries = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4
        messages.append(BinaryDLQMessage(msg_id, msg_data, error, retries))
    return BinaryFetchDLQResponse(messages=messages)


@dataclass
class BinaryReplayDLQRequest:
    """Binary replay DLQ request."""
    topic: str
    message_id: str


def encode_replay_dlq_request(req: BinaryReplayDLQRequest) -> bytes:
    """Encode replay DLQ request. Format: [2B topic_len][topic][2B msg_id_len][msg_id]"""
    topic_bytes = req.topic.encode("utf-8")
    msg_id_bytes = req.message_id.encode("utf-8")
    return (
        struct.pack(">H", len(topic_bytes)) + topic_bytes +
        struct.pack(">H", len(msg_id_bytes)) + msg_id_bytes
    )


@dataclass
class BinaryPurgeDLQRequest:
    """Binary purge DLQ request."""
    topic: str


def encode_purge_dlq_request(req: BinaryPurgeDLQRequest) -> bytes:
    """Encode purge DLQ request. Format: [2B topic_len][topic]"""
    topic_bytes = req.topic.encode("utf-8")
    return struct.pack(">H", len(topic_bytes)) + topic_bytes


# =============================================================================
# Cluster Operations
# =============================================================================

@dataclass
class BinaryClusterJoinRequest:
    """Binary cluster join request."""
    peer: str


def encode_cluster_join_request(req: BinaryClusterJoinRequest) -> bytes:
    """Encode cluster join request. Format: [2B peer_len][peer]"""
    peer_bytes = req.peer.encode("utf-8")
    return struct.pack(">H", len(peer_bytes)) + peer_bytes


# =============================================================================
# Authentication Operations
# =============================================================================

@dataclass
class BinaryAuthRequest:
    """Binary auth request."""
    username: str
    password: str


def encode_auth_request(req: BinaryAuthRequest) -> bytes:
    """Encode auth request. Format: [2B user_len][user][2B pass_len][pass]"""
    user_bytes = req.username.encode("utf-8")
    pass_bytes = req.password.encode("utf-8")
    return (
        struct.pack(">H", len(user_bytes)) + user_bytes +
        struct.pack(">H", len(pass_bytes)) + pass_bytes
    )


@dataclass
class BinaryAuthResponse:
    """Binary auth response."""
    success: bool
    username: str
    roles: list[str]
    error: str


def decode_auth_response(data: bytes) -> BinaryAuthResponse:
    """
    Decode auth response.

    Format: [1B success][2B user_len][user][4B role_count][1B role_len][role]...[2B error_len][error]
    """
    if len(data) < 1:
        raise ValueError("Buffer too small")
    pos = 0
    success = data[pos] == 1
    pos += 1

    user_len = struct.unpack(">H", data[pos:pos+2])[0]
    pos += 2
    username = data[pos:pos+user_len].decode("utf-8")
    pos += user_len

    role_count = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    roles: list[str] = []
    for _ in range(role_count):
        role_len = data[pos]  # 1 byte for role length
        pos += 1
        roles.append(data[pos:pos+role_len].decode("utf-8"))
        pos += role_len

    # Error string
    error_len = struct.unpack(">H", data[pos:pos+2])[0]
    pos += 2
    error = data[pos:pos+error_len].decode("utf-8") if error_len > 0 else ""

    return BinaryAuthResponse(success=success, username=username, roles=roles, error=error)


@dataclass
class BinaryWhoAmIResponse:
    """Binary whoami response."""
    authenticated: bool
    username: str
    roles: list[str]
    permissions: list[str]


def decode_whoami_response(data: bytes) -> BinaryWhoAmIResponse:
    """
    Decode whoami response.
    
    Format: [1B authenticated][2B user_len][user][4B role_count][roles...][4B perm_count][perms...]
    """
    if len(data) < 1:
        raise ValueError("Buffer too small")
    pos = 0
    authenticated = data[pos] == 1
    pos += 1
    
    user_len = struct.unpack(">H", data[pos:pos+2])[0]
    pos += 2
    username = data[pos:pos+user_len].decode("utf-8")
    pos += user_len
    
    role_count = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    roles: list[str] = []
    for _ in range(role_count):
        role_len = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        roles.append(data[pos:pos+role_len].decode("utf-8"))
        pos += role_len
    
    perm_count = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4
    permissions: list[str] = []
    for _ in range(perm_count):
        perm_len = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        permissions.append(data[pos:pos+perm_len].decode("utf-8"))
        pos += perm_len

    return BinaryWhoAmIResponse(authenticated=authenticated, username=username, roles=roles, permissions=permissions)


# =============================================================================
# Cluster Metadata Request/Response (for Partition Routing)
# =============================================================================

@dataclass
class BinaryClusterMetadataRequest:
    """Request for cluster partition metadata."""
    topic: str = ""  # Empty = all topics


@dataclass
class PartitionMetadata:
    """Metadata for a single partition."""
    partition: int
    leader_id: str
    leader_addr: str
    epoch: int
    state: str = "online"  # Partition state: online, offline, reassigning, syncing
    replicas: list[str] = None  # Node IDs of all replicas
    isr: list[str] = None  # In-Sync Replicas (node IDs)

    def __post_init__(self):
        if self.replicas is None:
            self.replicas = []
        if self.isr is None:
            self.isr = []


@dataclass
class TopicMetadata:
    """Metadata for a topic's partitions."""
    topic: str
    partitions: list[PartitionMetadata]


@dataclass
class BinaryClusterMetadataResponse:
    """Response containing partition-to-node mappings."""
    cluster_id: str
    topics: list[TopicMetadata]


def encode_cluster_metadata_request(req: BinaryClusterMetadataRequest) -> bytes:
    """
    Encode cluster metadata request to binary.

    Format:
        [2B topic_len][topic]
    """
    topic_bytes = req.topic.encode("utf-8")
    return struct.pack(">H", len(topic_bytes)) + topic_bytes


def decode_cluster_metadata_response(data: bytes) -> BinaryClusterMetadataResponse:
    """
    Decode cluster metadata response from binary.

    Format:
        [2B cluster_id_len][cluster_id]
        [4B topic_count]
          [2B topic_len][topic]
          [4B partition_count]
            [4B partition][2B leader_id_len][leader_id][2B leader_addr_len][leader_addr][8B epoch]
    """
    pos = 0

    # Cluster ID
    cluster_id_len = struct.unpack(">H", data[pos:pos+2])[0]
    pos += 2
    cluster_id = data[pos:pos+cluster_id_len].decode("utf-8")
    pos += cluster_id_len

    # Topic count
    topic_count = struct.unpack(">I", data[pos:pos+4])[0]
    pos += 4

    topics: list[TopicMetadata] = []
    for _ in range(topic_count):
        # Topic name
        topic_len = struct.unpack(">H", data[pos:pos+2])[0]
        pos += 2
        topic_name = data[pos:pos+topic_len].decode("utf-8")
        pos += topic_len

        # Partition count
        partition_count = struct.unpack(">I", data[pos:pos+4])[0]
        pos += 4

        partitions: list[PartitionMetadata] = []
        for _ in range(partition_count):
            # Partition number
            partition = struct.unpack(">I", data[pos:pos+4])[0]
            pos += 4

            # Leader ID
            leader_id_len = struct.unpack(">H", data[pos:pos+2])[0]
            pos += 2
            leader_id = data[pos:pos+leader_id_len].decode("utf-8")
            pos += leader_id_len

            # Leader address
            leader_addr_len = struct.unpack(">H", data[pos:pos+2])[0]
            pos += 2
            leader_addr = data[pos:pos+leader_addr_len].decode("utf-8")
            pos += leader_addr_len

            # Epoch
            epoch = struct.unpack(">Q", data[pos:pos+8])[0]
            pos += 8

            # State
            state_len = struct.unpack(">H", data[pos:pos+2])[0]
            pos += 2
            state = data[pos:pos+state_len].decode("utf-8")
            pos += state_len

            # Replicas
            replica_count = struct.unpack(">I", data[pos:pos+4])[0]
            pos += 4
            replicas: list[str] = []
            for _ in range(replica_count):
                replica_len = struct.unpack(">H", data[pos:pos+2])[0]
                pos += 2
                replicas.append(data[pos:pos+replica_len].decode("utf-8"))
                pos += replica_len

            # ISR
            isr_count = struct.unpack(">I", data[pos:pos+4])[0]
            pos += 4
            isr: list[str] = []
            for _ in range(isr_count):
                isr_len = struct.unpack(">H", data[pos:pos+2])[0]
                pos += 2
                isr.append(data[pos:pos+isr_len].decode("utf-8"))
                pos += isr_len

            partitions.append(PartitionMetadata(
                partition=partition,
                leader_id=leader_id,
                leader_addr=leader_addr,
                epoch=epoch,
                state=state,
                replicas=replicas,
                isr=isr,
            ))

        topics.append(TopicMetadata(topic=topic_name, partitions=partitions))

    return BinaryClusterMetadataResponse(cluster_id=cluster_id, topics=topics)

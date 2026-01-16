# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
FlyMQ Binary Protocol Implementation.

Protocol Format:
    +-------+-------+-------+-------+-------+-------+-------+-------+
    | Magic | Ver   | Op    | Flags | Length (4 bytes, big-endian) |
    +-------+-------+-------+-------+-------+-------+-------+-------+
    |                      Payload (Length bytes)                   |
    +---------------------------------------------------------------+

Header Fields:
    - Magic (1 byte): 0xAF - Identifies FlyMQ protocol
    - Version (1 byte): Protocol version (0x01)
    - Op (1 byte): Operation code
    - Flags (1 byte): Reserved
    - Length (4 bytes): Payload length (big-endian)
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import BinaryIO

# Protocol constants
MAGIC_BYTE: int = 0xAF
PROTOCOL_VERSION: int = 0x01
HEADER_SIZE: int = 8
MAX_MESSAGE_SIZE: int = 32 * 1024 * 1024  # 32MB


class OpCode(IntEnum):
    """Operation codes for FlyMQ protocol messages."""

    # Core Operations (0x01-0x0F)
    PRODUCE = 0x01
    CONSUME = 0x02
    CREATE_TOPIC = 0x03
    METADATA = 0x04
    SUBSCRIBE = 0x05
    COMMIT = 0x06
    FETCH = 0x07
    LIST_TOPICS = 0x08
    DELETE_TOPIC = 0x09

    # Schema Operations (0x10-0x1F)
    REGISTER_SCHEMA = 0x10
    GET_SCHEMA = 0x11
    LIST_SCHEMAS = 0x12
    VALIDATE_SCHEMA = 0x13
    PRODUCE_WITH_SCHEMA = 0x14
    DELETE_SCHEMA = 0x15

    # Dead Letter Queue Operations (0x20-0x2F)
    FETCH_DLQ = 0x20
    REPLAY_DLQ = 0x21
    PURGE_DLQ = 0x22

    # Delayed Message Operations (0x30-0x3F)
    PRODUCE_DELAYED = 0x30
    CANCEL_DELAYED = 0x31
    PRODUCE_WITH_TTL = 0x35

    # Transaction Operations (0x40-0x4F)
    BEGIN_TX = 0x40
    COMMIT_TX = 0x41
    ABORT_TX = 0x42
    PRODUCE_TX = 0x43

    # Cluster Operations (0x50-0x5F)
    CLUSTER_JOIN = 0x50
    CLUSTER_LEAVE = 0x51
    CLUSTER_STATUS = 0x52

    # Consumer Group Operations (0x60-0x6F)
    GET_OFFSET = 0x60
    RESET_OFFSET = 0x61
    LIST_GROUPS = 0x62
    DESCRIBE_GROUP = 0x63
    GET_LAG = 0x64
    DELETE_GROUP = 0x65
    SEEK_TO_TIMESTAMP = 0x66

    # Authentication Operations (0x70-0x7F)
    AUTH = 0x70
    AUTH_RESPONSE = 0x71
    WHOAMI = 0x72

    # Error Response
    ERROR = 0xFF


class SubscribeMode:
    """Subscribe modes for consumer positioning."""

    EARLIEST = "earliest"
    LATEST = "latest"
    COMMIT = "commit"


@dataclass(frozen=True)
class Header:
    """Protocol message header."""

    magic: int
    version: int
    op: OpCode
    flags: int
    length: int

    def to_bytes(self) -> bytes:
        """Serialize header to bytes."""
        return struct.pack(
            ">BBBBI",  # Big-endian: 4 unsigned chars + unsigned int
            self.magic,
            self.version,
            self.op,
            self.flags,
            self.length,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> Header:
        """Deserialize header from bytes."""
        if len(data) != HEADER_SIZE:
            raise ValueError(f"Invalid header size: {len(data)}, expected {HEADER_SIZE}")

        magic, version, op, flags, length = struct.unpack(">BBBBI", data)

        # Check magic byte first to give a better error message
        if magic != MAGIC_BYTE:
            # Check if this looks like HTTP response
            if data[:4] == b"HTTP":
                raise ValueError(
                    "Received HTTP response instead of FlyMQ protocol. "
                    "Check that you're connecting to the correct port and that "
                    "no other service is using the same port."
                )
            raise ValueError(
                f"Invalid magic byte: expected 0x{MAGIC_BYTE:02X}, got 0x{magic:02X}. "
                "This may indicate a port conflict with another service."
            )

        # Try to parse OpCode, with fallback for unknown codes
        try:
            op_code = OpCode(op)
        except ValueError:
            raise ValueError(
                f"Unknown operation code: 0x{op:02X}. "
                "This may indicate a protocol version mismatch."
            )

        return cls(
            magic=magic,
            version=version,
            op=op_code,
            flags=flags,
            length=length,
        )


@dataclass(frozen=True)
class Message:
    """Complete protocol message with header and payload."""

    header: Header
    payload: bytes

    @property
    def op(self) -> OpCode:
        """Get the operation code."""
        return self.header.op


class ProtocolError(Exception):
    """Base exception for protocol errors."""


class InvalidMagicError(ProtocolError):
    """Raised when magic byte doesn't match."""

    def __init__(self, received: int) -> None:
        super().__init__(f"Invalid magic byte: 0x{received:02X}, expected 0x{MAGIC_BYTE:02X}")
        self.received = received


class InvalidVersionError(ProtocolError):
    """Raised when protocol version is not supported."""

    def __init__(self, received: int) -> None:
        super().__init__(
            f"Unsupported protocol version: 0x{received:02X}, expected 0x{PROTOCOL_VERSION:02X}"
        )
        self.received = received


class MessageTooLargeError(ProtocolError):
    """Raised when message exceeds maximum size."""

    def __init__(self, size: int) -> None:
        super().__init__(
            f"Message too large: {size} bytes, maximum is {MAX_MESSAGE_SIZE} bytes"
        )
        self.size = size


def read_header(reader: BinaryIO) -> Header:
    """
    Read and validate a message header from a binary stream.

    Args:
        reader: Binary stream to read from.

    Returns:
        Parsed Header object.

    Raises:
        InvalidMagicError: If magic byte doesn't match.
        InvalidVersionError: If protocol version is unsupported.
        MessageTooLargeError: If payload length exceeds maximum.
        EOFError: If stream ends unexpectedly.
    """
    data = reader.read(HEADER_SIZE)
    if len(data) == 0:
        raise EOFError("Connection closed")
    if len(data) < HEADER_SIZE:
        raise EOFError(f"Incomplete header: got {len(data)} bytes, expected {HEADER_SIZE}")

    header = Header.from_bytes(data)

    if header.magic != MAGIC_BYTE:
        raise InvalidMagicError(header.magic)

    if header.version != PROTOCOL_VERSION:
        raise InvalidVersionError(header.version)

    if header.length > MAX_MESSAGE_SIZE:
        raise MessageTooLargeError(header.length)

    return header


def write_header(writer: BinaryIO, header: Header) -> None:
    """
    Write a message header to a binary stream.

    Args:
        writer: Binary stream to write to.
        header: Header to write.
    """
    writer.write(header.to_bytes())


def read_message(reader: BinaryIO) -> Message:
    """
    Read a complete message (header + payload) from a binary stream.

    Args:
        reader: Binary stream to read from.

    Returns:
        Complete Message object.

    Raises:
        ProtocolError: If header validation fails.
        EOFError: If stream ends unexpectedly.
    """
    header = read_header(reader)

    payload = b""
    if header.length > 0:
        payload = reader.read(header.length)
        if len(payload) < header.length:
            raise EOFError(
                f"Incomplete payload: got {len(payload)} bytes, expected {header.length}"
            )

    return Message(header=header, payload=payload)


def write_message(writer: BinaryIO, op: OpCode, payload: bytes = b"") -> None:
    """
    Write a complete message (header + payload) to a binary stream.

    Args:
        writer: Binary stream to write to.
        op: Operation code for the message.
        payload: Message payload (default: empty).
    """
    header = Header(
        magic=MAGIC_BYTE,
        version=PROTOCOL_VERSION,
        op=op,
        flags=0,
        length=len(payload),
    )
    write_header(writer, header)
    if payload:
        writer.write(payload)


def write_error(writer: BinaryIO, error: str | Exception) -> None:
    """
    Write an error response to a binary stream.

    Args:
        writer: Binary stream to write to.
        error: Error message or exception.
    """
    error_msg = str(error).encode("utf-8")
    write_message(writer, OpCode.ERROR, error_msg)

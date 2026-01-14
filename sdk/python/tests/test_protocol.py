# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""Tests for FlyMQ protocol implementation."""

import io

import pytest

from pyflymq.protocol import (
    HEADER_SIZE,
    MAGIC_BYTE,
    MAX_MESSAGE_SIZE,
    PROTOCOL_VERSION,
    Header,
    InvalidMagicError,
    InvalidVersionError,
    Message,
    MessageTooLargeError,
    OpCode,
    read_header,
    read_message,
    write_message,
)


class TestHeader:
    """Tests for Header class."""

    def test_header_to_bytes(self) -> None:
        """Test header serialization."""
        header = Header(
            magic=MAGIC_BYTE,
            version=PROTOCOL_VERSION,
            op=OpCode.PRODUCE,
            flags=0,
            length=100,
        )
        data = header.to_bytes()
        assert len(data) == HEADER_SIZE
        assert data[0] == MAGIC_BYTE
        assert data[1] == PROTOCOL_VERSION
        assert data[2] == OpCode.PRODUCE
        assert data[3] == 0
        # Length is big-endian 4 bytes
        assert int.from_bytes(data[4:8], "big") == 100

    def test_header_from_bytes(self) -> None:
        """Test header deserialization."""
        header = Header(
            magic=MAGIC_BYTE,
            version=PROTOCOL_VERSION,
            op=OpCode.CONSUME,
            flags=0,
            length=42,
        )
        data = header.to_bytes()
        restored = Header.from_bytes(data)

        assert restored.magic == MAGIC_BYTE
        assert restored.version == PROTOCOL_VERSION
        assert restored.op == OpCode.CONSUME
        assert restored.flags == 0
        assert restored.length == 42

    def test_header_from_bytes_invalid_size(self) -> None:
        """Test header deserialization with invalid size."""
        with pytest.raises(ValueError, match="Invalid header size"):
            Header.from_bytes(b"\x00" * 4)

    def test_header_roundtrip(self) -> None:
        """Test header serialization/deserialization roundtrip."""
        for op in [OpCode.PRODUCE, OpCode.CONSUME, OpCode.CREATE_TOPIC, OpCode.FETCH]:
            header = Header(
                magic=MAGIC_BYTE,
                version=PROTOCOL_VERSION,
                op=op,
                flags=0,
                length=12345,
            )
            restored = Header.from_bytes(header.to_bytes())
            assert restored == header


class TestMessage:
    """Tests for Message class."""

    def test_message_op_property(self) -> None:
        """Test message op property."""
        header = Header(
            magic=MAGIC_BYTE,
            version=PROTOCOL_VERSION,
            op=OpCode.PRODUCE,
            flags=0,
            length=5,
        )
        message = Message(header=header, payload=b"hello")
        assert message.op == OpCode.PRODUCE


class TestReadHeader:
    """Tests for read_header function."""

    def test_read_header_success(self) -> None:
        """Test reading a valid header."""
        header = Header(
            magic=MAGIC_BYTE,
            version=PROTOCOL_VERSION,
            op=OpCode.PRODUCE,
            flags=0,
            length=100,
        )
        reader = io.BytesIO(header.to_bytes())
        result = read_header(reader)

        assert result.magic == MAGIC_BYTE
        assert result.version == PROTOCOL_VERSION
        assert result.op == OpCode.PRODUCE
        assert result.length == 100

    def test_read_header_invalid_magic(self) -> None:
        """Test reading header with invalid magic byte."""
        header = Header(
            magic=0x00,  # Invalid magic
            version=PROTOCOL_VERSION,
            op=OpCode.PRODUCE,
            flags=0,
            length=100,
        )
        reader = io.BytesIO(header.to_bytes())

        with pytest.raises(InvalidMagicError) as exc_info:
            read_header(reader)
        assert exc_info.value.received == 0x00

    def test_read_header_invalid_version(self) -> None:
        """Test reading header with invalid version."""
        header = Header(
            magic=MAGIC_BYTE,
            version=0xFF,  # Invalid version
            op=OpCode.PRODUCE,
            flags=0,
            length=100,
        )
        reader = io.BytesIO(header.to_bytes())

        with pytest.raises(InvalidVersionError) as exc_info:
            read_header(reader)
        assert exc_info.value.received == 0xFF

    def test_read_header_message_too_large(self) -> None:
        """Test reading header with payload too large."""
        header = Header(
            magic=MAGIC_BYTE,
            version=PROTOCOL_VERSION,
            op=OpCode.PRODUCE,
            flags=0,
            length=MAX_MESSAGE_SIZE + 1,
        )
        reader = io.BytesIO(header.to_bytes())

        with pytest.raises(MessageTooLargeError) as exc_info:
            read_header(reader)
        assert exc_info.value.size == MAX_MESSAGE_SIZE + 1

    def test_read_header_eof(self) -> None:
        """Test reading header from empty stream."""
        reader = io.BytesIO(b"")
        with pytest.raises(EOFError, match="Connection closed"):
            read_header(reader)

    def test_read_header_incomplete(self) -> None:
        """Test reading incomplete header."""
        reader = io.BytesIO(b"\xAF\x01\x01")  # Only 3 bytes
        with pytest.raises(EOFError, match="Incomplete header"):
            read_header(reader)


class TestReadWriteMessage:
    """Tests for read_message and write_message functions."""

    def test_write_message_empty_payload(self) -> None:
        """Test writing message with empty payload."""
        writer = io.BytesIO()
        write_message(writer, OpCode.LIST_TOPICS)

        writer.seek(0)
        data = writer.read()

        assert len(data) == HEADER_SIZE
        assert data[0] == MAGIC_BYTE
        assert data[2] == OpCode.LIST_TOPICS
        assert int.from_bytes(data[4:8], "big") == 0

    def test_write_message_with_payload(self) -> None:
        """Test writing message with payload."""
        payload = b'{"topic": "test", "data": "hello"}'
        writer = io.BytesIO()
        write_message(writer, OpCode.PRODUCE, payload)

        writer.seek(0)
        data = writer.read()

        assert len(data) == HEADER_SIZE + len(payload)
        assert int.from_bytes(data[4:8], "big") == len(payload)
        assert data[HEADER_SIZE:] == payload

    def test_read_message_success(self) -> None:
        """Test reading a complete message."""
        payload = b'{"topic": "test"}'
        writer = io.BytesIO()
        write_message(writer, OpCode.PRODUCE, payload)

        writer.seek(0)
        message = read_message(writer)

        assert message.op == OpCode.PRODUCE
        assert message.payload == payload

    def test_read_write_roundtrip(self) -> None:
        """Test message serialization/deserialization roundtrip."""
        test_cases = [
            (OpCode.PRODUCE, b'{"topic": "test", "data": "hello"}'),
            (OpCode.CONSUME, b'{"topic": "test", "offset": 42}'),
            (OpCode.LIST_TOPICS, b""),
            (OpCode.ERROR, b"Something went wrong"),
        ]

        for op, payload in test_cases:
            writer = io.BytesIO()
            write_message(writer, op, payload)

            writer.seek(0)
            message = read_message(writer)

            assert message.op == op
            assert message.payload == payload

    def test_read_message_incomplete_payload(self) -> None:
        """Test reading message with incomplete payload."""
        # Write header claiming 100 bytes, but only provide 10
        header = Header(
            magic=MAGIC_BYTE,
            version=PROTOCOL_VERSION,
            op=OpCode.PRODUCE,
            flags=0,
            length=100,
        )
        reader = io.BytesIO(header.to_bytes() + b"x" * 10)

        with pytest.raises(EOFError, match="Incomplete payload"):
            read_message(reader)


class TestOpCode:
    """Tests for OpCode enum."""

    def test_opcode_values(self) -> None:
        """Test OpCode enum values match protocol spec."""
        assert OpCode.PRODUCE == 0x01
        assert OpCode.CONSUME == 0x02
        assert OpCode.CREATE_TOPIC == 0x03
        assert OpCode.SUBSCRIBE == 0x05
        assert OpCode.COMMIT == 0x06
        assert OpCode.FETCH == 0x07
        assert OpCode.LIST_TOPICS == 0x08
        assert OpCode.DELETE_TOPIC == 0x09

        # Schema operations
        assert OpCode.REGISTER_SCHEMA == 0x10
        assert OpCode.GET_SCHEMA == 0x11
        assert OpCode.LIST_SCHEMAS == 0x12
        assert OpCode.VALIDATE_SCHEMA == 0x13
        assert OpCode.PRODUCE_WITH_SCHEMA == 0x14

        # DLQ operations
        assert OpCode.FETCH_DLQ == 0x20
        assert OpCode.REPLAY_DLQ == 0x21
        assert OpCode.PURGE_DLQ == 0x22

        # Delayed/TTL operations
        assert OpCode.PRODUCE_DELAYED == 0x30
        assert OpCode.PRODUCE_WITH_TTL == 0x35

        # Transaction operations
        assert OpCode.BEGIN_TX == 0x40
        assert OpCode.COMMIT_TX == 0x41
        assert OpCode.ABORT_TX == 0x42
        assert OpCode.PRODUCE_TX == 0x43

        # Cluster operations
        assert OpCode.CLUSTER_JOIN == 0x50
        assert OpCode.CLUSTER_LEAVE == 0x51
        assert OpCode.CLUSTER_STATUS == 0x52

        # Error
        assert OpCode.ERROR == 0xFF

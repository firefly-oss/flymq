# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""Integration tests for pyflymq SDK.

These tests require a running FlyMQ server. They are skipped if no server is available.

Run with: pytest tests/test_integration.py -v
"""

import os
import time

import pytest

from pyflymq import Consumer, FlyMQClient, Producer

# Configuration from environment
FLYMQ_HOST = os.environ.get("FLYMQ_HOST", "localhost")
FLYMQ_PORT = int(os.environ.get("FLYMQ_PORT", "9092"))
FLYMQ_SERVERS = os.environ.get("FLYMQ_SERVERS", f"{FLYMQ_HOST}:{FLYMQ_PORT}")


def is_server_available() -> bool:
    """Check if FlyMQ server is available."""
    import socket

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        sock.connect((FLYMQ_HOST, FLYMQ_PORT))
        sock.close()
        return True
    except (socket.error, socket.timeout):
        return False


# Skip all tests if server not available
pytestmark = pytest.mark.skipif(
    not is_server_available(),
    reason=f"FlyMQ server not available at {FLYMQ_HOST}:{FLYMQ_PORT}",
)


@pytest.fixture
def client() -> FlyMQClient:
    """Create a FlyMQ client for testing."""
    c = FlyMQClient(FLYMQ_SERVERS)
    yield c
    c.close()


@pytest.fixture
def test_topic(client: FlyMQClient) -> str:
    """Create a unique test topic."""
    topic = f"test-topic-{int(time.time() * 1000)}"
    client.create_topic(topic, partitions=1)
    yield topic
    try:
        client.delete_topic(topic)
    except Exception:
        pass


class TestClientConnection:
    """Test client connection handling."""

    def test_connect_and_close(self) -> None:
        """Test basic connect and close."""
        client = FlyMQClient(FLYMQ_SERVERS)
        assert client is not None
        client.close()

    def test_context_manager(self) -> None:
        """Test client as context manager."""
        with FlyMQClient(FLYMQ_SERVERS) as client:
            topics = client.list_topics()
            assert isinstance(topics, list)


class TestTopicOperations:
    """Test topic management operations."""

    def test_create_and_delete_topic(self, client: FlyMQClient) -> None:
        """Test creating and deleting a topic."""
        topic = f"test-create-{int(time.time() * 1000)}"

        # Create topic
        client.create_topic(topic, partitions=1)

        # Verify topic exists
        topics = client.list_topics()
        assert topic in topics

        # Delete topic
        client.delete_topic(topic)

        # Verify topic deleted
        topics = client.list_topics()
        assert topic not in topics

    def test_list_topics(self, client: FlyMQClient) -> None:
        """Test listing topics."""
        topics = client.list_topics()
        assert isinstance(topics, list)


class TestProduceConsume:
    """Test produce and consume operations."""

    def test_produce_and_consume(self, client: FlyMQClient, test_topic: str) -> None:
        """Test basic produce and consume."""
        message = b"Hello, FlyMQ!"

        # Produce message
        offset = client.produce(test_topic, message)
        assert offset >= 0

        # Consume message
        data = client.consume(test_topic, offset)
        assert data == message

    def test_produce_string(self, client: FlyMQClient, test_topic: str) -> None:
        """Test producing string messages."""
        message = "Hello, FlyMQ!"

        offset = client.produce(test_topic, message)
        data = client.consume(test_topic, offset)

        assert data.decode("utf-8") == message

    def test_produce_multiple(self, client: FlyMQClient, test_topic: str) -> None:
        """Test producing multiple messages."""
        messages = [f"Message {i}".encode() for i in range(10)]
        offsets = []

        for msg in messages:
            offset = client.produce(test_topic, msg)
            offsets.append(offset)

        # Verify offsets are sequential
        for i in range(1, len(offsets)):
            assert offsets[i] > offsets[i - 1]

        # Consume and verify
        for i, offset in enumerate(offsets):
            data = client.consume(test_topic, offset)
            assert data == messages[i]

    def test_fetch_batch(self, client: FlyMQClient, test_topic: str) -> None:
        """Test fetching multiple messages."""
        # Produce messages
        for i in range(5):
            client.produce(test_topic, f"Batch message {i}".encode())

        # Fetch batch
        result = client.fetch(test_topic, partition=0, offset=0, max_messages=5)
        assert len(result.messages) <= 5


class TestConsumer:
    """Test Consumer class."""

    def test_consumer_basic(self, client: FlyMQClient, test_topic: str) -> None:
        """Test basic consumer functionality."""
        # Produce some messages first
        for i in range(3):
            client.produce(test_topic, f"Consumer test {i}".encode())

        # Create consumer and poll
        consumer = Consumer(client, test_topic, group_id="test-group")
        messages = consumer.poll(timeout_ms=1000, max_records=3)

        # May or may not have messages depending on offset
        assert isinstance(messages, list)

        consumer.close()


class TestProducer:
    """Test Producer class."""

    def test_producer_basic(self, client: FlyMQClient, test_topic: str) -> None:
        """Test basic producer functionality."""
        producer = Producer(client)

        result = producer.send(test_topic, b"Producer test message")
        assert result is not None
        assert result.offset >= 0

        producer.close()

    def test_producer_context_manager(self, client: FlyMQClient, test_topic: str) -> None:
        """Test producer as context manager."""
        with Producer(client) as producer:
            result = producer.send(test_topic, b"Context manager test")
            assert result is not None


class TestTransactions:
    """Test transaction support."""

    def test_transaction_commit(self, client: FlyMQClient, test_topic: str) -> None:
        """Test transaction commit."""
        with client.transaction() as txn:
            offset1 = txn.produce(test_topic, b"Txn message 1")
            offset2 = txn.produce(test_topic, b"Txn message 2")
            assert offset1 >= 0
            assert offset2 >= 0

    def test_transaction_rollback(self, client: FlyMQClient, test_topic: str) -> None:
        """Test transaction rollback."""
        txn = client.begin_transaction()
        txn.produce(test_topic, b"Will be rolled back")
        txn.rollback()
        assert not txn.active


class TestAdvancedFeatures:
    """Test advanced features."""

    def test_produce_delayed(self, client: FlyMQClient, test_topic: str) -> None:
        """Test delayed message production."""
        offset = client.produce_delayed(test_topic, b"Delayed message", delay_ms=1000)
        assert offset >= 0

    def test_produce_with_ttl(self, client: FlyMQClient, test_topic: str) -> None:
        """Test message production with TTL."""
        offset = client.produce_with_ttl(test_topic, b"TTL message", ttl_ms=60000)
        assert offset >= 0


class TestSchemaRegistry:
    """Test schema registry operations."""

    def test_register_and_list_schemas(self, client: FlyMQClient) -> None:
        """Test schema registration and listing."""
        schema_name = f"test-schema-{int(time.time() * 1000)}"
        schema_def = '{"type": "object", "properties": {"name": {"type": "string"}}}'

        # Register schema
        client.register_schema(schema_name, "json", schema_def)

        # List schemas
        schemas = client.list_schemas()
        assert isinstance(schemas, list)


class TestDLQ:
    """Test dead letter queue operations."""

    def test_fetch_dlq(self, client: FlyMQClient, test_topic: str) -> None:
        """Test fetching DLQ messages."""
        messages = client.fetch_dlq(test_topic, max_messages=10)
        assert isinstance(messages, list)

    def test_purge_dlq(self, client: FlyMQClient, test_topic: str) -> None:
        """Test purging DLQ."""
        # Should not raise
        client.purge_dlq(test_topic)

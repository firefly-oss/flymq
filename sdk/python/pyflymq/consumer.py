# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
FlyMQ Consumer implementation.

Provides high-level consumer abstractions similar to Kafka:
- Consumer: Single consumer with manual offset management
- ConsumerGroup: Managed consumer group with automatic offset commits
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

from .exceptions import ConsumerError
from .protocol import SubscribeMode
from .types import ConsumedMessage, ConsumerConfig, FetchResult

if TYPE_CHECKING:
    from .client import FlyMQClient


class Consumer:
    """
    FlyMQ Consumer for consuming messages from topics.

    Supports both single message consumption and batch fetching.

    Example:
        >>> consumer = Consumer(client, "my-topic", group_id="my-group")
        >>> for message in consumer:
        ...     print(message.decode())
        ...     consumer.commit()
    """

    def __init__(
        self,
        client: FlyMQClient,
        topic: str,
        *,
        partition: int = 0,
        group_id: str | None = None,
        config: ConsumerConfig | None = None,
    ) -> None:
        """
        Initialize consumer.

        Args:
            client: FlyMQ client instance.
            topic: Topic to consume from.
            partition: Partition to consume from.
            group_id: Consumer group ID (optional).
            config: Consumer configuration.
        """
        self._client = client
        self._topic = topic
        self._partition = partition
        self._group_id = group_id or ""
        self._config = config or ConsumerConfig(group_id=group_id)
        self._offset = 0
        self._closed = False
        self._lock = threading.Lock()

        # Subscribe if group_id provided
        if self._group_id:
            mode = (
                SubscribeMode.EARLIEST
                if self._config.auto_offset_reset == "earliest"
                else SubscribeMode.LATEST
            )
            self._offset = client.subscribe(topic, self._group_id, partition, mode)

    @property
    def topic(self) -> str:
        """Get topic name."""
        return self._topic

    @property
    def partition(self) -> int:
        """Get partition number."""
        return self._partition

    @property
    def group_id(self) -> str:
        """Get consumer group ID."""
        return self._group_id

    @property
    def offset(self) -> int:
        """Get current offset."""
        return self._offset

    def poll(self, timeout_ms: int = 1000, max_records: int | None = None) -> list[ConsumedMessage]:
        """
        Poll for messages.

        Args:
            timeout_ms: Maximum time to wait for messages.
            max_records: Maximum records to return.

        Returns:
            List of consumed messages.
        """
        if self._closed:
            raise ConsumerError("Consumer is closed")

        max_records = max_records or self._config.max_poll_records

        with self._lock:
            result = self._client.fetch(
                self._topic,
                self._partition,
                self._offset,
                max_records,
            )

            if result.messages:
                self._offset = result.next_offset

            return result.messages

    def consume(self, timeout_ms: int = 1000) -> ConsumedMessage | None:
        """
        Consume a single message.

        Args:
            timeout_ms: Maximum time to wait.

        Returns:
            Consumed message or None if no message available.
        """
        messages = self.poll(timeout_ms, max_records=1)
        return messages[0] if messages else None

    def commit(self, offset: int | None = None) -> None:
        """
        Commit the current offset.

        Args:
            offset: Offset to commit (defaults to current offset).
        """
        if not self._group_id:
            return  # No-op without group

        commit_offset = offset if offset is not None else self._offset

        with self._lock:
            self._client.commit_offset(
                self._topic,
                self._group_id,
                self._partition,
                commit_offset,
            )

    def seek(self, offset: int) -> None:
        """
        Seek to a specific offset.

        Args:
            offset: Offset to seek to.
        """
        with self._lock:
            self._offset = offset

    def seek_to_beginning(self) -> None:
        """Seek to the beginning of the partition."""
        self.seek(0)

    def seek_to_end(self) -> None:
        """
        Seek to the end of the partition (latest offset).

        This positions the consumer to receive only new messages.
        """
        # Subscribe with latest mode to get the latest offset
        if self._group_id:
            latest_offset = self._client.subscribe(
                self._topic, self._group_id, self._partition, SubscribeMode.LATEST
            )
            with self._lock:
                self._offset = latest_offset

    def get_position(self) -> int:
        """
        Get the current position (offset) of the consumer.

        Returns:
            The current offset position.
        """
        return self._offset

    def get_committed_offset(self) -> int | None:
        """
        Get the last committed offset for this consumer's group.

        Returns:
            The committed offset, or None if no group is configured.
        """
        if not self._group_id:
            return None
        return self._client.get_committed_offset(
            self._topic, self._group_id, self._partition
        )

    def get_lag(self) -> int | None:
        """
        Get the consumer lag (messages behind the latest offset).

        Returns:
            The lag in number of messages, or None if no group is configured.
        """
        if not self._group_id:
            return None
        lag_info = self._client.get_lag(
            self._topic, self._group_id, self._partition
        )
        return lag_info.get("lag", 0)

    def close(self) -> None:
        """Close the consumer."""
        self._closed = True

    def __iter__(self) -> Iterator[ConsumedMessage]:
        """Iterate over messages."""
        while not self._closed:
            messages = self.poll()
            yield from messages

    def __enter__(self) -> Consumer:
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.close()


class ConsumerGroup:
    """
    Managed consumer group with automatic offset commits and rebalancing.

    Example:
        >>> def process_message(msg: ConsumedMessage) -> None:
        ...     print(f"Received: {msg.decode()}")
        ...
        >>> group = ConsumerGroup(
        ...     client, "my-topic",
        ...     group_id="my-group",
        ...     handler=process_message
        ... )
        >>> group.start()
        >>> # ... later
        >>> group.stop()
    """

    def __init__(
        self,
        client: FlyMQClient,
        topics: str | list[str],
        *,
        group_id: str,
        handler: Callable[[ConsumedMessage], None] | None = None,
        config: ConsumerConfig | None = None,
    ) -> None:
        """
        Initialize consumer group.

        Args:
            client: FlyMQ client instance.
            topics: Topic or list of topics to consume from.
            group_id: Consumer group ID.
            handler: Message handler callback.
            config: Consumer configuration.
        """
        self._client = client
        self._topics = [topics] if isinstance(topics, str) else list(topics)
        self._group_id = group_id
        self._handler = handler
        self._config = config or ConsumerConfig(group_id=group_id)
        self._consumers: list[Consumer] = []
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._messages: list[ConsumedMessage] = []

    @property
    def group_id(self) -> str:
        """Get consumer group ID."""
        return self._group_id

    @property
    def topics(self) -> list[str]:
        """Get subscribed topics."""
        return self._topics

    @property
    def running(self) -> bool:
        """Check if consumer group is running."""
        return self._running

    def start(self) -> None:
        """Start consuming messages in a background thread."""
        if self._running:
            return

        self._running = True

        # Create consumers for each topic
        for topic in self._topics:
            consumer = Consumer(
                self._client,
                topic,
                group_id=self._group_id,
                config=self._config,
            )
            self._consumers.append(consumer)

        # Start background thread
        self._thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """
        Stop consuming messages.

        Args:
            timeout: Maximum time to wait for shutdown.
        """
        self._running = False

        if self._thread:
            self._thread.join(timeout)
            self._thread = None

        for consumer in self._consumers:
            consumer.close()
        self._consumers.clear()

    def poll(self, timeout_ms: int = 1000) -> list[ConsumedMessage]:
        """
        Poll for messages (when not using handler).

        Args:
            timeout_ms: Maximum time to wait.

        Returns:
            List of consumed messages.
        """
        with self._lock:
            messages = self._messages.copy()
            self._messages.clear()
            return messages

    def _consume_loop(self) -> None:
        """Background consume loop."""
        auto_commit_interval = self._config.auto_commit_interval_ms / 1000.0
        last_commit = time.time()

        while self._running:
            for consumer in self._consumers:
                if not self._running:
                    break

                try:
                    messages = consumer.poll(timeout_ms=100)

                    for msg in messages:
                        if self._handler:
                            try:
                                self._handler(msg)
                            except Exception:
                                # Log error but continue processing
                                pass
                        else:
                            with self._lock:
                                self._messages.append(msg)

                    # Auto-commit if enabled
                    if self._config.enable_auto_commit:
                        now = time.time()
                        if now - last_commit >= auto_commit_interval:
                            consumer.commit()
                            last_commit = now

                except Exception:
                    # Brief pause on error
                    time.sleep(0.1)

    def get_lag(self) -> dict[str, int]:
        """
        Get consumer lag for all subscribed topics.

        Returns:
            Dictionary mapping topic names to their lag values.
        """
        lag_info = {}
        for topic in self._topics:
            try:
                result = self._client.get_lag(topic, self._group_id, 0)
                lag_info[topic] = result.get("lag", 0)
            except Exception:
                lag_info[topic] = -1  # Unknown
        return lag_info

    def reset_offsets(self, mode: str = "earliest") -> None:
        """
        Reset offsets for all subscribed topics.

        Args:
            mode: Reset mode ("earliest" or "latest").
        """
        for topic in self._topics:
            self._client.reset_offset(topic, self._group_id, 0, mode)

    def reset_offsets_to_earliest(self) -> None:
        """Reset offsets to the earliest position for all topics."""
        self.reset_offsets("earliest")

    def reset_offsets_to_latest(self) -> None:
        """Reset offsets to the latest position for all topics."""
        self.reset_offsets("latest")

    def describe(self) -> dict[str, Any]:
        """
        Get detailed information about this consumer group.

        Returns:
            Dictionary with group information.
        """
        return self._client.describe_consumer_group(self._group_id)

    def __enter__(self) -> ConsumerGroup:
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.stop()

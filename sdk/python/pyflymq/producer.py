# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
FlyMQ Producer implementation.

Provides high-level producer abstractions:
- Producer: Basic producer with optional batching
- AsyncProducer: Asynchronous producer with callbacks
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any

from .types import ProducerConfig, ProduceResult

if TYPE_CHECKING:
    from .client import FlyMQClient


@dataclass
class ProduceRequest:
    """Internal request for producer queue."""

    topic: str
    data: bytes
    callback: Callable[[ProduceResult | None, Exception | None], None] | None = None
    timestamp: float = field(default_factory=time.time)


class Producer:
    """
    FlyMQ Producer for sending messages to topics.

    Supports batching for improved throughput.

    Example:
        >>> producer = Producer(client)
        >>> producer.send("my-topic", b"Hello!")
        >>> producer.flush()  # Ensure all messages are sent
    """

    def __init__(
        self,
        client: FlyMQClient,
        *,
        config: ProducerConfig | None = None,
    ) -> None:
        """
        Initialize producer.

        Args:
            client: FlyMQ client instance.
            config: Producer configuration.
        """
        self._client = client
        self._config = config or ProducerConfig()
        self._closed = False
        self._lock = threading.Lock()
        self._batch: list[ProduceRequest] = []
        self._batch_size = 0
        self._last_flush = time.time()

    def send(
        self,
        topic: str,
        data: bytes | str,
        *,
        callback: Callable[[ProduceResult | None, Exception | None], None] | None = None,
    ) -> ProduceResult | None:
        """
        Send a message to a topic.

        If linger_ms > 0, the message may be batched.

        Args:
            topic: Target topic.
            data: Message data.
            callback: Optional callback for async notification.

        Returns:
            ProduceResult if sent immediately, None if batched.
        """
        if self._closed:
            raise RuntimeError("Producer is closed")

        if isinstance(data, str):
            data = data.encode("utf-8")

        request = ProduceRequest(topic=topic, data=data, callback=callback)

        with self._lock:
            # If no batching, send immediately
            if self._config.linger_ms == 0:
                return self._send_immediate(request)

            # Add to batch
            self._batch.append(request)
            self._batch_size += len(data)

            # Check if batch should be flushed
            should_flush = (
                self._batch_size >= self._config.batch_size
                or len(self._batch) >= self._config.max_batch_messages
            )

            if should_flush:
                self._flush_batch()

        return None

    def _send_immediate(self, request: ProduceRequest) -> ProduceResult:
        """Send a message immediately."""
        try:
            offset = self._client.produce(request.topic, request.data)
            result = ProduceResult(topic=request.topic, offset=offset)

            if request.callback:
                request.callback(result, None)

            return result
        except Exception as e:
            if request.callback:
                request.callback(None, e)
            raise

    def _flush_batch(self) -> None:
        """Flush the current batch."""
        if not self._batch:
            return

        for request in self._batch:
            try:
                offset = self._client.produce(request.topic, request.data)
                result = ProduceResult(topic=request.topic, offset=offset)

                if request.callback:
                    request.callback(result, None)
            except Exception as e:
                if request.callback:
                    request.callback(None, e)

        self._batch.clear()
        self._batch_size = 0
        self._last_flush = time.time()

    def flush(self, timeout_ms: int = 10000) -> None:
        """
        Flush all pending messages.

        Args:
            timeout_ms: Maximum time to wait.
        """
        with self._lock:
            self._flush_batch()

    def close(self, timeout_ms: int = 10000) -> None:
        """
        Close the producer.

        Args:
            timeout_ms: Maximum time to wait for pending messages.
        """
        self.flush(timeout_ms)
        self._closed = True

    def __enter__(self) -> Producer:
        """Context manager entry."""
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.close()


class AsyncProducer:
    """
    Asynchronous FlyMQ Producer with background sending.

    Messages are queued and sent in a background thread.

    Example:
        >>> def on_complete(result, error):
        ...     if error:
        ...         print(f"Error: {error}")
        ...     else:
        ...         print(f"Sent at offset {result.offset}")
        ...
        >>> producer = AsyncProducer(client)
        >>> producer.start()
        >>> producer.send("my-topic", b"Hello!", callback=on_complete)
        >>> producer.stop()
    """

    def __init__(
        self,
        client: FlyMQClient,
        *,
        config: ProducerConfig | None = None,
        queue_size: int = 10000,
    ) -> None:
        """
        Initialize async producer.

        Args:
            client: FlyMQ client instance.
            config: Producer configuration.
            queue_size: Maximum queue size.
        """
        self._client = client
        self._config = config or ProducerConfig()
        self._queue: Queue[ProduceRequest | None] = Queue(maxsize=queue_size)
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the background sender thread."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._send_loop, daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        """
        Stop the producer.

        Args:
            timeout: Maximum time to wait for pending messages.
        """
        self._running = False

        # Signal thread to stop
        self._queue.put(None)

        if self._thread:
            self._thread.join(timeout)
            self._thread = None

    def send(
        self,
        topic: str,
        data: bytes | str,
        *,
        callback: Callable[[ProduceResult | None, Exception | None], None] | None = None,
    ) -> None:
        """
        Queue a message for sending.

        Args:
            topic: Target topic.
            data: Message data.
            callback: Callback for completion notification.
        """
        if not self._running:
            raise RuntimeError("Producer is not running")

        if isinstance(data, str):
            data = data.encode("utf-8")

        request = ProduceRequest(topic=topic, data=data, callback=callback)
        self._queue.put(request)

    def _send_loop(self) -> None:
        """Background send loop."""
        batch: list[ProduceRequest] = []
        batch_size = 0
        last_flush = time.time()

        while self._running or not self._queue.empty():
            try:
                # Get request with timeout
                request = self._queue.get(timeout=0.1)

                if request is None:
                    # Shutdown signal
                    break

                batch.append(request)
                batch_size += len(request.data)

                # Check if we should flush
                should_flush = (
                    batch_size >= self._config.batch_size
                    or len(batch) >= self._config.max_batch_messages
                    or (
                        self._config.linger_ms > 0
                        and (time.time() - last_flush) * 1000 >= self._config.linger_ms
                    )
                )

                if should_flush:
                    self._flush_batch(batch)
                    batch = []
                    batch_size = 0
                    last_flush = time.time()

            except Empty:
                # Check linger timeout
                if batch and self._config.linger_ms > 0:
                    if (time.time() - last_flush) * 1000 >= self._config.linger_ms:
                        self._flush_batch(batch)
                        batch = []
                        batch_size = 0
                        last_flush = time.time()

        # Final flush
        if batch:
            self._flush_batch(batch)

    def _flush_batch(self, batch: list[ProduceRequest]) -> None:
        """Flush a batch of messages."""
        for request in batch:
            try:
                offset = self._client.produce(request.topic, request.data)
                result = ProduceResult(topic=request.topic, offset=offset)

                if request.callback:
                    request.callback(result, None)
            except Exception as e:
                if request.callback:
                    request.callback(None, e)

    def __enter__(self) -> AsyncProducer:
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        """Context manager exit."""
        self.stop()

# Copyright (c) 2026 Firefly Software Solutions Inc.
# Licensed under the Apache License, Version 2.0

"""
Reactive Streams support for FlyMQ Python SDK.

Provides RxPY-based reactive programming patterns for message consumption
and production, enabling backpressure handling and stream composition.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Callable, TypeVar

import reactivex as rx
from reactivex import Observable, Subject, operators as ops
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.scheduler.eventloop import AsyncIOScheduler

from .models import ConsumedMessage, FetchResult, ProduceResult

if TYPE_CHECKING:
    from .client import FlyMQClient


T = TypeVar("T")


class ReactiveConsumer:
    """
    Reactive message consumer using RxPY Observable streams.
    
    Provides backpressure-aware message consumption with stream composition.
    
    Example:
        >>> consumer = ReactiveConsumer(client, "my-topic", "my-group")
        >>> consumer.messages().pipe(
        ...     ops.filter(lambda m: b"important" in m.data),
        ...     ops.map(lambda m: m.decode()),
        ...     ops.buffer_with_count(10),
        ... ).subscribe(on_next=process_batch)
    """
    
    def __init__(
        self,
        client: FlyMQClient,
        topic: str,
        group_id: str,
        partition: int = 0,
        poll_interval_ms: int = 100,
        max_poll_records: int = 100,
    ) -> None:
        """
        Initialize reactive consumer.
        
        Args:
            client: FlyMQ client instance.
            topic: Topic to consume from.
            group_id: Consumer group ID.
            partition: Partition to consume from.
            poll_interval_ms: Polling interval in milliseconds.
            max_poll_records: Maximum records per poll.
        """
        self._client = client
        self._topic = topic
        self._group_id = group_id
        self._partition = partition
        self._poll_interval_ms = poll_interval_ms
        self._max_poll_records = max_poll_records
        self._running = False
        self._subject: Subject[ConsumedMessage] = Subject()
        self._scheduler = ThreadPoolScheduler(max_workers=4)
    
    def start(self, start_offset: int | None = None) -> None:
        """Start consuming messages."""
        if self._running:
            return
        
        self._running = True
        
        def poll_loop() -> None:
            offset = start_offset or 0
            while self._running:
                try:
                    result = self._client.fetch(
                        self._topic,
                        self._partition,
                        offset,
                        self._max_poll_records
                    )
                    for msg in result.messages:
                        self._subject.on_next(msg)
                    offset = result.next_offset
                except Exception as e:
                    self._subject.on_error(e)
                    break
                
                import time
                time.sleep(self._poll_interval_ms / 1000)
        
        import threading
        self._thread = threading.Thread(target=poll_loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop consuming messages."""
        self._running = False
        self._subject.on_completed()
    
    def messages(self) -> Observable[ConsumedMessage]:
        """
        Get observable stream of messages.
        
        Returns:
            Observable stream of ConsumedMessage objects.
        """
        return self._subject.pipe(ops.observe_on(self._scheduler))
    
    def __enter__(self) -> ReactiveConsumer:
        self.start()
        return self
    
    def __exit__(self, *args: Any) -> None:
        self.stop()


class ReactiveProducer:
    """
    Reactive message producer using RxPY Observable streams.
    
    Enables stream-based message production with backpressure.
    
    Example:
        >>> producer = ReactiveProducer(client, "my-topic")
        >>> source = rx.of("msg1", "msg2", "msg3")
        >>> source.pipe(
        ...     ops.map(lambda s: s.encode()),
        ...     producer.publish(),
        ... ).subscribe(on_next=lambda r: print(f"Published at {r.offset}"))
    """
    
    def __init__(
        self,
        client: FlyMQClient,
        topic: str,
        buffer_size: int = 100,
    ) -> None:
        """
        Initialize reactive producer.
        
        Args:
            client: FlyMQ client instance.
            topic: Topic to produce to.
            buffer_size: Buffer size for backpressure.
        """
        self._client = client
        self._topic = topic
        self._buffer_size = buffer_size
        self._scheduler = ThreadPoolScheduler(max_workers=4)
    
    def publish(self) -> Callable[[Observable[bytes]], Observable[ProduceResult]]:
        """
        Create an operator to publish messages.
        
        Returns:
            Operator function for use with pipe().
        """
        def _publish(source: Observable[bytes]) -> Observable[ProduceResult]:
            def subscribe(observer: Any, scheduler: Any = None) -> Any:
                def on_next(data: bytes) -> None:
                    try:
                        offset = self._client.produce(self._topic, data)
                        result = ProduceResult(
                            topic=self._topic,
                            offset=offset,
                            partition=0
                        )
                        observer.on_next(result)
                    except Exception as e:
                        observer.on_error(e)
                
                return source.subscribe(
                    on_next=on_next,
                    on_error=observer.on_error,
                    on_completed=observer.on_completed,
                    scheduler=scheduler
                )
            
            return rx.create(subscribe)
        
        return _publish
    
    def publish_batch(
        self,
        batch_size: int = 10,
        timeout_ms: int = 1000
    ) -> Callable[[Observable[bytes]], Observable[list[ProduceResult]]]:
        """
        Create an operator to publish messages in batches.
        
        Args:
            batch_size: Maximum batch size.
            timeout_ms: Maximum time to wait for batch.
            
        Returns:
            Operator function for use with pipe().
        """
        def _publish_batch(source: Observable[bytes]) -> Observable[list[ProduceResult]]:
            return source.pipe(
                ops.buffer_with_time_or_count(
                    timespan=timeout_ms / 1000,
                    count=batch_size
                ),
                ops.filter(lambda batch: len(batch) > 0),
                ops.map(lambda batch: [
                    ProduceResult(
                        topic=self._topic,
                        offset=self._client.produce(self._topic, data),
                        partition=0
                    )
                    for data in batch
                ])
            )
        
        return _publish_batch


class AsyncReactiveConsumer:
    """
    Async reactive message consumer for asyncio integration.
    
    Example:
        >>> async with AsyncReactiveConsumer(client, "topic", "group") as consumer:
        ...     async for message in consumer:
        ...         print(message.decode())
    """
    
    def __init__(
        self,
        client: FlyMQClient,
        topic: str,
        group_id: str,
        partition: int = 0,
        poll_interval_ms: int = 100,
        max_poll_records: int = 100,
    ) -> None:
        self._client = client
        self._topic = topic
        self._group_id = group_id
        self._partition = partition
        self._poll_interval_ms = poll_interval_ms
        self._max_poll_records = max_poll_records
        self._running = False
        self._queue: asyncio.Queue[ConsumedMessage | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
    
    async def start(self, start_offset: int | None = None) -> None:
        """Start consuming messages asynchronously."""
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._poll_loop(start_offset or 0))
    
    async def _poll_loop(self, start_offset: int) -> None:
        """Internal polling loop."""
        offset = start_offset
        executor = ThreadPoolExecutor(max_workers=1)
        loop = asyncio.get_event_loop()
        
        while self._running:
            try:
                # Run blocking fetch in executor
                result = await loop.run_in_executor(
                    executor,
                    lambda: self._client.fetch(
                        self._topic,
                        self._partition,
                        offset,
                        self._max_poll_records
                    )
                )
                
                for msg in result.messages:
                    await self._queue.put(msg)
                offset = result.next_offset
                
            except Exception:
                break
            
            await asyncio.sleep(self._poll_interval_ms / 1000)
        
        await self._queue.put(None)  # Signal completion
        executor.shutdown(wait=False)
    
    async def stop(self) -> None:
        """Stop consuming messages."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    def __aiter__(self) -> AsyncReactiveConsumer:
        return self
    
    async def __anext__(self) -> ConsumedMessage:
        msg = await self._queue.get()
        if msg is None:
            raise StopAsyncIteration
        return msg
    
    async def __aenter__(self) -> AsyncReactiveConsumer:
        await self.start()
        return self
    
    async def __aexit__(self, *args: Any) -> None:
        await self.stop()


def from_consumer(
    client: FlyMQClient,
    topic: str,
    group_id: str,
    partition: int = 0,
    poll_interval_ms: int = 100,
) -> Observable[ConsumedMessage]:
    """
    Create an Observable from a FlyMQ consumer.
    
    Args:
        client: FlyMQ client instance.
        topic: Topic to consume from.
        group_id: Consumer group ID.
        partition: Partition to consume from.
        poll_interval_ms: Polling interval in milliseconds.
        
    Returns:
        Observable stream of consumed messages.
    """
    consumer = ReactiveConsumer(
        client, topic, group_id, partition, poll_interval_ms
    )
    consumer.start()
    return consumer.messages()


def to_producer(
    client: FlyMQClient,
    topic: str,
) -> Callable[[Observable[bytes]], Observable[ProduceResult]]:
    """
    Create a producer operator for publishing messages.
    
    Args:
        client: FlyMQ client instance.
        topic: Topic to produce to.
        
    Returns:
        Operator function for use with pipe().
    """
    producer = ReactiveProducer(client, topic)
    return producer.publish()

#!/usr/bin/env python3
"""
10_advanced_patterns.py - Advanced Usage Patterns

This example demonstrates:
- Batch processing with context managers
- Connection pooling for multiple topics
- Custom error recovery strategies
- Performance optimization patterns
- Dead letter queues (DLQ)
- TTL and delayed messages

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 10_advanced_patterns.py
"""

from pyflymq import FlyMQClient
import json
import time


def batch_processing():
    """Batch processing with transactions"""
    print("Batch Processing with Transactions")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        try:
            client.create_topic("batch-topic", partitions=1)
        except:
            pass
        
        # Process batch of items
        items = [
            {"id": i, "value": i * 100} for i in range(10)
        ]
        
        print(f"Processing {len(items)} items in batch...")
        
        # Use transaction for atomic batch
        with client.transaction() as txn:
            for item in items:
                msg = json.dumps(item).encode()
                txn.produce("batch-topic", msg)
        
        print(f"✓ {len(items)} messages produced atomically")
        
        # Process batch of consumed messages
        print("\nConsuming messages in batches...")
        batch_size = 5
        batch = []
        
        for i in range(len(items)):
            msg = client.consume("batch-topic", offset=i)
            batch.append(msg)
            
            if len(batch) >= batch_size:
                print(f"  Processing batch of {len(batch)} messages")
                batch.clear()
        
    finally:
        client.close()


def multi_topic_producer():
    """Producing to multiple topics efficiently"""
    print("\nMulti-Topic Producer Pattern")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        topics = ["topic-a", "topic-b", "topic-c"]
        
        # Create topics
        for topic in topics:
            try:
                client.create_topic(topic, partitions=1)
            except:
                pass
        
        print(f"Producing to {len(topics)} topics...")
        
        # Produce to multiple topics
        for i in range(5):
            for topic in topics:
                msg = json.dumps({
                    "message_num": i,
                    "topic": topic
                }).encode()
                client.produce(topic, msg)
        
        print(f"✓ Messages produced to all topics")
        
    finally:
        client.close()


def ttl_and_delayed_messages():
    """Using TTL and delayed message delivery"""
    print("\nTTL and Delayed Message Delivery")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        try:
            client.create_topic("expiring-messages", partitions=1)
        except:
            pass
        
        # Produce message with TTL (expires in 30 seconds)
        print("Producing message with 30 second TTL...")
        msg = b"This message will expire in 30 seconds"
        try:
            offset = client.produce_with_ttl(
                "expiring-messages",
                msg,
                ttl_ms=30000
            )
            print(f"✓ Message produced at offset {offset}")
        except AttributeError:
            print("  Note: TTL API may not be available in this version")
        
        # Produce delayed message (5 second delay)
        print("\nProducing message with 5 second delay...")
        delayed_msg = b"This message will appear after 5 seconds"
        try:
            offset = client.produce_delayed(
                "expiring-messages",
                delayed_msg,
                delay_ms=5000
            )
            print(f"✓ Delayed message scheduled at offset {offset}")
        except AttributeError:
            print("  Note: Delayed API may not be available in this version")
        
    finally:
        client.close()


def deadletter_queue_handling():
    """Working with Dead Letter Queues"""
    print("\nDead Letter Queue (DLQ) Handling")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        topic = "dlq-test"
        
        try:
            client.create_topic(topic, partitions=1)
        except:
            pass
        
        print("Working with DLQ...")
        
        # Fetch DLQ messages
        try:
            dlq_messages = client.fetch_dlq(topic, max_messages=10)
            print(f"✓ Fetched {len(dlq_messages)} DLQ messages")

            # Replay specific DLQ message
            if dlq_messages:
                msg = dlq_messages[0]
                print(f"  Replaying message ID: {msg.id}")
                print(f"    Error: {msg.error}")
                print(f"    Retries: {msg.retries}")
                # Replay the message back to the original topic
                # client.replay_dlq(topic, msg.id)
        except Exception as e:
            print(f"  Note: DLQ operations - {type(e).__name__}")
        
    finally:
        client.close()


def connection_pooling():
    """Pattern for connection pooling"""
    print("\nConnection Pooling Pattern")
    print("-" * 50)
    
    # Simple connection pool example
    class FlyMQPool:
        def __init__(self, bootstrap_servers, pool_size=3):
            self.bootstrap_servers = bootstrap_servers
            self.pool_size = pool_size
            self.connections = [
                FlyMQClient(bootstrap_servers) for _ in range(pool_size)
            ]
            self.current_idx = 0
        
        def get_client(self):
            """Get next client from pool (round-robin)"""
            client = self.connections[self.current_idx]
            self.current_idx = (self.current_idx + 1) % self.pool_size
            return client
        
        def close_all(self):
            """Close all connections"""
            for conn in self.connections:
                conn.close()
    
    print("Creating connection pool with 3 connections...")
    pool = FlyMQPool("localhost:9092", pool_size=3)
    
    try:
        # Use clients from pool
        for i in range(5):
            client = pool.get_client()
            try:
                client.create_topic(f"pool-topic-{i}", partitions=1)
            except:
                pass
            print(f"✓ Used client {i % 3} from pool")
    
    finally:
        pool.close_all()
        print("✓ Pool closed")


def error_recovery_strategy():
    """Implementing error recovery strategies"""
    print("\nError Recovery Strategy")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        try:
            client.create_topic("recovery-test", partitions=1)
        except:
            pass
        
        print("Implementing exponential backoff retry...")
        
        def produce_with_backoff(topic, message, max_attempts=3):
            """Produce with exponential backoff"""
            backoff = 0.1  # Start with 100ms
            
            for attempt in range(max_attempts):
                try:
                    offset = client.produce(topic, message)
                    print(f"✓ Message produced at offset {offset}")
                    return offset
                except Exception as e:
                    if attempt < max_attempts - 1:
                        print(f"  Attempt {attempt + 1} failed, backing off {backoff}s")
                        time.sleep(backoff)
                        backoff *= 2  # Exponential backoff
                    else:
                        print(f"✗ Failed after {max_attempts} attempts")
                        raise
        
        produce_with_backoff("recovery-test", b"Test message")
    
    finally:
        client.close()


def main():
    print("=" * 50)
    print("FlyMQ Advanced Patterns Example")
    print("=" * 50)
    
    batch_processing()
    multi_topic_producer()
    ttl_and_delayed_messages()
    deadletter_queue_handling()
    connection_pooling()
    error_recovery_strategy()
    
    print("\n" + "=" * 50)
    print("✓ All advanced pattern examples completed!")
    print("\nKey Patterns:")
    print("  - Batch processing with transactions")
    print("  - Multi-topic production")
    print("  - TTL and message scheduling")
    print("  - Dead letter queue handling")
    print("  - Connection pooling")
    print("  - Exponential backoff retry")


if __name__ == "__main__":
    main()

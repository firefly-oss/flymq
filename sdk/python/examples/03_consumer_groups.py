#!/usr/bin/env python3
"""
03_consumer_groups.py - Consumer Groups Example with High-Level APIs

Demonstrates consumer groups using the HighLevelConsumer for coordinated parallel processing.

What are Consumer Groups?
    Consumer groups enable multiple consumers to share the work of processing
    messages from a topic. Each consumer in a group processes a subset of
    partitions, and the group automatically rebalances when consumers join
    or leave.

Key Benefits:
    - Automatic load distribution across consumers
    - Fault tolerance (if one consumer fails, others take over)
    - Auto-commit for simplified offset management
    - Horizontal scaling (add more consumers to process faster)

This example demonstrates:
    - Using connect() for one-liner connection
    - Using HighLevelProducer for efficient message sending
    - Using HighLevelConsumer with auto-commit
    - Poll-based consumption pattern

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 03_consumer_groups.py
"""

from pyflymq import connect
import time


def produce_messages(client, topic: str, count: int):
    """Produce messages using HighLevelProducer"""
    print(f"\n=== Producing {count} messages ===")

    with client.producer(batch_size=100) as producer:
        for i in range(count):
            message = f"Message {i+1}/{count}".encode()
            key = f"user-{i % 3}"  # Distribute across 3 keys

            producer.send(topic, message, key=key)
            print(f"  Sent: {message.decode()} (key={key})")

        producer.flush()

    print("Producer: Done!")


def consume_messages(client, topic: str, group_id: str, max_messages: int = 10):
    """Consume messages using HighLevelConsumer with auto-commit"""
    print(f"\n=== Consuming from group '{group_id}' ===")

    # HighLevelConsumer with auto-commit (default)
    with client.consumer(topic, group_id) as consumer:
        count = 0
        for message in consumer:
            count += 1
            print(f"  Received: Key={message.key}, Value={message.decode()}")
            # Auto-commit handles offset management automatically

            if count >= max_messages:
                break

    print(f"Consumer: Processed {count} messages!")


def consume_with_manual_commit(client, topic: str, group_id: str, max_messages: int = 5):
    """Consume messages with manual commit for at-least-once semantics"""
    print(f"\n=== Consuming with manual commit ===")

    # Disable auto-commit for manual control
    with client.consumer(topic, group_id, auto_commit=False) as consumer:
        count = 0
        for message in consumer:
            count += 1
            print(f"  Processing: {message.decode()}")

            # Simulate processing
            time.sleep(0.1)

            # Commit after successful processing
            consumer.commit()
            print(f"  ✓ Committed offset")

            if count >= max_messages:
                break

    print(f"Consumer: Processed {count} messages with manual commit!")


def main():
    topic = "notifications"

    print("Consumer Groups Example with High-Level APIs")
    print("=" * 50)

    # Connect using one-liner
    client = connect("localhost:9092")

    try:
        # Create topic
        print(f"\nCreating topic '{topic}' with 2 partitions...")
        try:
            client.create_topic(topic, partitions=2)
        except Exception as e:
            print(f"Topic may already exist: {e}")

        # Produce messages
        produce_messages(client, topic, 10)

        # Consume with auto-commit (default)
        consume_messages(client, topic, "notifications-group", max_messages=5)

        # Consume with manual commit
        consume_with_manual_commit(client, topic, "notifications-group-manual", max_messages=5)

        print("\n✓ Consumer group example completed!")
        print("\nNote: In production, you would:")
        print("  - Run multiple consumer instances simultaneously")
        print("  - Partitions would be distributed among consumers")
        print("  - Use auto-commit for simplicity or manual commit for at-least-once")

    finally:
        client.close()


if __name__ == "__main__":
    main()

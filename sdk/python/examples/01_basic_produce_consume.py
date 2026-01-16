#!/usr/bin/env python3
"""
01_basic_produce_consume.py - FlyMQ Basic Producer and Consumer Example

This is the foundational example for understanding FlyMQ's high-level APIs.

What this example demonstrates:
- Using connect() for one-liner connection
- Using HighLevelProducer with batching and callbacks
- Using HighLevelConsumer with auto-commit
- Proper resource cleanup with context managers

Key Concepts:
- connect(): One-liner connection to FlyMQ
- producer(): Creates a HighLevelProducer with batching
- consumer(): Creates a HighLevelConsumer with auto-commit
- Offset: The position of a message in a topic partition

Prerequisites:
    - FlyMQ server running on localhost:9092 (default port)
    - pyflymq installed: pip install pyflymq

Expected Output:
    Connecting to FlyMQ at localhost:9092...
    Creating topic 'hello-world'...

    === Using High-Level Producer ===
    ✓ Sent to hello-world @ offset 0

    === Using High-Level Consumer ===
    Received: Key=user-123, Value=Hello, FlyMQ! This is my first message.

    ✓ Successfully produced and consumed a message!

Run with:
    python 01_basic_produce_consume.py
"""

from pyflymq import connect


def main():
    # Connect to FlyMQ using one-liner
    print("Connecting to FlyMQ at localhost:9092...")
    client = connect("localhost:9092")

    try:
        # Create a topic (optional, auto-created if not exists)
        print("Creating topic 'hello-world'...")
        try:
            client.create_topic("hello-world", partitions=1)
        except Exception as e:
            print(f"Topic may already exist: {e}")

        # === High-Level Producer ===
        print("\n=== Using High-Level Producer ===")
        with client.producer(batch_size=100) as producer:
            # Send with callback
            def on_success(metadata):
                print(f"✓ Sent to {metadata.topic} @ offset {metadata.offset}")

            producer.send(
                topic="hello-world",
                value=b"Hello, FlyMQ! This is my first message.",
                key="user-123",
                on_success=on_success
            )
            producer.flush()  # Ensure message is sent

        # === High-Level Consumer ===
        print("\n=== Using High-Level Consumer ===")
        with client.consumer("hello-world", "demo-group") as consumer:
            # Consume one message
            for msg in consumer:
                print(f"Received: Key={msg.key}, Value={msg.decode()}")
                break  # Just consume one message for demo

        print("\n✓ Successfully produced and consumed a message!")

    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()

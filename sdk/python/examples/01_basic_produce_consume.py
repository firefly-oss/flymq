#!/usr/bin/env python3
"""
01_basic_produce_consume.py - FlyMQ Basic Producer and Consumer Example

This is the foundational example for understanding FlyMQ's core operations.

What this example demonstrates:
- Creating and managing a FlyMQ client connection
- Producing a message to a topic
- Consuming the message back from the topic
- Proper resource cleanup

Key Concepts:
- Topic: A named channel for messages (like a queue or stream)
- Offset: The position of a message in a topic partition
- Producer: Sends messages to topics
- Consumer: Reads messages from topics

Prerequisites:
    - FlyMQ server running on localhost:9092 (default port)
    - pyflymq installed: pip install pyflymq

Expected Output:
    Connecting to FlyMQ at localhost:9092...
    Creating topic 'hello-world'...
    Producing message: Hello, FlyMQ! This is my first message.
    Message written at offset 0 in partition 0
    Consuming message from offset 0...
    Received: Hello, FlyMQ! This is my first message.
    ✓ Successfully produced and consumed a message!

Run with:
    python 01_basic_produce_consume.py
"""

from pyflymq import FlyMQClient


def main():
    # Connect to FlyMQ
    print("Connecting to FlyMQ at localhost:9092...")
    client = FlyMQClient("localhost:9092")

    try:
        # Create a topic (optional, auto-created if not exists)
        print("Creating topic 'hello-world'...")
        try:
            client.create_topic("hello-world", partitions=1)
        except Exception as e:
            print(f"Topic may already exist: {e}")

        # Produce a message
        message = b"Hello, FlyMQ! This is my first message."
        print(f"\nProducing message: {message.decode()}")
        offset = client.produce("hello-world", message)
        print(f"Message written at offset {offset} in partition 0")

        # Consume the message back
        print(f"\nConsuming message from offset {offset}...")
        consumed_msg = client.consume("hello-world", offset)
        print(f"Received: {consumed_msg.decode()}")
        print("\n✓ Successfully produced and consumed a message!")

    finally:
        # Always close the client
        client.close()


if __name__ == "__main__":
    main()

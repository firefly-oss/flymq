#!/usr/bin/env python3
"""
01_basic_produce_consume.py - FlyMQ Basic Producer and Consumer Example

This is the simplest example to get started with FlyMQ.
It demonstrates:
- Creating a FlyMQ client
- Producing a message to a topic
- Consuming the message from a topic

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed: pip install pyflymq

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

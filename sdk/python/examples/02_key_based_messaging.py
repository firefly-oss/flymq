#!/usr/bin/env python3
"""
02_key_based_messaging.py - Key-Based Messaging (Kafka-style Partitioning)

This example demonstrates:
- Producing messages with keys
- Messages with the same key go to the same partition
- Consuming messages and accessing the key
- Batch fetching messages with keys

Key-based partitioning ensures:
- Ordering: Messages with the same key are always processed in order
- Locality: Related messages stay together in the same partition

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 02_key_based_messaging.py
"""

from pyflymq import FlyMQClient
import json


def main():
    client = FlyMQClient("localhost:9092")

    try:
        # Create topic with 3 partitions
        topic = "orders"
        print(f"Creating topic '{topic}' with 3 partitions...")
        try:
            client.create_topic(topic, partitions=3)
        except Exception as e:
            print(f"Topic may already exist: {e}")

        # Produce messages with keys
        # Messages with the same key go to the same partition
        print(f"\nProducing messages with keys to topic '{topic}':")

        orders = [
            ("user-123", {"order_id": 1001, "item": "Laptop", "price": 999.99}),
            ("user-123", {"order_id": 1002, "item": "Mouse", "price": 29.99}),
            ("user-456", {"order_id": 2001, "item": "Monitor", "price": 299.99}),
            ("user-123", {"order_id": 1003, "item": "Keyboard", "price": 79.99}),
            ("user-789", {"order_id": 3001, "item": "Headphones", "price": 149.99}),
        ]

        offsets = []
        for key, order in orders:
            value = json.dumps(order).encode()
            offset = client.produce(topic, value, key=key)
            partition = offset // 1000  # Approximate for display
            print(f"  Key: {key:10} | Order: {order['order_id']} | Offset: {offset}")
            offsets.append(offset)

        # Consume messages individually
        print(f"\nConsuming messages individually:")
        for i, offset in enumerate(offsets):
            msg = client.consume(topic, offset)
            key = msg.decode_key() if msg.key else "None"
            value = json.loads(msg.decode())
            print(f"  Message {i}: Key={key}, Order={value['order_id']}")

        # Fetch messages in batch
        print(f"\nFetching messages in batch (from offset 0, max 10 messages):")
        result = client.fetch(topic, partition=0, offset=0, max_messages=10)
        print(f"Fetched {len(result.messages)} messages:")
        for msg in result.messages:
            key = msg.decode_key() if msg.key else "None"
            value = json.loads(msg.decode())
            print(f"  Offset: {msg.offset}, Key: {key}, Order: {value['order_id']}")

        print("\n✓ Key-based messaging example completed!")

    finally:
        client.close()


if __name__ == "__main__":
    main()

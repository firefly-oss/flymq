#!/usr/bin/env python3
"""
03_consumer_groups.py - Consumer Groups Example

Demonstrates consumer groups for coordinated parallel processing.

What are Consumer Groups?
    Consumer groups enable multiple consumers to share the work of processing
    messages from a topic. Each consumer in a group processes a subset of
    partitions, and the group automatically rebalances when consumers join
    or leave.

Key Benefits:
    - Automatic load distribution across consumers
    - Fault tolerance (if one consumer fails, others take over)
    - Offset management (group tracks progress)
    - Horizontal scaling (add more consumers to process faster)

Use Cases:
    - Parallel data processing pipelines
    - Distributed log aggregation
    - Real-time analytics systems
    - Microservice event processing

How It Works:
    1. Multiple consumers join the same group (same group_id)
    2. FlyMQ assigns partitions to consumers
    3. Each consumer processes its assigned partitions
    4. Offsets are committed to track progress
    5. On failure, partitions are reassigned

This example demonstrates:
    - Multiple consumers sharing a topic
    - Automatic offset management
    - Committing offsets after processing

Consumer groups allow:
    - Distributed processing: Multiple consumers process messages in parallel
    - Load balancing: Partitions are distributed among consumers
    - Fault tolerance: If a consumer fails, its partitions are reassigned

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 03_consumer_groups.py
"""

from pyflymq import FlyMQClient, Consumer
import time


def producer_thread(topic: str, count: int):
    """Produce messages to a topic"""
    client = FlyMQClient("localhost:9092")
    try:
        print(f"Producer: Sending {count} messages to topic '{topic}'...")
        for i in range(count):
            message = f"Message {i+1}/{count}".encode()
            offset = client.produce(topic, message)
            print(f"  Produced: {message.decode()} at offset {offset}")
            time.sleep(0.1)
        print("Producer: Done!")
    finally:
        client.close()


def consumer_thread(topic: str, group_id: str, consumer_id: str, max_messages: int = 5):
    """Consume messages from a consumer group"""
    client = FlyMQClient("localhost:9092")
    try:
        consumer = Consumer(client, topic, group_id=group_id)
        print(f"Consumer {consumer_id}: Starting to consume from group '{group_id}'...")
        
        count = 0
        for message in consumer:
            count += 1
            print(f"  Consumer {consumer_id}: {message.decode()}")
            consumer.commit()
            
            if count >= max_messages:
                break
        
        print(f"Consumer {consumer_id}: Done!")
    finally:
        client.close()


def main():
    topic = "notifications"
    
    print("Consumer Groups Example")
    print("=" * 50)
    
    # Create topic
    client = FlyMQClient("localhost:9092")
    try:
        print(f"\nCreating topic '{topic}' with 2 partitions...")
        try:
            client.create_topic(topic, partitions=2)
        except Exception as e:
            print(f"Topic may already exist: {e}")
    finally:
        client.close()
    
    # In a real scenario, you would run producers and consumers in separate processes
    # For this demo, we'll show the single-consumer case
    
    print(f"\nStarting producer...")
    producer_thread(topic, 10)
    
    print(f"\nStarting consumer with group 'notifications-group'...")
    consumer_thread(topic, "notifications-group", "consumer-1", max_messages=10)
    
    print("\n✓ Consumer group example completed!")
    print("\nNote: In production, you would:")
    print("  - Run multiple consumer instances simultaneously")
    print("  - Partitions would be distributed among consumers")
    print("  - Failed consumers would have their partitions reassigned")


if __name__ == "__main__":
    main()

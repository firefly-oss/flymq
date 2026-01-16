#!/usr/bin/env python3
"""
03_consumer_groups.py - Consumer Groups Example

This example demonstrates:
- Creating consumer groups
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

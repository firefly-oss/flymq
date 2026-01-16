#!/usr/bin/env python3
"""
06_reactive_streams.py - Reactive Streams / Message Processing

This example demonstrates:
- Consumer iteration over messages
- Message filtering and transformation
- Batch processing
- Error handling in message loops

Reactive patterns provide:
- Non-blocking I/O
- Composable operations
- Structured error handling

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 06_reactive_streams.py
"""

from pyflymq import FlyMQClient, ReactiveConsumer, ReactiveProducer
import json


def consumer_example():
    """Consumer iteration example"""
    print("Consumer Iteration Example")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        topic = "logs"
        try:
            client.create_topic(topic, partitions=1)
        except:
            pass
        
        # Produce some messages first
        print("Producing messages...")
        for i in range(5):
            level = "ERROR" if i % 2 == 0 else "INFO"
            msg = json.dumps({"level": level, "message": f"Message {i}"}).encode()
            client.produce(topic, msg)
        
        # Create consumer
        consumer = ReactiveConsumer(client, topic, "log-group")
        
        print("\nConsuming messages...")
        count = 0
        
        for message in consumer:
            count += 1
            data = json.loads(message.decode())
            print(f"  Message {count}: Level={data['level']}, {data['message']}")
            
            if count >= 5:
                break
        
        print("✓ Consumer iteration completed!")
        
    finally:
        client.close()


def advanced_pipeline():
    """Advanced message processing"""
    print("\nAdvanced Message Processing")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        topic = "transactions"
        try:
            client.create_topic(topic, partitions=1)
        except:
            pass
        
        # Produce transactions
        print("Producing transactions...")
        transactions = []
        for i in range(10):
            tx = {
                "id": i,
                "amount": 100 + (i * 10),
                "status": "completed"
            }
            msg = json.dumps(tx).encode()
            client.produce(topic, msg)
            transactions.append(tx)
        
        # Process transactions
        print("\nProcessing transactions:")
        print("  Filtering: amount >= 150")
        
        total = 0
        count = 0
        for tx in transactions:
            if tx["amount"] >= 150:
                count += 1
                total += tx["amount"]
                print(f"    Transaction {tx['id']}: ${tx['amount']}")
        
        print(f"\nSummary: {count} transactions, Total: ${total}")
        print("✓ Advanced processing completed!")
        
    finally:
        client.close()


def error_handling():
    """Error handling in message processing"""
    print("\nError Handling in Message Processing")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        topic = "processing"
        try:
            client.create_topic(topic, partitions=1)
        except:
            pass
        
        # Produce messages
        print("Producing messages...")
        for i in range(5):
            msg = f"Message {i}".encode()
            client.produce(topic, msg)
        
        # Process with error handling
        consumer = ReactiveConsumer(client, topic, "error-group")
        
        print("\nProcessing with error handling...")
        count = 0
        
        for message in consumer:
            try:
                text = message.decode()
                count += 1
                
                # Simulate error on message 3
                if "3" in text:
                    raise ValueError(f"Cannot process {text}")
                
                print(f"  ✓ Processed: {text.upper()}")
                
                if count >= 5:
                    break
                    
            except ValueError as e:
                print(f"  ✗ Error: {e}")
                count += 1
                if count >= 5:
                    break
        
        print("✓ Error handling completed!")
        
    finally:
        client.close()


def main():
    print("=" * 50)
    print("FlyMQ Message Processing Patterns")
    print("=" * 50)
    
    consumer_example()
    advanced_pipeline()
    error_handling()
    
    print("\n" + "=" * 50)
    print("✓ All examples completed!")
    print("\nKey Patterns:")
    print("  - Iteration: Loop over consumed messages")
    print("  - Filtering: Process specific messages")
    print("  - Transformation: Map and transform content")
    print("  - Error handling: Graceful failure handling")


if __name__ == "__main__":
    main()

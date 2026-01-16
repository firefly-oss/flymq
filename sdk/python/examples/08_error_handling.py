#!/usr/bin/env python3
"""
08_error_handling.py - Error Handling with Hints

This example demonstrates:
- Using connect() for one-liner connection
- Exception hints for actionable suggestions
- High-level producer/consumer error handling
- Retry logic with error hints

Key Feature: Exception Hints
    All FlyMQ exceptions include a `hint` property:

    try:
        client.produce("topic", data)
    except FlyMQError as e:
        print(f"Error: {e}")
        print(f"Hint: {e.hint}")  # Actionable suggestion

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 08_error_handling.py
"""

from pyflymq import connect
from pyflymq.exceptions import (
    FlyMQError, ConnectionError, ConnectionTimeoutError,
    TopicNotFoundError, MessageTooLargeError,
    ServerError, AuthenticationError
)
import time


def connection_error_handling():
    """Handling connection errors with hints"""
    print("Connection Error Handling")
    print("-" * 50)

    # Simulate connection to unavailable server
    try:
        print("Attempting to connect to invalid server...")
        client = connect("localhost:19999")
        client.close()
    except ConnectionTimeoutError as e:
        print(f"✓ Caught connection timeout: {e}")
        print(f"  Hint: {e.hint}")  # Actionable suggestion
    except ConnectionError as e:
        print(f"✓ Caught connection error: {e}")
        print(f"  Hint: {e.hint}")
    except Exception as e:
        print(f"✓ Caught error: {type(e).__name__}: {e}")


def producer_error_handling():
    """Handling producer errors with hints"""
    print("\nProducer Error Handling")
    print("-" * 50)

    client = connect("localhost:9092")
    try:
        # Create topic for testing
        try:
            client.create_topic("test-errors", partitions=1)
        except:
            pass

        # Using HighLevelProducer
        print("Producing with HighLevelProducer...")
        with client.producer() as producer:
            try:
                producer.send("test-errors", b"Valid message")
                producer.flush()
                print(f"✓ Message produced successfully")
            except FlyMQError as e:
                print(f"✗ Error: {e}")
                print(f"  Hint: {e.hint}")

        # Handle topic not found
        print("\nProducing to non-existent topic...")
        try:
            with client.producer() as producer:
                producer.send("non-existent-topic", b"Test")
                producer.flush()
        except TopicNotFoundError as e:
            print(f"✓ Caught TopicNotFoundError: {e}")
            print(f"  Hint: {e.hint}")
        except FlyMQError as e:
            print(f"✓ Caught error: {type(e).__name__}")
            print(f"  Hint: {e.hint}")

    finally:
        client.close()


def consumer_error_handling():
    """Handling consumer errors with hints"""
    print("\nConsumer Error Handling")
    print("-" * 50)

    client = connect("localhost:9092")
    try:
        try:
            client.create_topic("test-consume", partitions=1)
        except:
            pass

        # Produce some messages using HighLevelProducer
        with client.producer() as producer:
            for i in range(3):
                producer.send("test-consume", f"Message {i}".encode())
            producer.flush()

        # Consume with HighLevelConsumer
        print("Consuming with HighLevelConsumer...")
        try:
            with client.consumer("test-consume", "error-test-group") as consumer:
                count = 0
                for msg in consumer:
                    print(f"✓ Message: {msg.decode()}")
                    count += 1
                    if count >= 3:
                        break
        except FlyMQError as e:
            print(f"✗ Consume error: {e}")
            print(f"  Hint: {e.hint}")
    
    finally:
        client.close()


def demonstrate_error_hints():
    """Demonstrate the error hints feature"""
    print("\nError Hints Feature")
    print("-" * 50)

    print("All FlyMQ exceptions include a 'hint' property:")
    print()
    print("  try:")
    print("      client.produce('topic', data)")
    print("  except FlyMQError as e:")
    print("      print(f'Error: {e}')")
    print("      print(f'Hint: {e.hint}')  # Actionable suggestion")
    print()
    print("Example hints:")
    print("  - ConnectionError: 'Check that the broker is running'")
    print("  - AuthenticationError: 'Verify username and password'")
    print("  - TopicNotFoundError: 'Create the topic first'")
    print("  - MessageTooLargeError: 'Reduce message size'")


def context_manager_safety():
    """Using context managers for safe cleanup"""
    print("\nContext Manager Safety")
    print("-" * 50)

    def safe_producer():
        # connect() + producer() with context managers
        client = connect("localhost:9092")
        try:
            try:
                client.create_topic("test-context", partitions=1)
            except:
                pass

            print("Producing with HighLevelProducer...")
            with client.producer() as producer:
                producer.send("test-context", b"Safe message")
                producer.flush()
                print(f"✓ Message produced successfully")
        finally:
            client.close()

        print("✓ Client automatically closed")

    try:
        safe_producer()
    except FlyMQError as e:
        print(f"✗ Error: {e}")
        print(f"  Hint: {e.hint}")


def main():
    print("=" * 50)
    print("FlyMQ Error Handling with Hints")
    print("=" * 50)

    connection_error_handling()
    producer_error_handling()
    consumer_error_handling()
    demonstrate_error_hints()
    context_manager_safety()

    print("\n" + "=" * 50)
    print("✓ Error handling examples completed!")
    print("\nBest Practices:")
    print("  1. Always check e.hint for actionable suggestions")
    print("  2. Use context managers (with statement)")
    print("  3. Catch specific exceptions first")
    print("  4. Use FlyMQError as a fallback catch-all")
    print("  5. Log errors with context for debugging")


if __name__ == "__main__":
    main()

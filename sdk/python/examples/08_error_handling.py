#!/usr/bin/env python3
"""
08_error_handling.py - Error Handling Patterns

This example demonstrates:
- Connection error handling
- Message production error handling
- Consumer error handling
- Retry logic
- Graceful degradation

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 08_error_handling.py
"""

from pyflymq import FlyMQClient
from pyflymq.exceptions import (
    ConnectionError, ConnectionTimeoutError,
    TopicNotFoundError, MessageTooLargeError,
    ServerError, AuthenticationError
)
import time


def connection_error_handling():
    """Handling connection errors"""
    print("Connection Error Handling")
    print("-" * 50)
    
    # Simulate connection to unavailable server
    try:
        print("Attempting to connect to invalid server...")
        client = FlyMQClient("localhost:19999", connect_timeout_ms=2000)
        client.close()
    except ConnectionTimeoutError as e:
        print(f"✓ Caught connection timeout: {e}")
    except ConnectionError as e:
        print(f"✓ Caught connection error: {e}")
    except Exception as e:
        print(f"✓ Caught error: {type(e).__name__}: {e}")


def producer_error_handling():
    """Handling producer errors"""
    print("\nProducer Error Handling")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        # Create topic for testing
        try:
            client.create_topic("test-errors", partitions=1)
        except:
            pass
        
        # Successful message
        print("Producing valid message...")
        try:
            offset = client.produce("test-errors", b"Valid message")
            print(f"✓ Message produced at offset {offset}")
        except Exception as e:
            print(f"✗ Error: {e}")
        
        # Handle topic not found
        print("\nProducing to non-existent topic...")
        try:
            client.produce("non-existent-topic", b"Test")
        except TopicNotFoundError as e:
            print(f"✓ Caught TopicNotFoundError: {e}")
        except Exception as e:
            print(f"✓ Caught error: {type(e).__name__}")
        
    finally:
        client.close()


def consumer_error_handling():
    """Handling consumer errors"""
    print("\nConsumer Error Handling")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        try:
            client.create_topic("test-consume", partitions=1)
        except:
            pass
        
        # Produce some messages
        for i in range(3):
            client.produce("test-consume", f"Message {i}".encode())
        
        # Consume with error handling
        print("Consuming messages with error handling...")
        try:
            for i in range(10):
                try:
                    msg = client.consume("test-consume", offset=i)
                    print(f"✓ Message {i}: {msg.decode()}")
                except Exception as e:
                    if "offset" in str(e).lower():
                        print(f"✓ Reached end of topic at offset {i}")
                        break
                    raise
        except Exception as e:
            print(f"✗ Consume error: {e}")
    
    finally:
        client.close()


def retry_logic():
    """Implementing retry logic"""
    print("\nRetry Logic")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        try:
            client.create_topic("test-retry", partitions=1)
        except:
            pass
        
        # Produce with retry
        def produce_with_retry(topic, message, max_retries=3, delay=0.5):
            for attempt in range(max_retries):
                try:
                    offset = client.produce(topic, message)
                    print(f"✓ Message produced at offset {offset}")
                    return offset
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"  Attempt {attempt + 1} failed: {type(e).__name__}")
                        print(f"  Retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        print(f"✗ Failed after {max_retries} attempts")
                        raise
        
        print("Producing message with retry logic...")
        produce_with_retry("test-retry", b"Test message")
    
    finally:
        client.close()


def context_manager_safety():
    """Using context managers for safe cleanup"""
    print("\nContext Manager Safety")
    print("-" * 50)
    
    def safe_producer():
        # Context manager ensures cleanup even on error
        with FlyMQClient("localhost:9092") as client:
            try:
                client.create_topic("test-context", partitions=1)
            except:
                pass
            
            print("Producing message...")
            offset = client.produce("test-context", b"Safe message")
            print(f"✓ Message produced at offset {offset}")
        
        print("✓ Client automatically closed")
    
    try:
        safe_producer()
    except Exception as e:
        print(f"✗ Error: {e}")


def main():
    print("=" * 50)
    print("FlyMQ Error Handling Example")
    print("=" * 50)
    
    connection_error_handling()
    producer_error_handling()
    consumer_error_handling()
    retry_logic()
    context_manager_safety()
    
    print("\n" + "=" * 50)
    print("✓ Error handling examples completed!")
    print("\nBest Practices:")
    print("  - Always use context managers (with statement)")
    print("  - Handle specific exceptions")
    print("  - Implement retry logic with exponential backoff")
    print("  - Log errors for debugging")
    print("  - Provide graceful degradation")


if __name__ == "__main__":
    main()

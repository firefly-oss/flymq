#!/usr/bin/env python3
"""
04_transactions.py - Transactional Messaging Example

This example demonstrates:
- Atomic message production across multiple topics
- Automatic rollback on errors
- Manual transaction control (begin, commit, rollback)
- Context manager for automatic commit/rollback

Transactions ensure:
- Exactly-once semantics: Messages are either all committed or all rolled back
- Atomicity: Multiple message operations succeed together or fail together
- Consistency: No partial updates

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 04_transactions.py
"""

from pyflymq import FlyMQClient
import json


def context_manager_example():
    """Using transactions with context manager (auto-commit/rollback)"""
    print("Transaction Example 1: Using Context Manager")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        # Create topics
        for topic in ["accounts", "ledger"]:
            try:
                client.create_topic(topic, partitions=1)
            except:
                pass
        
        # Use context manager for automatic transaction handling
        print("Starting transaction...")
        try:
            with client.transaction() as txn:
                # Produce multiple messages atomically
                msg1 = json.dumps({
                    "account_id": "ACC-001",
                    "balance_before": 1000.00,
                    "balance_after": 900.00
                }).encode()
                
                msg2 = json.dumps({
                    "account_id": "ACC-002",
                    "balance_before": 500.00,
                    "balance_after": 600.00
                }).encode()
                
                print("  Producing to 'accounts' and 'ledger' topics...")
                txn.produce("accounts", msg1)
                txn.produce("ledger", msg2)
                
            print("✓ Transaction committed successfully!")
        except Exception as e:
            print(f"✗ Transaction rolled back: {e}")
    
    finally:
        client.close()


def manual_transaction_example():
    """Manual transaction control with explicit commit/rollback"""
    print("\nTransaction Example 2: Manual Control")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        # Create topics
        for topic in ["orders", "payments"]:
            try:
                client.create_topic(topic, partitions=1)
            except:
                pass
        
        # Manual transaction control
        print("Beginning transaction...")
        txn = client.begin_transaction()
        
        try:
            # Produce order message
            order = json.dumps({
                "order_id": "ORD-12345",
                "items": ["Item A", "Item B"],
                "total": 199.99
            }).encode()
            
            # Produce payment message
            payment = json.dumps({
                "order_id": "ORD-12345",
                "amount": 199.99,
                "status": "pending"
            }).encode()
            
            print("  Producing order and payment messages...")
            txn.produce("orders", order)
            txn.produce("payments", payment)
            
            # Simulate successful processing
            print("  Order and payment processed successfully")
            print("  Committing transaction...")
            txn.commit()
            print("✓ Transaction committed!")
            
        except Exception as e:
            print(f"  Error during transaction: {e}")
            print("  Rolling back transaction...")
            txn.rollback()
            print("✓ Transaction rolled back!")
            raise
    
    finally:
        client.close()


def failed_transaction_example():
    """Demonstrating rollback on failure"""
    print("\nTransaction Example 3: Rollback on Failure")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        # Create topics
        for topic in ["inventory", "sales"]:
            try:
                client.create_topic(topic, partitions=1)
            except:
                pass
        
        print("Starting transaction...")
        try:
            with client.transaction() as txn:
                # Check inventory first
                inventory = {"product_id": "PROD-001", "quantity": 0}
                
                # Try to reserve stock
                if inventory["quantity"] < 1:
                    raise ValueError("Insufficient inventory for PROD-001")
                
                # This code is never reached if exception is raised
                sale = json.dumps({
                    "product_id": "PROD-001",
                    "quantity": 1,
                    "price": 99.99
                }).encode()
                txn.produce("sales", sale)
                
        except ValueError as e:
            print(f"✗ Transaction failed: {e}")
            print("  No messages were committed (automatic rollback)")
    
    finally:
        client.close()


def main():
    print("=" * 50)
    print("FlyMQ Transactions Example")
    print("=" * 50)
    
    context_manager_example()
    manual_transaction_example()
    failed_transaction_example()
    
    print("\n" + "=" * 50)
    print("✓ All transaction examples completed!")
    print("\nKey Takeaways:")
    print("  - Use context manager for automatic handling")
    print("  - Manual control for complex workflows")
    print("  - Automatic rollback on any error")
    print("  - Exactly-once semantics guaranteed")


if __name__ == "__main__":
    main()

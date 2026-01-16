#!/usr/bin/env python3
"""
07_schema_validation.py - Schema Registry and Validation

This example demonstrates:
- Registering JSON schemas
- Validating messages against schemas
- Producing with schema validation
- Schema type support (JSON, Avro, Protobuf)

Schema validation provides:
- Data contract enforcement
- Type safety
- Early error detection
- Documentation

Prerequisites:
    - FlyMQ server running on localhost:9092
    - pyflymq installed

Run with:
    python 07_schema_validation.py
"""

from pyflymq import FlyMQClient
import json


def json_schema_example():
    """JSON schema registration and validation"""
    print("JSON Schema Validation")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        # Define a JSON schema for users
        user_schema = {
            "type": "object",
            "properties": {
                "user_id": {"type": "integer"},
                "name": {"type": "string"},
                "email": {"type": "string", "format": "email"},
                "age": {"type": "integer", "minimum": 0, "maximum": 150}
            },
            "required": ["user_id", "name", "email"]
        }
        
        print("Registering user schema...")
        schema_json = json.dumps(user_schema)
        client.register_schema("user-schema", "json", schema_json)
        print("✓ Schema registered")
        
        # Create topic
        try:
            client.create_topic("users", partitions=1)
        except:
            pass
        
        # Produce valid message
        print("\nProducing valid user message...")
        valid_user = json.dumps({
            "user_id": 1,
            "name": "Alice Smith",
            "email": "alice@example.com",
            "age": 30
        }).encode()
        
        try:
            offset = client.produce_with_schema("users", valid_user, "user-schema")
            print(f"✓ Valid message produced at offset {offset}")
        except Exception as e:
            print(f"✗ Error: {e}")
        
        # Try to produce invalid message (missing required field)
        print("\nProducing invalid user message (missing email)...")
        invalid_user = json.dumps({
            "user_id": 2,
            "name": "Bob Jones"
        }).encode()
        
        try:
            offset = client.produce_with_schema("users", invalid_user, "user-schema")
            print(f"✓ Message produced (validation may be server-side)")
        except Exception as e:
            print(f"✗ Validation failed: {e}")
    
    finally:
        client.close()


def schema_versioning():
    """Handling schema versions"""
    print("\nSchema Versioning")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        # Version 1: Basic product schema
        product_schema_v1 = {
            "type": "object",
            "properties": {
                "product_id": {"type": "string"},
                "name": {"type": "string"},
                "price": {"type": "number"}
            },
            "required": ["product_id", "name", "price"]
        }
        
        print("Registering product schema v1...")
        schema_json = json.dumps(product_schema_v1)
        client.register_schema("product-schema", "json", schema_json)
        print("✓ Schema v1 registered")
        
        # Create topic
        try:
            client.create_topic("products", partitions=1)
        except:
            pass
        
        # Produce with v1 schema
        print("\nProducing product with v1 schema...")
        product = json.dumps({
            "product_id": "PROD-001",
            "name": "Laptop",
            "price": 999.99
        }).encode()
        
        try:
            offset = client.produce_with_schema("products", product, "product-schema")
            print(f"✓ Product produced at offset {offset}")
        except Exception as e:
            print(f"✗ Error: {e}")
        
        # Note: Schema evolution (v2) would typically include backwards compatibility
        print("\nNote: Schema versioning should maintain backwards compatibility")
        print("  - Allow new optional fields")
        print("  - Don't remove required fields")
        print("  - Don't change field types")
    
    finally:
        client.close()


def schema_list_and_get():
    """List and retrieve schemas"""
    print("\nListing Schemas")
    print("-" * 50)
    
    client = FlyMQClient("localhost:9092")
    try:
        print("Registered schemas:")
        try:
            schemas = client.list_schemas()
            for schema in schemas:
                print(f"  - {schema.name} ({schema.type})")
        except Exception as e:
            print(f"  Note: {e}")
        
        print("\nRetrieving schema details:")
        try:
            schema_info = client.get_schema("user-schema")
            print(f"  Schema: user-schema")
            print(f"  Type: {schema_info.get('type', 'unknown')}")
        except Exception as e:
            print(f"  Note: {e}")
    
    finally:
        client.close()


def main():
    print("=" * 50)
    print("FlyMQ Schema Validation Example")
    print("=" * 50)
    
    json_schema_example()
    schema_versioning()
    schema_list_and_get()
    
    print("\n" + "=" * 50)
    print("✓ Schema examples completed!")
    print("\nBest Practices:")
    print("  - Define clear contracts for topics")
    print("  - Version schemas from the start")
    print("  - Maintain backwards compatibility")
    print("  - Document schema changes")


if __name__ == "__main__":
    main()

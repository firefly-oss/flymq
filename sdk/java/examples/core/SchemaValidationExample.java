/*
 * SchemaValidationExample.java - FlyMQ Schema Validation Example
 *
 * This example demonstrates:
 * - Registering JSON schemas
 * - Producing messages with schema validation
 * - Listing and managing schemas
 * - Schema compatibility modes
 *
 * Prerequisites:
 *     - FlyMQ server running on localhost:9092
 *
 * Run with:
 *     javac -cp flymq-client.jar SchemaValidationExample.java
 *     java -cp .:flymq-client.jar SchemaValidationExample
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;

import java.util.List;
import java.util.Map;

public class SchemaValidationExample {

    public static void main(String[] args) {
        System.out.println("FlyMQ Schema Validation Example");
        System.out.println("================================");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            // Create topic for schema testing
            createTopicSafe(client, "users");

            // Example 1: Register a JSON schema
            registerSchema(client);

            // Example 2: Produce with schema validation
            produceWithSchema(client);

            // Example 3: List schemas
            listSchemas(client);

            // Example 4: Schema evolution
            schemaEvolution(client);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Register a JSON schema for user data.
     */
    private static void registerSchema(FlyMQClient client) throws FlyMQException {
        System.out.println("\n1. Register JSON Schema");
        System.out.println("-----------------------");

        String userSchema = """
            {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "email": {"type": "string", "format": "email"},
                    "age": {"type": "integer", "minimum": 0}
                },
                "required": ["id", "name", "email"]
            }
            """;

        try {
            client.registerSchema("user-schema", "json", userSchema);
            System.out.println("✓ Schema 'user-schema' registered successfully");
        } catch (FlyMQException e) {
            if (e.getMessage().contains("exists")) {
                System.out.println("  Schema already exists");
            } else {
                throw e;
            }
        }
    }

    /**
     * Produce messages with schema validation.
     */
    private static void produceWithSchema(FlyMQClient client) throws FlyMQException {
        System.out.println("\n2. Produce with Schema Validation");
        System.out.println("----------------------------------");

        // Valid message - matches schema
        String validUser = """
            {"id": "user-001", "name": "Alice", "email": "alice@example.com", "age": 30}
            """;

        try {
            var meta = client.produceWithSchema(
                "users",
                validUser.getBytes(),
                "user-schema"
            );
            System.out.println("✓ Valid message produced at offset: " + meta.offset());
        } catch (FlyMQException e) {
            System.out.println("  Production failed: " + e.getMessage());
        }

        // Invalid message - missing required field
        String invalidUser = """
            {"id": "user-002", "name": "Bob"}
            """;

        try {
            client.produceWithSchema("users", invalidUser.getBytes(), "user-schema");
            System.out.println("  Invalid message was accepted (schema not enforced)");
        } catch (FlyMQException e) {
            System.out.println("✓ Invalid message rejected: " + e.getMessage());
        }
    }

    /**
     * List all registered schemas.
     */
    private static void listSchemas(FlyMQClient client) throws FlyMQException {
        System.out.println("\n3. List Registered Schemas");
        System.out.println("--------------------------");

        try {
            // Pass empty string to list all schemas, or topic name to filter
            List<Map<String, Object>> schemas = client.listSchemas("");
            System.out.println("Registered schemas: " + schemas.size());
            for (Map<String, Object> schema : schemas) {
                System.out.println("  - " + schema.get("name") +
                    " (type: " + schema.get("type") + ")");
            }
        } catch (FlyMQException e) {
            System.out.println("  Could not list schemas: " + e.getMessage());
        }
    }

    /**
     * Demonstrate schema evolution.
     */
    private static void schemaEvolution(FlyMQClient client) throws FlyMQException {
        System.out.println("\n4. Schema Evolution");
        System.out.println("-------------------");

        // Register evolved schema with new optional field
        String evolvedSchema = """
            {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "name": {"type": "string"},
                    "email": {"type": "string"},
                    "age": {"type": "integer"},
                    "phone": {"type": "string"}
                },
                "required": ["id", "name", "email"]
            }
            """;

        try {
            client.registerSchema("user-schema-v2", "json", evolvedSchema);
            System.out.println("✓ Evolved schema 'user-schema-v2' registered");
            System.out.println("  Added optional 'phone' field");
        } catch (FlyMQException e) {
            System.out.println("  Schema evolution: " + e.getMessage());
        }
    }

    private static void createTopicSafe(FlyMQClient client, String topic) {
        try {
            client.createTopic(topic, 1);
        } catch (Exception e) {
            // Topic may exist
        }
    }
}


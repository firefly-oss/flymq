/*
 * AdvancedPatternsExample.java - FlyMQ Advanced Usage Patterns
 *
 * This example demonstrates:
 * - Batch processing with transactions
 * - Connection pooling pattern
 * - TTL and delayed messages
 * - Multi-topic production
 *
 * Prerequisites:
 *     - FlyMQ server running on localhost:9092
 *
 * Run with:
 *     javac -cp flymq-client.jar AdvancedPatternsExample.java
 *     java -cp .:flymq-client.jar AdvancedPatternsExample
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

public class AdvancedPatternsExample {

    public static void main(String[] args) {
        System.out.println("FlyMQ Advanced Patterns Example");
        System.out.println("================================");

        // Example 1: Batch processing
        batchProcessing();

        // Example 2: Connection pooling
        connectionPooling();

        // Example 3: TTL and delayed messages
        ttlAndDelayedMessages();

        // Example 4: Multi-topic fan-out
        multiTopicFanOut();
    }

    /**
     * Batch processing with transactions for atomicity.
     */
    private static void batchProcessing() {
        System.out.println("\n1. Batch Processing with Transactions");
        System.out.println("--------------------------------------");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            createTopicSafe(client, "batch-topic");

            // Prepare batch of messages
            List<String> batch = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                batch.add("{\"id\": " + i + ", \"value\": \"item-" + i + "\"}");
            }

            // Process in transaction for atomicity
            try (Transaction txn = client.beginTransaction()) {
                for (String msg : batch) {
                    txn.produce("batch-topic", msg);
                }
                txn.commit();
                System.out.println("✓ Batch of " + batch.size() + 
                    " messages committed atomically");
            }

        } catch (Exception e) {
            System.out.println("Batch processing failed: " + e.getMessage());
        }
    }

    /**
     * Simple connection pooling pattern.
     */
    private static void connectionPooling() {
        System.out.println("\n2. Connection Pooling Pattern");
        System.out.println("------------------------------");

        // Simple round-robin connection pool
        class ConnectionPool {
            private final List<FlyMQClient> clients = new ArrayList<>();
            private int current = 0;

            ConnectionPool(String servers, int size) throws Exception {
                for (int i = 0; i < size; i++) {
                    clients.add(new FlyMQClient(servers));
                }
            }

            FlyMQClient getClient() {
                FlyMQClient client = clients.get(current);
                current = (current + 1) % clients.size();
                return client;
            }

            void closeAll() {
                for (FlyMQClient client : clients) {
                    try { client.close(); } catch (Exception e) {}
                }
            }
        }

        try {
            ConnectionPool pool = new ConnectionPool("localhost:9092", 3);
            System.out.println("✓ Created pool with 3 connections");

            // Use connections from pool
            for (int i = 0; i < 5; i++) {
                FlyMQClient client = pool.getClient();
                createTopicSafe(client, "pool-test-" + i);
                System.out.println("  Used connection " + (i % 3));
            }

            pool.closeAll();
            System.out.println("✓ Pool closed");

        } catch (Exception e) {
            System.out.println("Pool error: " + e.getMessage());
        }
    }

    /**
     * TTL and delayed message delivery.
     */
    private static void ttlAndDelayedMessages() {
        System.out.println("\n3. TTL and Delayed Messages");
        System.out.println("----------------------------");

        try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
            createTopicSafe(client, "scheduled-topic");

            // Message with TTL (expires in 30 seconds)
            try {
                var meta = client.produceWithTTL(
                    "scheduled-topic",
                    "Expires in 30s".getBytes(),
                    30000
                );
                System.out.println("✓ TTL message at offset: " + meta.offset());
            } catch (Exception e) {
                System.out.println("  TTL: " + e.getMessage());
            }

            // Delayed message (visible after 5 seconds)
            try {
                var meta = client.produceDelayed(
                    "scheduled-topic",
                    "Delayed 5s".getBytes(),
                    5000
                );
                System.out.println("✓ Delayed message at offset: " + meta.offset());
            } catch (Exception e) {
                System.out.println("  Delayed: " + e.getMessage());
            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    /**
     * Multi-topic fan-out pattern.
     */
    private static void multiTopicFanOut() {
        System.out.println("\n4. Multi-Topic Fan-Out");
        System.out.println("-----------------------");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            String[] topics = {"analytics", "audit", "notifications"};
            
            for (String topic : topics) {
                createTopicSafe(client, topic);
            }

            // Fan-out: send same event to multiple topics
            String event = "{\"type\": \"user.created\", \"userId\": \"123\"}";
            
            for (String topic : topics) {
                client.produce(topic, event.getBytes());
                System.out.println("  → " + topic);
            }
            System.out.println("✓ Event fanned out to " + topics.length + " topics");

        } catch (Exception e) {
            System.out.println("Fan-out error: " + e.getMessage());
        }
    }

    private static void createTopicSafe(FlyMQClient client, String topic) {
        try { client.createTopic(topic, 1); } catch (Exception e) {}
    }
}


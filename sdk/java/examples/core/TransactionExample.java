/*
 * TransactionExample.java - FlyMQ Transaction Example
 *
 * This example demonstrates:
 * - Beginning a transaction
 * - Producing multiple messages atomically
 * - Committing transactions
 * - Rolling back on failure
 *
 * Prerequisites:
 *     - FlyMQ server running on localhost:9092
 *
 * Run with:
 *     javac -cp flymq-client.jar TransactionExample.java
 *     java -cp .:flymq-client.jar TransactionExample
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.transaction.Transaction;

public class TransactionExample {

    public static void main(String[] args) {
        System.out.println("FlyMQ Transaction Example");
        System.out.println("=========================");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            // Create topics for the transaction
            createTopicSafe(client, "orders");
            createTopicSafe(client, "payments");
            createTopicSafe(client, "notifications");

            // Example 1: Basic transaction with commit
            basicTransaction(client);

            // Example 2: Transaction with rollback on error
            transactionWithRollback(client);

            // Example 3: Multi-topic atomic write
            multiTopicTransaction(client);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Basic transaction example - produces messages atomically.
     */
    private static void basicTransaction(FlyMQClient client) throws FlyMQException {
        System.out.println("\n1. Basic Transaction");
        System.out.println("--------------------");

        // Begin a transaction
        Transaction txn = client.beginTransaction();
        System.out.println("Transaction started: " + txn.getTxnId());

        try {
            // Produce messages within the transaction
            long offset1 = txn.produce("orders", "Order #1001 created");
            System.out.println("  Produced order at offset: " + offset1);

            long offset2 = txn.produce("orders", "Order #1002 created");
            System.out.println("  Produced order at offset: " + offset2);

            // Commit the transaction - messages become visible
            txn.commit();
            System.out.println("✓ Transaction committed successfully");

        } catch (Exception e) {
            // Rollback on any error
            txn.rollback();
            System.out.println("✗ Transaction rolled back: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Transaction with intentional rollback demonstration.
     */
    private static void transactionWithRollback(FlyMQClient client) throws FlyMQException {
        System.out.println("\n2. Transaction with Rollback");
        System.out.println("----------------------------");

        Transaction txn = client.beginTransaction();
        System.out.println("Transaction started: " + txn.getTxnId());

        try {
            txn.produce("orders", "Order #2001 - will be rolled back");
            System.out.println("  Produced message (not yet visible)");

            // Simulate a business logic failure
            boolean paymentSucceeded = false;
            if (!paymentSucceeded) {
                throw new RuntimeException("Payment processing failed");
            }

            txn.commit();

        } catch (Exception e) {
            // Rollback - messages are discarded
            txn.rollback();
            System.out.println("✓ Transaction rolled back: " + e.getMessage());
            System.out.println("  Messages were NOT persisted");
        }
    }

    /**
     * Multi-topic atomic write - all or nothing across topics.
     */
    private static void multiTopicTransaction(FlyMQClient client) throws FlyMQException {
        System.out.println("\n3. Multi-Topic Atomic Write");
        System.out.println("---------------------------");

        // Using try-with-resources - auto-rollback if not committed
        try (Transaction txn = client.beginTransaction()) {
            System.out.println("Transaction started: " + txn.getTxnId());

            // Write to multiple topics atomically
            txn.produce("orders", "{\"orderId\": \"3001\", \"status\": \"created\"}");
            System.out.println("  → orders: Order created");

            txn.produce("payments", "{\"orderId\": \"3001\", \"amount\": 99.99}");
            System.out.println("  → payments: Payment recorded");

            txn.produce("notifications", "{\"orderId\": \"3001\", \"type\": \"email\"}");
            System.out.println("  → notifications: Notification queued");

            // Commit - all messages become visible atomically
            txn.commit();
            System.out.println("✓ All messages committed atomically across 3 topics");
        }
        // If commit() not called, transaction auto-rolls back on close
    }

    private static void createTopicSafe(FlyMQClient client, String topic) {
        try {
            client.createTopic(topic, 1);
            System.out.println("Created topic: " + topic);
        } catch (Exception e) {
            // Topic may already exist
        }
    }
}


/*
 * DLQExample.java - FlyMQ Dead Letter Queue Example
 *
 * This example demonstrates:
 * - Fetching messages from dead letter queue
 * - Inspecting failed messages
 * - Replaying DLQ messages
 * - Purging the DLQ
 *
 * Prerequisites:
 *     - FlyMQ server running on localhost:9092
 *
 * Run with:
 *     javac -cp flymq-client.jar DLQExample.java
 *     java -cp .:flymq-client.jar DLQExample
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;

import java.util.List;
import java.util.Map;

public class DLQExample {

    public static void main(String[] args) {
        System.out.println("FlyMQ Dead Letter Queue Example");
        System.out.println("================================");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            String topic = "orders";
            
            // Create topic
            createTopicSafe(client, topic);

            // Example 1: Fetch DLQ messages
            fetchDLQMessages(client, topic);

            // Example 2: Inspect and process DLQ
            processDLQ(client, topic);

            // Example 3: Replay a DLQ message
            replayDLQMessage(client, topic);

            // Example 4: Purge DLQ
            purgeDLQ(client, topic);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Fetch messages from the dead letter queue.
     */
    private static void fetchDLQMessages(FlyMQClient client, String topic) 
            throws FlyMQException {
        System.out.println("\n1. Fetch DLQ Messages");
        System.out.println("---------------------");

        try {
            List<Map<String, Object>> dlqMessages = client.fetchDLQ(topic, 10);
            System.out.println("DLQ messages found: " + dlqMessages.size());

            if (dlqMessages.isEmpty()) {
                System.out.println("  (No messages in DLQ - this is normal)");
            }

            for (Map<String, Object> msg : dlqMessages) {
                System.out.println("  Message ID: " + msg.get("id"));
                System.out.println("    Error: " + msg.get("error"));
                System.out.println("    Retries: " + msg.get("retries"));
            }
        } catch (FlyMQException e) {
            System.out.println("  DLQ fetch: " + e.getMessage());
        }
    }

    /**
     * Process DLQ messages with business logic.
     */
    private static void processDLQ(FlyMQClient client, String topic) 
            throws FlyMQException {
        System.out.println("\n2. Process DLQ Messages");
        System.out.println("-----------------------");

        try {
            List<Map<String, Object>> dlqMessages = client.fetchDLQ(topic, 100);
            
            int replayable = 0;
            int permanent = 0;

            for (Map<String, Object> msg : dlqMessages) {
                String error = (String) msg.get("error");
                int retries = (Integer) msg.getOrDefault("retries", 0);

                // Categorize failures
                if (isTransientError(error) && retries < 5) {
                    replayable++;
                } else {
                    permanent++;
                }
            }

            System.out.println("DLQ Analysis:");
            System.out.println("  Replayable (transient errors): " + replayable);
            System.out.println("  Permanent failures: " + permanent);

        } catch (FlyMQException e) {
            System.out.println("  DLQ processing: " + e.getMessage());
        }
    }

    /**
     * Replay a message from DLQ back to the original topic.
     */
    private static void replayDLQMessage(FlyMQClient client, String topic) 
            throws FlyMQException {
        System.out.println("\n3. Replay DLQ Message");
        System.out.println("---------------------");

        try {
            List<Map<String, Object>> dlqMessages = client.fetchDLQ(topic, 1);

            if (!dlqMessages.isEmpty()) {
                Map<String, Object> msg = dlqMessages.get(0);
                String messageId = (String) msg.get("id");

                System.out.println("Replaying message: " + messageId);
                client.replayDLQ(topic, messageId);
                System.out.println("✓ Message replayed to topic: " + topic);
            } else {
                System.out.println("  No messages to replay");
            }
        } catch (FlyMQException e) {
            System.out.println("  Replay: " + e.getMessage());
        }
    }

    /**
     * Purge all messages from the DLQ.
     */
    private static void purgeDLQ(FlyMQClient client, String topic) 
            throws FlyMQException {
        System.out.println("\n4. Purge DLQ");
        System.out.println("------------");

        try {
            client.purgeDLQ(topic);
            System.out.println("✓ DLQ purged for topic: " + topic);
        } catch (FlyMQException e) {
            System.out.println("  Purge: " + e.getMessage());
        }
    }

    private static boolean isTransientError(String error) {
        if (error == null) return false;
        String lower = error.toLowerCase();
        return lower.contains("timeout") || 
               lower.contains("temporary") ||
               lower.contains("retry");
    }

    private static void createTopicSafe(FlyMQClient client, String topic) {
        try {
            client.createTopic(topic, 1);
        } catch (Exception e) {
            // Topic may exist
        }
    }
}


/*
 * BasicProduceConsume.java - FlyMQ High-Level API Example
 *
 * Demonstrates:
 * - FlyMQClient.connect() for one-liner connection
 * - HighLevelProducer with batching and CompletableFuture callbacks
 * - Consumer with auto-commit
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;
import java.time.Duration;

public class BasicProduceConsume {

    public static void main(String[] args) {
        System.out.println("FlyMQ High-Level API Example");
        System.out.println("============================");

        // Use connect() for one-liner connection
        try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

            // Create topic
            String topic = "hello-world";
            System.out.println("Creating topic: " + topic);
            try {
                client.createTopic(topic, 1);
            } catch (Exception e) {
                System.out.println("Topic may exist: " + e.getMessage());
            }

            // === High-Level Producer ===
            System.out.println("\n=== Using High-Level Producer ===");
            try (var producer = client.producer()) {
                String msg = "Hello, FlyMQ!";

                // Send with CompletableFuture callback
                producer.send(topic, msg.getBytes(), "user-123")
                    .thenAccept(metadata -> {
                        System.out.println("✓ Sent to " + metadata.topic() +
                            " @ offset " + metadata.offset());
                    })
                    .exceptionally(e -> {
                        System.err.println("✗ Send failed: " + e.getMessage());
                        return null;
                    });

                producer.flush();  // Ensure message is sent
            }

            // === Consumer ===
            System.out.println("\n=== Using Consumer ===");
            try (var consumer = client.consumer(topic, "demo-group")) {
                consumer.subscribe();

                // Poll for messages
                var messages = consumer.poll(Duration.ofSeconds(5));
                for (var msg : messages) {
                    System.out.println("Received: Key=" + msg.keyAsString() +
                        ", Value=" + new String(msg.value()));
                }
            }

            System.out.println("\n✓ Successfully produced and consumed!");

        } catch (FlyMQException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Hint: " + e.getHint());  // Actionable suggestion
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

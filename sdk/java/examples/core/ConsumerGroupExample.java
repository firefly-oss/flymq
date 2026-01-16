/*
 * ConsumerGroupExample.java - Consumer Groups with High-Level APIs
 *
 * Demonstrates:
 * - FlyMQClient.connect() for one-liner connection
 * - HighLevelProducer with batching
 * - Consumer with auto-commit
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;
import java.time.Duration;

public class ConsumerGroupExample {

    public static void main(String[] args) throws Exception {
        System.out.println("Consumer Group Example with High-Level APIs");
        System.out.println("============================================");

        String topic = "notifications";
        String groupId = "notifications-group";

        // Use connect() for one-liner connection
        try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {

            // Create topic
            try {
                client.createTopic(topic, 2);
            } catch (Exception e) {
                System.out.println("Topic exists");
            }

            // === High-Level Producer ===
            System.out.println("\n=== Producing messages ===");
            try (var producer = client.producer()) {
                for (int i = 0; i < 10; i++) {
                    String msg = "Message " + (i + 1) + "/10";
                    String key = "user-" + (i % 3);  // Distribute across 3 keys

                    producer.send(topic, msg.getBytes(), key);
                    System.out.println("  Sent: " + msg + " (key=" + key + ")");
                }
                producer.flush();
            }
            System.out.println("Producer: Done!");

            // === Consumer with auto-commit ===
            System.out.println("\n=== Consuming with auto-commit ===");
            try (var consumer = client.consumer(topic, groupId)) {
                consumer.subscribe();

                int count = 0;
                while (count < 10) {
                    var messages = consumer.poll(Duration.ofSeconds(1));
                    for (var msg : messages) {
                        System.out.println("  Received: Key=" + msg.keyAsString() +
                            ", Value=" + new String(msg.value()));
                        // Auto-commit handles offset management
                        count++;
                        if (count >= 10) break;
                    }
                }
            }
            System.out.println("Consumer: Done!");

            // === Consumer with manual commit ===
            System.out.println("\n=== Consuming with manual commit ===");
            try (var consumer = client.consumer(topic, groupId + "-manual", 0,
                    com.firefly.flymq.consumer.ConsumerConfig.builder()
                        .enableAutoCommit(false)
                        .build())) {
                consumer.subscribe();

                var messages = consumer.poll(Duration.ofSeconds(1));
                for (var msg : messages) {
                    System.out.println("  Processing: " + new String(msg.value()));
                    // Commit after successful processing
                    consumer.commitSync();
                    System.out.println("  ✓ Committed offset");
                }
            }

            System.out.println("\n✓ Consumer group example completed!");

        } catch (FlyMQException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Hint: " + e.getHint());
        }
    }
}

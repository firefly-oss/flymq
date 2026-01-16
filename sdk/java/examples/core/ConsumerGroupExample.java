/*
 * ConsumerGroupExample.java - Consumer Groups Example
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.consumer.Consumer;
import com.firefly.flymq.protocol.Records.ConsumedMessage;
import com.firefly.flymq.protocol.Records.SubscribeMode;

import java.time.Duration;
import java.util.List;

public class ConsumerGroupExample {

    static class Producer implements Runnable {
        private final String topic;
        private final int count;

        public Producer(String topic, int count) {
            this.topic = topic;
            this.count = count;
        }

        @Override
        public void run() {
            try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
                System.out.println("Producer: Sending " + count + " messages...");
                for (int i = 0; i < count; i++) {
                    String msg = "Message " + (i + 1) + "/" + count;
                    client.produce(topic, msg.getBytes());
                }
                System.out.println("Producer: Done!");
            } catch (Exception e) {
                System.err.println("Producer error: " + e.getMessage());
            }
        }
    }

    static class ConsumerWorker implements Runnable {
        private final String topic;
        private final String groupId;
        private final String consumerId;
        private final int maxMessages;

        public ConsumerWorker(String topic, String groupId, String consumerId, int maxMessages) {
            this.topic = topic;
            this.groupId = groupId;
            this.consumerId = consumerId;
            this.maxMessages = maxMessages;
        }

        @Override
        public void run() {
            try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
                Consumer consumer = new Consumer(client, topic, groupId);
                consumer.subscribe();
                
                System.out.println("Consumer " + consumerId + ": Started consuming...");
                
                int count = 0;
                while (count < maxMessages) {
                    List<ConsumedMessage> messages = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumedMessage msg : messages) {
                        System.out.println("  Consumer " + consumerId + ": " + msg.dataAsString());
                        consumer.commitSync();
                        count++;
                        if (count >= maxMessages) break;
                    }
                }
                
                System.out.println("Consumer " + consumerId + ": Done!");
                consumer.close();
            } catch (Exception e) {
                System.err.println("Consumer error: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Consumer Group Example");
        System.out.println("=====================");

        String topic = "notifications";
        String groupId = "notifications-group";

        // Create topic
        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            try {
                client.createTopic(topic, 2);
            } catch (Exception e) {
                System.out.println("Topic exists");
            }
        }

        // Start producer
        new Thread(new Producer(topic, 10)).start();
        Thread.sleep(1000); // Let producer start

        // Start consumer
        new Thread(new ConsumerWorker(topic, groupId, "consumer-1", 10)).start();

        System.out.println("\nRunning example... wait for completion");
        Thread.sleep(5000);
        System.out.println("Example completed!");
    }
}

/*
 * KeyBasedMessaging.java - Key-Based Messaging (Kafka-style Partitioning)
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.protocol.Records.ConsumedMessage;
import com.firefly.flymq.protocol.Records.FetchResult;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KeyBasedMessaging {

    static class Order {
        public int orderId;
        public String item;
        public double price;

        public Order(int orderId, String item, double price) {
            this.orderId = orderId;
            this.item = item;
            this.price = price;
        }

        public Order() {}
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Key-Based Messaging Example");
        System.out.println("============================");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            String topic = "orders";
            try {
                client.createTopic(topic, 3);
            } catch (Exception e) {
                System.out.println("Topic exists");
            }

            ObjectMapper mapper = new ObjectMapper();

            // Produce with keys
            System.out.println("\nProducing messages with keys:");
            String[] keys = {"user-123", "user-123", "user-456", "user-123", "user-789"};
            Order[] orders = {
                new Order(1001, "Laptop", 999.99),
                new Order(1002, "Mouse", 29.99),
                new Order(2001, "Monitor", 299.99),
                new Order(1003, "Keyboard", 79.99),
                new Order(3001, "Headphones", 149.99)
            };

            long[] offsets = new long[keys.length];
            for (int i = 0; i < keys.length; i++) {
                String json = mapper.writeValueAsString(orders[i]);
                long offset = client.produceWithKey(topic, keys[i], json.getBytes());
                offsets[i] = offset;
                System.out.println("  Key: " + keys[i] + ", Order: " + orders[i].orderId);
            }

            // Consume
            System.out.println("\nConsuming messages:");
            for (int i = 0; i < offsets.length; i++) {
                ConsumedMessage msg = client.consumeWithKey(topic, offsets[i]);
                Order order = mapper.readValue(msg.data(), Order.class);
                System.out.println("  Message " + i + ": Key=" + msg.keyAsString() + ", Order=" + order.orderId);
            }

            // Fetch batch
            System.out.println("\nFetching batch:");
            FetchResult result = client.fetch(topic, 0, 0, 10);
            System.out.println("Fetched " + result.messages().size() + " messages");

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

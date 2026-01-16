/*
 * BasicProduceConsume.java - FlyMQ Basic Producer and Consumer Example
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.protocol.Records.ConsumedMessage;

public class BasicProduceConsume {

    public static void main(String[] args) {
        System.out.println("FlyMQ Basic Example");
        System.out.println("==================");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            // Create topic
            String topic = "hello-world";
            System.out.println("Creating topic: " + topic);
            try {
                client.createTopic(topic, 1);
            } catch (Exception e) {
                System.out.println("Topic may exist: " + e.getMessage());
            }

            // Produce
            String msg = "Hello, FlyMQ!";
            System.out.println("Producing: " + msg);
            long offset = client.produce(topic, msg.getBytes());
            System.out.println("Offset: " + offset);

            // Consume
            System.out.println("Consuming from offset: " + offset);
            ConsumedMessage consumed = client.consumeWithKey(topic, offset);
            System.out.println("Received: " + consumed.dataAsString());

        } catch (FlyMQException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}

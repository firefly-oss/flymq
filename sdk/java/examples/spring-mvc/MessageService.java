/*
 * MessageService.java - Spring Boot MVC Service Example
 * 
 * Demonstrates injecting FlyMQClient into a Spring service.
 * Add to your Spring Boot application to use.
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.protocol.Records.ConsumedMessage;
import com.firefly.flymq.protocol.Records.FetchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MessageService {
    
    private static final Logger log = LoggerFactory.getLogger(MessageService.class);
    private final FlyMQClient client;

    public MessageService(FlyMQClient client) {
        this.client = client;
    }

    /**
     * Send a message to a topic
     */
    public long sendMessage(String topic, String message) throws Exception {
        log.info("Sending message to {}: {}", topic, message);
        var meta = client.produce(topic, message.getBytes());
        log.info("Message sent at offset {}", meta.offset());
        return meta.offset();
    }

    /**
     * Send a message with a key (ensures ordering)
     */
    public long sendMessageWithKey(String topic, String key, String message) throws Exception {
        log.info("Sending keyed message to {}: key={}, msg={}", topic, key, message);
        var meta = client.produceWithKey(topic, key, message.getBytes());
        log.info("Message sent at offset {}", meta.offset());
        return meta.offset();
    }

    /**
     * Retrieve a message by offset
     */
    public String getMessage(String topic, long offset) throws Exception {
        ConsumedMessage msg = client.consumeWithKey(topic, offset);
        return msg.dataAsString();
    }

    /**
     * Fetch a batch of messages
     */
    public String[] getMessages(String topic, int partition, long offset, int count) throws Exception {
        FetchResult result = client.fetch(topic, partition, offset, count);
        String[] messages = new String[result.messages().size()];
        for (int i = 0; i < result.messages().size(); i++) {
            messages[i] = result.messages().get(i).dataAsString();
        }
        return messages;
    }

    /**
     * Create a topic
     */
    public void createTopic(String topic, int partitions) throws Exception {
        log.info("Creating topic: {} with {} partitions", topic, partitions);
        client.createTopic(topic, partitions);
        log.info("Topic created");
    }
}

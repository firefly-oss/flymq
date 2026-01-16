/*
 * ErrorHandlingExample.java - FlyMQ Error Handling Example
 *
 * This example demonstrates:
 * - Handling common FlyMQ exceptions
 * - Implementing retry logic with exponential backoff
 * - Graceful degradation patterns
 * - Connection recovery
 *
 * Prerequisites:
 *     - FlyMQ server running on localhost:9092
 *
 * Run with:
 *     javac -cp flymq-client.jar ErrorHandlingExample.java
 *     java -cp .:flymq-client.jar ErrorHandlingExample
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;

public class ErrorHandlingExample {

    public static void main(String[] args) {
        System.out.println("FlyMQ Error Handling Example");
        System.out.println("============================");

        // Example 1: Basic exception handling
        basicExceptionHandling();

        // Example 2: Retry with exponential backoff
        retryWithBackoff();

        // Example 3: Handling specific error types
        handleSpecificErrors();

        // Example 4: Circuit breaker pattern
        circuitBreakerPattern();
    }

    /**
     * Basic exception handling for FlyMQ operations.
     */
    private static void basicExceptionHandling() {
        System.out.println("\n1. Basic Exception Handling");
        System.out.println("---------------------------");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            // Attempt to consume from non-existent topic
            client.consume("non-existent-topic", 0);

        } catch (FlyMQException e) {
            System.out.println("FlyMQ error caught:");
            System.out.println("  Message: " + e.getMessage());
            System.out.println("  This is expected for non-existent topic");
        } catch (Exception e) {
            System.out.println("Unexpected error: " + e.getMessage());
        }
    }

    /**
     * Retry logic with exponential backoff.
     */
    private static void retryWithBackoff() {
        System.out.println("\n2. Retry with Exponential Backoff");
        System.out.println("----------------------------------");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            // Create topic for testing
            try {
                client.createTopic("retry-test", 1);
            } catch (Exception e) {
                // Topic may exist
            }

            // Produce with retry
            byte[] message = "Important message".getBytes();
            long offset = produceWithRetry(client, "retry-test", message, 3);
            System.out.println("✓ Message produced at offset: " + offset);

        } catch (Exception e) {
            System.out.println("Failed after all retries: " + e.getMessage());
        }
    }

    /**
     * Produces a message with retry logic.
     */
    private static long produceWithRetry(FlyMQClient client, String topic, 
            byte[] data, int maxRetries) throws Exception {
        
        int attempt = 0;
        long backoffMs = 100;

        while (attempt < maxRetries) {
            try {
                return client.produce(topic, data);
            } catch (FlyMQException e) {
                attempt++;
                if (attempt >= maxRetries) {
                    throw e;
                }
                System.out.println("  Attempt " + attempt + " failed, " +
                    "retrying in " + backoffMs + "ms...");
                Thread.sleep(backoffMs);
                backoffMs *= 2; // Exponential backoff
            }
        }
        throw new Exception("Failed after " + maxRetries + " attempts");
    }

    /**
     * Handle specific error types differently.
     */
    private static void handleSpecificErrors() {
        System.out.println("\n3. Handling Specific Error Types");
        System.out.println("---------------------------------");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            // Try various operations that might fail
            try {
                client.consume("test-topic", -1); // Invalid offset
            } catch (FlyMQException e) {
                String msg = e.getMessage().toLowerCase();
                if (msg.contains("offset")) {
                    System.out.println("Offset error: Invalid offset specified");
                } else if (msg.contains("not found")) {
                    System.out.println("Topic error: Topic does not exist");
                } else if (msg.contains("timeout")) {
                    System.out.println("Timeout error: Operation timed out");
                } else if (msg.contains("auth")) {
                    System.out.println("Auth error: Authentication required");
                } else {
                    System.out.println("Other error: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println("Connection error: " + e.getMessage());
        }
    }

    /**
     * Simple circuit breaker pattern implementation.
     */
    private static void circuitBreakerPattern() {
        System.out.println("\n4. Circuit Breaker Pattern");
        System.out.println("--------------------------");

        CircuitBreaker breaker = new CircuitBreaker(3, 5000);

        for (int i = 0; i < 5; i++) {
            try {
                if (breaker.isOpen()) {
                    System.out.println("  Circuit OPEN - skipping operation " + i);
                    continue;
                }

                // Simulate operation
                performOperation(i);
                breaker.recordSuccess();
                System.out.println("  Operation " + i + " succeeded");

            } catch (Exception e) {
                breaker.recordFailure();
                System.out.println("  Operation " + i + " failed: " + e.getMessage());
            }
        }
    }

    private static void performOperation(int i) throws Exception {
        if (i == 1 || i == 2) {
            throw new Exception("Simulated failure");
        }
    }

    /**
     * Simple circuit breaker implementation.
     */
    static class CircuitBreaker {
        private final int threshold;
        private final long resetTimeMs;
        private int failures = 0;
        private long lastFailureTime = 0;

        CircuitBreaker(int threshold, long resetTimeMs) {
            this.threshold = threshold;
            this.resetTimeMs = resetTimeMs;
        }

        boolean isOpen() {
            if (failures >= threshold) {
                if (System.currentTimeMillis() - lastFailureTime > resetTimeMs) {
                    failures = 0; // Reset after timeout
                    return false;
                }
                return true;
            }
            return false;
        }

        void recordSuccess() { failures = 0; }
        void recordFailure() { 
            failures++; 
            lastFailureTime = System.currentTimeMillis();
        }
    }
}


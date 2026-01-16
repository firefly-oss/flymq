/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.firefly.flymq.exception;

/**
 * Base exception for all FlyMQ errors.
 *
 * <p>FlyMQ exceptions include helpful hints to guide developers toward solutions.
 * Check the {@link #getHint()} method for actionable suggestions.
 */
public class FlyMQException extends Exception {

    private final String hint;

    public FlyMQException(String message) {
        super(message);
        this.hint = null;
    }

    public FlyMQException(String message, Throwable cause) {
        super(message, cause);
        this.hint = null;
    }

    public FlyMQException(String message, String hint) {
        super(message);
        this.hint = hint;
    }

    public FlyMQException(String message, String hint, Throwable cause) {
        super(message, cause);
        this.hint = hint;
    }

    /**
     * Gets a helpful hint for resolving this error.
     *
     * @return hint string, or null if no hint is available
     */
    public String getHint() {
        return hint;
    }

    /**
     * Gets the full message including the hint.
     *
     * @return message with hint if available
     */
    public String getFullMessage() {
        if (hint != null && !hint.isEmpty()) {
            return getMessage() + "\n  Hint: " + hint;
        }
        return getMessage();
    }

    @Override
    public String toString() {
        if (hint != null && !hint.isEmpty()) {
            return getClass().getName() + ": " + getMessage() + "\n  Hint: " + hint;
        }
        return super.toString();
    }

    // =========================================================================
    // Factory methods for common errors with helpful hints
    // =========================================================================

    /**
     * Creates an exception for connection failures.
     */
    public static FlyMQException connectionFailed(String server, Throwable cause) {
        return new FlyMQException(
            "Failed to connect to " + server,
            "Check that the FlyMQ server is running and accessible. " +
            "Verify the host and port are correct. " +
            "If using TLS, ensure certificates are properly configured.",
            cause
        );
    }

    /**
     * Creates an exception for topic not found errors.
     */
    public static FlyMQException topicNotFound(String topic) {
        return new FlyMQException(
            "Topic not found: " + topic,
            "Create the topic first with client.createTopic(\"" + topic + "\", partitions) " +
            "or use the CLI: flymq-cli topic create " + topic
        );
    }

    /**
     * Creates an exception for authentication failures.
     */
    public static FlyMQException authenticationFailed(String username) {
        return new FlyMQException(
            "Authentication failed for user: " + username,
            "Check that the username and password are correct. " +
            "Verify the user exists and has the required permissions."
        );
    }

    /**
     * Creates an exception for consumer group errors.
     */
    public static FlyMQException consumerGroupError(String groupId, String message) {
        return new FlyMQException(
            "Consumer group error for '" + groupId + "': " + message,
            "Ensure the consumer group exists and is not in a rebalancing state. " +
            "Check that no other consumer is using the same group with incompatible settings."
        );
    }

    /**
     * Creates an exception for offset out of range errors.
     */
    public static FlyMQException offsetOutOfRange(String topic, int partition, long offset) {
        return new FlyMQException(
            "Offset " + offset + " is out of range for " + topic + "[" + partition + "]",
            "Use subscribe() with EARLIEST or LATEST mode to reset to a valid offset, " +
            "or use seekToBeginning()/seekToEnd() to reposition the consumer."
        );
    }

    /**
     * Creates an exception for timeout errors.
     */
    public static FlyMQException timeout(String operation) {
        return new FlyMQException(
            "Operation timed out: " + operation,
            "The server may be overloaded or unreachable. " +
            "Try increasing the timeout or check server health."
        );
    }
}

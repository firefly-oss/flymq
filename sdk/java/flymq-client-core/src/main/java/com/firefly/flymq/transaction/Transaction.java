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
package com.firefly.flymq.transaction;

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.exception.FlyMQException;
import com.firefly.flymq.protocol.BinaryProtocol;

import java.nio.charset.StandardCharsets;

/**
 * Represents a FlyMQ transaction.
 *
 * <p>Transactions provide exactly-once semantics for producing messages
 * to multiple topics atomically.
 *
 * <p>Example usage with try-with-resources:
 * <pre>{@code
 * Transaction txn = client.beginTransaction();
 * try {
 *     txn.produce("topic1", "message1".getBytes());
 *     txn.produce("topic2", "message2".getBytes());
 *     txn.commit();
 * } catch (Exception e) {
 *     txn.rollback();
 *     throw e;
 * }
 * }</pre>
 */
public class Transaction implements AutoCloseable {

    private final FlyMQClient client;
    private final String txnId;
    private volatile boolean active;

    /**
     * Creates a new transaction (called internally by FlyMQClient).
     *
     * @param client the FlyMQ client
     * @param txnId  the transaction ID
     */
    public Transaction(FlyMQClient client, String txnId) {
        this.client = client;
        this.txnId = txnId;
        this.active = true;
    }

    /**
     * Gets the transaction ID.
     *
     * @return the transaction ID
     */
    public String getTxnId() {
        return txnId;
    }

    /**
     * Checks if the transaction is active.
     *
     * @return true if the transaction is active
     */
    public boolean isActive() {
        return active;
    }

    /**
     * Produces a message within the transaction.
     *
     * @param topic target topic
     * @param data  message data
     * @return RecordMetadata with topic, partition, offset, timestamp, key_size, value_size
     * @throws FlyMQException if the operation fails or transaction is not active
     */
    public BinaryProtocol.RecordMetadata produce(String topic, byte[] data) throws FlyMQException {
        ensureActive();
        return client.produceInTransaction(txnId, topic, data);
    }

    /**
     * Produces a string message within the transaction.
     *
     * @param topic   target topic
     * @param message message string
     * @return RecordMetadata with topic, partition, offset, timestamp, key_size, value_size
     * @throws FlyMQException if the operation fails or transaction is not active
     */
    public BinaryProtocol.RecordMetadata produce(String topic, String message) throws FlyMQException {
        return produce(topic, message.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Commits the transaction.
     *
     * <p>After commit, the transaction is no longer active and all produced
     * messages become visible to consumers.
     *
     * @throws FlyMQException if the operation fails or transaction is not active
     */
    public void commit() throws FlyMQException {
        ensureActive();
        try {
            client.commitTransaction(txnId);
        } finally {
            active = false;
        }
    }

    /**
     * Rolls back the transaction.
     *
     * <p>After rollback, the transaction is no longer active and all produced
     * messages are discarded.
     *
     * @throws FlyMQException if the operation fails or transaction is not active
     */
    public void rollback() throws FlyMQException {
        ensureActive();
        try {
            client.abortTransaction(txnId);
        } finally {
            active = false;
        }
    }

    /**
     * Alias for rollback().
     *
     * @throws FlyMQException if the operation fails
     */
    public void abort() throws FlyMQException {
        rollback();
    }

    private void ensureActive() throws FlyMQException {
        if (!active) {
            throw new FlyMQException("Transaction is not active");
        }
    }

    /**
     * Closes the transaction, rolling back if still active.
     */
    @Override
    public void close() {
        if (active) {
            try {
                rollback();
            } catch (FlyMQException e) {
                // Ignore - best effort cleanup
            }
        }
    }
}

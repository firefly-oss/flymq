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
package com.firefly.flymq.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Future representing a pending produce operation.
 *
 * <p>Similar to Kafka's Future&lt;RecordMetadata&gt;, this allows async
 * send operations with callbacks.
 *
 * <p>Example:
 * <pre>{@code
 * ProduceFuture future = producer.send("topic", "message".getBytes());
 * 
 * // Option 1: Block and wait
 * ProduceMetadata metadata = future.get();
 * 
 * // Option 2: Add callback
 * future.whenComplete((metadata, error) -> {
 *     if (error != null) {
 *         System.err.println("Failed: " + error);
 *     } else {
 *         System.out.println("Sent at offset: " + metadata.offset());
 *     }
 * });
 * }</pre>
 */
public class ProduceFuture {

    private final CompletableFuture<ProduceMetadata> future;

    public ProduceFuture() {
        this.future = new CompletableFuture<>();
    }

    /**
     * Sets the successful result.
     */
    void complete(ProduceMetadata metadata) {
        future.complete(metadata);
    }

    /**
     * Sets the error result.
     */
    void completeExceptionally(Throwable error) {
        future.completeExceptionally(error);
    }

    /**
     * Waits for the result with default timeout (30 seconds).
     *
     * @return the produce metadata
     * @throws ExecutionException if the send failed
     * @throws InterruptedException if interrupted while waiting
     * @throws TimeoutException if timeout expires
     */
    public ProduceMetadata get() throws ExecutionException, InterruptedException, TimeoutException {
        return get(30, TimeUnit.SECONDS);
    }

    /**
     * Waits for the result with specified timeout.
     *
     * @param timeout the timeout value
     * @param unit the timeout unit
     * @return the produce metadata
     * @throws ExecutionException if the send failed
     * @throws InterruptedException if interrupted while waiting
     * @throws TimeoutException if timeout expires
     */
    public ProduceMetadata get(long timeout, TimeUnit unit) 
            throws ExecutionException, InterruptedException, TimeoutException {
        return future.get(timeout, unit);
    }

    /**
     * Checks if the operation is complete.
     *
     * @return true if complete (success or failure)
     */
    public boolean isDone() {
        return future.isDone();
    }

    /**
     * Adds a callback to be invoked when the operation completes.
     *
     * @param callback the callback function
     * @return this future for chaining
     */
    public ProduceFuture whenComplete(
            java.util.function.BiConsumer<? super ProduceMetadata, ? super Throwable> callback) {
        future.whenComplete(callback);
        return this;
    }

    /**
     * Gets the underlying CompletableFuture for advanced use cases.
     *
     * @return the CompletableFuture
     */
    public CompletableFuture<ProduceMetadata> toCompletableFuture() {
        return future;
    }
}


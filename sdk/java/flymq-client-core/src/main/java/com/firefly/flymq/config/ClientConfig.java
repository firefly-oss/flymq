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
package com.firefly.flymq.config;

import com.firefly.flymq.serialization.Deserializer;
import com.firefly.flymq.serialization.Serdes;
import com.firefly.flymq.serialization.Serializer;
import lombok.Builder;
import lombok.Data;

import java.util.Arrays;
import java.util.List;

/**
 * Configuration for FlyMQ client.
 */
@Data
@Builder
public class ClientConfig {

    /** Bootstrap servers (comma-separated or list). */
    @Builder.Default
    private String bootstrapServers = "localhost:9092";

    /** Connection timeout in milliseconds. */
    @Builder.Default
    private int connectTimeoutMs = 10000;

    /** Request timeout in milliseconds. */
    @Builder.Default
    private int requestTimeoutMs = 30000;

    /** Maximum number of retry attempts. */
    @Builder.Default
    private int maxRetries = 3;

    /** Delay between retries in milliseconds. */
    @Builder.Default
    private int retryDelayMs = 1000;

    /** Enable TLS/SSL connection. */
    @Builder.Default
    private boolean tlsEnabled = false;

    /** Path to CA certificate file. */
    private String tlsCaFile;

    /** Path to client certificate file (for mTLS). */
    private String tlsCertFile;

    /** Path to client key file (for mTLS). */
    private String tlsKeyFile;

    /** Skip server certificate verification (testing only). */
    @Builder.Default
    private boolean tlsInsecureSkipVerify = false;

    /** Username for authentication (optional). */
    private String username;

    /** Password for authentication (optional). */
    private String password;

    /** AES-256 encryption key (64-char hex string) for data-in-motion encryption. */
    private String encryptionKey;

    /** Client identifier. */
    @Builder.Default
    private String clientId = "flymq-java-client";

    /** Default serializer for values. */
    @Builder.Default
    private Serializer<?> valueSerializer = Serdes.BytesSerializer();

    /** Default deserializer for values. */
    @Builder.Default
    private Deserializer<?> valueDeserializer = Serdes.BytesDeserializer();

    /**
     * Gets the list of bootstrap servers.
     *
     * @return list of server addresses
     */
    public List<String> getServerList() {
        return Arrays.stream(bootstrapServers.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    /**
     * Creates a default configuration.
     *
     * @return default ClientConfig
     */
    public static ClientConfig defaultConfig() {
        return ClientConfig.builder().build();
    }

    /**
     * Creates a configuration for a single server.
     *
     * @param server server address (host:port)
     * @return ClientConfig for the server
     */
    public static ClientConfig forServer(String server) {
        return ClientConfig.builder()
                .bootstrapServers(server)
                .build();
    }

    /**
     * Creates a configuration for multiple servers (HA mode).
     *
     * @param servers comma-separated server addresses
     * @return ClientConfig for the servers
     */
    public static ClientConfig forServers(String servers) {
        return ClientConfig.builder()
                .bootstrapServers(servers)
                .build();
    }
}

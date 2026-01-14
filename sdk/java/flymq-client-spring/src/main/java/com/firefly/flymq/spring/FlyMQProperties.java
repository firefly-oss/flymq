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
package com.firefly.flymq.spring;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot configuration properties for FlyMQ client.
 *
 * <p>Example application.properties:
 * <pre>
 * flymq.bootstrap-servers=localhost:9092,localhost:9192
 * flymq.connect-timeout-ms=10000
 * flymq.tls-enabled=true
 * flymq.tls-ca-file=/path/to/ca.crt
 * </pre>
 */
@Data
@ConfigurationProperties(prefix = "flymq")
public class FlyMQProperties {

    /**
     * Whether FlyMQ client is enabled.
     */
    private boolean enabled = true;

    /**
     * Comma-separated list of bootstrap servers.
     */
    private String bootstrapServers = "localhost:9092";

    /**
     * Connection timeout in milliseconds.
     */
    private int connectTimeoutMs = 10000;

    /**
     * Request timeout in milliseconds.
     */
    private int requestTimeoutMs = 30000;

    /**
     * Maximum number of retry attempts.
     */
    private int maxRetries = 3;

    /**
     * Delay between retries in milliseconds.
     */
    private int retryDelayMs = 1000;

    /**
     * Enable TLS/SSL connection.
     */
    private boolean tlsEnabled = false;

    /**
     * Path to CA certificate file.
     */
    private String tlsCaFile;

    /**
     * Path to client certificate file (for mTLS).
     */
    private String tlsCertFile;

    /**
     * Path to client key file (for mTLS).
     */
    private String tlsKeyFile;

    /**
     * Skip server certificate verification (testing only).
     */
    private boolean tlsInsecureSkipVerify = false;

    /**
     * AES-256 encryption key (64-char hex string) for data-in-motion encryption.
     */
    private String encryptionKey;

    /**
     * Client identifier.
     */
    private String clientId = "flymq-spring-client";
}

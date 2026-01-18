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

/**
 * TLS/SSL configuration for secure FlyMQ connections.
 * 
 * <p>Security Levels:</p>
 * <ul>
 *   <li><b>No TLS</b>: Plain text communication (development only)</li>
 *   <li><b>TLS (Server Auth)</b>: Server certificate validation (production basic)</li>
 *   <li><b>TLS + CA</b>: Custom CA certificate (private PKI)</li>
 *   <li><b>Mutual TLS</b>: Client + server certificates (high security)</li>
 *   <li><b>Insecure TLS</b>: Skip certificate verification (testing/debugging only)</li>
 * </ul>
 * 
 * <p>Examples:</p>
 * <pre>{@code
 * // Secure TLS with certificate verification
 * TLSConfig tls = TLSConfig.builder()
 *     .enabled(true)
 *     .certFile("/path/to/client.crt")
 *     .keyFile("/path/to/client.key")
 *     .caFile("/path/to/ca.crt")
 *     .serverName("flymq-server")
 *     .build();
 * 
 * // Insecure TLS (skip certificate verification)
 * TLSConfig tls = TLSConfig.builder()
 *     .enabled(true)
 *     .insecureSkipVerify(true)
 *     .build();
 * 
 * // System CA certificates
 * TLSConfig tls = TLSConfig.builder()
 *     .enabled(true)
 *     .build();
 * }</pre>
 */
public class TLSConfig {
    private final boolean enabled;
    private final String certFile;
    private final String keyFile;
    private final String caFile;
    private final String serverName;
    private final boolean insecureSkipVerify;

    private TLSConfig(Builder builder) {
        this.enabled = builder.enabled;
        this.certFile = builder.certFile;
        this.keyFile = builder.keyFile;
        this.caFile = builder.caFile;
        this.serverName = builder.serverName;
        this.insecureSkipVerify = builder.insecureSkipVerify;
    }

    /**
     * Check if TLS is enabled.
     * @return true if TLS is enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Get client certificate file path.
     * @return client certificate file path (for mutual TLS)
     */
    public String getCertFile() {
        return certFile;
    }

    /**
     * Get client private key file path.
     * @return client private key file path (for mutual TLS)
     */
    public String getKeyFile() {
        return keyFile;
    }

    /**
     * Get CA certificate file path.
     * @return CA certificate file path for server verification
     */
    public String getCaFile() {
        return caFile;
    }

    /**
     * Get expected server name in certificate.
     * @return server name for SNI and verification
     */
    public String getServerName() {
        return serverName;
    }

    /**
     * Check if certificate verification should be skipped.
     * @return true if certificate verification is disabled (INSECURE)
     */
    public boolean isInsecureSkipVerify() {
        return insecureSkipVerify;
    }

    /**
     * Create a new builder.
     * @return TLSConfig builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for TLSConfig.
     */
    public static class Builder {
        private boolean enabled = false;
        private String certFile;
        private String keyFile;
        private String caFile;
        private String serverName;
        private boolean insecureSkipVerify = false;

        /**
         * Enable TLS encryption.
         * @param enabled true to enable TLS
         * @return this builder
         */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * Set client certificate file path (for mutual TLS).
         * @param certFile path to client certificate file
         * @return this builder
         */
        public Builder certFile(String certFile) {
            this.certFile = certFile;
            return this;
        }

        /**
         * Set client private key file path (for mutual TLS).
         * @param keyFile path to client private key file
         * @return this builder
         */
        public Builder keyFile(String keyFile) {
            this.keyFile = keyFile;
            return this;
        }

        /**
         * Set CA certificate file path for server verification.
         * @param caFile path to CA certificate file
         * @return this builder
         */
        public Builder caFile(String caFile) {
            this.caFile = caFile;
            return this;
        }

        /**
         * Set expected server name in certificate (for SNI and verification).
         * @param serverName expected server name
         * @return this builder
         */
        public Builder serverName(String serverName) {
            this.serverName = serverName;
            return this;
        }

        /**
         * Skip certificate verification (INSECURE - use only for testing).
         * @param insecureSkipVerify true to skip verification
         * @return this builder
         */
        public Builder insecureSkipVerify(boolean insecureSkipVerify) {
            this.insecureSkipVerify = insecureSkipVerify;
            return this;
        }

        /**
         * Build the TLSConfig.
         * @return configured TLSConfig instance
         */
        public TLSConfig build() {
            return new TLSConfig(this);
        }
    }
}

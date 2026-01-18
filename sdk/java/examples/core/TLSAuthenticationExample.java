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

package com.firefly.flymq.examples;

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.config.TLSConfig;
import com.firefly.flymq.protocol.BinaryProtocol;

/**
 * TLS and Authentication Examples for FlyMQ Java SDK.
 * 
 * This example demonstrates all TLS security levels:
 * 1. No TLS (plain text) - development only
 * 2. TLS with system CA - basic production
 * 3. TLS with custom CA - private PKI
 * 4. Mutual TLS - high security
 * 5. Insecure TLS - testing/debugging only
 */
public class TLSAuthenticationExample {

    /**
     * Example 1: Plain text connection (development only).
     */
    public static void example1NoTLS() throws Exception {
        System.out.println("============================================================");
        System.out.println("Example 1: No TLS (Plain Text)");
        System.out.println("============================================================");

        try (FlyMQClient client = FlyMQClient.connect("localhost:9092")) {
            // Create topic and produce message
            client.createTopic("tls-test", 1);
            BinaryProtocol.RecordMetadata meta = client.produce("tls-test", "Hello without TLS!".getBytes());
            System.out.println("✓ Produced message at offset " + meta.offset);

            // Consume message
            byte[] data = client.consume("tls-test", meta.offset);
            System.out.println("✓ Consumed: " + new String(data));
        }

        System.out.println();
    }

    /**
     * Example 2: TLS with system CA certificates.
     */
    public static void example2TLSSystemCA() throws Exception {
        System.out.println("============================================================");
        System.out.println("Example 2: TLS with System CA");
        System.out.println("============================================================");

        // Method 1: Using TLSConfig (recommended)
        TLSConfig tls = TLSConfig.builder()
            .enabled(true)
            .build();

        try (FlyMQClient client = FlyMQClient.builder()
                .address("localhost:9093")
                .tls(tls)
                .build()) {

            BinaryProtocol.RecordMetadata meta = client.produce("tls-test", "Hello with TLS!".getBytes());
            System.out.println("✓ Produced message at offset " + meta.offset);

            byte[] data = client.consume("tls-test", meta.offset);
            System.out.println("✓ Consumed: " + new String(data));
        }

        System.out.println();
    }

    /**
     * Example 3: TLS with custom CA certificate.
     */
    public static void example3TLSCustomCA() throws Exception {
        System.out.println("============================================================");
        System.out.println("Example 3: TLS with Custom CA");
        System.out.println("============================================================");

        TLSConfig tls = TLSConfig.builder()
            .enabled(true)
            .caFile("/etc/flymq/ca.crt")
            .build();

        try (FlyMQClient client = FlyMQClient.builder()
                .address("localhost:9093")
                .tls(tls)
                .build()) {

            BinaryProtocol.RecordMetadata meta = client.produce("tls-test", "Hello with custom CA!".getBytes());
            System.out.println("✓ Produced message at offset " + meta.offset);

            byte[] data = client.consume("tls-test", meta.offset);
            System.out.println("✓ Consumed: " + new String(data));
        }

        System.out.println();
    }

    /**
     * Example 4: Mutual TLS (client + server certificates).
     */
    public static void example4MutualTLS() throws Exception {
        System.out.println("============================================================");
        System.out.println("Example 4: Mutual TLS (High Security)");
        System.out.println("============================================================");

        TLSConfig tls = TLSConfig.builder()
            .enabled(true)
            .certFile("/etc/flymq/client.crt")
            .keyFile("/etc/flymq/client.key")
            .caFile("/etc/flymq/ca.crt")
            .serverName("flymq-server")  // Optional: override hostname verification
            .build();

        try (FlyMQClient client = FlyMQClient.builder()
                .address("localhost:9093")
                .tls(tls)
                .build()) {

            BinaryProtocol.RecordMetadata meta = client.produce("tls-test", "Hello with mutual TLS!".getBytes());
            System.out.println("✓ Produced message at offset " + meta.offset);

            byte[] data = client.consume("tls-test", meta.offset);
            System.out.println("✓ Consumed: " + new String(data));
        }

        System.out.println();
    }

    /**
     * Example 5: Insecure TLS (skip certificate verification).
     */
    public static void example5InsecureTLS() throws Exception {
        System.out.println("============================================================");
        System.out.println("Example 5: Insecure TLS (Testing Only)");
        System.out.println("============================================================");
        System.out.println("⚠️  WARNING: This skips certificate verification!");
        System.out.println("⚠️  Use only for testing/debugging, NOT for production!");
        System.out.println();

        TLSConfig tls = TLSConfig.builder()
            .enabled(true)
            .insecureSkipVerify(true)
            .build();

        try (FlyMQClient client = FlyMQClient.builder()
                .address("localhost:9093")
                .tls(tls)
                .build()) {

            BinaryProtocol.RecordMetadata meta = client.produce("tls-test", "Hello with insecure TLS!".getBytes());
            System.out.println("✓ Produced message at offset " + meta.offset);

            byte[] data = client.consume("tls-test", meta.offset);
            System.out.println("✓ Consumed: " + new String(data));
        }

        System.out.println();
    }

    /**
     * Example 6: TLS + Authentication.
     */
    public static void example6TLSWithAuthentication() throws Exception {
        System.out.println("============================================================");
        System.out.println("Example 6: TLS + Authentication");
        System.out.println("============================================================");

        TLSConfig tls = TLSConfig.builder()
            .enabled(true)
            .caFile("/etc/flymq/ca.crt")
            .build();

        try (FlyMQClient client = FlyMQClient.builder()
                .address("localhost:9093")
                .tls(tls)
                .username("admin")
                .password("secret")
                .build()) {

            // Check authentication status
            FlyMQClient.WhoAmIResponse whoami = client.whoami();
            System.out.println("✓ Authenticated as: " + whoami.username);
            System.out.println("✓ Roles: " + String.join(", ", whoami.roles));

            BinaryProtocol.RecordMetadata meta = client.produce("tls-test", "Secure and authenticated!".getBytes());
            System.out.println("✓ Produced message at offset " + meta.offset);
        }

        System.out.println();
    }

    /**
     * Example 7: Complete security stack (TLS + Authentication).
     */
    public static void example7FullSecurityStack() throws Exception {
        System.out.println("============================================================");
        System.out.println("Example 7: Complete Security Stack");
        System.out.println("============================================================");

        TLSConfig tls = TLSConfig.builder()
            .enabled(true)
            .caFile("/etc/flymq/ca.crt")
            .build();

        // TLS encrypts data in transit
        // Authentication provides access control
        // Server handles data-at-rest encryption with FLYMQ_ENCRYPTION_KEY
        try (FlyMQClient client = FlyMQClient.builder()
                .address("localhost:9093")
                .tls(tls)
                .username("admin")
                .password("secret")
                .build()) {

            // Check authentication status
            FlyMQClient.WhoAmIResponse whoami = client.whoami();
            System.out.println("✓ Authenticated as: " + whoami.username);
            System.out.println("✓ Roles: " + String.join(", ", whoami.roles));

            BinaryProtocol.RecordMetadata meta = client.produce("tls-test", "Fully secured message!".getBytes());
            System.out.println("✓ Produced message at offset " + meta.offset);
            System.out.println("✓ Data encrypted in transit (TLS)");
            System.out.println("✓ Data encrypted at rest (server-side)");

            byte[] data = client.consume("tls-test", meta.offset);
            System.out.println("✓ Consumed: " + new String(data));
        }

        System.out.println();
    }

    /**
     * Example 8: TLS with try-with-resources (recommended).
     */
    public static void example8TryWithResources() throws Exception {
        System.out.println("============================================================");
        System.out.println("Example 8: TLS with Try-With-Resources");
        System.out.println("============================================================");

        TLSConfig tls = TLSConfig.builder()
            .enabled(true)
            .caFile("/etc/flymq/ca.crt")
            .build();

        try (FlyMQClient client = FlyMQClient.builder()
                .address("localhost:9093")
                .tls(tls)
                .build()) {

            BinaryProtocol.RecordMetadata meta = client.produce("tls-test", "Hello from try-with-resources!".getBytes());
            System.out.println("✓ Produced message at offset " + meta.offset);

            byte[] data = client.consume("tls-test", meta.offset);
            System.out.println("✓ Consumed: " + new String(data));
        } // Connection automatically closed

        System.out.println("✓ Connection automatically closed");
        System.out.println();
    }

    /**
     * Example 9: High availability with TLS.
     */
    public static void example9HighAvailabilityWithTLS() throws Exception {
        System.out.println("============================================================");
        System.out.println("Example 9: High Availability with TLS");
        System.out.println("============================================================");

        TLSConfig tls = TLSConfig.builder()
            .enabled(true)
            .caFile("/etc/flymq/ca.crt")
            .build();

        // Multiple servers for automatic failover
        String[] servers = {"server1:9093", "server2:9093", "server3:9093"};

        try (FlyMQClient client = FlyMQClient.builder()
                .addresses(servers)
                .tls(tls)
                .build()) {

            BinaryProtocol.RecordMetadata meta = client.produce("tls-test", "HA with TLS!".getBytes());
            System.out.println("✓ Produced message at offset " + meta.offset);
            System.out.println("✓ Client will automatically failover if a server goes down");
        }

        System.out.println();
    }

    /**
     * Main method to run all examples.
     */
    public static void main(String[] args) {
        System.out.println("\n============================================================");
        System.out.println("FlyMQ Java SDK TLS Examples");
        System.out.println("============================================================\n");

        try {
            // Run examples (comment out those that require specific setup)
            example1NoTLS();

            // Uncomment these when you have TLS configured:
            // example2TLSSystemCA();
            // example3TLSCustomCA();
            // example4MutualTLS();
            // example5InsecureTLS();
            // example6TLSWithAuthentication();
            // example7FullSecurityStack();
            // example8TryWithResources();
            // example9HighAvailabilityWithTLS();

            System.out.println("============================================================");
            System.out.println("✓ All examples completed successfully!");
            System.out.println("============================================================");

        } catch (Exception e) {
            System.err.println("\n❌ Error: " + e.getMessage());
            System.err.println("\nNote: Some examples require TLS to be configured on the server.");
            System.err.println("See docs/SECURITY.md for setup instructions.");
            e.printStackTrace();
        }
    }
}

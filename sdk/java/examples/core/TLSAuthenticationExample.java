/*
 * TLSAuthenticationExample.java - FlyMQ TLS and Authentication Example
 *
 * This example demonstrates:
 * - Connecting with TLS encryption
 * - Username/password authentication
 * - Token-based authentication
 * - Verifying connection security
 *
 * Prerequisites:
 *     - FlyMQ server with TLS enabled on port 9093
 *     - Valid certificates and credentials
 *
 * Run with:
 *     javac -cp flymq-client.jar TLSAuthenticationExample.java
 *     java -cp .:flymq-client.jar TLSAuthenticationExample
 */

import com.firefly.flymq.FlyMQClient;
import com.firefly.flymq.config.ClientConfig;
import com.firefly.flymq.exception.FlyMQException;

public class TLSAuthenticationExample {

    public static void main(String[] args) {
        System.out.println("FlyMQ TLS & Authentication Example");
        System.out.println("===================================");

        // Example 1: TLS connection
        tlsConnection();

        // Example 2: Username/password authentication
        usernamePasswordAuth();

        // Example 3: Token-based authentication
        tokenAuth();

        // Example 4: Full secure connection
        fullSecureConnection();
    }

    /**
     * Connect using TLS encryption.
     */
    private static void tlsConnection() {
        System.out.println("\n1. TLS Connection");
        System.out.println("-----------------");

        ClientConfig config = ClientConfig.builder()
            .bootstrapServers("localhost:9093")
            .tlsEnabled(true)
            .tlsCaFile("/path/to/ca.crt")
            .tlsVerifyHostname(true)
            .build();

        try (FlyMQClient client = new FlyMQClient(config)) {
            System.out.println("✓ Connected with TLS encryption");
            
            // Verify connection works
            client.createTopic("tls-test", 1);
            client.produce("tls-test", "Secure message".getBytes());
            System.out.println("✓ Secure message sent successfully");

        } catch (FlyMQException e) {
            System.out.println("Connection failed: " + e.getMessage());
            System.out.println("  (This is expected if TLS is not configured)");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    /**
     * Authenticate with username and password.
     */
    private static void usernamePasswordAuth() {
        System.out.println("\n2. Username/Password Authentication");
        System.out.println("------------------------------------");

        try (FlyMQClient client = new FlyMQClient("localhost:9092")) {
            // Authenticate with credentials
            String token = client.authenticate("admin", "secret123");
            System.out.println("✓ Authenticated successfully");
            System.out.println("  Token: " + token.substring(0, 20) + "...");

            // Verify identity
            String identity = client.whoAmI();
            System.out.println("  Identity: " + identity);

            // Now perform operations as authenticated user
            client.produce("secure-topic", "Authenticated message".getBytes());
            System.out.println("✓ Message sent as authenticated user");

        } catch (FlyMQException e) {
            System.out.println("Authentication failed: " + e.getMessage());
            System.out.println("  (This is expected if auth is not configured)");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    /**
     * Authenticate with a pre-existing token.
     */
    private static void tokenAuth() {
        System.out.println("\n3. Token-Based Authentication");
        System.out.println("------------------------------");

        // In production, token would come from a secure source
        String existingToken = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...";

        ClientConfig config = ClientConfig.builder()
            .bootstrapServers("localhost:9092")
            .authToken(existingToken)
            .build();

        try (FlyMQClient client = new FlyMQClient(config)) {
            System.out.println("✓ Connected with token authentication");

            String identity = client.whoAmI();
            System.out.println("  Identity: " + identity);

        } catch (FlyMQException e) {
            System.out.println("Token auth failed: " + e.getMessage());
            System.out.println("  (This is expected with invalid token)");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    /**
     * Full secure connection with TLS + authentication.
     */
    private static void fullSecureConnection() {
        System.out.println("\n4. Full Secure Connection (TLS + Auth)");
        System.out.println("---------------------------------------");

        ClientConfig config = ClientConfig.builder()
            .bootstrapServers("localhost:9093")
            // TLS settings
            .tlsEnabled(true)
            .tlsCaFile("/path/to/ca.crt")
            .tlsCertFile("/path/to/client.crt")
            .tlsKeyFile("/path/to/client.key")
            .tlsVerifyHostname(true)
            // Connection settings
            .connectTimeoutMs(10000)
            .requestTimeoutMs(30000)
            .build();

        try (FlyMQClient client = new FlyMQClient(config)) {
            // Authenticate after TLS handshake
            client.authenticate("admin", "secret123");
            System.out.println("✓ Fully secure connection established");
            System.out.println("  - TLS encryption: enabled");
            System.out.println("  - Authentication: verified");

        } catch (Exception e) {
            System.out.println("Secure connection failed: " + e.getMessage());
            System.out.println("  (This is expected without proper certificates)");
        }
    }
}


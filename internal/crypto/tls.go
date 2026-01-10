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

/*
TLS Configuration for FlyMQ.

This file provides TLS configuration for both server and client connections.
FlyMQ supports mutual TLS (mTLS) for strong authentication.

SECURITY DEFAULTS:
==================
- Minimum TLS version: 1.2
- Strong cipher suites only (ECDHE + AES-GCM or ChaCha20)
- Optional client certificate verification (mTLS)

CERTIFICATE SETUP:
==================
Generate self-signed certificates for testing:

	# Generate CA
	openssl genrsa -out ca.key 4096
	openssl req -new -x509 -days 365 -key ca.key -out ca.crt

	# Generate server certificate
	openssl genrsa -out server.key 2048
	openssl req -new -key server.key -out server.csr
	openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key -out server.crt

For production, use certificates from a trusted CA.
*/
package crypto

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

var (
	// ErrCertNotFound is returned when the certificate file cannot be read.
	ErrCertNotFound = errors.New("tls: certificate file not found")

	// ErrKeyNotFound is returned when the key file cannot be read.
	ErrKeyNotFound = errors.New("tls: key file not found")

	// ErrInvalidCertificate is returned when the certificate is invalid.
	ErrInvalidCertificate = errors.New("tls: invalid certificate")

	// ErrCANotFound is returned when the CA certificate file cannot be read.
	ErrCANotFound = errors.New("tls: CA certificate file not found")
)

// TLSConfig holds TLS configuration options.
type TLSConfig struct {
	// CertFile is the path to the server certificate file (PEM format).
	CertFile string

	// KeyFile is the path to the server private key file (PEM format).
	KeyFile string

	// CAFile is the path to the CA certificate file for client verification (optional).
	CAFile string

	// ClientAuth specifies the client authentication policy.
	// Use tls.NoClientCert, tls.RequestClientCert, tls.RequireAndVerifyClientCert, etc.
	ClientAuth tls.ClientAuthType

	// MinVersion is the minimum TLS version (default: TLS 1.2).
	MinVersion uint16

	// InsecureSkipVerify disables certificate verification (for testing only).
	InsecureSkipVerify bool
}

// NewServerTLSConfig creates a TLS configuration for the server.
func NewServerTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrCertNotFound, cfg.CertFile)
		}
		return nil, fmt.Errorf("tls: failed to load certificate: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
	}

	if cfg.MinVersion != 0 {
		tlsConfig.MinVersion = cfg.MinVersion
	}

	// Configure client certificate verification if CA file is provided
	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrCANotFound, cfg.CAFile)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, ErrInvalidCertificate
		}

		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = cfg.ClientAuth
	}

	return tlsConfig, nil
}

// NewClientTLSConfig creates a TLS configuration for clients.
func NewClientTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}

	// Load client certificate if provided
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificate for server verification
	if cfg.CAFile != "" {
		caCert, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", ErrCANotFound, cfg.CAFile)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, ErrInvalidCertificate
		}

		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}

// ValidateTLSFiles checks if the TLS certificate and key files exist and are valid.
func ValidateTLSFiles(certFile, keyFile string) error {
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		return fmt.Errorf("%w: %s", ErrCertNotFound, certFile)
	}
	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		return fmt.Errorf("%w: %s", ErrKeyNotFound, keyFile)
	}

	// Try to load the certificate to validate it
	_, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("tls: invalid certificate or key: %w", err)
	}

	return nil
}

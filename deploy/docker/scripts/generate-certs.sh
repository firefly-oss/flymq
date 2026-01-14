#!/bin/bash
# =============================================================================
# FlyMQ TLS Certificate Generator
# =============================================================================
#
# Generates self-signed TLS certificates for FlyMQ cluster deployment.
# Creates CA, server, and client certificates for secure communication.
#
# Usage: ./generate-certs.sh [output_dir]
#
# =============================================================================

set -e

# Configuration
CERT_DIR="${1:-./certs}"
DAYS_VALID=365
KEY_SIZE=4096
CA_SUBJECT="/C=US/ST=California/L=San Francisco/O=FlyMQ/OU=Security/CN=FlyMQ CA"
SERVER_SUBJECT="/C=US/ST=California/L=San Francisco/O=FlyMQ/OU=Server/CN=flymq-server"
CLIENT_SUBJECT="/C=US/ST=California/L=San Francisco/O=FlyMQ/OU=Client/CN=flymq-client"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

print_step() { echo -e "${CYAN}→${NC} $1"; }
print_success() { echo -e "${GREEN}✓${NC} $1"; }
print_warning() { echo -e "${YELLOW}⚠${NC} $1"; }

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}  FlyMQ TLS Certificate Generator${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Create certificate directory
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

# Generate CA private key and certificate
print_step "Generating CA certificate..."
openssl genrsa -out ca.key $KEY_SIZE 2>/dev/null
openssl req -new -x509 -days $DAYS_VALID -key ca.key -out ca.crt \
    -subj "$CA_SUBJECT" 2>/dev/null
print_success "CA certificate created"

# Create server certificate config with SANs
cat > server.cnf << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = California
L = San Francisco
O = FlyMQ
OU = Server
CN = flymq-server

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = flymq-bootstrap
DNS.3 = flymq-agent
DNS.4 = flymq-standalone
DNS.5 = *.flymq.local
DNS.6 = flymq-node-1
DNS.7 = flymq-node-2
DNS.8 = flymq-node-3
IP.1 = 127.0.0.1
IP.2 = 0.0.0.0
EOF

# Generate server private key and CSR
print_step "Generating server certificate..."
openssl genrsa -out server.key $KEY_SIZE 2>/dev/null
openssl req -new -key server.key -out server.csr -config server.cnf 2>/dev/null

# Sign server certificate with CA
openssl x509 -req -days $DAYS_VALID -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt -extensions v3_req -extfile server.cnf 2>/dev/null
print_success "Server certificate created"

# Create client certificate config
cat > client.cnf << EOF
[req]
distinguished_name = req_distinguished_name
req_extensions = v3_req
prompt = no

[req_distinguished_name]
C = US
ST = California
L = San Francisco
O = FlyMQ
OU = Client
CN = flymq-client

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

# Generate client private key and CSR
print_step "Generating client certificate..."
openssl genrsa -out client.key $KEY_SIZE 2>/dev/null
openssl req -new -key client.key -out client.csr -config client.cnf 2>/dev/null

# Sign client certificate with CA
openssl x509 -req -days $DAYS_VALID -in client.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out client.crt -extensions v3_req -extfile client.cnf 2>/dev/null
print_success "Client certificate created"

# Clean up CSR and config files
rm -f *.csr *.cnf *.srl

# Set permissions
chmod 600 *.key
chmod 644 *.crt

echo ""
print_success "All certificates generated in: $(pwd)"
echo ""
echo "  Files created:"
echo "    ca.crt      - CA certificate (distribute to clients)"
echo "    ca.key      - CA private key (keep secure)"
echo "    server.crt  - Server certificate"
echo "    server.key  - Server private key"
echo "    client.crt  - Client certificate"
echo "    client.key  - Client private key"
echo ""


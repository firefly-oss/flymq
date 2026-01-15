#!/bin/bash
# FlyMQ Security Test Script
# 
# This script demonstrates how to test authentication, RBAC, and ACL features
# via the CLI. Run this against a server with authentication enabled.
#
# Prerequisites:
# 1. Start FlyMQ with auth enabled:
#    FLYMQ_AUTH_ENABLED=true FLYMQ_AUTH_ADMIN_USERNAME=admin FLYMQ_AUTH_ADMIN_PASSWORD=secret ./bin/flymq
#
# 2. Set environment variables or use --username/-u and --password/-P flags
#    export FLYMQ_USERNAME=admin
#    export FLYMQ_PASSWORD=secret

set -e

CLI="./bin/flymq-cli"
ADMIN_USER="${FLYMQ_USERNAME:-admin}"
ADMIN_PASS="${FLYMQ_PASSWORD:-secret}"

echo "=================================="
echo "FlyMQ Security Test Suite"
echo "=================================="
echo ""

# Helper function to run CLI commands
run() {
    echo "$ $CLI $*"
    $CLI -u "$ADMIN_USER" -P "$ADMIN_PASS" "$@" || true
    echo ""
}

# Helper for unauthenticated commands
run_noauth() {
    echo "$ $CLI $* (no auth)"
    $CLI "$@" || true
    echo ""
}

echo "=== 1. Authentication Tests ==="
echo ""

echo "1.1 Check current auth status (WhoAmI):"
run whoami

echo "1.2 Authenticate with valid credentials:"
run auth --username "$ADMIN_USER" --password "$ADMIN_PASS"

echo "1.3 Attempt authentication with invalid credentials (expect error):"
$CLI auth --username "admin" --password "wrongpassword" 2>&1 || echo "(Expected: authentication failed)"
echo ""

echo "=== 2. User Management Tests ==="
echo ""

echo "2.1 List all users:"
run users list

echo "2.2 Create a new consumer user:"
run users create testconsumer testpass --roles consumer

echo "2.3 Create a new producer user:"
run users create testproducer testpass --roles producer

echo "2.4 List users again:"
run users list

echo "2.5 Get user details:"
run users get testconsumer

echo "=== 3. Role Tests ==="
echo ""

echo "3.1 List all available roles:"
run roles list

echo "=== 4. Topic Security Tests ==="
echo ""

echo "4.1 Create a test topic (admin):"
run create secure-topic --partitions 1

echo "4.2 Produce a message (admin):"
run produce secure-topic "Admin message"

echo "4.3 Set topic ACL to private with specific user:"
run acl set secure-topic --users testconsumer

echo "4.4 Get topic ACL:"
run acl get secure-topic

echo "4.5 List all ACLs:"
run acl list

echo "=== 5. RBAC Enforcement Tests ==="
echo ""

echo "5.1 Consumer can read from topic:"
echo "$ $CLI -u testconsumer -P testpass consume secure-topic --count 1"
$CLI -u testconsumer -P testpass consume secure-topic --count 1 2>&1 || echo "(Result varies based on permissions)"
echo ""

echo "5.2 Consumer cannot create topics (expect error):"
echo "$ $CLI -u testconsumer -P testpass create consumer-topic"
$CLI -u testconsumer -P testpass create consumer-topic 2>&1 || echo "(Expected: unauthorized)"
echo ""

echo "5.3 Producer can write to topic:"
echo "$ $CLI -u testproducer -P testpass produce secure-topic 'Producer message'"
$CLI -u testproducer -P testpass produce secure-topic "Producer message" 2>&1 || echo "(Result varies based on permissions)"
echo ""

echo "=== 6. Cleanup ==="
echo ""

echo "6.1 Delete topic ACL:"
run acl delete secure-topic

echo "6.2 Delete test users:"
run users delete testconsumer
run users delete testproducer

echo "6.3 Delete test topic:"
run delete secure-topic

echo "=================================="
echo "Security tests completed!"
echo "=================================="

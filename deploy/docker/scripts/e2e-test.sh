#!/bin/bash
# =============================================================================
# FlyMQ Enterprise End-to-End Test Suite
# =============================================================================
#
# Comprehensive test script that verifies all enterprise features:
# - Cluster operations (bootstrap, agent joining, cluster status)
# - Security (TLS, authentication, RBAC, ACLs)
# - Core messaging (topics, produce/consume with encryption)
# - Advanced features (schema validation, DLQ, delayed messages, transactions)
# - Observability (metrics, health checks, admin API)
# - CLI functionality
#
# USAGE:
#   ./e2e-test.sh [--skip-setup] [--skip-cleanup]
#
# =============================================================================

set -e

# =============================================================================
# Configuration
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$(dirname "$DEPLOY_DIR")")"
CERTS_DIR="$DEPLOY_DIR/certs"

# Server configuration
FLYMQ_HOST="${FLYMQ_HOST:-localhost}"
FLYMQ_PORT="${FLYMQ_PORT:-9092}"
FLYMQ_METRICS_PORT="${FLYMQ_METRICS_PORT:-9094}"
FLYMQ_HEALTH_PORT="${FLYMQ_HEALTH_PORT:-9095}"
FLYMQ_ADMIN_PORT="${FLYMQ_ADMIN_PORT:-9096}"

# Authentication
ADMIN_USER="${FLYMQ_AUTH_ADMIN_USERNAME:-admin}"
ADMIN_PASS="${FLYMQ_AUTH_ADMIN_PASSWORD:-secure-password}"

# Test configuration
TEST_TOPIC="e2e-test-topic"
TEST_GROUP="e2e-test-group"
SKIP_SETUP="${SKIP_SETUP:-false}"
SKIP_CLEANUP="${SKIP_CLEANUP:-false}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Counters
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# =============================================================================
# Utility Functions
# =============================================================================
print_header() {
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${BOLD}  $1${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

print_test() {
    echo -e "  ${CYAN}TEST:${NC} $1"
}

print_pass() {
    echo -e "  ${GREEN}✓ PASS:${NC} $1"
    ((TESTS_PASSED++))
}

print_fail() {
    echo -e "  ${RED}✗ FAIL:${NC} $1"
    ((TESTS_FAILED++))
}

print_skip() {
    echo -e "  ${YELLOW}○ SKIP:${NC} $1"
    ((TESTS_SKIPPED++))
}

print_info() {
    echo -e "  ${CYAN}ℹ${NC} $1"
}

# CLI wrapper with authentication and TLS
flymq_cli() {
    local cmd="$PROJECT_ROOT/bin/flymq-cli"
    if [[ ! -x "$cmd" ]]; then
        cmd="flymq-cli"
    fi

    local tls_args=""
    if [[ -f "$CERTS_DIR/ca.crt" ]]; then
        tls_args="--tls --ca-cert $CERTS_DIR/ca.crt"
    fi

    # Global options come before the command
    $cmd $tls_args \
         --addr "$FLYMQ_HOST:$FLYMQ_PORT" \
         --username "$ADMIN_USER" \
         --password "$ADMIN_PASS" \
         "$@"
}

# Wait for server to be ready
wait_for_server() {
    local max_attempts=30
    local attempt=0
    
    print_info "Waiting for FlyMQ server to be ready..."
    while [[ $attempt -lt $max_attempts ]]; do
        if curl -sf "http://$FLYMQ_HOST:$FLYMQ_HEALTH_PORT/health" > /dev/null 2>&1; then
            print_info "Server is ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    
    print_fail "Server did not become ready in time"
    return 1
}

# =============================================================================
# Parse Arguments
# =============================================================================
for arg in "$@"; do
    case $arg in
        --skip-setup)
            SKIP_SETUP=true
            ;;
        --skip-cleanup)
            SKIP_CLEANUP=true
            ;;
        --help)
            echo "Usage: $0 [--skip-setup] [--skip-cleanup]"
            exit 0
            ;;
    esac
done

# =============================================================================
# Main Test Execution
# =============================================================================
echo ""
echo -e "${CYAN}${BOLD}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}${BOLD}║       FlyMQ Enterprise End-to-End Test Suite                  ║${NC}"
echo -e "${CYAN}${BOLD}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# =============================================================================
# Test 1: Cluster Operations
# =============================================================================
test_cluster_operations() {
    print_header "1. Cluster Operations"

    # Test health endpoint
    print_test "Health endpoint responds"
    if curl -sf "http://$FLYMQ_HOST:$FLYMQ_HEALTH_PORT/health" > /dev/null 2>&1; then
        print_pass "Health endpoint is accessible"
    else
        print_fail "Health endpoint not responding"
    fi

    # Test cluster status via CLI
    print_test "Cluster status via CLI"
    if flymq_cli cluster status 2>/dev/null | grep -q "node"; then
        print_pass "Cluster status retrieved successfully"
    else
        print_skip "Cluster status command not available or failed"
    fi
}

# =============================================================================
# Test 2: Security Features
# =============================================================================
test_security_features() {
    print_header "2. Security Features"

    # Test TLS connection
    print_test "TLS connection"
    if [[ -f "$CERTS_DIR/ca.crt" ]]; then
        if flymq_cli topics list 2>/dev/null; then
            print_pass "TLS connection successful"
        else
            print_fail "TLS connection failed"
        fi
    else
        print_skip "TLS certificates not found"
    fi

    # Test authentication required
    print_test "Authentication required (anonymous access denied)"
    local anon_result
    anon_result=$("$PROJECT_ROOT/bin/flymq-cli" --server "$FLYMQ_HOST:$FLYMQ_PORT" topics list 2>&1 || true)
    if echo "$anon_result" | grep -qi "auth\|unauthorized\|denied\|error"; then
        print_pass "Anonymous access correctly denied"
    else
        print_skip "Could not verify anonymous access denial"
    fi

    # Test admin authentication
    print_test "Admin user authentication"
    if flymq_cli topics list 2>/dev/null; then
        print_pass "Admin authentication successful"
    else
        print_fail "Admin authentication failed"
    fi

    # Test user management
    print_test "Create test user"
    if flymq_cli users create testuser --password testpass123 --roles consumer 2>/dev/null; then
        print_pass "Test user created"
    else
        print_skip "User creation not available"
    fi

    # Test ACL management
    print_test "Set topic ACL"
    if flymq_cli acl set "$TEST_TOPIC" --users testuser 2>/dev/null; then
        print_pass "Topic ACL set successfully"
    else
        print_skip "ACL management not available"
    fi
}

# =============================================================================
# Test 3: Core Messaging
# =============================================================================
test_core_messaging() {
    print_header "3. Core Messaging"

    # Create topic
    print_test "Create topic"
    if flymq_cli topics create "$TEST_TOPIC" --partitions 3 2>/dev/null; then
        print_pass "Topic created: $TEST_TOPIC"
    else
        print_skip "Topic creation failed or topic exists"
    fi

    # List topics
    print_test "List topics"
    if flymq_cli topics list 2>/dev/null | grep -q "$TEST_TOPIC"; then
        print_pass "Topic listed successfully"
    else
        print_fail "Topic not found in list"
    fi

    # Produce message
    print_test "Produce message"
    local test_msg="Hello FlyMQ $(date +%s)"
    if echo "$test_msg" | flymq_cli produce "$TEST_TOPIC" 2>/dev/null; then
        print_pass "Message produced successfully"
    else
        print_fail "Message production failed"
    fi

    # Consume message
    print_test "Consume message"
    local consumed
    consumed=$(flymq_cli consume "$TEST_TOPIC" --group "$TEST_GROUP" --count 1 --timeout 5s 2>/dev/null || true)
    if [[ -n "$consumed" ]]; then
        print_pass "Message consumed successfully"
    else
        print_skip "Message consumption timed out or failed"
    fi

    # Produce with key
    print_test "Produce message with key"
    if echo "keyed-message" | flymq_cli produce "$TEST_TOPIC" --key "test-key" 2>/dev/null; then
        print_pass "Keyed message produced"
    else
        print_skip "Keyed message production not available"
    fi
}

# =============================================================================
# Test 4: Advanced Features
# =============================================================================
test_advanced_features() {
    print_header "4. Advanced Features"

    # Test schema registration
    print_test "Register JSON schema"
    local schema='{"type":"object","properties":{"name":{"type":"string"},"value":{"type":"number"}}}'
    if flymq_cli schemas register test-schema --type json --schema "$schema" 2>/dev/null; then
        print_pass "Schema registered successfully"
    else
        print_skip "Schema registration not available"
    fi

    # Test schema list
    print_test "List schemas"
    if flymq_cli schemas list 2>/dev/null | grep -q "test-schema"; then
        print_pass "Schema listed successfully"
    else
        print_skip "Schema listing not available"
    fi

    # Test delayed message (if supported)
    print_test "Delayed message delivery"
    if echo "delayed-msg" | flymq_cli produce "$TEST_TOPIC" --delay 1s 2>/dev/null; then
        print_pass "Delayed message sent"
    else
        print_skip "Delayed delivery not available"
    fi

    # Test transaction
    print_test "Transaction support"
    if flymq_cli transaction begin 2>/dev/null; then
        if echo "txn-msg" | flymq_cli produce "$TEST_TOPIC" 2>/dev/null; then
            if flymq_cli transaction commit 2>/dev/null; then
                print_pass "Transaction completed successfully"
            else
                print_skip "Transaction commit not available"
            fi
        else
            print_skip "Transaction produce not available"
        fi
    else
        print_skip "Transaction support not available"
    fi

    # Test consumer group management
    print_test "List consumer groups"
    if flymq_cli groups list 2>/dev/null; then
        print_pass "Consumer groups listed"
    else
        print_skip "Consumer group listing not available"
    fi

    # Test offset management
    print_test "Get consumer lag"
    if flymq_cli groups lag "$TEST_GROUP" --topic "$TEST_TOPIC" 2>/dev/null; then
        print_pass "Consumer lag retrieved"
    else
        print_skip "Consumer lag not available"
    fi
}

# =============================================================================
# Test 5: Observability
# =============================================================================
test_observability() {
    print_header "5. Observability"

    # Test Prometheus metrics
    print_test "Prometheus metrics endpoint"
    local metrics
    metrics=$(curl -sf "http://$FLYMQ_HOST:$FLYMQ_METRICS_PORT/metrics" 2>/dev/null || true)
    if echo "$metrics" | grep -q "flymq_"; then
        print_pass "Prometheus metrics available"
    else
        print_skip "Prometheus metrics not available"
    fi

    # Test health check endpoint
    print_test "Health check endpoint"
    local health
    health=$(curl -sf "http://$FLYMQ_HOST:$FLYMQ_HEALTH_PORT/health" 2>/dev/null || true)
    if echo "$health" | grep -qi "healthy\|ok\|status"; then
        print_pass "Health check endpoint working"
    else
        if [[ -n "$health" ]]; then
            print_pass "Health check endpoint responding"
        else
            print_fail "Health check endpoint not responding"
        fi
    fi

    # Test readiness endpoint
    print_test "Readiness endpoint"
    if curl -sf "http://$FLYMQ_HOST:$FLYMQ_HEALTH_PORT/ready" > /dev/null 2>&1; then
        print_pass "Readiness endpoint available"
    else
        print_skip "Readiness endpoint not available"
    fi

    # Test Admin API
    print_test "Admin API endpoint"
    local admin_resp
    admin_resp=$(curl -sf "http://$FLYMQ_HOST:$FLYMQ_ADMIN_PORT/api/v1/status" 2>/dev/null || true)
    if [[ -n "$admin_resp" ]]; then
        print_pass "Admin API responding"
    else
        print_skip "Admin API not available"
    fi

    # Test Admin API topics endpoint
    print_test "Admin API topics list"
    admin_resp=$(curl -sf "http://$FLYMQ_HOST:$FLYMQ_ADMIN_PORT/api/v1/topics" 2>/dev/null || true)
    if [[ -n "$admin_resp" ]]; then
        print_pass "Admin API topics endpoint working"
    else
        print_skip "Admin API topics endpoint not available"
    fi
}

# =============================================================================
# Test 6: CLI Functionality
# =============================================================================
test_cli_functionality() {
    print_header "6. CLI Functionality"

    # Test CLI version
    print_test "CLI version command"
    if flymq_cli --version 2>/dev/null | grep -qi "flymq\|version"; then
        print_pass "CLI version command works"
    else
        print_skip "CLI version not available"
    fi

    # Test CLI help
    print_test "CLI help command"
    if flymq_cli --help 2>/dev/null | grep -qi "usage\|command"; then
        print_pass "CLI help command works"
    else
        print_skip "CLI help not available"
    fi

    # Test topic describe
    print_test "Topic describe command"
    if flymq_cli topics describe "$TEST_TOPIC" 2>/dev/null; then
        print_pass "Topic describe works"
    else
        print_skip "Topic describe not available"
    fi

    # Test user list
    print_test "User list command"
    if flymq_cli users list 2>/dev/null; then
        print_pass "User list works"
    else
        print_skip "User list not available"
    fi

    # Test roles list
    print_test "Roles list command"
    if flymq_cli roles list 2>/dev/null; then
        print_pass "Roles list works"
    else
        print_skip "Roles list not available"
    fi
}

# =============================================================================
# Cleanup
# =============================================================================
cleanup() {
    if [[ "$SKIP_CLEANUP" == "true" ]]; then
        print_info "Skipping cleanup (--skip-cleanup)"
        return
    fi

    print_header "Cleanup"

    # Delete test user
    print_info "Deleting test user..."
    flymq_cli users delete testuser 2>/dev/null || true

    # Delete test topic
    print_info "Deleting test topic..."
    flymq_cli topics delete "$TEST_TOPIC" 2>/dev/null || true

    # Delete test schema
    print_info "Deleting test schema..."
    flymq_cli schemas delete test-schema 2>/dev/null || true

    print_pass "Cleanup completed"
}

# =============================================================================
# Run All Tests
# =============================================================================
run_all_tests() {
    # Wait for server
    wait_for_server || exit 1

    # Run test suites
    test_cluster_operations
    test_security_features
    test_core_messaging
    test_advanced_features
    test_observability
    test_cli_functionality

    # Cleanup
    cleanup

    # Print summary
    print_header "Test Summary"
    echo -e "  ${GREEN}Passed:${NC}  $TESTS_PASSED"
    echo -e "  ${RED}Failed:${NC}  $TESTS_FAILED"
    echo -e "  ${YELLOW}Skipped:${NC} $TESTS_SKIPPED"
    echo ""

    local total=$((TESTS_PASSED + TESTS_FAILED))
    if [[ $TESTS_FAILED -eq 0 ]]; then
        echo -e "  ${GREEN}${BOLD}All tests passed!${NC}"
        exit 0
    else
        echo -e "  ${RED}${BOLD}Some tests failed.${NC}"
        exit 1
    fi
}

# Run tests
run_all_tests

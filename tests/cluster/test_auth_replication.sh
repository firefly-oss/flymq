#!/bin/bash
# Test script for verifying user and ACL replication across a FlyMQ cluster
# This script tests that user/ACL changes made on the leader are replicated to followers

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
NODE1_ADDR="localhost:9092"
NODE2_ADDR="localhost:9192"
NODE3_ADDR="localhost:9292"
ADMIN_USER="admin"
ADMIN_PASS="adminpass"

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[PASS]${NC} $1"; ((TESTS_PASSED++)); }
log_error() { echo -e "${RED}[FAIL]${NC} $1"; ((TESTS_FAILED++)); }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# Wait for cluster to be healthy
wait_for_cluster() {
    log_info "Waiting for cluster to be healthy..."
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -sf http://localhost:9095/health > /dev/null 2>&1 && \
           curl -sf http://localhost:9195/health > /dev/null 2>&1 && \
           curl -sf http://localhost:9295/health > /dev/null 2>&1; then
            log_success "All nodes are healthy"
            return 0
        fi
        ((attempt++))
        sleep 2
    done

    log_error "Cluster did not become healthy in time"
    return 1
}

# Run CLI command on a specific node
run_cli() {
    local addr=$1
    shift
    FLYMQ_ADDR="$addr" FLYMQ_USERNAME="$ADMIN_USER" FLYMQ_PASSWORD="$ADMIN_PASS" ./bin/flymq-cli "$@"
}

# Find the leader node
find_leader() {
    # Try each node to find the leader
    for addr in "$NODE1_ADDR" "$NODE2_ADDR" "$NODE3_ADDR"; do
        # Try to create a test topic - if it succeeds, this is the leader
        # If it fails with "not leader", extract the leader address
        local result=$(run_cli "$addr" create __leader_test 2>&1)
        if echo "$result" | grep -q "not leader"; then
            # Extract leader from error message using sed (macOS compatible)
            local leader=$(echo "$result" | sed -n 's/.*leader is \([^:]*\).*/\1/p' | head -1)
            if [ -n "$leader" ]; then
                case "$leader" in
                    flymq-1) echo "$NODE1_ADDR"; return 0 ;;
                    flymq-2) echo "$NODE2_ADDR"; return 0 ;;
                    flymq-3) echo "$NODE3_ADDR"; return 0 ;;
                esac
            fi
        elif echo "$result" | grep -q "created\|already exists"; then
            # This node is the leader
            run_cli "$addr" delete __leader_test 2>/dev/null || true
            echo "$addr"
            return 0
        fi
    done
    # Default to node 1
    echo "$NODE1_ADDR"
}

# Get follower addresses (nodes that are not the leader)
get_followers() {
    local leader=$1
    local followers=""
    for addr in "$NODE1_ADDR" "$NODE2_ADDR" "$NODE3_ADDR"; do
        if [ "$addr" != "$leader" ]; then
            if [ -n "$followers" ]; then
                followers="$followers $addr"
            else
                followers="$addr"
            fi
        fi
    done
    echo "$followers"
}

# Test 1: Create user on leader and verify on followers
test_user_creation_replication() {
    log_info "Test 1: User creation replication"

    if run_cli "$LEADER_ADDR" users create testuser1 testpass123 --roles producer,consumer 2>&1; then
        log_success "Created user 'testuser1' on leader"
    else
        log_error "Failed to create user on leader"
        return 1
    fi

    sleep 2

    # Check on all followers
    for follower in $FOLLOWER_ADDRS; do
        if run_cli "$follower" users get testuser1 2>&1 | grep -q "testuser1"; then
            log_success "User 'testuser1' replicated to $follower"
        else
            log_error "User 'testuser1' NOT found on $follower"
        fi
    done
}

# Test 2: Update user on leader and verify on followers
test_user_update_replication() {
    log_info "Test 2: User update replication"

    if run_cli "$LEADER_ADDR" users update testuser1 --roles admin 2>&1; then
        log_success "Updated user 'testuser1' roles on leader"
    else
        log_error "Failed to update user on leader"
        return 1
    fi

    sleep 2

    for follower in $FOLLOWER_ADDRS; do
        if run_cli "$follower" users get testuser1 2>&1 | grep -q "admin"; then
            log_success "User update replicated to $follower"
        else
            log_error "User update NOT replicated to $follower"
        fi
    done
}

# Test 3: Set ACL on leader and verify on followers
test_acl_set_replication() {
    log_info "Test 3: ACL set replication"

    if run_cli "$LEADER_ADDR" acl set test-topic --users testuser1 --roles admin 2>&1; then
        log_success "Set ACL for 'test-topic' on leader"
    else
        log_error "Failed to set ACL on leader"
        return 1
    fi

    sleep 2

    for follower in $FOLLOWER_ADDRS; do
        if run_cli "$follower" acl get test-topic 2>&1 | grep -q "testuser1"; then
            log_success "ACL replicated to $follower"
        else
            log_error "ACL NOT replicated to $follower"
        fi
    done
}

# Test 4: Delete ACL on leader and verify on followers
test_acl_delete_replication() {
    log_info "Test 4: ACL delete replication"

    if run_cli "$LEADER_ADDR" acl delete test-topic 2>&1; then
        log_success "Deleted ACL for 'test-topic' on leader"
    else
        log_error "Failed to delete ACL on leader"
        return 1
    fi

    sleep 2

    for follower in $FOLLOWER_ADDRS; do
        if run_cli "$follower" acl get test-topic 2>&1 | grep -q "No explicit ACL"; then
            log_success "ACL deletion replicated to $follower"
        else
            log_error "ACL deletion NOT replicated to $follower"
        fi
    done
}

# Test 5: Delete user on leader and verify on followers
test_user_deletion_replication() {
    log_info "Test 5: User deletion replication"

    if run_cli "$LEADER_ADDR" users delete testuser1 2>&1; then
        log_success "Deleted user 'testuser1' on leader"
    else
        log_error "Failed to delete user on leader"
        return 1
    fi

    sleep 2

    for follower in $FOLLOWER_ADDRS; do
        if run_cli "$follower" users get testuser1 2>&1 | grep -qi "not found\|error\|failed"; then
            log_success "User deletion replicated to $follower"
        else
            log_error "User deletion NOT replicated to $follower"
        fi
    done
}

# Test 6: Authenticate with replicated user on different nodes
test_cross_node_authentication() {
    log_info "Test 6: Cross-node authentication"

    if run_cli "$LEADER_ADDR" users create authtest authpass --roles producer 2>&1; then
        log_success "Created user 'authtest' on leader"
    else
        log_error "Failed to create user 'authtest'"
        return 1
    fi

    sleep 2

    for follower in $FOLLOWER_ADDRS; do
        if FLYMQ_ADDR="$follower" FLYMQ_USERNAME=authtest FLYMQ_PASSWORD=authpass ./bin/flymq-cli whoami 2>&1 | grep -q "authtest"; then
            log_success "User 'authtest' can authenticate on $follower"
        else
            log_error "User 'authtest' cannot authenticate on $follower"
        fi
    done

    run_cli "$LEADER_ADDR" users delete authtest 2>&1 || true
}

# Print summary
print_summary() {
    echo ""
    echo "========================================"
    echo "           TEST SUMMARY"
    echo "========================================"
    echo -e "Tests Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Tests Failed: ${RED}$TESTS_FAILED${NC}"
    echo "========================================"

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed!${NC}"
        return 0
    else
        echo -e "${RED}Some tests failed!${NC}"
        return 1
    fi
}

# Main execution
main() {
    echo "========================================"
    echo "  FlyMQ Cluster Auth Replication Test"
    echo "========================================"
    echo ""

    if [ ! -f "./bin/flymq-cli" ]; then
        log_error "flymq-cli not found. Please build it first."
        exit 1
    fi

    wait_for_cluster || exit 1
    echo ""

    # Find the leader
    log_info "Finding cluster leader..."
    LEADER_ADDR=$(find_leader)
    FOLLOWER_ADDRS=$(get_followers "$LEADER_ADDR")
    log_success "Leader: $LEADER_ADDR"
    log_info "Followers: $FOLLOWER_ADDRS"
    echo ""

    test_user_creation_replication
    echo ""
    test_user_update_replication
    echo ""
    test_acl_set_replication
    echo ""
    test_acl_delete_replication
    echo ""
    test_user_deletion_replication
    echo ""
    test_cross_node_authentication
    echo ""

    print_summary
}

main "$@"

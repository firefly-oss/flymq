#!/bin/bash
# =============================================================================
# FlyMQ Encryption Key Generator
# =============================================================================
#
# Generates a 256-bit AES encryption key for data-at-rest encryption.
# The key is output as a 64-character hexadecimal string.
#
# Usage: ./generate-encryption-key.sh [output_file]
#
# =============================================================================

set -e

OUTPUT_FILE="${1:-}"

# Generate 32 bytes (256 bits) of random data and convert to hex
ENCRYPTION_KEY=$(openssl rand -hex 32)

if [[ -n "$OUTPUT_FILE" ]]; then
    echo "$ENCRYPTION_KEY" > "$OUTPUT_FILE"
    chmod 600 "$OUTPUT_FILE"
    echo "Encryption key saved to: $OUTPUT_FILE"
else
    echo "$ENCRYPTION_KEY"
fi


#!/bin/bash
# update-protocol-23.sh
# Automated script to update all services to Protocol 23 dependencies

set -e

STELLAR_GO_VERSION="v23.0.0"
STELLAR_RPC_VERSION="v23.0.4"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# List of directories to update
SERVICES=(
    "stellar-live-source/go"
    "stellar-live-source-datalake/go"
    "ttp-processor/go"
    "contract-invocation-processor/go"
    "contract-data-processor/go"
    "stellar-arrow-source/go"
    "cli_tool/go"
)

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Protocol 23 Dependency Update Script${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""
echo "This script will update the following services:"
for service in "${SERVICES[@]}"; do
    echo "  - $service"
done
echo ""
echo "Target versions:"
echo "  - github.com/stellar/go: $STELLAR_GO_VERSION"
echo "  - github.com/stellar/stellar-rpc: $STELLAR_RPC_VERSION"
echo ""

# Confirm before proceeding
read -p "Continue with updates? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Update cancelled.${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}Starting Protocol 23 dependency updates...${NC}"
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0
SKIPPED_COUNT=0

for service in "${SERVICES[@]}"; do
    echo ""
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}Updating $service${NC}"
    echo -e "${BLUE}================================================${NC}"

    if [ -d "$service" ]; then
        cd "$service"

        # Update stellar/go
        echo -e "${YELLOW}Updating github.com/stellar/go to $STELLAR_GO_VERSION${NC}"
        if go get github.com/stellar/go@$STELLAR_GO_VERSION; then
            echo -e "${GREEN}✓ stellar/go updated${NC}"
        else
            echo -e "${RED}✗ Failed to update stellar/go${NC}"
            cd - > /dev/null
            FAIL_COUNT=$((FAIL_COUNT + 1))
            continue
        fi

        # Update stellar-rpc if present
        if grep -q "github.com/stellar/stellar-rpc" go.mod; then
            echo -e "${YELLOW}Updating github.com/stellar/stellar-rpc to $STELLAR_RPC_VERSION${NC}"
            if go get github.com/stellar/stellar-rpc@$STELLAR_RPC_VERSION; then
                echo -e "${GREEN}✓ stellar-rpc updated${NC}"
            else
                echo -e "${RED}✗ Failed to update stellar-rpc${NC}"
                cd - > /dev/null
                FAIL_COUNT=$((FAIL_COUNT + 1))
                continue
            fi
        else
            echo -e "${YELLOW}⊘ stellar-rpc not used in this service${NC}"
        fi

        # Tidy dependencies
        echo -e "${YELLOW}Running go mod tidy${NC}"
        if go mod tidy; then
            echo -e "${GREEN}✓ Dependencies tidied${NC}"
        else
            echo -e "${RED}✗ Failed to tidy dependencies${NC}"
            cd - > /dev/null
            FAIL_COUNT=$((FAIL_COUNT + 1))
            continue
        fi

        # Return to root
        cd - > /dev/null

        echo -e "${GREEN}✓ $service updated successfully${NC}"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo -e "${RED}⚠ Warning: $service directory not found${NC}"
        SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
    fi
done

echo ""
echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Update Summary${NC}"
echo -e "${BLUE}================================================${NC}"
echo -e "${GREEN}Successful: $SUCCESS_COUNT${NC}"
echo -e "${RED}Failed: $FAIL_COUNT${NC}"
echo -e "${YELLOW}Skipped: $SKIPPED_COUNT${NC}"
echo ""

if [ $FAIL_COUNT -eq 0 ]; then
    echo -e "${GREEN}All services updated to Protocol 23!${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Review changes: git diff"
    echo "  2. Build all services: ./scripts/build-all.sh"
    echo "  3. Run tests: ./scripts/test-all.sh"
    echo "  4. Test integration"
    exit 0
else
    echo -e "${RED}Some updates failed. Please review errors above.${NC}"
    exit 1
fi

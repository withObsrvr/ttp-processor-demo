#!/bin/bash
# test-all.sh
# Run tests for all Go services

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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
echo -e "${BLUE}Running Tests for All Services${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0
NO_TESTS_COUNT=0

for service in "${SERVICES[@]}"; do
    echo -e "${BLUE}Testing $service...${NC}"

    if [ -d "$service" ]; then
        cd "$service"

        # Run tests
        if go test ./... -v 2>&1 | tee /tmp/test-output.log; then
            # Check if there were any tests
            if grep -q "no test files" /tmp/test-output.log; then
                echo -e "${YELLOW}⊘ No tests found in $service${NC}"
                NO_TESTS_COUNT=$((NO_TESTS_COUNT + 1))
            else
                echo -e "${GREEN}✓ $service tests passed${NC}"
                SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
            fi
        else
            echo -e "${RED}✗ $service tests failed${NC}"
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi

        cd - > /dev/null
    else
        echo -e "${RED}⚠ Directory $service not found${NC}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    echo ""
done

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}================================================${NC}"
echo -e "${GREEN}Passed: $SUCCESS_COUNT${NC}"
echo -e "${RED}Failed: $FAIL_COUNT${NC}"
echo -e "${YELLOW}No tests: $NO_TESTS_COUNT${NC}"
echo ""

if [ $FAIL_COUNT -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed. Please review errors above.${NC}"
    exit 1
fi

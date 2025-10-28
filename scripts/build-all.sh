#!/bin/bash
# build-all.sh
# Build all services in the ttp-processor-demo project

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SERVICES=(
    "stellar-live-source"
    "stellar-live-source-datalake"
    "ttp-processor"
    "contract-invocation-processor"
    "contract-data-processor"
    "stellar-arrow-source"
    "cli_tool"
)

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Building All Services${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0

for service in "${SERVICES[@]}"; do
    echo -e "${BLUE}Building $service...${NC}"

    if [ -d "$service" ]; then
        cd "$service"

        # Check if Makefile exists
        if [ -f "Makefile" ]; then
            if make build; then
                echo -e "${GREEN}✓ $service built successfully${NC}"
                SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
            else
                echo -e "${RED}✗ Failed to build $service${NC}"
                FAIL_COUNT=$((FAIL_COUNT + 1))
            fi
        else
            echo -e "${YELLOW}⚠ No Makefile found in $service, trying go build...${NC}"
            cd go 2>/dev/null || cd .
            if go build -o ../"$service" .; then
                echo -e "${GREEN}✓ $service built successfully${NC}"
                SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
            else
                echo -e "${RED}✗ Failed to build $service${NC}"
                FAIL_COUNT=$((FAIL_COUNT + 1))
            fi
        fi

        cd - > /dev/null
    else
        echo -e "${RED}⚠ Directory $service not found${NC}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    echo ""
done

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Build Summary${NC}"
echo -e "${BLUE}================================================${NC}"
echo -e "${GREEN}Successful: $SUCCESS_COUNT${NC}"
echo -e "${RED}Failed: $FAIL_COUNT${NC}"
echo ""

if [ $FAIL_COUNT -eq 0 ]; then
    echo -e "${GREEN}All services built successfully!${NC}"
    exit 0
else
    echo -e "${RED}Some builds failed. Please review errors above.${NC}"
    exit 1
fi

#!/bin/bash
set -e

echo "🚀 Contract Events Pipeline Setup"
echo "=================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DB_NAME="contract_events"
DB_USER="${POSTGRES_USER:-postgres}"
SCHEMA_FILE="contract-events-postgres-consumer/config/schema/contract_events.sql"

# Check prerequisites
echo "📋 Checking prerequisites..."

# Check PostgreSQL
if ! command -v psql &> /dev/null; then
    echo -e "${RED}✗ PostgreSQL client not found${NC}"
    echo "  Install with: sudo apt install postgresql-client"
    exit 1
fi
echo -e "${GREEN}✓ PostgreSQL client found${NC}"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker not found${NC}"
    echo "  Install from: https://docs.docker.com/get-docker/"
    exit 1
fi
echo -e "${GREEN}✓ Docker found${NC}"

# Check if we're using datalake source (needs GCS)
if [ "$1" == "datalake" ]; then
    echo ""
    echo "🗄️  Datalake mode selected - checking GCS credentials..."

    # Check for GCS credentials
    if [ -f "$HOME/.config/gcloud/application_default_credentials.json" ]; then
        echo -e "${GREEN}✓ GCS credentials found${NC}"
    else
        echo -e "${YELLOW}⚠ GCS credentials not found${NC}"
        echo "  Run: gcloud auth application-default login"
        read -p "Continue anyway? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
fi

# Database setup
echo ""
echo "🗄️  Setting up PostgreSQL database..."

# Check if database exists
if psql -U "$DB_USER" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo -e "${YELLOW}⚠ Database '$DB_NAME' already exists${NC}"
    read -p "Drop and recreate? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Dropping database..."
        dropdb -U "$DB_USER" "$DB_NAME" || true
        echo "Creating database..."
        createdb -U "$DB_USER" "$DB_NAME"
        echo -e "${GREEN}✓ Database recreated${NC}"
    else
        echo "Using existing database"
    fi
else
    echo "Creating database..."
    createdb -U "$DB_USER" "$DB_NAME"
    echo -e "${GREEN}✓ Database created${NC}"
fi

# Initialize schema
echo "Initializing schema..."
if [ ! -f "$SCHEMA_FILE" ]; then
    echo -e "${RED}✗ Schema file not found: $SCHEMA_FILE${NC}"
    exit 1
fi

psql -U "$DB_USER" -d "$DB_NAME" -f "$SCHEMA_FILE" > /dev/null 2>&1
echo -e "${GREEN}✓ Schema initialized${NC}"

# Verify schema
echo "Verifying schema..."
TABLE_COUNT=$(psql -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'contract_events'" | tr -d ' ')
if [ "$TABLE_COUNT" -eq "1" ]; then
    echo -e "${GREEN}✓ Schema verified${NC}"
else
    echo -e "${RED}✗ Schema verification failed${NC}"
    exit 1
fi

# Build Docker images
echo ""
echo "🐳 Building Docker images..."

BUILD_IMAGES="${BUILD_IMAGES:-true}"
if [ "$BUILD_IMAGES" == "false" ]; then
    echo "Skipping Docker image builds (BUILD_IMAGES=false)"
else
    # Determine which source to build
    if [ "$1" == "datalake" ]; then
        SOURCE_DIR="stellar-live-source-datalake"
    else
        SOURCE_DIR="stellar-live-source"
    fi

    if [ -d "$SOURCE_DIR" ]; then
        echo "Building $SOURCE_DIR..."
        (cd "$SOURCE_DIR" && make docker-build) > /dev/null 2>&1 || echo -e "${YELLOW}⚠ Failed to build $SOURCE_DIR${NC}"
    else
        echo -e "${YELLOW}⚠ $SOURCE_DIR not found, skipping${NC}"
    fi

    echo "Building contract-events-processor..."
    (cd contract-events-processor && make docker-build) > /dev/null 2>&1 || echo -e "${YELLOW}⚠ Failed to build contract-events-processor${NC}"

    echo "Building contract-events-postgres-consumer..."
    (cd contract-events-postgres-consumer && make docker-build) > /dev/null 2>&1 || echo -e "${YELLOW}⚠ Failed to build contract-events-postgres-consumer${NC}"

    echo -e "${GREEN}✓ Docker images built${NC}"
fi

# List Docker images
echo ""
echo "📦 Available Docker images:"
docker images | grep -E "withobsrvr/(stellar-live-source|contract-events)" | head -10

# Summary
echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo ""

if [ "$1" == "datalake" ]; then
    echo "  1. Review configuration in: contract-events-flowctl-pipeline.secret.yaml"
    echo "  2. Update GCS bucket settings if needed"
    echo "  3. Run the pipeline:"
    echo "     flowctl run contract-events-flowctl-pipeline.secret.yaml"
else
    echo "  1. Review configuration in: contract-events-flowctl-pipeline-live.yaml"
    echo "  2. Update RPC endpoint if needed"
    echo "  3. Run the pipeline:"
    echo "     flowctl run contract-events-flowctl-pipeline-live.yaml"
fi

echo ""
echo "  Or run services manually:"
if [ "$1" == "datalake" ]; then
    echo "     cd stellar-live-source-datalake && make run"
else
    echo "     cd stellar-live-source && make run"
fi
echo "     cd contract-events-processor && make run"
echo "     cd contract-events-postgres-consumer && ./run.sh 1000 0"
echo ""
echo "  Monitor health:"
echo "     curl http://localhost:8081/health  # Source"
echo "     curl http://localhost:8089/health  # Processor"
echo "     curl http://localhost:8090/health  # Consumer"
echo ""
echo "  Query events:"
echo "     psql -d $DB_NAME -c 'SELECT COUNT(*) FROM contract_events'"
echo ""

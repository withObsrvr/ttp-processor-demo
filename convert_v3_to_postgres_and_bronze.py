#!/usr/bin/env python3
"""
Convert V3 DuckDB schemas to PostgreSQL and Bronze formats.
Extracts CREATE TABLE from V3 tables.go and generates compatible SQL.
"""

import re
import sys

def convert_to_postgres(duckdb_sql, table_name):
    """Convert DuckDB CREATE TABLE to PostgreSQL UNLOGGED format."""
    pg_sql = duckdb_sql

    # Remove placeholders
    pg_sql = re.sub(r'%s\.%s\.', '', pg_sql)

    # Convert types
    pg_sql = pg_sql.replace('VARCHAR', 'TEXT')
    pg_sql = pg_sql.replace('TIMESTAMP', 'TIMESTAMPTZ')
    pg_sql = pg_sql.replace('UINTEGER', 'BIGINT')
    pg_sql = pg_sql.replace('CREATE TABLE IF NOT EXISTS', 'CREATE UNLOGGED TABLE IF NOT EXISTS')

    return pg_sql

def convert_to_bronze(duckdb_sql, table_name):
    """Convert to Bronze (DuckLake) format."""
    bronze_sql = duckdb_sql
    
    # Replace schema prefix with bronze.
    bronze_sql = re.sub(r'%s\.%s\.', 'bronze.', bronze_sql)
    
    return bronze_sql

def extract_v3_schemas(tables_go_path):
    """Extract all CREATE TABLE statements from V3 tables.go."""
    with open(tables_go_path, 'r') as f:
        content = f.read()
    
    # Find all CREATE TABLE blocks
    # Pattern: CREATE TABLE ... )`
    pattern = r'(CREATE TABLE IF NOT EXISTS.*?\))`'
    matches = re.findall(pattern, content, re.DOTALL)
    
    schemas = {}
    for match in matches:
        # Extract table name
        name_match = re.search(r'\.(\w+) \(', match)
        if name_match:
            table_name = name_match.group(1)
            # Skip metadata tables for now
            if not table_name.startswith('_meta'):
                schemas[table_name] = match
    
    return schemas

# Main execution
v3_schemas = extract_v3_schemas('ducklake-ingestion-obsrvr-v3/go/tables.go')

print(f"Extracted {len(v3_schemas)} table schemas from V3")
print("\nData tables found:")
for i, table_name in enumerate(sorted(v3_schemas.keys()), 1):
    print(f"  {i}. {table_name}")

# Generate PostgreSQL schema
print("\n" + "="*60)
print("Generating PostgreSQL schema...")
print("="*60)

pg_output = "-- PostgreSQL schema from V3 (UNLOGGED tables for hot buffer)\n\n"
for table_name in sorted(v3_schemas.keys()):
    pg_sql = convert_to_postgres(v3_schemas[table_name], table_name)
    pg_output += f"-- {table_name}\n"
    pg_output += pg_sql + ";\n\n"

with open('v3_postgres_schema.sql', 'w') as f:
    f.write(pg_output)
    
print(f"✅ Written to v3_postgres_schema.sql ({len(pg_output)} bytes)")

# Generate Bronze schema
print("\nGenerating Bronze (DuckLake) schema...")
bronze_output = "-- Bronze schema from V3 (for DuckLake catalog)\n\n"
for table_name in sorted(v3_schemas.keys()):
    bronze_sql = convert_to_bronze(v3_schemas[table_name], table_name)
    bronze_output += f"-- {table_name}\n"
    bronze_output += bronze_sql + ";\n\n"

with open('v3_bronze_schema.sql', 'w') as f:
    f.write(bronze_output)
    
print(f"✅ Written to v3_bronze_schema.sql ({len(bronze_output)} bytes)")
print("\nConversion complete!")

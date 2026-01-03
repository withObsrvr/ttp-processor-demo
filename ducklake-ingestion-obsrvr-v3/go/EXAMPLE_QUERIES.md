# Example Queries for DuckLake HTTP Query API

This document provides common query patterns for accessing the DuckLake catalog via the HTTP Query API.

## API Endpoint

```
POST http://localhost:8081/query
Content-Type: application/json

{
  "sql": "YOUR SQL QUERY HERE",
  "limit": 1000,    // Optional, default: 1000, max: 10000
  "offset": 0       // Optional, default: 0
}
```

## Common Queries

### 1. Ledger Statistics

**Total ledger count:**
```json
{
  "sql": "SELECT COUNT(*) as total_ledgers FROM testnet.ledgers_row_v2"
}
```

**Ledger summary:**
```json
{
  "sql": "SELECT COUNT(*) as total_ledgers, MIN(sequence) as first_ledger, MAX(sequence) as last_ledger, SUM(transaction_count) as total_txs FROM testnet.ledgers_row_v2"
}
```

**Recent ledgers:**
```json
{
  "sql": "SELECT sequence, closed_at, transaction_count, operation_count, successful_tx_count, failed_tx_count FROM testnet.ledgers_row_v2 ORDER BY sequence DESC LIMIT 10"
}
```

**Ledgers by date range:**
```json
{
  "sql": "SELECT sequence, closed_at, transaction_count FROM testnet.ledgers_row_v2 WHERE closed_at >= '2025-12-01' AND closed_at < '2025-12-02' ORDER BY sequence"
}
```

### 2. Transaction Analysis

**Transaction count by ledger:**
```json
{
  "sql": "SELECT sequence, transaction_count, successful_tx_count, failed_tx_count, (failed_tx_count::FLOAT / NULLIF(transaction_count, 0) * 100) as failure_rate FROM testnet.ledgers_row_v2 WHERE transaction_count > 0 ORDER BY sequence DESC LIMIT 20"
}
```

**Busiest ledgers:**
```json
{
  "sql": "SELECT sequence, closed_at, transaction_count, operation_count FROM testnet.ledgers_row_v2 ORDER BY transaction_count DESC LIMIT 20"
}
```

**Transaction throughput over time:**
```json
{
  "sql": "SELECT DATE_TRUNC('hour', closed_at) as hour, SUM(transaction_count) as total_txs, AVG(transaction_count) as avg_txs_per_ledger FROM testnet.ledgers_row_v2 GROUP BY hour ORDER BY hour DESC LIMIT 24"
}
```

### 3. Operation Statistics

**Operations per ledger:**
```json
{
  "sql": "SELECT sequence, closed_at, operation_count, (operation_count::FLOAT / NULLIF(transaction_count, 0)) as ops_per_tx FROM testnet.ledgers_row_v2 WHERE transaction_count > 0 ORDER BY sequence DESC LIMIT 20"
}
```

**Total operations:**
```json
{
  "sql": "SELECT SUM(operation_count) as total_operations, AVG(operation_count) as avg_ops_per_ledger, MAX(operation_count) as max_ops_in_ledger FROM testnet.ledgers_row_v2"
}
```

### 4. Success/Failure Analysis

**Ledgers with failures:**
```json
{
  "sql": "SELECT sequence, closed_at, transaction_count, failed_tx_count, (failed_tx_count::FLOAT / transaction_count * 100) as failure_rate FROM testnet.ledgers_row_v2 WHERE failed_tx_count > 0 ORDER BY failure_rate DESC LIMIT 20"
}
```

**Success rate over time:**
```json
{
  "sql": "SELECT DATE_TRUNC('day', closed_at) as day, SUM(successful_tx_count) as successful, SUM(failed_tx_count) as failed, (SUM(successful_tx_count)::FLOAT / NULLIF(SUM(transaction_count), 0) * 100) as success_rate FROM testnet.ledgers_row_v2 GROUP BY day ORDER BY day DESC"
}
```

### 5. Time-Based Queries

**Ledgers in specific time range:**
```json
{
  "sql": "SELECT sequence, closed_at, transaction_count FROM testnet.ledgers_row_v2 WHERE closed_at BETWEEN '2025-12-11 00:00:00' AND '2025-12-11 12:00:00' ORDER BY sequence"
}
```

**Latest ledgers in last hour:**
```json
{
  "sql": "SELECT sequence, closed_at, transaction_count FROM testnet.ledgers_row_v2 WHERE closed_at >= NOW() - INTERVAL 1 HOUR ORDER BY sequence DESC"
}
```

### 6. Aggregation Queries

**Hourly statistics:**
```json
{
  "sql": "SELECT DATE_TRUNC('hour', closed_at) as hour, COUNT(*) as ledger_count, SUM(transaction_count) as total_txs, SUM(operation_count) as total_ops FROM testnet.ledgers_row_v2 GROUP BY hour ORDER BY hour DESC LIMIT 24"
}
```

**Daily statistics:**
```json
{
  "sql": "SELECT DATE_TRUNC('day', closed_at) as day, COUNT(*) as ledger_count, SUM(transaction_count) as total_txs, AVG(transaction_count) as avg_txs FROM testnet.ledgers_row_v2 GROUP BY day ORDER BY day DESC"
}
```

### 7. Pagination Examples

**Page 1 (first 100 ledgers):**
```json
{
  "sql": "SELECT sequence, closed_at, transaction_count FROM testnet.ledgers_row_v2 ORDER BY sequence DESC",
  "limit": 100,
  "offset": 0
}
```

**Page 2 (next 100 ledgers):**
```json
{
  "sql": "SELECT sequence, closed_at, transaction_count FROM testnet.ledgers_row_v2 ORDER BY sequence DESC",
  "limit": 100,
  "offset": 100
}
```

### 8. Schema Introspection

**Show all tables:**
```json
{
  "sql": "SHOW TABLES"
}
```

**Describe table schema:**
```json
{
  "sql": "DESCRIBE testnet.ledgers_row_v2"
}
```

**Explain query plan:**
```json
{
  "sql": "EXPLAIN SELECT * FROM testnet.ledgers_row_v2 WHERE sequence BETWEEN 1000 AND 2000"
}
```

## Using curl

**Basic query:**
```bash
curl -X POST http://localhost:8081/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM testnet.ledgers_row_v2"}'
```

**Query with pagination:**
```bash
curl -X POST http://localhost:8081/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT sequence, transaction_count FROM testnet.ledgers_row_v2 ORDER BY sequence DESC",
    "limit": 50,
    "offset": 0
  }'
```

**Pretty-print results with jq:**
```bash
curl -s -X POST http://localhost:8081/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM testnet.ledgers_row_v2 LIMIT 5"}' | jq .
```

## Response Format

All query responses follow this format:

```json
{
  "columns": ["sequence", "closed_at", "transaction_count"],
  "rows": [
    [10999, "2025-12-11T08:00:51Z", 2],
    [10998, "2025-12-11T08:00:46Z", 3],
    [10997, "2025-12-11T08:00:41Z", 1]
  ],
  "row_count": 3,
  "execution_time_ms": 2
}
```

## Query Limits and Timeouts

- **Default result limit:** 1,000 rows
- **Maximum result limit:** 10,000 rows
- **Query timeout:** 30 seconds
- **Allowed operations:** SELECT, WITH, SHOW, DESCRIBE, EXPLAIN
- **Forbidden operations:** DROP, DELETE, INSERT, UPDATE, ALTER, CREATE

## Error Handling

**Invalid SQL:**
```json
{
  "error": "Invalid SQL: operation not allowed: DROP"
}
```

**Query timeout:**
```json
{
  "error": "Query execution failed: context deadline exceeded"
}
```

## Monitoring

**Health check:**
```bash
curl http://localhost:8081/health
```

Response:
```json
{
  "status": "ok",
  "db_connected": true
}
```

**Metrics endpoint (Prometheus format):**
```bash
curl http://localhost:8081/metrics
```

Response:
```
# HELP ducklake_db_connections_open Number of open database connections
# TYPE ducklake_db_connections_open gauge
ducklake_db_connections_open 1

# HELP ducklake_db_connections_in_use Number of connections currently in use
# TYPE ducklake_db_connections_in_use gauge
ducklake_db_connections_in_use 0
...
```

## Best Practices

1. **Use LIMIT clauses** for large result sets to avoid memory issues
2. **Add appropriate WHERE filters** to reduce query scope
3. **Use ORDER BY** for consistent pagination
4. **Monitor execution_time_ms** in responses to identify slow queries
5. **Check /metrics endpoint** to monitor connection pool health
6. **Use date filters** on `closed_at` for time-based analysis
7. **Aggregate when possible** instead of fetching all rows

## Multi-Network Queries

When running with multiple networks, each has its own schema:

**Testnet queries:**
```json
{
  "sql": "SELECT COUNT(*) FROM testnet.ledgers_row_v2"
}
```

**Mainnet queries:**
```json
{
  "sql": "SELECT COUNT(*) FROM mainnet.ledgers_row_v2"
}
```

**Cross-network comparison:**
```json
{
  "sql": "SELECT 'testnet' as network, COUNT(*) as ledger_count FROM testnet.ledgers_row_v2 UNION ALL SELECT 'mainnet', COUNT(*) FROM mainnet.ledgers_row_v2"
}
```

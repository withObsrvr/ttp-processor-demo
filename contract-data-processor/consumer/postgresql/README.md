# PostgreSQL Consumer for Contract Data

This consumer reads Stellar contract data from Apache Arrow Flight and stores it in PostgreSQL for SQL-based analytics.

## Features

- High-performance bulk inserts using PostgreSQL COPY
- Automatic schema initialization
- Batch processing with configurable size
- Transaction safety with rollback on errors
- Prometheus metrics export
- Health and readiness endpoints
- Materialized views for current contract states
- Built-in analytics functions

## Prerequisites

- PostgreSQL 12+ with a database created
- Access to Arrow Flight server (contract-data-processor)
- Database user with CREATE SCHEMA privileges

## Configuration

Configure via environment variables:

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DB_HOST` | PostgreSQL host | `localhost` | No |
| `DB_PORT` | PostgreSQL port | `5432` | No |
| `DB_NAME` | Database name | `stellar_contracts` | No |
| `DB_USER` | Database user | `postgres` | No |
| `DB_PASSWORD` | Database password | - | **Yes** |
| `DB_SSLMODE` | SSL mode | `disable` | No |
| `FLIGHT_ENDPOINT` | Arrow Flight server | `localhost:8816` | No |
| `BATCH_SIZE` | Records per batch | `1000` | No |
| `COMMIT_INTERVAL` | Batch commit interval | `5s` | No |
| `MAX_RETRIES` | Max retry attempts | `3` | No |
| `METRICS_PORT` | Prometheus metrics port | `9090` | No |

## Quick Start

### 1. Create PostgreSQL Database

```sql
CREATE DATABASE stellar_contracts;
CREATE USER stellar WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE stellar_contracts TO stellar;
```

### 2. Run the Consumer

```bash
# Using Docker
docker run -d \
  --name contract-postgresql-consumer \
  -e DB_PASSWORD=your_password \
  -e DB_HOST=postgres.example.com \
  -e FLIGHT_ENDPOINT=processor.example.com:8816 \
  -p 9090:9090 \
  contract-postgresql-consumer:latest

# Using binary
export DB_PASSWORD=your_password
./postgresql-consumer
```

### 3. Verify Schema

The consumer automatically creates the required schema on startup:

```sql
\dt contract_data.*
```

## Database Schema

### Main Table: `contract_data.entries`

Stores all contract data changes with full history:

```sql
SELECT * FROM contract_data.entries 
WHERE contract_id = 'CAAAA...' 
ORDER BY ledger_sequence DESC 
LIMIT 10;
```

### Materialized View: `contract_data.current_states`

Latest state of each contract (automatically refreshed):

```sql
SELECT * FROM contract_data.current_states 
WHERE asset_code = 'USDC' 
  AND balance > 1000000;
```

### Helper Views

- `contract_data.asset_summary` - Asset statistics
- `contract_data.recent_activity` - Last 24 hours of activity

## Analytics Queries

### Get Top Token Holders

```sql
SELECT * FROM contract_data.get_top_holders('USDC', 'GAA...', 100);
```

### Track Balance History

```sql
SELECT * FROM contract_data.get_balance_history('CAAA...', 50);
```

### Asset Distribution

```sql
SELECT 
    asset_code,
    COUNT(DISTINCT balance_holder) as holders,
    SUM(balance) / 10000000.0 as total_supply,
    AVG(balance) / 10000000.0 as avg_balance
FROM contract_data.current_states
WHERE asset_code IS NOT NULL
GROUP BY asset_code
ORDER BY holders DESC;
```

### Contract Activity Heatmap

```sql
SELECT 
    DATE_TRUNC('hour', closed_at) as hour,
    COUNT(DISTINCT contract_id) as active_contracts,
    COUNT(*) as total_changes
FROM contract_data.entries
WHERE closed_at > NOW() - INTERVAL '7 days'
GROUP BY hour
ORDER BY hour;
```

## Performance Tuning

### PostgreSQL Configuration

Add to `postgresql.conf` for better bulk insert performance:

```ini
# Increase shared buffers
shared_buffers = 1GB

# Tune checkpoint settings
checkpoint_timeout = 15min
max_wal_size = 4GB

# Parallel workers
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
```

### Indexes

The schema includes optimized indexes. Monitor with:

```sql
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_stat_user_indexes
WHERE schemaname = 'contract_data'
ORDER BY pg_relation_size(indexrelid) DESC;
```

### Partitioning

For high-volume deployments, consider partitioning by ledger sequence:

```sql
-- Convert to partitioned table
ALTER TABLE contract_data.entries 
RENAME TO entries_old;

-- Create partitioned table
CREATE TABLE contract_data.entries (
    LIKE contract_data.entries_old INCLUDING ALL
) PARTITION BY RANGE (ledger_sequence);

-- Create partitions
CREATE TABLE contract_data.entries_2024_01 
PARTITION OF contract_data.entries 
FOR VALUES FROM (1000000) TO (2000000);
```

## Monitoring

### Prometheus Metrics

Available at `http://localhost:9090/metrics`:

- `postgresql_consumer_records_received_total`
- `postgresql_consumer_records_inserted_total`
- `postgresql_consumer_batches_processed_total`
- `postgresql_consumer_errors_total`
- `postgresql_consumer_batch_insert_duration_seconds`
- `postgresql_consumer_batch_size`

### Health Endpoints

- `GET /health` - Overall health status
- `GET /ready` - Readiness check
- `GET /stats` - Detailed statistics

### Grafana Dashboard

Import the included dashboard for visualization:

```bash
curl -X POST http://grafana:3000/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d @grafana-dashboard.json
```

## Troubleshooting

### Common Issues

1. **"relation does not exist" errors**
   - Consumer creates schema automatically
   - Ensure DB user has CREATE SCHEMA privilege

2. **Slow insert performance**
   - Increase BATCH_SIZE
   - Tune PostgreSQL checkpoint settings
   - Consider partitioning for large datasets

3. **Connection refused to Flight server**
   - Verify FLIGHT_ENDPOINT is correct
   - Check network connectivity
   - Ensure processor is running

4. **High memory usage**
   - Reduce BATCH_SIZE
   - Check for long-running transactions

### Debug Logging

Enable debug logs:

```bash
export LOG_LEVEL=debug
./postgresql-consumer
```

## Development

### Building from Source

```bash
go mod download
go build -o postgresql-consumer .
```

### Running Tests

```bash
go test -v ./...
```

### Schema Updates

Modify `schema.sql` and the consumer will apply changes on next restart.

## Production Considerations

1. **Backup Strategy**: Regular pg_dump of contract_data schema
2. **Monitoring**: Set up alerts on error metrics
3. **Scaling**: Use read replicas for analytics queries
4. **Maintenance**: Schedule VACUUM and ANALYZE operations
5. **Security**: Use SSL connections in production

## License

[License Information]
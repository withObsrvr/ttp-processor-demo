# Extending stellar-query-api for Silver Layer Queries

The existing `stellar-query-api` can be extended to support Silver layer queries alongside Hot (PostgreSQL) and Cold (Bronze) queries.

## Architecture

**Current Query Flow:**
```
stellar-query-api
├─ Hot Reader:  PostgreSQL (recent data, 10-20 mins)
└─ Cold Reader: DuckLake Bronze (historical raw data)
```

**Extended Query Flow:**
```
stellar-query-api
├─ Hot Reader:      PostgreSQL (recent data, 10-20 mins)
├─ Bronze Reader:   DuckLake Bronze (historical raw data)
└─ Silver Reader:   DuckLake Silver (analytics-ready data) ← NEW
```

## Configuration Changes

Update `stellar-query-api/config.yaml`:

```yaml
service:
  name: stellar-query-api
  port: 8092
  read_timeout_seconds: 30
  write_timeout_seconds: 30

# PostgreSQL Hot Buffer (recent data)
postgres:
  host: localhost
  port: 5434
  database: stellar_hot
  user: stellar
  password: stellar_dev_password
  sslmode: disable
  max_connections: 10

# DuckLake Bronze (historical raw data)
ducklake_bronze:
  catalog_path: "ducklake:postgres:postgresql://..."
  data_path: "s3://bucket/network_bronze/"
  catalog_name: network_catalog
  schema_name: bronze
  metadata_schema: bronze_meta
  aws_access_key_id: "..."
  aws_secret_access_key: "..."
  aws_region: "us-west-004"
  aws_endpoint: "https://s3.us-west-004.backblazeb2.com"

# DuckLake Silver (analytics-ready data) - NEW
ducklake_silver:
  # Same catalog as Bronze (shared PostgreSQL catalog)
  catalog_path: "ducklake:postgres:postgresql://..."
  data_path: "s3://bucket/network_silver/"  # Different S3 path
  catalog_name: network_catalog
  schema_name: silver  # Different schema
  metadata_schema: bronze_meta
  # Reuse same S3 credentials
  aws_access_key_id: "..."
  aws_secret_access_key: "..."
  aws_region: "us-west-004"
  aws_endpoint: "https://s3.us-west-004.backblazeb2.com"

query:
  default_limit: 100
  max_limit: 10000
  cache_ttl_seconds: 60
```

## New API Endpoints

### Current State Endpoints (Silver Layer)

```
GET /api/v1/silver/accounts/current
GET /api/v1/silver/accounts/current/{account_id}
GET /api/v1/silver/trustlines/current?account_id={id}
GET /api/v1/silver/offers/current
GET /api/v1/silver/liquidity_pools/current
GET /api/v1/silver/contract_data/current
```

### Historical Snapshot Endpoints (SCD Type 2)

```
GET /api/v1/silver/accounts/history/{account_id}
  Query params:
    - start_ledger (optional)
    - end_ledger (optional)
    - start_time (optional)
    - end_time (optional)

GET /api/v1/silver/trustlines/history/{account_id}
  Query params:
    - asset_code (optional)
    - asset_issuer (optional)
    - as_of (optional) - point-in-time query
```

### Enriched Operations Endpoints

```
GET /api/v1/silver/operations/enriched
  Query params:
    - account_id (optional)
    - type (optional) - e.g., payment, path_payment
    - start_ledger (optional)
    - end_ledger (optional)
    - limit (default: 100)

GET /api/v1/silver/operations/soroban
  Query params:
    - account_id (optional)
    - start_ledger (optional)
    - end_ledger (optional)
    - limit (default: 100)
```

### Token Transfers Analytics Endpoints

```
GET /api/v1/silver/transfers
  Query params:
    - source_type (optional) - classic, soroban, or both
    - asset_code (optional)
    - from_account (optional)
    - to_account (optional)
    - start_time (optional)
    - end_time (optional)
    - limit (default: 100)

GET /api/v1/silver/transfers/stats
  Query params:
    - group_by (required) - asset, source_type, hour, day
    - start_time (optional)
    - end_time (optional)
```

## Implementation Example

### Go Code Structure

```go
// stellar-query-api/go/silver_reader.go
package main

import (
	"context"
	"database/sql"
	"fmt"
)

type SilverReader struct {
	db          *sql.DB
	catalogName string
	schemaName  string
}

func NewSilverReader(config *DuckLakeConfig) (*SilverReader, error) {
	// Attach DuckLake catalog with Silver schema
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}

	// Install extensions and attach catalog (same as Bronze reader)
	// ... setup code ...

	return &SilverReader{
		db:          db,
		catalogName: config.CatalogName,
		schemaName:  config.SchemaName, // "silver"
	}, nil
}

func (r *SilverReader) GetCurrentAccounts(ctx context.Context, limit int) ([]Account, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			ledger_sequence,
			closed_at
		FROM %s.%s.accounts_current
		ORDER BY ledger_sequence DESC
		LIMIT ?
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []Account
	for rows.Next() {
		var acc Account
		if err := rows.Scan(&acc.AccountID, &acc.Balance, &acc.SequenceNumber,
			&acc.NumSubentries, &acc.LedgerSequence, &acc.ClosedAt); err != nil {
			return nil, err
		}
		accounts = append(accounts, acc)
	}

	return accounts, nil
}

func (r *SilverReader) GetAccountHistory(ctx context.Context, accountID string) ([]AccountSnapshot, error) {
	query := fmt.Sprintf(`
		SELECT
			account_id,
			balance,
			ledger_sequence,
			valid_from,
			valid_to
		FROM %s.%s.accounts_snapshot
		WHERE account_id = ?
		ORDER BY ledger_sequence DESC
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []AccountSnapshot
	for rows.Next() {
		var snap AccountSnapshot
		if err := rows.Scan(&snap.AccountID, &snap.Balance, &snap.LedgerSequence,
			&snap.ValidFrom, &snap.ValidTo); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

func (r *SilverReader) GetEnrichedOperations(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, error) {
	query := fmt.Sprintf(`
		SELECT
			transaction_hash,
			ledger_sequence,
			ledger_closed_at,
			source_account,
			type,
			destination,
			asset_code,
			amount,
			tx_successful,
			tx_fee_charged,
			is_payment_op,
			is_soroban_op
		FROM %s.%s.enriched_history_operations
		WHERE 1=1
	`, r.catalogName, r.schemaName)

	// Add filters dynamically
	args := []interface{}{}
	if filters.AccountID != "" {
		query += " AND source_account = ?"
		args = append(args, filters.AccountID)
	}
	if filters.StartLedger > 0 {
		query += " AND ledger_sequence >= ?"
		args = append(args, filters.StartLedger)
	}

	query += " ORDER BY ledger_sequence DESC LIMIT ?"
	args = append(args, filters.Limit)

	// Execute query and return results
	// ... implementation ...

	return nil, nil
}
```

### API Handlers

```go
// stellar-query-api/go/handlers_silver.go
package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func (s *Server) handleSilverAccountsCurrent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Parse limit from query params
	limitStr := r.URL.Query().Get("limit")
	limit := 100 // default
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}

	// Query Silver layer
	accounts, err := s.silverReader.GetCurrentAccounts(ctx, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":  accounts,
		"count": len(accounts),
	})
}

func (s *Server) handleSilverAccountHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Extract account_id from path
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		http.Error(w, "account_id required", http.StatusBadRequest)
		return
	}

	// Query Silver layer
	history, err := s.silverReader.GetAccountHistory(ctx, accountID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"account_id": accountID,
		"history":    history,
		"count":      len(history),
	})
}

func (s *Server) handleSilverTransfers(w http.ResponseWriter, r *http.Request) {
	// Implementation for token_transfers_raw queries
	// ...
}
```

## Usage Examples

### Query Current Accounts via API

```bash
# Get current accounts (default 100)
curl http://localhost:8092/api/v1/silver/accounts/current

# Get specific account current state
curl http://localhost:8092/api/v1/silver/accounts/current/GXXXXXXXXX

# Get account history (SCD Type 2)
curl http://localhost:8092/api/v1/silver/accounts/history?account_id=GXXXXXXXXX

# Get enriched payment operations
curl http://localhost:8092/api/v1/silver/operations/enriched?type=payment&limit=50

# Get token transfer stats by asset
curl http://localhost:8092/api/v1/silver/transfers/stats?group_by=asset&start_time=2024-01-01T00:00:00Z
```

### Response Format

```json
{
  "data": [
    {
      "account_id": "GXXXXXXXXX",
      "balance": "1000000000",
      "sequence_number": 12345,
      "num_subentries": 5,
      "ledger_sequence": 2137918,
      "closed_at": "2024-12-16T10:00:00Z"
    }
  ],
  "count": 1,
  "query_time_ms": 123
}
```

## Benefits of API Integration

1. **Unified Interface**: Query Hot (PostgreSQL), Bronze, and Silver through one API
2. **Access Control**: Add authentication/authorization for analytics queries
3. **Rate Limiting**: Protect DuckLake from expensive queries
4. **Query Optimization**: Cache frequently accessed Silver tables
5. **Monitoring**: Track Silver query patterns and performance

## Alternative: Direct DuckDB Access

For heavy analytics workloads, direct DuckDB access is more efficient:
- BI tools (Tableau, Metabase, Superset) can connect directly to DuckDB
- Data scientists can query with Python/R DuckDB clients
- No API overhead for large analytical queries

## Recommendation

**Use stellar-query-api extension for:**
- Application integration (web apps, mobile apps)
- Real-time dashboards with controlled query patterns
- Public-facing analytics APIs

**Use direct DuckDB access for:**
- Ad-hoc analytics and exploration
- BI tool integration
- Data science workflows
- Complex analytical queries (JOINs, aggregations)

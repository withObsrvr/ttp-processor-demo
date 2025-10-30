# Multi-Table Ingestion Design

## Overview

Extend the current parallel ingestion architecture to process all 4 stellar-etl tables simultaneously from a single data stream.

**Current**: 6 workers → ledgers table only
**Target**: 6 workers → ledgers + transactions + operations + native_balances

## Architecture

### Core Principle: Single Read, Multiple Writes

```
┌─────────────────────────────────────────────────────────────┐
│ Worker 1 (Ledgers 1-166666)                                 │
│   stellar-live-source stream                                │
│   ↓                                                          │
│   Extract: ledger, transactions[], operations[], balances[] │
│   ↓                                                          │
│   Buffer: 4 independent buffers (1 per table)               │
│   ↓                                                          │
│   Flush: Multi-row INSERT to all 4 tables                   │
└─────────────────────────────────────────────────────────────┘
... (6 workers total)
```

### Performance Characteristics

- **stellar-live-source load**: 6 streams (same as current)
- **Throughput**: 179.5 ledgers/sec
- **Tables written**: 4 tables × 179.5 rows/sec (aggregate)
- **Mainnet backfill**: ~3.9 days for ALL tables
- **Circuit breaker**: Safe (stays at 6 workers)

## Implementation Plan

### Phase 1: Data Structure Extensions (Day 1, Morning)

**File**: `go/main.go`

#### 1.1 Add Transaction Data Structure

```go
type TransactionData struct {
	LedgerSequence       uint32
	TransactionHash      string
	TransactionIndex     uint32
	ApplicationOrder     uint32
	Account              string
	AccountSequence      string
	MaxFee               uint32
	FeeCharged           int64
	OperationCount       uint32
	TxEnvelope           string
	TxResult             string
	TxMeta               string
	TxFeeMeta            string
	CreatedAt            string
	MemoType             string
	Memo                 string
	TimeBounds           string
	Successful           bool
	TransactionResultCode string
	InclusionFeeBid      int64
	InclusionFeeCharged  int64
	ResourceFeeRefund    int64
	NonRefundableResourceFeeCharged int64
	RefundableResourceFeeCharged     int64
	RentFeeCharged       int64
	// Add remaining stellar-etl fields as needed
}
```

#### 1.2 Add Operation Data Structure

```go
type OperationData struct {
	LedgerSequence       uint32
	TransactionHash      string
	OperationIndex       uint32
	ApplicationOrder     uint32
	TypeString           string
	DetailsJSON          string  // Store as JSON string
	SourceAccount        string
	SourceAccountMuxed   string

	// Common operation fields
	Asset                string
	Amount               string
	From                 string
	To                   string

	// Add remaining stellar-etl fields as needed
}
```

#### 1.3 Add Balance Data Structure

```go
type BalanceData struct {
	LedgerSequence    uint32
	AccountID         string
	Balance           int64
	BuyingLiabilities int64
	SellingLiabilities int64
	SequenceNumber    int64
	NumSubentries     uint32
	InflationDest     string
	Flags             uint32
	HomeDomain        string
	MasterWeight      int32
	ThresholdLow      int32
	ThresholdMedium   int32
	ThresholdHigh     int32
	Sponsor           string
}
```

#### 1.4 Add Multi-Table Buffers

```go
type WorkerBuffers struct {
	ledgers       []LedgerData
	transactions  []TransactionData
	operations    []OperationData
	balances      []BalanceData

	// Metadata
	batchSize     int
	lastFlushedSeq uint32
}

func newWorkerBuffers(batchSize int) *WorkerBuffers {
	return &WorkerBuffers{
		ledgers:      make([]LedgerData, 0, batchSize),
		transactions: make([]TransactionData, 0, batchSize*10),   // ~10 tx per ledger
		operations:   make([]OperationData, 0, batchSize*100),    // ~100 ops per ledger
		balances:     make([]BalanceData, 0, batchSize*1000),     // ~1000 balances per ledger
		batchSize:    batchSize,
	}
}
```

### Phase 2: Extraction Functions (Day 1, Afternoon)

**File**: `go/extractors.go` (new file)

#### 2.1 Transaction Extractor

```go
func extractTransactions(ledgerMeta *xdr.LedgerCloseMeta) []TransactionData {
	txs := make([]TransactionData, 0)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(config.NetworkPassphrase, ledgerMeta)
	if err != nil {
		return txs
	}
	defer txReader.Close()

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}

		txData := TransactionData{
			LedgerSequence:   ledgerMeta.LedgerSequence(),
			TransactionHash:  tx.Result.TransactionHash.HexString(),
			TransactionIndex: tx.Index,
			Account:          tx.Envelope.SourceAccount().ToAccountId().Address(),
			// ... extract all fields using stellar-etl patterns
			Successful:       tx.Result.Successful(),
		}
		txs = append(txs, txData)
	}

	return txs
}
```

#### 2.2 Operation Extractor

```go
func extractOperations(ledgerMeta *xdr.LedgerCloseMeta) []OperationData {
	ops := make([]OperationData, 0)

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(config.NetworkPassphrase, ledgerMeta)
	if err != nil {
		return ops
	}
	defer txReader.Close()

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}

		operations := tx.Envelope.Operations()
		for i, op := range operations {
			opData := OperationData{
				LedgerSequence:  ledgerMeta.LedgerSequence(),
				TransactionHash: tx.Result.TransactionHash.HexString(),
				OperationIndex:  uint32(i),
				TypeString:      op.Body.Type.String(),
				SourceAccount:   getOperationSourceAccount(op, tx),
				// ... extract operation details to JSON
			}
			ops = append(ops, opData)
		}
	}

	return ops
}
```

#### 2.3 Balance Extractor

```go
func extractBalances(ledgerMeta *xdr.LedgerCloseMeta) []BalanceData {
	balances := make([]BalanceData, 0)
	ledgerSeq := ledgerMeta.LedgerSequence()

	// Get all ledger changes
	changeReader, err := ingest.NewLedgerChangeReaderFromLedgerCloseMeta(config.NetworkPassphrase, ledgerMeta)
	if err != nil {
		return balances
	}
	defer changeReader.Close()

	// Track account changes
	accountChanges := make(map[string]*BalanceData)

	for {
		change, err := changeReader.Read()
		if err == io.EOF {
			break
		}

		// Only process account entries
		if change.Type == xdr.LedgerEntryTypeAccount {
			if change.Post != nil {
				accountData := change.Post.Data.MustAccount()
				accountID := accountData.AccountId.Address()

				accountChanges[accountID] = &BalanceData{
					LedgerSequence:     ledgerSeq,
					AccountID:          accountID,
					Balance:            int64(accountData.Balance),
					BuyingLiabilities:  int64(accountData.Ext.V1.Liabilities.Buying),
					SellingLiabilities: int64(accountData.Ext.V1.Liabilities.Selling),
					// ... extract remaining fields
				}
			}
		}
	}

	// Convert map to slice
	for _, balance := range accountChanges {
		balances = append(balances, *balance)
	}

	return balances
}
```

### Phase 3: Multi-Table Flush (Day 2, Morning)

**File**: `go/main.go`

#### 3.1 Extend Flush Function

```go
func (buffers *WorkerBuffers) flush(db *sql.DB, config *Config) error {
	if len(buffers.ledgers) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Flush all tables in a single transaction
	if err := flushLedgers(tx, buffers.ledgers, config); err != nil {
		return fmt.Errorf("flush ledgers: %w", err)
	}

	if err := flushTransactions(tx, buffers.transactions, config); err != nil {
		return fmt.Errorf("flush transactions: %w", err)
	}

	if err := flushOperations(tx, buffers.operations, config); err != nil {
		return fmt.Errorf("flush operations: %w", err)
	}

	if err := flushBalances(tx, buffers.balances, config); err != nil {
		return fmt.Errorf("flush balances: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	// Clear all buffers
	buffers.ledgers = buffers.ledgers[:0]
	buffers.transactions = buffers.transactions[:0]
	buffers.operations = buffers.operations[:0]
	buffers.balances = buffers.balances[:0]

	return nil
}
```

#### 3.2 Individual Table Flush Functions

```go
func flushTransactions(tx *sql.Tx, transactions []TransactionData, config *Config) error {
	if len(transactions) == 0 {
		return nil
	}

	// Build multi-row INSERT
	tableName := fmt.Sprintf("%s.%s.transactions", config.DuckLake.CatalogName, config.DuckLake.SchemaName)

	query := fmt.Sprintf(`INSERT INTO %s (
		ledger_sequence, transaction_hash, transaction_index,
		account, account_sequence, max_fee, fee_charged,
		operation_count, successful, created_at
	) VALUES `, tableName)

	values := make([]string, len(transactions))
	args := make([]interface{}, 0, len(transactions)*10)

	for i, tx := range transactions {
		values[i] = fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
			i*10+1, i*10+2, i*10+3, i*10+4, i*10+5,
			i*10+6, i*10+7, i*10+8, i*10+9, i*10+10)

		args = append(args,
			tx.LedgerSequence, tx.TransactionHash, tx.TransactionIndex,
			tx.Account, tx.AccountSequence, tx.MaxFee, tx.FeeCharged,
			tx.OperationCount, tx.Successful, tx.CreatedAt,
		)
	}

	query += strings.Join(values, ",")

	_, err := tx.Exec(query, args...)
	return err
}

// Similar functions for flushOperations() and flushBalances()
```

### Phase 4: Worker Integration (Day 2, Afternoon)

**File**: `go/main.go` - modify `runWorker()`

#### 4.1 Update Worker Loop

```go
func runWorker(ctx context.Context, workerID int, start, end uint32, config *Config) error {
	// ... existing gRPC setup ...

	buffers := newWorkerBuffers(config.DuckLake.BatchSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}

		ledgerMeta := resp.GetLedger()

		// Extract all table data from single ledger
		ledgerData := extractLedgerData(ledgerMeta)
		transactions := extractTransactions(ledgerMeta)
		operations := extractOperations(ledgerMeta)
		balances := extractBalances(ledgerMeta)

		// Buffer all tables
		buffers.ledgers = append(buffers.ledgers, ledgerData)
		buffers.transactions = append(buffers.transactions, transactions...)
		buffers.operations = append(buffers.operations, operations...)
		buffers.balances = append(buffers.balances, balances...)

		// Flush when ledger buffer reaches batch size
		if len(buffers.ledgers) >= buffers.batchSize {
			if err := buffers.flush(db, config); err != nil {
				return fmt.Errorf("flush error: %w", err)
			}

			log.Printf("[Worker %d] Flushed batch: ledgers=%d, tx=%d, ops=%d, balances=%d",
				workerID, len(buffers.ledgers), len(buffers.transactions),
				len(buffers.operations), len(buffers.balances))
		}
	}

	// Final flush
	if len(buffers.ledgers) > 0 {
		if err := buffers.flush(db, config); err != nil {
			return fmt.Errorf("final flush error: %w", err)
		}
	}

	return nil
}
```

## Testing Strategy

### Phase 1: Unit Tests (Day 2, Evening)

```bash
# Test extraction functions with known ledgers
go test ./... -v -run TestExtractTransactions
go test ./... -v -run TestExtractOperations
go test ./... -v -run TestExtractBalances
```

### Phase 2: Single Worker Test (Day 3, Morning)

```yaml
# config/testnet-multi-table-1worker.yaml
ducklake:
  num_workers: 1
  batch_size: 100

source:
  start_ledger: 100000
  end_ledger: 100100  # Small range for testing
```

```bash
./ducklake-ingestion --config config/testnet-multi-table-1worker.yaml
```

**Validation queries**:
```sql
-- Check data consistency
SELECT
  l.sequence,
  COUNT(DISTINCT t.transaction_hash) as tx_count,
  COUNT(DISTINCT o.operation_index) as op_count,
  COUNT(DISTINCT b.account_id) as balance_count
FROM ledgers_v2 l
LEFT JOIN transactions t ON l.sequence = t.ledger_sequence
LEFT JOIN operations o ON l.sequence = o.ledger_sequence
LEFT JOIN native_balances b ON l.sequence = b.ledger_sequence
WHERE l.sequence BETWEEN 100000 AND 100100
GROUP BY l.sequence
ORDER BY l.sequence;
```

### Phase 3: Multi-Worker Test (Day 3, Afternoon)

```yaml
# config/testnet-multi-table-6workers.yaml
ducklake:
  num_workers: 6
  batch_size: 1000

source:
  start_ledger: 100000
  end_ledger: 120000  # 20,000 ledgers
```

```bash
./ducklake-ingestion --config config/testnet-multi-table-6workers.yaml
```

**Expected performance**: ~179.5 ledgers/sec (same as single table)

### Phase 4: Data Validation (Day 3, Evening)

```bash
# Compare against stellar-etl reference data
./scripts/validate-against-stellar-etl.sh 100000 120000
```

## Risk Mitigation

### Risk 1: Memory Pressure from Large Buffers

**Mitigation**:
- Monitor operation and balance buffer sizes
- Add adaptive flushing: flush when ANY buffer reaches threshold
- Set conservative initial batch_size (e.g., 100 ledgers)

```go
func (buffers *WorkerBuffers) shouldFlush() bool {
	return len(buffers.ledgers) >= buffers.batchSize ||
	       len(buffers.transactions) >= buffers.batchSize*10 ||
	       len(buffers.operations) >= buffers.batchSize*100 ||
	       len(buffers.balances) >= buffers.batchSize*500  // Conservative
}
```

### Risk 2: Partial Failures During Multi-Table Flush

**Mitigation**:
- Use database transactions (all or nothing)
- Log failed batches for manual recovery
- Add retry logic with exponential backoff

### Risk 3: Schema Mismatches with stellar-etl

**Mitigation**:
- Cross-reference with stellar-etl export_*.go files
- Add schema validation tests
- Document any intentional deviations

### Risk 4: Performance Degradation

**Mitigation**:
- Start with 1 worker, validate performance
- Scale to 2, 4, 6 workers incrementally
- Monitor stellar-live-source logs for early circuit breaker warnings
- Keep batch_size tunable via config

## Configuration

### New Config Fields

```yaml
ducklake:
  # Existing
  num_workers: 6
  batch_size: 1000

  # New: Enable/disable specific tables
  enable_transactions: true
  enable_operations: true
  enable_balances: true

  # New: Table-specific settings
  tables:
    transactions:
      table_name: "transactions"
    operations:
      table_name: "operations"
      details_as_json: true  # Store operation details as JSON
    balances:
      table_name: "native_balances"
```

## Success Criteria

- ✅ All 4 tables ingest at 179.5 ledgers/sec (6 workers)
- ✅ No circuit breaker triggers (stays at 6 streams)
- ✅ Data consistency: ledger N has all transactions/operations/balances from ledger N
- ✅ Memory usage stable (no leaks)
- ✅ Schema matches stellar-etl 100%
- ✅ Validation queries pass for 10,000+ ledger range

## Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| Phase 1 | 4 hours | Data structures and buffer management |
| Phase 2 | 4 hours | Extraction functions (transactions, operations, balances) |
| Phase 3 | 4 hours | Multi-table flush logic |
| Phase 4 | 4 hours | Worker integration and single-worker testing |
| Phase 5 | 4 hours | Multi-worker testing and validation |
| **Total** | **20 hours** | **~2.5 days with buffer** |

## Next Steps After Completion

1. **Production Run**: Backfill mainnet (3.9 days for all tables)
2. **Monitoring**: Set up Grafana dashboards for multi-table metrics
3. **Optimization**: Tune batch sizes for optimal throughput
4. **Documentation**: Update README with multi-table architecture

## References

- stellar-etl repository: https://github.com/stellar/stellar-etl
- Current parallel implementation: `go/main.go:202-323`
- Performance baseline: `PERFORMANCE_OPTIMIZATION_GUIDE.md:76-84`

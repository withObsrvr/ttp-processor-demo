package transformations

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// EnrichedTransformer handles enriched operations transformations
type EnrichedTransformer struct {
	db             *sql.DB
	catalogName    string
	bronzeSchema   string
	silverSchema   string
	silverDataPath string
}

// NewEnrichedTransformer creates a new enriched transformer
func NewEnrichedTransformer(db *sql.DB, catalogName, bronzeSchema, silverSchema, silverDataPath string) *EnrichedTransformer {
	return &EnrichedTransformer{
		db:             db,
		catalogName:    catalogName,
		bronzeSchema:   bronzeSchema,
		silverSchema:   silverSchema,
		silverDataPath: silverDataPath,
	}
}

// TransformAll runs all 2 enriched operations transformations
func (t *EnrichedTransformer) TransformAll(ctx context.Context, startLedger, endLedger int64) error {
	log.Printf("🔄 Transforming enriched operations tables (ledgers %d to %d)...", startLedger, endLedger)

	transformations := []struct {
		name string
		fn   func(context.Context, int64, int64) error
	}{
		{"enriched_history_operations", t.transformEnrichedHistoryOperations},
		{"enriched_history_operations_soroban", t.transformEnrichedHistoryOperationsSoroban},
	}

	for _, tf := range transformations {
		log.Printf("  → Transforming %s...", tf.name)
		if err := tf.fn(ctx, startLedger, endLedger); err != nil {
			return fmt.Errorf("failed to transform %s: %w", tf.name, err)
		}
		log.Printf("  ✅ %s complete", tf.name)
	}

	log.Printf("✅ All enriched operations tables transformed")
	return nil
}

// transformEnrichedHistoryOperations creates/appends to enriched_history_operations with incremental 3-way JOIN
func (t *EnrichedTransformer) transformEnrichedHistoryOperations(ctx context.Context, startLedger, endLedger int64) error {
	// Step 1: Create table if it doesn't exist
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.enriched_history_operations AS
		SELECT
			o.*,
			-- Transaction fields
			t.successful AS tx_successful,
			t.fee_charged AS tx_fee_charged,
			t.max_fee AS tx_max_fee,
			t.operation_count AS tx_operation_count,
			t.memo_type AS tx_memo_type,
			t.memo AS tx_memo,
			-- Ledger fields
			l.closed_at AS ledger_closed_at,
			l.total_coins AS ledger_total_coins,
			l.fee_pool AS ledger_fee_pool,
			l.base_fee AS ledger_base_fee,
			l.base_reserve AS ledger_base_reserve,
			l.transaction_count AS ledger_transaction_count,
			l.operation_count AS ledger_operation_count,
			l.successful_tx_count AS ledger_successful_tx_count,
			l.failed_tx_count AS ledger_failed_tx_count,
			-- Derived fields
			CASE
				WHEN o.type IN (1, 2, 13) THEN true
				ELSE false
			END AS is_payment_op,
			CASE
				WHEN o.type = 24 THEN true
				ELSE false
			END AS is_soroban_op
		FROM %s.%s.operations_row_v2 o
		INNER JOIN %s.%s.transactions_row_v2 t
			ON o.transaction_hash = t.transaction_hash
			AND o.ledger_sequence = t.ledger_sequence
		INNER JOIN %s.%s.ledgers_row_v2 l
			ON t.ledger_sequence = l.sequence
		WHERE 1=0  -- Create empty table with correct schema
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	if _, err := t.db.ExecContext(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Step 2: Insert only NEW ledgers (incremental append)
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s.enriched_history_operations
		SELECT
			o.*,
			-- Transaction fields
			t.successful AS tx_successful,
			t.fee_charged AS tx_fee_charged,
			t.max_fee AS tx_max_fee,
			t.operation_count AS tx_operation_count,
			t.memo_type AS tx_memo_type,
			t.memo AS tx_memo,
			-- Ledger fields
			l.closed_at AS ledger_closed_at,
			l.total_coins AS ledger_total_coins,
			l.fee_pool AS ledger_fee_pool,
			l.base_fee AS ledger_base_fee,
			l.base_reserve AS ledger_base_reserve,
			l.transaction_count AS ledger_transaction_count,
			l.operation_count AS ledger_operation_count,
			l.successful_tx_count AS ledger_successful_tx_count,
			l.failed_tx_count AS ledger_failed_tx_count,
			-- Derived fields
			CASE
				WHEN o.type IN (1, 2, 13) THEN true
				ELSE false
			END AS is_payment_op,
			CASE
				WHEN o.type = 24 THEN true
				ELSE false
			END AS is_soroban_op
		FROM %s.%s.operations_row_v2 o
		INNER JOIN %s.%s.transactions_row_v2 t
			ON o.transaction_hash = t.transaction_hash
			AND o.ledger_sequence = t.ledger_sequence
		INNER JOIN %s.%s.ledgers_row_v2 l
			ON t.ledger_sequence = l.sequence
		WHERE o.ledger_sequence BETWEEN %d AND %d
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema,
		startLedger, endLedger)

	_, err := t.db.ExecContext(ctx, insertQuery)
	return err
}

// transformEnrichedHistoryOperationsSoroban creates/appends to enriched_history_operations_soroban (filtered for Soroban only)
func (t *EnrichedTransformer) transformEnrichedHistoryOperationsSoroban(ctx context.Context, startLedger, endLedger int64) error {
	// Step 1: Create table if it doesn't exist
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.enriched_history_operations_soroban AS
		SELECT *
		FROM %s.%s.enriched_history_operations
		WHERE 1=0  -- Create empty table with correct schema
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.silverSchema)

	if _, err := t.db.ExecContext(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Step 2: Insert only NEW Soroban operations
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s.enriched_history_operations_soroban
		SELECT *
		FROM %s.%s.enriched_history_operations
		WHERE type = 24  -- OP_INVOKE_HOST_FUNCTION (Soroban contract invocations)
		  AND ledger_sequence BETWEEN %d AND %d
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.silverSchema,
		startLedger, endLedger)

	_, err := t.db.ExecContext(ctx, insertQuery)
	return err
}

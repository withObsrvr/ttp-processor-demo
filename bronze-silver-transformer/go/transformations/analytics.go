package transformations

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// AnalyticsTransformer handles analytics transformations
type AnalyticsTransformer struct {
	db             *sql.DB
	catalogName    string
	bronzeSchema   string
	silverSchema   string
	silverDataPath string
}

// NewAnalyticsTransformer creates a new analytics transformer
func NewAnalyticsTransformer(db *sql.DB, catalogName, bronzeSchema, silverSchema, silverDataPath string) *AnalyticsTransformer {
	return &AnalyticsTransformer{
		db:             db,
		catalogName:    catalogName,
		bronzeSchema:   bronzeSchema,
		silverSchema:   silverSchema,
		silverDataPath: silverDataPath,
	}
}

// TransformAll runs all analytics transformations
func (t *AnalyticsTransformer) TransformAll(ctx context.Context, startLedger, endLedger int64) error {
	log.Printf("🔄 Transforming analytics tables (ledgers %d to %d)...", startLedger, endLedger)

	transformations := []struct {
		name string
		fn   func(context.Context, int64, int64) error
	}{
		{"token_transfers_raw", t.transformTokenTransfersRaw},
	}

	for _, tf := range transformations {
		log.Printf("  → Transforming %s...", tf.name)
		if err := tf.fn(ctx, startLedger, endLedger); err != nil {
			return fmt.Errorf("failed to transform %s: %w", tf.name, err)
		}
		log.Printf("  ✅ %s complete", tf.name)
	}

	log.Printf("✅ All analytics tables transformed")
	return nil
}

// transformTokenTransfersRaw creates/appends to unified token transfers from classic payments + Soroban events
func (t *AnalyticsTransformer) transformTokenTransfersRaw(ctx context.Context, startLedger, endLedger int64) error {
	// Step 1: Create table if it doesn't exist
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.token_transfers_raw AS
		SELECT
			l.closed_at AS timestamp,
			o.transaction_hash,
			o.ledger_sequence,
			'classic' AS source_type,
			o.source_account AS from_account,
			o.destination AS to_account,
			CASE
				WHEN o.asset_type = 'native' THEN 'XLM'
				ELSE o.asset_code
			END AS asset_code,
			CASE
				WHEN o.asset_type = 'native' THEN NULL
				ELSE o.asset_issuer
			END AS asset_issuer,
			o.amount,
			NULL AS token_contract_id,
			o.type AS operation_type,
			t.successful AS transaction_successful
		FROM %s.%s.operations_row_v2 o
		INNER JOIN %s.%s.transactions_row_v2 t
			ON o.transaction_hash = t.transaction_hash
			AND o.ledger_sequence = t.ledger_sequence
		INNER JOIN %s.%s.ledgers_row_v2 l
			ON o.ledger_sequence = l.sequence
		WHERE 1=0  -- Create empty table with correct schema
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	if _, err := t.db.ExecContext(ctx, createTableQuery); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Step 2: Insert only NEW token transfers (incremental append)
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s.token_transfers_raw
		-- Classic Stellar Payments (Payment, PathPaymentStrictReceive, PathPaymentStrictSend)
		SELECT
			l.closed_at AS timestamp,
			o.transaction_hash,
			o.ledger_sequence,
			'classic' AS source_type,
			o.source_account AS from_account,
			CASE
				WHEN o.type = 1 THEN o.destination  -- Payment
				WHEN o.type = 2 THEN o.destination  -- PathPaymentStrictReceive
				WHEN o.type = 13 THEN o.destination -- PathPaymentStrictSend
			END AS to_account,
			CASE
				WHEN o.asset_type = 'native' THEN 'XLM'
				ELSE o.asset_code
			END AS asset_code,
			CASE
				WHEN o.asset_type = 'native' THEN NULL
				ELSE o.asset_issuer
			END AS asset_issuer,
			CASE
				WHEN o.type = 1 THEN o.amount  -- Payment
				WHEN o.type = 2 THEN o.amount  -- PathPaymentStrictReceive (destination amount)
				WHEN o.type = 13 THEN o.source_amount -- PathPaymentStrictSend
			END AS amount,
			NULL AS token_contract_id,
			o.type AS operation_type,
			t.successful AS transaction_successful
		FROM %s.%s.operations_row_v2 o
		INNER JOIN %s.%s.transactions_row_v2 t
			ON o.transaction_hash = t.transaction_hash
			AND o.ledger_sequence = t.ledger_sequence
		INNER JOIN %s.%s.ledgers_row_v2 l
			ON o.ledger_sequence = l.sequence
		WHERE o.type IN (1, 2, 13)  -- Payment operations only
		  AND o.ledger_sequence BETWEEN %d AND %d

		UNION ALL

		-- Soroban Token Transfers (from contract events)
		SELECT
			l.closed_at AS timestamp,
			e.transaction_hash,
			e.ledger_sequence,
			'soroban' AS source_type,
			NULL AS from_account,  -- Would need to parse event data
			NULL AS to_account,    -- Would need to parse event data
			NULL AS asset_code,    -- Token contract determines this
			NULL AS asset_issuer,  -- Not applicable for Soroban
			NULL AS amount,        -- Would need to parse event data
			e.contract_id AS token_contract_id,
			24 AS operation_type,  -- OP_INVOKE_HOST_FUNCTION
			t.successful AS transaction_successful
		FROM %s.%s.contract_events_stream_v1 e
		INNER JOIN %s.%s.transactions_row_v2 t
			ON e.transaction_hash = t.transaction_hash
			AND e.ledger_sequence = t.ledger_sequence
		INNER JOIN %s.%s.ledgers_row_v2 l
			ON e.ledger_sequence = l.sequence
		WHERE e.ledger_sequence BETWEEN %d AND %d
		-- Note: Full Soroban event parsing would require decoding topics_decoded JSON
		-- This simplified version captures all contract events as potential transfers
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema,
		startLedger, endLedger,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema,
		startLedger, endLedger)

	_, err := t.db.ExecContext(ctx, insertQuery)
	return err
}

package main

import (
	"context"
	"database/sql"
	"fmt"
)

// BronzeReader reads data from bronze hot buffer (stellar_hot PostgreSQL)
type BronzeReader struct {
	db *sql.DB
}

// NewBronzeReader creates a new bronze reader
func NewBronzeReader(db *sql.DB) *BronzeReader {
	return &BronzeReader{db: db}
}

// GetMaxLedgerSequence returns the maximum ledger sequence available in bronze hot
func (br *BronzeReader) GetMaxLedgerSequence(ctx context.Context) (int64, error) {
	var maxSeq sql.NullInt64

	query := `
		SELECT MAX(sequence)
		FROM ledgers_row_v2
	`

	err := br.db.QueryRowContext(ctx, query).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to get max ledger sequence: %w", err)
	}

	if !maxSeq.Valid {
		return 0, nil // No ledgers yet
	}

	return maxSeq.Int64, nil
}

// QueryEnrichedOperations reads enriched operations from bronze hot for a ledger range
// Maps raw stellar_hot schema to enriched silver_hot schema
func (br *BronzeReader) QueryEnrichedOperations(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			-- Core operation fields (exist in stellar_hot)
			o.transaction_hash,
			o.operation_index,
			o.ledger_sequence,
			o.source_account,
			o.type,
			o.type_string,
			o.created_at,
			o.transaction_successful,
			o.operation_result_code,
			o.operation_trace_code,
			o.ledger_range,
			o.source_account_muxed,

			-- Asset fields (exist in stellar_hot)
			o.asset,
			o.asset_type,
			o.asset_code,
			o.asset_issuer,
			o.source_asset,
			o.source_asset_type,
			o.source_asset_code,
			o.source_asset_issuer,

			-- Payment/path payment fields
			o.destination,
			NULL::TEXT AS destination_muxed,  -- doesn't exist in raw schema
			o.amount,
			o.source_amount,

			-- Path payment specific (don't exist in raw schema)
			NULL::TEXT AS from_account,
			NULL::TEXT AS from_muxed,
			NULL::TEXT AS to_address,
			NULL::TEXT AS to_muxed,

			-- Trust line fields
			o.trustline_limit AS limit_amount,

			-- Offer fields
			o.offer_id,
			o.selling_asset,
			o.selling_asset_type,
			o.selling_asset_code,
			o.selling_asset_issuer,
			o.buying_asset,
			o.buying_asset_type,
			o.buying_asset_code,
			o.buying_asset_issuer,

			-- Price fields (price_r is JSONB with n/d, need to extract carefully)
			CASE WHEN o.price_r IS NOT NULL THEN (o.price_r::JSONB->>'n')::INTEGER ELSE NULL END AS price_n,
			CASE WHEN o.price_r IS NOT NULL THEN (o.price_r::JSONB->>'d')::INTEGER ELSE NULL END AS price_d,
			o.price,

			-- Account creation
			o.starting_balance,

			-- Account management
			o.home_domain,
			NULL::TEXT AS inflation_dest,  -- deprecated field

			-- Flags (arrays don't exist in raw schema as separate _s fields)
			o.set_flags,
			NULL::TEXT[] AS set_flags_s,
			o.clear_flags,
			NULL::TEXT[] AS clear_flags_s,

			-- Thresholds
			o.master_weight AS master_key_weight,
			o.low_threshold,
			o.medium_threshold AS med_threshold,
			o.high_threshold,

			-- Signer fields (don't exist in raw schema)
			NULL::TEXT AS signer_account_id,
			NULL::TEXT AS signer_key,
			NULL::INTEGER AS signer_weight,

			-- Data entry
			o.data_name,
			o.data_value,

			-- Soroban fields (map raw fields to enriched names)
			o.soroban_operation AS host_function_type,
			NULL::TEXT AS parameters,  -- doesn't exist
			NULL::TEXT AS address,     -- doesn't exist
			o.soroban_contract_id AS contract_id,
			o.soroban_function AS function_name,

			-- Claimable balance
			o.balance_id,
			NULL::TEXT AS claimant,       -- doesn't exist
			NULL::TEXT AS claimant_muxed, -- doesn't exist
			NULL::TEXT AS predicate,      -- doesn't exist

			-- Liquidity pool (don't exist in raw schema)
			NULL::TEXT AS liquidity_pool_id,
			NULL::TEXT AS reserve_a_asset,
			NULL::BIGINT AS reserve_a_amount,
			NULL::TEXT AS reserve_b_asset,
			NULL::BIGINT AS reserve_b_amount,
			NULL::BIGINT AS shares,
			NULL::BIGINT AS shares_received,

			-- Account merge (don't exist in raw schema)
			NULL::TEXT AS into_account,
			NULL::TEXT AS into_muxed,

			-- Sponsorship (sponsored_id exists, others don't)
			NULL::TEXT AS sponsor,
			o.sponsored_id,
			NULL::TEXT AS begin_sponsor,

			-- Transaction fields (enriched from JOIN)
			t.successful AS tx_successful,
			t.fee_charged AS tx_fee_charged,
			t.max_fee AS tx_max_fee,
			t.operation_count AS tx_operation_count,
			t.memo_type AS tx_memo_type,
			t.memo AS tx_memo,

			-- Ledger fields (enriched from JOIN)
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
			CASE WHEN o.type IN (1, 2, 13) THEN true ELSE false END AS is_payment_op,
			CASE WHEN o.type = 24 THEN true ELSE false END AS is_soroban_op
		FROM operations_row_v2 o
		INNER JOIN transactions_row_v2 t
			ON o.transaction_hash = t.transaction_hash
			AND o.ledger_sequence = t.ledger_sequence
		INNER JOIN ledgers_row_v2 l
			ON t.ledger_sequence = l.sequence
		WHERE o.ledger_sequence BETWEEN $1 AND $2
		ORDER BY o.ledger_sequence, o.operation_index
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query enriched operations: %w", err)
	}

	return rows, nil
}

// QueryTokenTransfers reads token transfers from bronze hot for a ledger range
func (br *BronzeReader) QueryTokenTransfers(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		-- Classic Stellar Payments
		SELECT
			l.closed_at AS timestamp,
			o.transaction_hash,
			o.ledger_sequence,
			'classic' AS source_type,
			o.source_account AS from_account,
			CASE
				WHEN o.type = 1 THEN o.destination
				WHEN o.type = 2 THEN o.destination
				WHEN o.type = 13 THEN o.destination
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
				WHEN o.type = 1 THEN o.amount
				WHEN o.type = 2 THEN o.amount
				WHEN o.type = 13 THEN o.source_amount
			END AS amount,
			NULL AS token_contract_id,
			o.type AS operation_type,
			t.successful AS transaction_successful
		FROM operations_row_v2 o
		INNER JOIN transactions_row_v2 t
			ON o.transaction_hash = t.transaction_hash
			AND o.ledger_sequence = t.ledger_sequence
		INNER JOIN ledgers_row_v2 l
			ON o.ledger_sequence = l.sequence
		WHERE o.type IN (1, 2, 13)
		  AND o.ledger_sequence BETWEEN $1 AND $2

		UNION ALL

		-- Soroban Token Transfers
		SELECT
			l.closed_at AS timestamp,
			e.transaction_hash,
			e.ledger_sequence,
			'soroban' AS source_type,
			NULL AS from_account,
			NULL AS to_account,
			NULL AS asset_code,
			NULL AS asset_issuer,
			NULL AS amount,
			e.contract_id AS token_contract_id,
			24 AS operation_type,
			t.successful AS transaction_successful
		FROM contract_events_stream_v1 e
		INNER JOIN transactions_row_v2 t
			ON e.transaction_hash = t.transaction_hash
			AND e.ledger_sequence = t.ledger_sequence
		INNER JOIN ledgers_row_v2 l
			ON e.ledger_sequence = l.sequence
		WHERE e.ledger_sequence BETWEEN $1 AND $2

		ORDER BY ledger_sequence, transaction_hash
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query token transfers: %w", err)
	}

	return rows, nil
}

// QueryAccountsSnapshot reads accounts from bronze hot snapshot for a ledger range
// Uses the latest state for each account in the range (deduplicated)
func (br *BronzeReader) QueryAccountsSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (account_id)
			account_id,
			balance,
			sequence_number,
			num_subentries,
			num_sponsoring,
			num_sponsored,
			home_domain,
			master_weight,
			low_threshold,
			med_threshold,
			high_threshold,
			flags,
			auth_required,
			auth_revocable,
			auth_immutable,
			auth_clawback_enabled,
			signers,
			sponsor_account,
			created_at,
			updated_at,
			ledger_sequence,
			ledger_range,
			era_id,
			version_label
		FROM accounts_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY account_id, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query accounts snapshot: %w", err)
	}

	return rows, nil
}

// QueryAccountsSnapshotAll reads all account snapshot changes (not deduplicated)
// Used for SCD Type 2 incremental append
func (br *BronzeReader) QueryAccountsSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			account_id,
			ledger_sequence,
			closed_at,
			balance,
			sequence_number,
			num_subentries,
			num_sponsoring,
			num_sponsored,
			home_domain,
			master_weight,
			low_threshold,
			med_threshold,
			high_threshold,
			flags,
			auth_required,
			auth_revocable,
			auth_immutable,
			auth_clawback_enabled,
			signers,
			sponsor_account,
			created_at,
			updated_at,
			ledger_range,
			era_id,
			version_label
		FROM accounts_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY account_id, ledger_sequence
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query accounts snapshot all: %w", err)
	}

	return rows, nil
}

// QueryTrustlinesSnapshotAll reads all trustline snapshot changes (not deduplicated)
// Used for SCD Type 2 incremental append
func (br *BronzeReader) QueryTrustlinesSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			t.account_id,
			t.asset_code,
			t.asset_issuer,
			t.asset_type,
			t.balance,
			t.trust_limit,
			t.buying_liabilities,
			t.selling_liabilities,
			t.authorized,
			t.authorized_to_maintain_liabilities,
			t.clawback_enabled,
			t.ledger_sequence,
			l.closed_at,
			t.created_at,
			t.ledger_range,
			t.era_id,
			t.version_label
		FROM trustlines_snapshot_v1 t
		INNER JOIN ledgers_row_v2 l ON t.ledger_sequence = l.sequence
		WHERE t.ledger_sequence BETWEEN $1 AND $2
		ORDER BY t.account_id, t.asset_code, t.asset_issuer, t.asset_type, t.ledger_sequence
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query trustlines snapshot all: %w", err)
	}

	return rows, nil
}

// QueryOffersSnapshotAll reads all offer snapshot changes (not deduplicated)
// Used for SCD Type 2 incremental append
func (br *BronzeReader) QueryOffersSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			offer_id,
			seller_account,
			ledger_sequence,
			closed_at,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			amount,
			price,
			flags,
			created_at,
			ledger_range,
			era_id,
			version_label
		FROM offers_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY offer_id, ledger_sequence
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers snapshot all: %w", err)
	}

	return rows, nil
}

// QueryAccountSignersSnapshotAll reads all account signer snapshot changes (not deduplicated)
// Used for SCD Type 2 incremental append
func (br *BronzeReader) QueryAccountSignersSnapshotAll(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			account_id,
			signer,
			ledger_sequence,
			closed_at,
			weight,
			sponsor,
			ledger_range,
			era_id,
			version_label
		FROM account_signers_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		  AND deleted = false
		ORDER BY account_id, signer, ledger_sequence
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query account signers snapshot all: %w", err)
	}

	return rows, nil
}

// QueryContractInvocations reads contract invocations from Bronze operations for a ledger range
// Filters for InvokeHostFunction operations (type 24) with contract invocation data
func (br *BronzeReader) QueryContractInvocations(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			o.ledger_sequence,
			o.transaction_index,
			o.operation_index,
			o.transaction_hash,
			o.source_account,
			o.soroban_contract_id,
			o.soroban_function,
			o.soroban_arguments_json,
			o.transaction_successful,
			o.created_at,
			o.ledger_range
		FROM operations_row_v2 o
		WHERE o.ledger_sequence BETWEEN $1 AND $2
		  AND o.type = 24  -- InvokeHostFunction
		  AND o.soroban_contract_id IS NOT NULL
		  AND o.soroban_function IS NOT NULL
		  AND o.soroban_arguments_json IS NOT NULL
		ORDER BY o.ledger_sequence, o.transaction_index, o.operation_index
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract invocations: %w", err)
	}

	return rows, nil
}

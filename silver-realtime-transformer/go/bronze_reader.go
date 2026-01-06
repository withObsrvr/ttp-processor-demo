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

// GetMinLedgerSequence returns the minimum ledger sequence available in bronze hot
func (br *BronzeReader) GetMinLedgerSequence(ctx context.Context) (int64, error) {
	var minSeq sql.NullInt64

	query := `
		SELECT MIN(sequence)
		FROM ledgers_row_v2
	`

	err := br.db.QueryRowContext(ctx, query).Scan(&minSeq)
	if err != nil {
		return 0, fmt.Errorf("failed to get min ledger sequence: %w", err)
	}

	if !minSeq.Valid {
		return 0, nil // No ledgers yet
	}

	return minSeq.Int64, nil
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

// QueryTrustlinesSnapshot reads trustline snapshots (deduplicated by account+asset)
// Used for trustlines_current upsert
func (br *BronzeReader) QueryTrustlinesSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (account_id, asset_type, asset_code, asset_issuer)
			account_id,
			asset_type,
			asset_issuer,
			asset_code,
			balance,
			trust_limit,
			buying_liabilities,
			selling_liabilities,
			authorized,
			authorized_to_maintain_liabilities,
			clawback_enabled,
			ledger_sequence,
			created_at,
			ledger_range
		FROM trustlines_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY account_id, asset_type, asset_code, asset_issuer, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query trustlines snapshot: %w", err)
	}

	return rows, nil
}

// QueryOffersSnapshot reads offer snapshots (deduplicated by offer_id)
// Used for offers_current upsert
func (br *BronzeReader) QueryOffersSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (offer_id)
			offer_id,
			seller_account,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			amount,
			price,
			flags,
			ledger_sequence,
			created_at,
			ledger_range
		FROM offers_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY offer_id, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers snapshot: %w", err)
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

// QueryContractCallGraphs reads operations with call graph data from Bronze for a ledger range
// Filters for InvokeHostFunction operations (type 24) with cross-contract calls
func (br *BronzeReader) QueryContractCallGraphs(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
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
			o.contract_calls_json,
			o.contracts_involved,
			o.max_call_depth,
			o.transaction_successful,
			o.created_at,
			o.ledger_range
		FROM operations_row_v2 o
		WHERE o.ledger_sequence BETWEEN $1 AND $2
		  AND o.type = 24  -- InvokeHostFunction
		  AND o.contract_calls_json IS NOT NULL
		  AND o.max_call_depth > 0
		ORDER BY o.ledger_sequence, o.transaction_index, o.operation_index
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract call graphs: %w", err)
	}

	return rows, nil
}

// QueryLiquidityPoolsSnapshot reads liquidity pool snapshots (deduplicated by liquidity_pool_id)
// Used for liquidity_pools_current upsert
func (br *BronzeReader) QueryLiquidityPoolsSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (liquidity_pool_id)
			liquidity_pool_id,
			pool_type,
			fee,
			trustline_count,
			total_pool_shares,
			asset_a_type,
			asset_a_code,
			asset_a_issuer,
			asset_a_amount,
			asset_b_type,
			asset_b_code,
			asset_b_issuer,
			asset_b_amount,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM liquidity_pools_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY liquidity_pool_id, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query liquidity pools snapshot: %w", err)
	}

	return rows, nil
}

// QueryClaimableBalancesSnapshot reads claimable balance snapshots (deduplicated by balance_id)
// Used for claimable_balances_current upsert
func (br *BronzeReader) QueryClaimableBalancesSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (balance_id)
			balance_id,
			sponsor,
			asset_type,
			asset_code,
			asset_issuer,
			amount,
			claimants_count,
			flags,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM claimable_balances_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY balance_id, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable balances snapshot: %w", err)
	}

	return rows, nil
}

// QueryNativeBalancesSnapshot reads native balance snapshots (deduplicated by account_id)
// Used for native_balances_current upsert
func (br *BronzeReader) QueryNativeBalancesSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (account_id)
			account_id,
			balance,
			buying_liabilities,
			selling_liabilities,
			num_subentries,
			num_sponsoring,
			num_sponsored,
			sequence_number,
			last_modified_ledger,
			ledger_sequence,
			ledger_range
		FROM native_balances_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY account_id, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query native balances snapshot: %w", err)
	}

	return rows, nil
}

// QueryTrades reads trade events from Bronze for a ledger range
// Event stream table - no deduplication, ordered by ledger and trade index
func (br *BronzeReader) QueryTrades(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			trade_index,
			trade_type,
			trade_timestamp,
			seller_account,
			selling_asset_code,
			selling_asset_issuer,
			selling_amount,
			buyer_account,
			buying_asset_code,
			buying_asset_issuer,
			buying_amount,
			price,
			created_at,
			ledger_range
		FROM trades_row_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY ledger_sequence, operation_index, trade_index
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query trades: %w", err)
	}

	return rows, nil
}

// QueryEffects reads effect events from Bronze for a ledger range
// Event stream table - no deduplication, ordered by ledger and effect index
func (br *BronzeReader) QueryEffects(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			ledger_sequence,
			transaction_hash,
			operation_index,
			effect_index,
			effect_type,
			effect_type_string,
			account_id,
			amount,
			asset_code,
			asset_issuer,
			asset_type,
			trustline_limit,
			authorize_flag,
			clawback_flag,
			signer_account,
			signer_weight,
			offer_id,
			seller_account,
			created_at,
			ledger_range
		FROM effects_row_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY ledger_sequence, operation_index, effect_index
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query effects: %w", err)
	}

	return rows, nil
}

// =============================================================================
// Phase 3: Soroban Tables
// =============================================================================

// QueryContractDataSnapshot reads contract data snapshots (deduplicated by contract_id + key_hash)
// Used for contract_data_current upsert
func (br *BronzeReader) QueryContractDataSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (contract_id, ledger_key_hash)
			contract_id,
			ledger_key_hash AS key_hash,
			contract_durability AS durability,
			asset_type,
			asset_code,
			asset_issuer,
			contract_data_xdr AS data_value,
			last_modified_ledger,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM contract_data_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		  AND deleted = false
		ORDER BY contract_id, ledger_key_hash, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract data snapshot: %w", err)
	}

	return rows, nil
}

// QueryContractCodeSnapshot reads contract code snapshots (deduplicated by contract_code_hash)
// Used for contract_code_current upsert
func (br *BronzeReader) QueryContractCodeSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (contract_code_hash)
			contract_code_hash,
			contract_code_ext_v,
			n_data_segment_bytes,
			n_data_segments,
			n_elem_segments,
			n_exports,
			n_functions,
			n_globals,
			n_imports,
			n_instructions,
			n_table_entries,
			n_types,
			last_modified_ledger,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM contract_code_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		  AND deleted = false
		ORDER BY contract_code_hash, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query contract code snapshot: %w", err)
	}

	return rows, nil
}

// QueryTTLSnapshot reads TTL snapshots (deduplicated by key_hash)
// Used for ttl_current upsert
func (br *BronzeReader) QueryTTLSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (key_hash)
			key_hash,
			live_until_ledger_seq,
			ttl_remaining,
			expired,
			last_modified_ledger,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM ttl_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		  AND deleted = false
		ORDER BY key_hash, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query TTL snapshot: %w", err)
	}

	return rows, nil
}

// QueryEvictedKeys reads evicted key events from Bronze for a ledger range
// Event stream table - no deduplication, ordered by ledger
func (br *BronzeReader) QueryEvictedKeys(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			contract_id,
			key_hash,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM evicted_keys_state_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY ledger_sequence, contract_id, key_hash
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query evicted keys: %w", err)
	}

	return rows, nil
}

// QueryRestoredKeys reads restored key events from Bronze for a ledger range
// Event stream table - no deduplication, ordered by ledger
func (br *BronzeReader) QueryRestoredKeys(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT
			contract_id,
			key_hash,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM restored_keys_state_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		ORDER BY ledger_sequence, contract_id, key_hash
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query restored keys: %w", err)
	}

	return rows, nil
}

// =============================================================================
// Phase 4: Config Settings
// =============================================================================

// QueryConfigSettingsSnapshot reads config settings snapshots (deduplicated by config_setting_id)
// Used for config_settings_current upsert
func (br *BronzeReader) QueryConfigSettingsSnapshot(ctx context.Context, startLedger, endLedger int64) (*sql.Rows, error) {
	query := `
		SELECT DISTINCT ON (config_setting_id)
			config_setting_id,
			ledger_max_instructions,
			tx_max_instructions,
			fee_rate_per_instructions_increment,
			tx_memory_limit,
			ledger_max_read_ledger_entries,
			ledger_max_read_bytes,
			ledger_max_write_ledger_entries,
			ledger_max_write_bytes,
			tx_max_read_ledger_entries,
			tx_max_read_bytes,
			tx_max_write_ledger_entries,
			tx_max_write_bytes,
			contract_max_size_bytes,
			config_setting_xdr,
			last_modified_ledger,
			ledger_sequence,
			closed_at,
			created_at,
			ledger_range
		FROM config_settings_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		  AND deleted = false
		ORDER BY config_setting_id, ledger_sequence DESC
	`

	rows, err := br.db.QueryContext(ctx, query, startLedger, endLedger)
	if err != nil {
		return nil, fmt.Errorf("failed to query config settings snapshot: %w", err)
	}

	return rows, nil
}

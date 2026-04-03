package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// transformSilver materializes silver-layer tables from bronze data.
func (w *DuckLakeWriter) transformSilver(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	if err := transformEnrichedHistoryOperations(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("enriched_history_operations: %w", err)
	}

	if err := transformContractInvocationsRaw(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("contract_invocations_raw: %w", err)
	}

	if err := transformTokenRegistry(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("token_registry: %w", err)
	}

	if err := transformContractMetadata(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("contract_metadata: %w", err)
	}

	if err := transformTokenTransfersRaw(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("token_transfers_raw: %w", err)
	}

	return nil
}

// transformEnrichedHistoryOperations joins operations with transactions and ledgers
// to produce a denormalized view suitable for querying.
func transformEnrichedHistoryOperations(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s.enriched_history_operations (
			id,
			transaction_hash,
			source_account,
			source_account_muxed,
			type,
			type_string,
			operation_index,
			transaction_index,
			ledger_sequence,
			closed_at,
			transaction_successful,
			fee_charged,
			memo_type,
			memo,
			amount,
			asset,
			asset_type,
			asset_code,
			asset_issuer,
			destination,
			source_asset,
			source_asset_type,
			source_asset_code,
			source_asset_issuer,
			source_amount,
			destination_min,
			starting_balance,
			trustline_limit,
			offer_id,
			price,
			buying_asset,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			selling_asset,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			soroban_operation,
			soroban_contract_id,
			soroban_function,
			ledger_range,
			pipeline_version
		)
		SELECT
			o.transaction_hash || '-' || CAST(o.operation_index AS TEXT) AS id,
			o.transaction_hash,
			o.source_account,
			o.source_account_muxed,
			o.op_type,
			o.type_string,
			o.operation_index,
			o.transaction_index,
			o.ledger_sequence,
			l.closed_at,
			o.transaction_successful,
			t.fee_charged,
			t.memo_type,
			t.memo,
			o.amount,
			o.asset,
			o.asset_type,
			o.asset_code,
			o.asset_issuer,
			o.destination,
			o.source_asset,
			o.source_asset_type,
			o.source_asset_code,
			o.source_asset_issuer,
			o.source_amount,
			o.destination_min,
			o.starting_balance,
			o.trustline_limit,
			o.offer_id,
			o.price,
			o.buying_asset,
			o.buying_asset_type,
			o.buying_asset_code,
			o.buying_asset_issuer,
			o.selling_asset,
			o.selling_asset_type,
			o.selling_asset_code,
			o.selling_asset_issuer,
			o.soroban_operation,
			o.soroban_contract_id,
			o.soroban_function,
			o.ledger_range,
			o.pipeline_version
		FROM %s.%s.operations_row_v2 o
		JOIN %s.%s.transactions_row_v2 t
			ON o.transaction_hash = t.transaction_hash
			AND o.ledger_sequence = t.ledger_sequence
		JOIN %s.%s.ledgers_row_v2 l
			ON o.ledger_sequence = l.sequence
		WHERE o.ledger_sequence BETWEEN $1 AND $2
	`, cat, silver,
		cat, bronze,
		cat, bronze,
		cat, bronze,
	)

	_, err := tx.ExecContext(ctx, query, startSeq, endSeq)
	return err
}

// transformContractInvocationsRaw extracts Soroban contract invocations from bronze operations.
func transformContractInvocationsRaw(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s.contract_invocations_raw (
			ledger_sequence, transaction_index, operation_index,
			transaction_hash, source_account,
			contract_id, function_name, arguments_json,
			successful, closed_at, ledger_range, pipeline_version
		)
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
			l.closed_at,
			o.ledger_range,
			o.pipeline_version
		FROM %s.%s.operations_row_v2 o
		JOIN %s.%s.ledgers_row_v2 l ON o.ledger_sequence = l.sequence
		WHERE o.ledger_sequence BETWEEN $1 AND $2
		  AND o.op_type = 24
		  AND o.soroban_contract_id IS NOT NULL
		  AND o.soroban_function IS NOT NULL
	`, cat, silver,
		cat, bronze,
		cat, bronze,
	)

	result, err := tx.ExecContext(ctx, query, startSeq, endSeq)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		log.Printf("  contract_invocations_raw: %d rows", rows)
	}
	return nil
}

// transformTokenRegistry upserts token metadata from bronze contract_data where token fields are present.
// DuckLake does not support ON CONFLICT, so we use DELETE + INSERT for updates.
func transformTokenRegistry(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	// First, delete existing entries for contracts we're about to update
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %s.%s.token_registry
		WHERE contract_id IN (
			SELECT DISTINCT contract_id
			FROM %s.%s.contract_data_snapshot_v1
			WHERE ledger_sequence BETWEEN $1 AND $2
			  AND contract_key_type = 'ScValTypeScvLedgerKeyContractInstance'
			  AND deleted = false
			  AND (token_name IS NOT NULL OR token_symbol IS NOT NULL OR asset_code IS NOT NULL)
		)
	`, cat, silver, cat, bronze)

	if _, err := tx.ExecContext(ctx, deleteQuery, startSeq, endSeq); err != nil {
		// Table may be empty on first run, ignore delete errors
		log.Printf("  token_registry delete (non-fatal): %v", err)
	}

	// Insert latest token metadata per contract
	insertQuery := fmt.Sprintf(`
		INSERT INTO %s.%s.token_registry (
			contract_id, token_name, token_symbol, token_decimals,
			asset_code, asset_issuer, token_type,
			first_seen_ledger, last_updated_ledger,
			created_at, updated_at
		)
		SELECT
			contract_id,
			token_name,
			token_symbol,
			COALESCE(token_decimals, 7),
			asset_code,
			asset_issuer,
			CASE
				WHEN asset_code IS NOT NULL AND asset_code != '' THEN 'sac'
				ELSE 'custom_soroban'
			END,
			MIN(ledger_sequence),
			MAX(ledger_sequence),
			MIN(closed_at),
			MAX(closed_at)
		FROM %s.%s.contract_data_snapshot_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		  AND contract_key_type = 'ScValTypeScvLedgerKeyContractInstance'
		  AND deleted = false
		  AND (token_name IS NOT NULL OR token_symbol IS NOT NULL OR asset_code IS NOT NULL)
		GROUP BY contract_id, token_name, token_symbol, token_decimals, asset_code, asset_issuer
	`, cat, silver, cat, bronze)

	result, err := tx.ExecContext(ctx, insertQuery, startSeq, endSeq)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		log.Printf("  token_registry: %d rows", rows)
	}
	return nil
}

// transformContractMetadata upserts contract creation records into silver.
func transformContractMetadata(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s.contract_metadata (
			contract_id, creator_address, wasm_hash,
			created_ledger, created_at,
			ledger_range, pipeline_version
		)
		SELECT
			contract_id, creator_address, wasm_hash,
			created_ledger, created_at,
			ledger_range, pipeline_version
		FROM %s.%s.contract_creations_v1
		WHERE created_ledger BETWEEN $1 AND $2
	`, cat, silver, cat, bronze)

	result, err := tx.ExecContext(ctx, query, startSeq, endSeq)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows > 0 {
		log.Printf("  contract_metadata: %d rows", rows)
	}
	return nil
}

// transformTokenTransfersRaw extracts unified token transfers from two sources:
// 1. Classic payments from silver.enriched_history_operations (types 1,2,13)
// 2. Soroban SEP-41 transfer events from bronze.contract_events_stream_v1
func transformTokenTransfersRaw(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	// Part 1: Classic Stellar payments
	classicQuery := fmt.Sprintf(`
		INSERT INTO %s.%s.token_transfers_raw (
			timestamp, transaction_hash, ledger_sequence, source_type,
			from_account, to_account, asset_code, asset_issuer, amount,
			token_contract_id, operation_type, transaction_successful,
			event_index, ledger_range, pipeline_version
		)
		SELECT
			e.closed_at,
			e.transaction_hash,
			e.ledger_sequence,
			'classic',
			e.source_account,
			e.destination,
			CASE WHEN e.asset_type = 'native' THEN 'XLM' ELSE e.asset_code END,
			CASE WHEN e.asset_type = 'native' THEN NULL ELSE e.asset_issuer END,
			CAST(e.amount AS DOUBLE),
			NULL,
			e.type,
			e.transaction_successful,
			NULL,
			e.ledger_range,
			e.pipeline_version
		FROM %s.%s.enriched_history_operations e
		WHERE e.ledger_sequence BETWEEN $1 AND $2
		  AND e.type IN (1, 2, 13)
		  AND e.amount IS NOT NULL
		  AND e.amount > 0
	`, cat, silver, cat, silver)

	result, err := tx.ExecContext(ctx, classicQuery, startSeq, endSeq)
	if err != nil {
		return fmt.Errorf("classic transfers: %w", err)
	}
	classicRows, _ := result.RowsAffected()

	// Part 2: Soroban SEP-41 token transfer events
	// Extract from bronze contract events where topic0 indicates a transfer/mint/burn
	sorobanQuery := fmt.Sprintf(`
		INSERT INTO %s.%s.token_transfers_raw (
			timestamp, transaction_hash, ledger_sequence, source_type,
			from_account, to_account, asset_code, asset_issuer, amount,
			token_contract_id, operation_type, transaction_successful,
			event_index, ledger_range, pipeline_version
		)
		SELECT
			e.closed_at,
			e.transaction_hash,
			e.ledger_sequence,
			'soroban',
			CASE
				WHEN e.topic0_decoded IN ('transfer', 'burn', 'clawback') THEN e.topic1_decoded
				ELSE NULL
			END,
			CASE
				WHEN e.topic0_decoded = 'transfer' THEN e.topic2_decoded
				WHEN e.topic0_decoded = 'mint' THEN e.topic1_decoded
				ELSE NULL
			END,
			NULL,
			NULL,
			CASE
				WHEN e.data_decoded IS NOT NULL AND e.data_decoded != ''
				THEN TRY_CAST(
					REPLACE(REPLACE(e.data_decoded, '"', ''), '{', '')
					AS DOUBLE
				)
				ELSE NULL
			END,
			e.contract_id,
			24,
			e.in_successful_contract_call,
			e.event_index,
			e.ledger_range,
			e.pipeline_version
		FROM %s.%s.contract_events_stream_v1 e
		WHERE e.ledger_sequence BETWEEN $1 AND $2
		  AND e.event_type = 'contract'
		  AND e.topic_count >= 2
		  AND e.topic0_decoded IN ('transfer', 'mint', 'burn', 'clawback')
	`, cat, silver, cat, bronze)

	result, err = tx.ExecContext(ctx, sorobanQuery, startSeq, endSeq)
	if err != nil {
		// Soroban events may not exist for every ledger - log but don't fail
		log.Printf("  token_transfers_raw soroban (non-fatal): %v", err)
	} else {
		sorobanRows, _ := result.RowsAffected()
		if classicRows+sorobanRows > 0 {
			log.Printf("  token_transfers_raw: %d classic + %d soroban", classicRows, sorobanRows)
		}
	}

	return nil
}

// transformSemantic materializes semantic-layer tables from silver + bronze data.
func (w *DuckLakeWriter) transformSemantic(ctx context.Context, tx *sql.Tx, cat, silver string, startSeq, endSeq int64) error {
	bronze := w.cfg.DuckLake.BronzeSchema

	if err := transformSemanticActivities(ctx, tx, cat, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("semantic_activities: %w", err)
	}
	if err := transformSemanticFlows(ctx, tx, cat, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("semantic_flows_value: %w", err)
	}
	if err := transformSemanticContractFunctions(ctx, tx, cat, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("semantic_contract_functions: %w", err)
	}
	if err := transformSemanticEntitiesContracts(ctx, tx, cat, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("semantic_entities_contracts: %w", err)
	}
	if err := transformSemanticDexPairs(ctx, tx, cat, bronze, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("semantic_dex_pairs: %w", err)
	}
	if err := transformSemanticAccountSummary(ctx, tx, cat, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("semantic_account_summary: %w", err)
	}
	if err := transformSemanticAssetStats(ctx, tx, cat, silver, startSeq, endSeq); err != nil {
		return fmt.Errorf("semantic_asset_stats: %w", err)
	}
	return nil
}

// transformSemanticActivities maps enriched operations to human-readable activity types.
func transformSemanticActivities(ctx context.Context, tx *sql.Tx, cat, silver string, startSeq, endSeq int64) error {
	query := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s.semantic_activities (
			id, ledger_sequence, timestamp, activity_type, description,
			source_account, destination_account, contract_id,
			asset_code, asset_issuer, amount,
			is_soroban, soroban_function_name,
			transaction_hash, operation_index, successful, fee_charged, created_at
		)
		SELECT
			e.id,
			e.ledger_sequence,
			e.closed_at,
			CASE
				WHEN e.type = 0 THEN 'account_created'
				WHEN e.type = 1 THEN 'payment'
				WHEN e.type = 2 THEN 'path_payment'
				WHEN e.type = 3 THEN 'manage_sell_offer'
				WHEN e.type = 4 THEN 'create_passive_sell_offer'
				WHEN e.type = 5 THEN 'set_options'
				WHEN e.type = 6 THEN 'change_trust'
				WHEN e.type = 7 THEN 'allow_trust'
				WHEN e.type = 8 THEN 'account_merge'
				WHEN e.type = 12 THEN 'manage_buy_offer'
				WHEN e.type = 13 THEN 'path_payment_strict_send'
				WHEN e.type = 24 AND e.soroban_function IS NOT NULL THEN 'contract_call'
				WHEN e.type = 24 THEN 'invoke_host_function'
				ELSE 'other'
			END,
			e.type_string,
			e.source_account,
			e.destination,
			e.soroban_contract_id,
			e.asset_code,
			e.asset_issuer,
			CAST(e.amount AS TEXT),
			e.soroban_contract_id IS NOT NULL,
			e.soroban_function,
			e.transaction_hash,
			e.operation_index,
			e.transaction_successful,
			e.fee_charged,
			e.closed_at
		FROM %[1]s.%[2]s.enriched_history_operations e
		WHERE e.ledger_sequence BETWEEN $1 AND $2
	`, cat, silver)

	_, err := tx.ExecContext(ctx, query, startSeq, endSeq)
	return err
}

// transformSemanticFlows derives value flow types (transfer/mint/burn) from token_transfers_raw.
func transformSemanticFlows(ctx context.Context, tx *sql.Tx, cat, silver string, startSeq, endSeq int64) error {
	query := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s.semantic_flows_value (
			id, ledger_sequence, timestamp, flow_type,
			from_account, to_account, contract_id,
			asset_code, asset_issuer, asset_type, amount,
			transaction_hash, operation_type, successful, created_at
		)
		SELECT
			t.transaction_hash || ':' || CAST(ROW_NUMBER() OVER (
				PARTITION BY t.transaction_hash ORDER BY t.from_account, t.to_account
			) AS TEXT),
			t.ledger_sequence,
			t.timestamp,
			CASE
				WHEN t.from_account IS NULL OR t.from_account = '' THEN 'mint'
				WHEN t.to_account IS NULL OR t.to_account = '' THEN 'burn'
				ELSE 'transfer'
			END,
			t.from_account,
			t.to_account,
			t.token_contract_id,
			t.asset_code,
			t.asset_issuer,
			CASE
				WHEN t.source_type = 'soroban' THEN 'soroban_token'
				WHEN t.asset_code IS NULL OR t.asset_code = '' OR t.asset_code = 'XLM' THEN 'native'
				WHEN LENGTH(t.asset_code) <= 4 THEN 'credit_alphanum4'
				ELSE 'credit_alphanum12'
			END,
			CAST(COALESCE(t.amount, 0) AS TEXT),
			t.transaction_hash,
			t.operation_type,
			t.transaction_successful,
			t.timestamp
		FROM %[1]s.%[2]s.token_transfers_raw t
		WHERE t.ledger_sequence BETWEEN $1 AND $2
		  AND t.amount IS NOT NULL
	`, cat, silver)

	_, err := tx.ExecContext(ctx, query, startSeq, endSeq)
	return err
}

// transformSemanticContractFunctions aggregates per-function call stats.
func transformSemanticContractFunctions(ctx context.Context, tx *sql.Tx, cat, silver string, startSeq, endSeq int64) error {
	// Delete existing rows for contracts seen in this range, then re-insert
	// (DuckLake lacks ON CONFLICT)
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[2]s.semantic_contract_functions
		WHERE (contract_id, function_name) IN (
			SELECT contract_id, function_name
			FROM %[1]s.%[2]s.contract_invocations_raw
			WHERE ledger_sequence BETWEEN $1 AND $2
			  AND function_name IS NOT NULL AND function_name != ''
			GROUP BY contract_id, function_name
		)
	`, cat, silver)
	_, _ = tx.ExecContext(ctx, deleteQuery, startSeq, endSeq)

	insertQuery := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s.semantic_contract_functions (
			contract_id, function_name,
			total_calls, successful_calls, failed_calls, unique_callers,
			first_called, last_called, updated_at
		)
		SELECT
			contract_id, function_name,
			COUNT(*),
			COUNT(*) FILTER (WHERE successful),
			COUNT(*) FILTER (WHERE NOT successful),
			COUNT(DISTINCT source_account),
			MIN(closed_at), MAX(closed_at), MAX(closed_at)
		FROM %[1]s.%[2]s.contract_invocations_raw
		WHERE ledger_sequence BETWEEN $1 AND $2
		  AND function_name IS NOT NULL AND function_name != ''
		GROUP BY contract_id, function_name
	`, cat, silver)

	_, err := tx.ExecContext(ctx, insertQuery, startSeq, endSeq)
	return err
}

// transformSemanticEntitiesContracts builds a contract registry with invocation stats.
func transformSemanticEntitiesContracts(ctx context.Context, tx *sql.Tx, cat, silver string, startSeq, endSeq int64) error {
	// Delete existing rows for contracts seen in this range
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[2]s.semantic_entities_contracts
		WHERE contract_id IN (
			SELECT DISTINCT contract_id
			FROM %[1]s.%[2]s.contract_invocations_raw
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
	`, cat, silver)
	_, _ = tx.ExecContext(ctx, deleteQuery, startSeq, endSeq)

	insertQuery := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s.semantic_entities_contracts (
			contract_id, contract_type,
			token_name, token_symbol, token_decimals,
			deployer_account, deployed_at, deployed_ledger,
			total_invocations, last_activity, unique_callers,
			observed_functions, created_at, updated_at
		)
		SELECT
			ci.contract_id,
			COALESCE(tr.token_type, 'unknown'),
			tr.token_name,
			tr.token_symbol,
			tr.token_decimals,
			cm.creator_address,
			cm.created_at,
			cm.created_ledger,
			COUNT(*),
			MAX(ci.closed_at),
			COUNT(DISTINCT ci.source_account),
			STRING_AGG(DISTINCT ci.function_name, ','),
			MIN(ci.closed_at),
			MAX(ci.closed_at)
		FROM %[1]s.%[2]s.contract_invocations_raw ci
		LEFT JOIN %[1]s.%[2]s.token_registry tr ON tr.contract_id = ci.contract_id
		LEFT JOIN %[1]s.%[2]s.contract_metadata cm ON cm.contract_id = ci.contract_id
		WHERE ci.ledger_sequence BETWEEN $1 AND $2
		GROUP BY ci.contract_id, tr.token_type, tr.token_name, tr.token_symbol, tr.token_decimals,
			cm.creator_address, cm.created_at, cm.created_ledger
	`, cat, silver)

	_, err := tx.ExecContext(ctx, insertQuery, startSeq, endSeq)
	return err
}

// transformSemanticDexPairs aggregates DEX trading pair stats from bronze trades.
func transformSemanticDexPairs(ctx context.Context, tx *sql.Tx, cat, bronze, silver string, startSeq, endSeq int64) error {
	// Delete pairs seen in this range
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[2]s.semantic_dex_pairs
		WHERE pair_key IN (
			SELECT
				COALESCE(selling_asset_code, 'XLM') || ':' || COALESCE(selling_asset_issuer, 'native')
				|| '/' || COALESCE(buying_asset_code, 'XLM') || ':' || COALESCE(buying_asset_issuer, 'native')
			FROM %[1]s.%[3]s.trades_row_v1
			WHERE ledger_sequence BETWEEN $1 AND $2
			GROUP BY selling_asset_code, selling_asset_issuer, buying_asset_code, buying_asset_issuer
		)
	`, cat, silver, bronze)
	_, _ = tx.ExecContext(ctx, deleteQuery, startSeq, endSeq)

	insertQuery := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s.semantic_dex_pairs (
			pair_key, selling_asset_code, selling_asset_issuer,
			buying_asset_code, buying_asset_issuer,
			trade_count, selling_volume, buying_volume,
			last_price, unique_sellers, unique_buyers,
			first_trade, last_trade, updated_at
		)
		SELECT
			COALESCE(selling_asset_code, 'XLM') || ':' || COALESCE(selling_asset_issuer, 'native')
			|| '/' || COALESCE(buying_asset_code, 'XLM') || ':' || COALESCE(buying_asset_issuer, 'native'),
			selling_asset_code, selling_asset_issuer,
			buying_asset_code, buying_asset_issuer,
			COUNT(*),
			SUM(CAST(selling_amount AS DOUBLE)),
			SUM(CAST(buying_amount AS DOUBLE)),
			LAST(CAST(price AS DOUBLE) ORDER BY trade_timestamp),
			COUNT(DISTINCT seller_account),
			COUNT(DISTINCT buyer_account),
			MIN(trade_timestamp), MAX(trade_timestamp), MAX(trade_timestamp)
		FROM %[1]s.%[3]s.trades_row_v1
		WHERE ledger_sequence BETWEEN $1 AND $2
		GROUP BY selling_asset_code, selling_asset_issuer, buying_asset_code, buying_asset_issuer
	`, cat, silver, bronze)

	_, err := tx.ExecContext(ctx, insertQuery, startSeq, endSeq)
	return err
}

// transformSemanticAccountSummary aggregates per-account activity stats.
func transformSemanticAccountSummary(ctx context.Context, tx *sql.Tx, cat, silver string, startSeq, endSeq int64) error {
	// Delete accounts seen in this range
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[2]s.semantic_account_summary
		WHERE account_id IN (
			SELECT DISTINCT source_account
			FROM %[1]s.%[2]s.enriched_history_operations
			WHERE ledger_sequence BETWEEN $1 AND $2
			  AND source_account IS NOT NULL AND source_account != ''
		)
	`, cat, silver)
	_, _ = tx.ExecContext(ctx, deleteQuery, startSeq, endSeq)

	insertQuery := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s.semantic_account_summary (
			account_id, total_operations,
			total_payments_sent, total_payments_received,
			total_contract_calls, unique_contracts_called,
			top_contract_id, top_contract_function,
			is_contract_deployer, contracts_deployed,
			first_activity, last_activity, updated_at
		)
		SELECT
			e.source_account,
			COUNT(*),
			COUNT(*) FILTER (WHERE e.type IN (1, 2, 13)),
			0,
			COUNT(*) FILTER (WHERE e.type = 24),
			COUNT(DISTINCT e.soroban_contract_id) FILTER (WHERE e.type = 24),
			FIRST(e.soroban_contract_id ORDER BY e.ledger_sequence DESC) FILTER (WHERE e.type = 24),
			FIRST(e.soroban_function ORDER BY e.ledger_sequence DESC) FILTER (WHERE e.type = 24),
			BOOL_OR(e.type_string = 'invoke_host_function'),
			COUNT(*) FILTER (WHERE e.type_string = 'invoke_host_function' AND e.soroban_operation = 'CreateContract'),
			MIN(e.closed_at),
			MAX(e.closed_at),
			MAX(e.closed_at)
		FROM %[1]s.%[2]s.enriched_history_operations e
		WHERE e.ledger_sequence BETWEEN $1 AND $2
		  AND e.source_account IS NOT NULL AND e.source_account != ''
		GROUP BY e.source_account
	`, cat, silver)

	_, err := tx.ExecContext(ctx, insertQuery, startSeq, endSeq)
	return err
}

// transformSemanticAssetStats aggregates per-asset transfer stats.
func transformSemanticAssetStats(ctx context.Context, tx *sql.Tx, cat, silver string, startSeq, endSeq int64) error {
	// Delete assets seen in this range
	deleteQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[2]s.semantic_asset_stats
		WHERE asset_key IN (
			SELECT DISTINCT
				CASE
					WHEN source_type = 'soroban' AND token_contract_id IS NOT NULL THEN token_contract_id
					WHEN asset_code IS NULL OR asset_code = '' OR asset_code = 'XLM' THEN 'native'
					ELSE asset_code || ':' || COALESCE(asset_issuer, '')
				END
			FROM %[1]s.%[2]s.token_transfers_raw
			WHERE ledger_sequence BETWEEN $1 AND $2
		)
	`, cat, silver)
	_, _ = tx.ExecContext(ctx, deleteQuery, startSeq, endSeq)

	insertQuery := fmt.Sprintf(`
		INSERT INTO %[1]s.%[2]s.semantic_asset_stats (
			asset_key, asset_code, asset_issuer, asset_type,
			token_name, token_symbol, token_decimals, contract_id,
			transfer_count, transfer_volume,
			first_seen, last_transfer, updated_at
		)
		SELECT
			CASE
				WHEN t.source_type = 'soroban' AND t.token_contract_id IS NOT NULL THEN t.token_contract_id
				WHEN t.asset_code IS NULL OR t.asset_code = '' OR t.asset_code = 'XLM' THEN 'native'
				ELSE t.asset_code || ':' || COALESCE(t.asset_issuer, '')
			END,
			t.asset_code,
			t.asset_issuer,
			CASE
				WHEN t.source_type = 'soroban' THEN 'soroban_token'
				WHEN t.asset_code IS NULL OR t.asset_code = '' OR t.asset_code = 'XLM' THEN 'native'
				WHEN LENGTH(t.asset_code) <= 4 THEN 'credit_alphanum4'
				ELSE 'credit_alphanum12'
			END,
			tr.token_name,
			tr.token_symbol,
			tr.token_decimals,
			t.token_contract_id,
			COUNT(*),
			COALESCE(SUM(t.amount), 0),
			MIN(t.timestamp),
			MAX(t.timestamp),
			MAX(t.timestamp)
		FROM %[1]s.%[2]s.token_transfers_raw t
		LEFT JOIN %[1]s.%[2]s.token_registry tr ON tr.contract_id = t.token_contract_id
		WHERE t.ledger_sequence BETWEEN $1 AND $2
		GROUP BY 1, t.asset_code, t.asset_issuer, t.source_type, t.token_contract_id,
			tr.token_name, tr.token_symbol, tr.token_decimals
	`, cat, silver)

	_, err := tx.ExecContext(ctx, insertQuery, startSeq, endSeq)
	return err
}

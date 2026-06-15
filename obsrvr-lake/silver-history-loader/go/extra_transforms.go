package main

import "fmt"

func metaSelect(l *Loader, start, end int64) string {
	return fmt.Sprintf("%s AS _schema_version, current_timestamp AS _loaded_at, %d::BIGINT AS _source_bronze_start_ledger, %d::BIGINT AS _source_bronze_end_ledger", q(schemaVersion), start, end)
}

func selectAccountsSnapshotAll(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, account_id, ledger_sequence, closed_at, balance, sequence_number,
		num_subentries, num_sponsoring, num_sponsored, home_domain, master_weight, low_threshold, med_threshold,
		high_threshold, flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled, signers,
		sponsor_account, closed_at AS created_at, closed_at AS updated_at, ledger_range, era_id, version_label,
		NULL::TIMESTAMP AS valid_to, closed_at AS ledger_closed_at, %s
		FROM %s WHERE ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("accounts_snapshot_v1"), start, end)
}

func selectTrustlinesSnapshotAll(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, t.account_id, t.asset_code, t.asset_issuer, t.asset_type, t.balance,
		t.trust_limit, t.buying_liabilities, t.selling_liabilities, t.authorized, t.authorized_to_maintain_liabilities,
		t.clawback_enabled, t.ledger_sequence, l.closed_at AS created_at, t.ledger_range, t.era_id, t.version_label,
		NULL::TIMESTAMP AS valid_to, l.closed_at AS ledger_closed_at, %s
		FROM %s t JOIN %s l ON t.ledger_sequence = l.sequence
		WHERE t.ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("trustlines_snapshot_v1"), l.bronze("ledgers_row_v2"), start, end)
}

func selectOffersSnapshotAll(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, offer_id, seller_account, ledger_sequence, closed_at,
		selling_asset_type, selling_asset_code, selling_asset_issuer, buying_asset_type, buying_asset_code,
		buying_asset_issuer, amount, price, flags, closed_at AS created_at, ledger_range, era_id, version_label,
		NULL::TIMESTAMP AS valid_to, closed_at AS ledger_closed_at, %s
		FROM %s WHERE ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("offers_snapshot_v1"), start, end)
}

func selectAccountSignersSnapshotAll(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, account_id, signer, ledger_sequence, closed_at, weight, sponsor,
		ledger_range, era_id, version_label, NULL::TIMESTAMP AS valid_to, closed_at AS ledger_closed_at, %s
		FROM %s WHERE ledger_sequence BETWEEN %d AND %d AND deleted = false`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("account_signers_snapshot_v1"), start, end)
}

func selectContractInvocations(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, o.ledger_sequence, o.transaction_index, o.operation_index, o.transaction_hash,
		o.source_account, o.soroban_contract_id AS contract_id, o.soroban_function AS function_name,
		o.soroban_arguments_json AS arguments_json, o.transaction_successful AS successful, o.created_at AS closed_at,
		o.ledger_range, o.era_id, o.version_label, o.created_at AS ledger_closed_at, %s
		FROM %s o
		WHERE o.ledger_sequence BETWEEN %d AND %d AND o.type = 24
		AND o.soroban_contract_id IS NOT NULL AND o.soroban_function IS NOT NULL AND o.soroban_arguments_json IS NOT NULL`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("operations_row_v2"), start, end)
}

func selectContractMetadata(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, contract_id, creator_address,
		wasm_hash, created_ledger, created_at,
		era_id, version_label, created_at AS ledger_closed_at, %s
		FROM %s
		WHERE created_ledger BETWEEN %d AND %d AND contract_id IS NOT NULL`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("contract_creations_v1"), start, end)
}

func selectTrades(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, ledger_sequence, transaction_hash, operation_index, trade_index,
		trade_type, trade_timestamp, seller_account, selling_asset_code, selling_asset_issuer, TRY_CAST(selling_amount AS BIGINT) AS selling_amount,
		buyer_account, buying_asset_code, buying_asset_issuer, TRY_CAST(buying_amount AS BIGINT) AS buying_amount,
		TRY_CAST(CASE
			WHEN price IS NULL THEN NULL
			WHEN contains(CAST(price AS VARCHAR), '/') THEN TRY_CAST(split_part(CAST(price AS VARCHAR), '/', 1) AS DOUBLE) / NULLIF(TRY_CAST(split_part(CAST(price AS VARCHAR), '/', 2) AS DOUBLE), 0)
			ELSE TRY_CAST(price AS DOUBLE)
		END AS DECIMAL(20, 7)) AS price,
		created_at, ledger_range,
		trade_timestamp AS ledger_closed_at, %s
		FROM %s WHERE ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("trades_row_v1"), start, end)
}

func selectEffects(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, ledger_sequence, transaction_hash, operation_index, effect_index,
		operation_id, effect_type, effect_type_string, account_id, amount, asset_code, asset_issuer, asset_type,
		details_json, trustline_limit, authorize_flag, clawback_flag, signer_account, signer_weight, offer_id,
		seller_account, created_at, ledger_range, created_at AS ledger_closed_at, %s
		FROM %s WHERE ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("effects_row_v1"), start, end)
}

func selectEvictedKeys(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, contract_id, key_hash, ledger_sequence, closed_at, closed_at AS created_at,
		ledger_range, closed_at AS ledger_closed_at, %s
		FROM %s WHERE ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("evicted_keys_state_v1"), start, end)
}

func selectRestoredKeys(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, contract_id, key_hash, ledger_sequence, closed_at, closed_at AS created_at,
		ledger_range, closed_at AS ledger_closed_at, %s
		FROM %s WHERE ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("restored_keys_state_v1"), start, end)
}

func selectSemanticActivities(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network,
		concat(o.transaction_hash, ':', o.operation_index::VARCHAR, ':activity') AS id,
		o.ledger_sequence, l.closed_at AS timestamp,
		CASE WHEN o.type = 24 THEN 'contract_call' WHEN o.type IN (1,2,13) THEN 'payment' ELSE coalesce(o.type_string, 'operation') END AS activity_type,
		o.type_string AS description, o.source_account, o.destination AS destination_account,
		o.soroban_contract_id AS contract_id,
		CASE WHEN o.asset IS NULL OR o.asset = 'native' THEN 'XLM' ELSE split_part(o.asset, ':', 1) END AS asset_code,
		CASE WHEN o.asset IS NULL OR o.asset = 'native' THEN NULL ELSE split_part(o.asset, ':', 2) END AS asset_issuer,
		TRY_CAST(o.amount AS DOUBLE) AS amount, o.type = 24 AS is_soroban, o.soroban_function AS soroban_function_name,
		o.transaction_hash, o.operation_index, COALESCE(t.successful, o.transaction_successful) AS successful, t.fee_charged, current_timestamp AS created_at,
		l.closed_at AS ledger_closed_at, %s
		FROM %s o LEFT JOIN %s t ON o.transaction_hash=t.transaction_hash AND o.ledger_sequence=t.ledger_sequence JOIN %s l ON o.ledger_sequence=l.sequence
		WHERE o.ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("operations_row_v2"), l.bronze("transactions_row_v2"), l.bronze("ledgers_row_v2"), start, end)
}

func selectSemanticFlowsValue(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network,
		concat(o.transaction_hash, ':', o.operation_index::VARCHAR, ':flow') AS id,
		o.ledger_sequence, l.closed_at AS timestamp, 'transfer' AS flow_type,
		o.source_account AS from_account, o.destination AS to_account, NULL::VARCHAR AS contract_id,
		CASE WHEN o.asset IS NULL OR o.asset = 'native' THEN 'XLM' ELSE split_part(o.asset, ':', 1) END AS asset_code,
		CASE WHEN o.asset IS NULL OR o.asset = 'native' THEN NULL ELSE split_part(o.asset, ':', 2) END AS asset_issuer,
		CASE WHEN o.asset IS NULL OR o.asset = 'native' THEN 'native' ELSE 'credit_alphanum' END AS asset_type,
		TRY_CAST(o.amount AS DOUBLE) AS amount, o.transaction_hash, o.type AS operation_type, COALESCE(t.successful, o.transaction_successful) AS successful,
		current_timestamp AS created_at, l.closed_at AS ledger_closed_at, %s
		FROM %s o LEFT JOIN %s t ON o.transaction_hash=t.transaction_hash AND o.ledger_sequence=t.ledger_sequence JOIN %s l ON o.ledger_sequence=l.sequence
		WHERE o.ledger_sequence BETWEEN %d AND %d AND o.type IN (1,2,13) AND COALESCE(t.successful, o.transaction_successful, true) = true AND o.amount IS NOT NULL`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("operations_row_v2"), l.bronze("transactions_row_v2"), l.bronze("ledgers_row_v2"), start, end)
}

func selectContractDataChanges(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT %s AS network, contract_id, ledger_key_hash AS key_hash, contract_key_type, contract_durability,
		asset_type, asset_code, asset_issuer, balance_holder, balance, contract_data_xdr AS data_value,
		last_modified_ledger, ledger_sequence, closed_at, deleted, ledger_range, closed_at AS ledger_closed_at, %s
		FROM %s WHERE ledger_sequence BETWEEN %d AND %d`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("contract_data_snapshot_v1"), start, end)
}

func selectBalanceChanges(l *Loader, start, end int64) string {
	return fmt.Sprintf(`SELECT * FROM (
		SELECT %s AS network, account_id AS address, 'native' AS asset_type, 'XLM' AS asset_code, NULL::VARCHAR AS asset_issuer,
			balance, ledger_sequence, closed_at AS ledger_closed_at, false AS deleted, ledger_range, %s
			FROM %s WHERE ledger_sequence BETWEEN %d AND %d
		UNION ALL
		SELECT %s AS network, account_id AS address, asset_type, asset_code, asset_issuer, balance, t.ledger_sequence,
			l.closed_at AS ledger_closed_at, false AS deleted, t.ledger_range, %s
			FROM %s t JOIN %s l ON t.ledger_sequence=l.sequence WHERE t.ledger_sequence BETWEEN %d AND %d
		)`, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("accounts_snapshot_v1"), start, end, q(l.cfg.Network), metaSelect(l, start, end), l.bronze("trustlines_snapshot_v1"), l.bronze("ledgers_row_v2"), start, end)
}

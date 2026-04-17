package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const addressBalanceStateStageTable = "temp_address_balances_state_stage"

type AddressBalanceStateStageRow struct {
	OwnerAddress      string
	AssetKey          string
	AssetTypeHint     string
	AssetCode         string
	AssetIssuer       string
	BalanceRaw        string
	LastUpdatedLedger int64
	LastUpdatedAt     time.Time
}

var addressBalanceStateStageColumns = []string{
	"owner_address",
	"asset_key",
	"asset_type_hint",
	"asset_code",
	"asset_issuer",
	"balance_raw",
	"last_updated_ledger",
	"last_updated_at",
}

func (sw *SilverWriter) ensureAddressBalanceStateStage(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS temp_address_balances_state_stage (
			owner_address TEXT NOT NULL,
			asset_key TEXT NOT NULL,
			asset_type_hint TEXT,
			asset_code TEXT,
			asset_issuer TEXT,
			balance_raw NUMERIC NOT NULL,
			last_updated_ledger BIGINT NOT NULL,
			last_updated_at TIMESTAMP NOT NULL
		) ON COMMIT DROP
	`)
	if err != nil {
		return fmt.Errorf("create address balance state stage table: %w", err)
	}
	return nil
}

func (sw *SilverWriter) mergeAddressBalanceStateStage(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO address_balances_current (
			owner_address, asset_key, asset_type, token_contract_id,
			asset_code, asset_issuer, symbol, decimals,
			balance_raw, balance_display, balance_source,
			last_updated_ledger, last_updated_at, updated_at
		)
		SELECT
			s.owner_address,
			s.asset_key,
			CASE
				WHEN s.asset_type_hint IS NULL OR s.asset_type_hint = '' THEN 'soroban_token'
				WHEN s.asset_code IS NOT NULL AND LENGTH(s.asset_code) <= 4 THEN 'credit_alphanum4'
				WHEN s.asset_code IS NOT NULL THEN 'credit_alphanum12'
				ELSE 'soroban_token'
			END AS asset_type,
			s.asset_key AS token_contract_id,
			COALESCE(s.asset_code, 'XLM') AS asset_code,
			s.asset_issuer,
			COALESCE(tr.token_symbol, s.asset_code, 'XLM') AS symbol,
			COALESCE(tr.token_decimals, 7) AS decimals,
			s.balance_raw,
			CASE
				WHEN COALESCE(tr.token_decimals, 7) = 0 THEN s.balance_raw::text
				ELSE (s.balance_raw / POWER(10::numeric, COALESCE(tr.token_decimals, 7)))::text
			END AS balance_display,
			'contract_storage_state' AS balance_source,
			s.last_updated_ledger,
			s.last_updated_at,
			NOW() AS updated_at
		FROM (
			SELECT DISTINCT ON (owner_address, asset_key)
				owner_address,
				asset_key,
				asset_type_hint,
				asset_code,
				asset_issuer,
				balance_raw,
				last_updated_ledger,
				last_updated_at
			FROM temp_address_balances_state_stage
			ORDER BY owner_address, asset_key, last_updated_ledger DESC, last_updated_at DESC
		) s
		LEFT JOIN token_registry tr ON tr.contract_id = s.asset_key
		ON CONFLICT (owner_address, asset_key) DO UPDATE SET
			balance_raw = EXCLUDED.balance_raw,
			balance_display = EXCLUDED.balance_display,
			balance_source = EXCLUDED.balance_source,
			last_updated_ledger = EXCLUDED.last_updated_ledger,
			last_updated_at = EXCLUDED.last_updated_at,
			updated_at = NOW()
		WHERE EXCLUDED.last_updated_ledger >= address_balances_current.last_updated_ledger
	`)
	if err != nil {
		return fmt.Errorf("merge address balance state stage: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `TRUNCATE temp_address_balances_state_stage`); err != nil {
		return fmt.Errorf("truncate address balance state stage: %w", err)
	}

	return nil
}

func (sw *SilverWriter) BulkUpsertAddressBalanceState(ctx context.Context, tx *sql.Tx, rows []AddressBalanceStateStageRow, batchSize int) error {
	if len(rows) == 0 {
		return nil
	}

	if err := sw.ensureAddressBalanceStateStage(ctx, tx); err != nil {
		return err
	}

	batch := NewBatchInserter(addressBalanceStateStageTable, addressBalanceStateStageColumns, "", batchSize)
	for _, row := range rows {
		if err := batch.Add(
			row.OwnerAddress,
			row.AssetKey,
			row.AssetTypeHint,
			row.AssetCode,
			row.AssetIssuer,
			row.BalanceRaw,
			row.LastUpdatedLedger,
			row.LastUpdatedAt,
		); err != nil {
			return fmt.Errorf("stage address balance state row: %w", err)
		}
	}

	if err := batch.Flush(ctx, tx); err != nil {
		return fmt.Errorf("flush address balance state stage rows: %w", err)
	}

	if err := sw.mergeAddressBalanceStateStage(ctx, tx); err != nil {
		return err
	}

	return nil
}

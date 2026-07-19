package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const addressBalanceStateStageTable = "temp_address_balances_state_stage"

type AddressBalanceStateStageRow struct {
	OwnerAddress      string
	AssetKey          string
	AssetTypeHint     string
	AssetCode         string
	AssetIssuer       string
	Symbol            string
	Decimals          sql.NullInt32
	BalanceRaw        string
	KeyHash           string
	Deleted           bool
	LastUpdatedLedger int64
	LastUpdatedAt     time.Time
}

var addressBalanceStateStageColumns = []string{
	"owner_address",
	"asset_key",
	"asset_type_hint",
	"asset_code",
	"asset_issuer",
	"symbol",
	"decimals",
	"balance_raw",
	"key_hash",
	"deleted",
	"last_updated_ledger",
	"last_updated_at",
}

// prepareAddressBalanceStateStageRow makes decoded display metadata safe for
// PostgreSQL TEXT while refusing to rewrite fields that identify a balance.
// DuckDB can carry embedded NUL bytes from fixed-width XDR strings, but
// PostgreSQL rejects them with SQLSTATE 22021.
func prepareAddressBalanceStateStageRow(row AddressBalanceStateStageRow) (AddressBalanceStateStageRow, error) {
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "owner_address", value: row.OwnerAddress},
		{name: "asset_key", value: row.AssetKey},
		{name: "balance_raw", value: row.BalanceRaw},
		{name: "key_hash", value: row.KeyHash},
	} {
		if strings.IndexByte(field.value, 0) >= 0 {
			return AddressBalanceStateStageRow{}, fmt.Errorf("%s contains an embedded NUL byte", field.name)
		}
	}

	row.AssetTypeHint = strings.ReplaceAll(row.AssetTypeHint, "\x00", "")
	row.AssetCode = strings.ReplaceAll(row.AssetCode, "\x00", "")
	row.AssetIssuer = strings.ReplaceAll(row.AssetIssuer, "\x00", "")
	row.Symbol = strings.ReplaceAll(row.Symbol, "\x00", "")
	return row, nil
}

func (sw *SilverWriter) ensureAddressBalanceStateStage(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS temp_address_balances_state_stage (
			owner_address TEXT NOT NULL,
			asset_key TEXT NOT NULL,
			asset_type_hint TEXT,
			asset_code TEXT,
			asset_issuer TEXT,
			symbol TEXT,
			decimals INTEGER,
			balance_raw NUMERIC NOT NULL,
			key_hash TEXT NOT NULL,
			deleted BOOLEAN NOT NULL,
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
	if _, err := tx.ExecContext(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (owner_address, asset_key) *
			FROM temp_address_balances_state_stage
			ORDER BY owner_address, asset_key, last_updated_ledger DESC, last_updated_at DESC
		), enriched AS (
			SELECT s.*,
				COALESCE(NULLIF(s.asset_code, ''), NULLIF(tr.asset_code, '')) AS resolved_asset_code,
				COALESCE(NULLIF(s.asset_issuer, ''), NULLIF(tr.asset_issuer, '')) AS resolved_asset_issuer,
				COALESCE(NULLIF(s.symbol, ''), NULLIF(tr.token_symbol, '')) AS resolved_symbol,
				COALESCE(s.decimals, tr.token_decimals, 7) AS resolved_decimals
			FROM latest s
			LEFT JOIN token_registry tr ON tr.contract_id = s.asset_key
		)
		INSERT INTO contract_balance_changes (
			owner_address, owner_type, asset_key, asset_type, token_contract_id,
			asset_code, asset_issuer, symbol, decimals, balance_raw,
			balance_source, key_hash, ledger_sequence, ledger_closed_at, deleted, inserted_at
		)
		SELECT
			owner_address,
			'contract',
			asset_key,
			CASE
				WHEN asset_type_hint = 'native' OR (resolved_asset_code = 'XLM' AND resolved_asset_issuer IS NULL) THEN 'native'
				WHEN resolved_asset_code IS NOT NULL AND resolved_asset_issuer IS NOT NULL AND LENGTH(resolved_asset_code) <= 4 THEN 'credit_alphanum4'
				WHEN resolved_asset_code IS NOT NULL AND resolved_asset_issuer IS NOT NULL THEN 'credit_alphanum12'
				ELSE 'soroban_token'
			END,
			asset_key,
			COALESCE(resolved_asset_code, CASE WHEN asset_type_hint = 'native' THEN 'XLM' END),
			resolved_asset_issuer,
			COALESCE(resolved_symbol, resolved_asset_code, CASE WHEN asset_type_hint = 'native' THEN 'XLM' END),
			resolved_decimals,
			balance_raw,
			'contract_storage_state',
			key_hash,
			last_updated_ledger,
			last_updated_at,
			deleted,
			NOW()
		FROM enriched
		ON CONFLICT (owner_address, asset_key, ledger_sequence) DO UPDATE SET
			asset_type = EXCLUDED.asset_type,
			asset_code = EXCLUDED.asset_code,
			asset_issuer = EXCLUDED.asset_issuer,
			symbol = EXCLUDED.symbol,
			decimals = EXCLUDED.decimals,
			balance_raw = EXCLUDED.balance_raw,
			key_hash = EXCLUDED.key_hash,
			ledger_closed_at = EXCLUDED.ledger_closed_at,
			deleted = EXCLUDED.deleted
	`); err != nil {
		return fmt.Errorf("append contract balance changes: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		WITH latest AS (
			SELECT DISTINCT ON (owner_address, asset_key)
				owner_address, asset_key, balance_raw, deleted, last_updated_ledger
			FROM temp_address_balances_state_stage
			ORDER BY owner_address, asset_key, last_updated_ledger DESC, last_updated_at DESC
		)
		DELETE FROM address_balances_current current
		USING latest
		WHERE current.owner_address = latest.owner_address
		  AND current.asset_key = latest.asset_key
		  AND (latest.deleted OR latest.balance_raw <= 0)
		  AND latest.last_updated_ledger >= COALESCE(current.last_updated_ledger, 0)
	`); err != nil {
		return fmt.Errorf("delete removed contract balance state: %w", err)
	}

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
				WHEN s.asset_type_hint = 'native' OR (COALESCE(NULLIF(s.asset_code, ''), NULLIF(tr.asset_code, '')) = 'XLM' AND COALESCE(NULLIF(s.asset_issuer, ''), NULLIF(tr.asset_issuer, '')) IS NULL) THEN 'native'
				WHEN COALESCE(NULLIF(s.asset_code, ''), NULLIF(tr.asset_code, '')) IS NOT NULL AND COALESCE(NULLIF(s.asset_issuer, ''), NULLIF(tr.asset_issuer, '')) IS NOT NULL AND LENGTH(COALESCE(NULLIF(s.asset_code, ''), NULLIF(tr.asset_code, ''))) <= 4 THEN 'credit_alphanum4'
				WHEN COALESCE(NULLIF(s.asset_code, ''), NULLIF(tr.asset_code, '')) IS NOT NULL AND COALESCE(NULLIF(s.asset_issuer, ''), NULLIF(tr.asset_issuer, '')) IS NOT NULL THEN 'credit_alphanum12'
				ELSE 'soroban_token'
			END AS asset_type,
			s.asset_key AS token_contract_id,
			COALESCE(NULLIF(s.asset_code, ''), NULLIF(tr.asset_code, ''), CASE WHEN s.asset_type_hint = 'native' THEN 'XLM' END) AS asset_code,
			COALESCE(NULLIF(s.asset_issuer, ''), NULLIF(tr.asset_issuer, '')) AS asset_issuer,
			COALESCE(NULLIF(s.symbol, ''), NULLIF(tr.token_symbol, ''), NULLIF(s.asset_code, ''), NULLIF(tr.asset_code, ''), CASE WHEN s.asset_type_hint = 'native' THEN 'XLM' END) AS symbol,
			COALESCE(s.decimals, tr.token_decimals, 7) AS decimals,
			s.balance_raw,
			CASE
				WHEN COALESCE(s.decimals, tr.token_decimals, 7) = 0 THEN s.balance_raw::text
				ELSE (s.balance_raw / POWER(10::numeric, COALESCE(s.decimals, tr.token_decimals, 7)))::text
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
				symbol,
				decimals,
				balance_raw,
				deleted,
				last_updated_ledger,
				last_updated_at
			FROM temp_address_balances_state_stage
			ORDER BY owner_address, asset_key, last_updated_ledger DESC, last_updated_at DESC
		) s
		LEFT JOIN token_registry tr ON tr.contract_id = s.asset_key
		WHERE s.deleted = false
		  AND s.balance_raw > 0
		ON CONFLICT (owner_address, asset_key) DO UPDATE SET
			asset_type = EXCLUDED.asset_type,
			token_contract_id = EXCLUDED.token_contract_id,
			asset_code = EXCLUDED.asset_code,
			asset_issuer = EXCLUDED.asset_issuer,
			symbol = EXCLUDED.symbol,
			decimals = EXCLUDED.decimals,
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
	for rowIndex, row := range rows {
		var err error
		row, err = prepareAddressBalanceStateStageRow(row)
		if err != nil {
			return fmt.Errorf("validate address balance state stage row %d: %w", rowIndex, err)
		}
		if err := batch.Add(
			row.OwnerAddress,
			row.AssetKey,
			row.AssetTypeHint,
			row.AssetCode,
			row.AssetIssuer,
			row.Symbol,
			row.Decimals,
			row.BalanceRaw,
			row.KeyHash,
			row.Deleted,
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

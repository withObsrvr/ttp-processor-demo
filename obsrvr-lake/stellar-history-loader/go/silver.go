package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// SilverTransformer runs DuckDB SQL transforms over bronze Parquet to produce silver tables.
type SilverTransformer struct {
	bronzeDir string
	silverDir string
	db        *sql.DB
}

func NewSilverTransformer(outputDir string) (*SilverTransformer, error) {
	bronzeDir := filepath.Join(outputDir, "bronze")
	silverDir := filepath.Join(outputDir, "silver")
	if err := os.MkdirAll(silverDir, 0o755); err != nil {
		return nil, fmt.Errorf("create silver dir: %w", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	return &SilverTransformer{
		bronzeDir: bronzeDir,
		silverDir: silverDir,
		db:        db,
	}, nil
}

func (st *SilverTransformer) Close() error {
	return st.db.Close()
}

// RunAll executes all silver transforms in order.
func (st *SilverTransformer) RunAll(ctx context.Context) error {
	transforms := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"accounts_current", st.deriveAccountsCurrent},
		{"trustlines_current", st.deriveTrustlinesCurrent},
		{"offers_current", st.deriveOffersCurrent},
		{"native_balances_current", st.deriveNativeBalancesCurrent},
		{"token_registry", st.deriveTokenRegistry},
		{"semantic_entities_contracts", st.deriveSemanticEntitiesContracts},
	}

	for _, t := range transforms {
		start := time.Now()
		log.Printf("[Silver] Running %s...", t.name)
		if err := t.fn(ctx); err != nil {
			log.Printf("[Silver] %s failed: %v", t.name, err)
			// Continue with other transforms
			continue
		}
		log.Printf("[Silver] %s completed in %s", t.name, time.Since(start).Round(time.Millisecond))
	}

	return nil
}

// bronzePath returns the glob path for a bronze table's Parquet files.
func (st *SilverTransformer) bronzePath(table string) string {
	return filepath.Join(st.bronzeDir, table, "**", "*.parquet")
}

// silverPath returns the output path for a silver table Parquet file.
func (st *SilverTransformer) silverPath(table string) string {
	return filepath.Join(st.silverDir, table+".parquet")
}

// execCopyToParquet runs a SELECT query and writes results to a Parquet file.
func (st *SilverTransformer) execCopyToParquet(ctx context.Context, query, outputPath string) (int64, error) {
	copySQL := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION SNAPPY)", query, outputPath)
	result, err := st.db.ExecContext(ctx, copySQL)
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return rows, nil
}

// --- Current-State Derivation ---
// For batch mode: take the latest snapshot per entity using ROW_NUMBER window function.

func (st *SilverTransformer) deriveAccountsCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		SELECT account_id, closed_at, balance, sequence_number,
			num_subentries, num_sponsoring, num_sponsored, home_domain,
			master_weight, low_threshold, med_threshold, high_threshold,
			flags, auth_required, auth_revocable, auth_immutable, auth_clawback_enabled,
			signers, sponsor_account, ledger_range, ledger_sequence
		FROM (
			SELECT *,
				ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) AS rn
			FROM read_parquet('%s')
		) WHERE rn = 1
	`, st.bronzePath("accounts_snapshot"))

	rows, err := st.execCopyToParquet(ctx, query, st.silverPath("accounts_current"))
	if err != nil {
		return err
	}
	log.Printf("[Silver]   accounts_current: %d rows", rows)
	return nil
}

func (st *SilverTransformer) deriveTrustlinesCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		SELECT account_id, asset_code, asset_issuer, asset_type,
			balance, trust_limit, buying_liabilities, selling_liabilities,
			authorized, authorized_to_maintain_liabilities, clawback_enabled,
			ledger_sequence, ledger_range
		FROM (
			SELECT *,
				ROW_NUMBER() OVER (
					PARTITION BY account_id, asset_type, asset_code, asset_issuer
					ORDER BY ledger_sequence DESC
				) AS rn
			FROM read_parquet('%s')
		) WHERE rn = 1
	`, st.bronzePath("trustlines_snapshot"))

	rows, err := st.execCopyToParquet(ctx, query, st.silverPath("trustlines_current"))
	if err != nil {
		return err
	}
	log.Printf("[Silver]   trustlines_current: %d rows", rows)
	return nil
}

func (st *SilverTransformer) deriveOffersCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		SELECT offer_id, seller_account, closed_at,
			selling_asset_type, selling_asset_code, selling_asset_issuer,
			buying_asset_type, buying_asset_code, buying_asset_issuer,
			amount, price, flags, ledger_range, ledger_sequence
		FROM (
			SELECT *,
				ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY ledger_sequence DESC) AS rn
			FROM read_parquet('%s')
		) WHERE rn = 1
	`, st.bronzePath("offers_snapshot"))

	rows, err := st.execCopyToParquet(ctx, query, st.silverPath("offers_current"))
	if err != nil {
		return err
	}
	log.Printf("[Silver]   offers_current: %d rows", rows)
	return nil
}

func (st *SilverTransformer) deriveNativeBalancesCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		SELECT account_id, balance, buying_liabilities, selling_liabilities,
			num_subentries, num_sponsoring, num_sponsored, sequence_number,
			last_modified_ledger, ledger_sequence, ledger_range
		FROM (
			SELECT *,
				ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY ledger_sequence DESC) AS rn
			FROM read_parquet('%s')
		) WHERE rn = 1
	`, st.bronzePath("native_balances"))

	rows, err := st.execCopyToParquet(ctx, query, st.silverPath("native_balances_current"))
	if err != nil {
		return err
	}
	log.Printf("[Silver]   native_balances_current: %d rows", rows)
	return nil
}

// --- Token Registry ---
// In batch mode: compute correct first-seen values using window functions.

func (st *SilverTransformer) deriveTokenRegistry(ctx context.Context) error {
	query := fmt.Sprintf(`
		SELECT
			contract_id,
			FIRST_VALUE(token_name IGNORE NULLS) OVER (
				PARTITION BY contract_id ORDER BY ledger_sequence
			) AS token_name,
			FIRST_VALUE(token_symbol IGNORE NULLS) OVER (
				PARTITION BY contract_id ORDER BY ledger_sequence
			) AS token_symbol,
			FIRST_VALUE(token_decimals IGNORE NULLS) OVER (
				PARTITION BY contract_id ORDER BY ledger_sequence
			) AS token_decimals,
			FIRST_VALUE(asset_code IGNORE NULLS) OVER (
				PARTITION BY contract_id ORDER BY ledger_sequence
			) AS asset_code,
			FIRST_VALUE(asset_issuer IGNORE NULLS) OVER (
				PARTITION BY contract_id ORDER BY ledger_sequence
			) AS asset_issuer,
			CASE WHEN asset_code IS NOT NULL THEN 'sac' ELSE 'custom_soroban' END AS token_type,
			MIN(ledger_sequence) OVER (PARTITION BY contract_id) AS first_seen_ledger,
			MAX(ledger_sequence) OVER (PARTITION BY contract_id) AS last_updated_ledger
		FROM read_parquet('%s')
		WHERE token_name IS NOT NULL OR token_symbol IS NOT NULL OR asset_code IS NOT NULL
		QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY ledger_sequence DESC) = 1
	`, st.bronzePath("contract_data_snapshot"))

	rows, err := st.execCopyToParquet(ctx, query, st.silverPath("token_registry"))
	if err != nil {
		return err
	}
	log.Printf("[Silver]   token_registry: %d rows", rows)
	return nil
}

// --- Semantic Entities Contracts ---
// In batch mode: aggregate from scratch over complete data (no ordering issues).

func (st *SilverTransformer) deriveSemanticEntitiesContracts(ctx context.Context) error {
	query := fmt.Sprintf(`
		SELECT
			contract_id,
			COUNT(*) AS total_events,
			COUNT(DISTINCT transaction_hash) AS unique_transactions,
			MAX(closed_at) AS last_activity,
			MIN(closed_at) AS first_seen,
			LIST(DISTINCT topic0_decoded) FILTER (WHERE topic0_decoded IS NOT NULL) AS observed_functions
		FROM read_parquet('%s')
		WHERE contract_id IS NOT NULL
		GROUP BY contract_id
	`, st.bronzePath("contract_events"))

	rows, err := st.execCopyToParquet(ctx, query, st.silverPath("semantic_entities_contracts"))
	if err != nil {
		return err
	}
	log.Printf("[Silver]   semantic_entities_contracts: %d rows", rows)
	return nil
}

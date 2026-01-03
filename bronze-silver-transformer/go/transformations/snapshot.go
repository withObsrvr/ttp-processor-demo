package transformations

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// SnapshotTransformer handles SCD Type 2 snapshot transformations
type SnapshotTransformer struct {
	db             *sql.DB
	catalogName    string
	bronzeSchema   string
	silverSchema   string
	silverDataPath string
}

// NewSnapshotTransformer creates a new snapshot transformer
func NewSnapshotTransformer(db *sql.DB, catalogName, bronzeSchema, silverSchema, silverDataPath string) *SnapshotTransformer {
	return &SnapshotTransformer{
		db:             db,
		catalogName:    catalogName,
		bronzeSchema:   bronzeSchema,
		silverSchema:   silverSchema,
		silverDataPath: silverDataPath,
	}
}

// TransformAll runs all 5 snapshot transformations with SCD Type 2
func (t *SnapshotTransformer) TransformAll(ctx context.Context, startLedger, endLedger int64) error {
	log.Printf("🔄 Transforming snapshot tables with SCD Type 2 (ledgers %d to %d)...", startLedger, endLedger)

	transformations := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"accounts_snapshot", t.transformAccountsSnapshot},
		{"trustlines_snapshot", t.transformTrustlinesSnapshot},
		{"liquidity_pools_snapshot", t.transformLiquidityPoolsSnapshot},
		{"contract_data_snapshot", t.transformContractDataSnapshot},
		{"evicted_keys_snapshot", t.transformEvictedKeysSnapshot},
	}

	for _, tf := range transformations {
		log.Printf("  → Transforming %s...", tf.name)
		if err := tf.fn(ctx); err != nil {
			return fmt.Errorf("failed to transform %s: %w", tf.name, err)
		}
		log.Printf("  ✅ %s complete", tf.name)
	}

	log.Printf("✅ All snapshot tables transformed")
	return nil
}

// transformAccountsSnapshot creates accounts_snapshot with SCD Type 2 tracking
func (t *SnapshotTransformer) transformAccountsSnapshot(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.accounts_snapshot AS
		SELECT
			*,
			closed_at AS valid_from,
			LEAD(closed_at) OVER (
				PARTITION BY account_id
				ORDER BY ledger_sequence
			) AS valid_to
		FROM %s.%s.accounts_snapshot_v1
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformTrustlinesSnapshot creates trustlines_snapshot with SCD Type 2 tracking
func (t *SnapshotTransformer) transformTrustlinesSnapshot(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.trustlines_snapshot AS
		SELECT
			*,
			created_at AS valid_from,
			LEAD(created_at) OVER (
				PARTITION BY account_id, asset_code, asset_issuer
				ORDER BY ledger_sequence
			) AS valid_to
		FROM %s.%s.trustlines_snapshot_v1
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformLiquidityPoolsSnapshot creates liquidity_pools_snapshot with SCD Type 2 tracking
func (t *SnapshotTransformer) transformLiquidityPoolsSnapshot(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.liquidity_pools_snapshot AS
		SELECT
			*,
			created_at AS valid_from,
			LEAD(created_at) OVER (
				PARTITION BY liquidity_pool_id
				ORDER BY ledger_sequence
			) AS valid_to
		FROM %s.%s.liquidity_pools_snapshot_v1
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformContractDataSnapshot creates contract_data_snapshot with SCD Type 2 tracking (Soroban)
func (t *SnapshotTransformer) transformContractDataSnapshot(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.contract_data_snapshot AS
		SELECT
			*,
			created_at AS valid_from,
			LEAD(created_at) OVER (
				PARTITION BY contract_id, contract_key_type
				ORDER BY ledger_sequence
			) AS valid_to
		FROM %s.%s.contract_data_snapshot_v1
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformEvictedKeysSnapshot creates evicted_keys_snapshot with SCD Type 2 tracking (Soroban)
func (t *SnapshotTransformer) transformEvictedKeysSnapshot(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.evicted_keys_snapshot AS
		SELECT
			*,
			created_at AS valid_from,
			LEAD(created_at) OVER (
				PARTITION BY key_hash
				ORDER BY ledger_sequence
			) AS valid_to
		FROM %s.%s.evicted_keys_state_v1
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

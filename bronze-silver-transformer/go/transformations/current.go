package transformations

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

// CurrentStateTransformer handles transformations for *_current tables
type CurrentStateTransformer struct {
	db          *sql.DB
	catalogName string
	bronzeSchema string
	silverSchema string
	silverDataPath string
}

// NewCurrentStateTransformer creates a new current state transformer
func NewCurrentStateTransformer(db *sql.DB, catalogName, bronzeSchema, silverSchema, silverDataPath string) *CurrentStateTransformer {
	return &CurrentStateTransformer{
		db:             db,
		catalogName:    catalogName,
		bronzeSchema:   bronzeSchema,
		silverSchema:   silverSchema,
		silverDataPath: silverDataPath,
	}
}

// TransformAll runs all 10 current state transformations
func (t *CurrentStateTransformer) TransformAll(ctx context.Context, startLedger, endLedger int64) error {
	log.Printf("🔄 Transforming current state tables (ledgers %d to %d)...", startLedger, endLedger)

	transformations := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"accounts_current", t.transformAccountsCurrent},
		{"account_signers_current", t.transformAccountSignersCurrent},
		{"trustlines_current", t.transformTrustlinesCurrent},
		{"offers_current", t.transformOffersCurrent},
		{"claimable_balances_current", t.transformClaimableBalancesCurrent},
		{"liquidity_pools_current", t.transformLiquidityPoolsCurrent},
		{"contract_data_current", t.transformContractDataCurrent},
		{"contract_code_current", t.transformContractCodeCurrent},
		{"config_settings_current", t.transformConfigSettingsCurrent},
		{"ttl_current", t.transformTTLCurrent},
	}

	for _, tf := range transformations {
		log.Printf("  → Transforming %s...", tf.name)
		if err := tf.fn(ctx); err != nil {
			return fmt.Errorf("failed to transform %s: %w", tf.name, err)
		}
		log.Printf("  ✅ %s complete", tf.name)
	}

	log.Printf("✅ All current state tables transformed")
	return nil
}

// transformAccountsCurrent creates accounts_current from accounts_snapshot_v1
func (t *CurrentStateTransformer) transformAccountsCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.accounts_current AS
		SELECT *
		FROM %s.%s.accounts_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformAccountSignersCurrent creates account_signers_current
func (t *CurrentStateTransformer) transformAccountSignersCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.account_signers_current AS
		SELECT *
		FROM %s.%s.account_signers_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformTrustlinesCurrent creates trustlines_current
func (t *CurrentStateTransformer) transformTrustlinesCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.trustlines_current AS
		SELECT *
		FROM %s.%s.trustlines_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformOffersCurrent creates offers_current
func (t *CurrentStateTransformer) transformOffersCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.offers_current AS
		SELECT *
		FROM %s.%s.offers_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformClaimableBalancesCurrent creates claimable_balances_current
func (t *CurrentStateTransformer) transformClaimableBalancesCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.claimable_balances_current AS
		SELECT *
		FROM %s.%s.claimable_balances_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformLiquidityPoolsCurrent creates liquidity_pools_current
func (t *CurrentStateTransformer) transformLiquidityPoolsCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.liquidity_pools_current AS
		SELECT *
		FROM %s.%s.liquidity_pools_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformContractDataCurrent creates contract_data_current (Soroban)
func (t *CurrentStateTransformer) transformContractDataCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.contract_data_current AS
		SELECT *
		FROM %s.%s.contract_data_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformContractCodeCurrent creates contract_code_current (Soroban)
func (t *CurrentStateTransformer) transformContractCodeCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.contract_code_current AS
		SELECT *
		FROM %s.%s.contract_code_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformConfigSettingsCurrent creates config_settings_current (Protocol 20+)
func (t *CurrentStateTransformer) transformConfigSettingsCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.config_settings_current AS
		SELECT *
		FROM %s.%s.config_settings_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

// transformTTLCurrent creates ttl_current (Soroban TTL - Time To Live)
func (t *CurrentStateTransformer) transformTTLCurrent(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE TABLE %s.%s.ttl_current AS
		SELECT *
		FROM %s.%s.ttl_snapshot_v1
		WHERE ledger_sequence = (SELECT MAX(sequence) FROM %s.%s.ledgers_row_v2)
	`, t.catalogName, t.silverSchema,
		t.catalogName, t.bronzeSchema,
		t.catalogName, t.bronzeSchema)

	_, err := t.db.ExecContext(ctx, query)
	return err
}

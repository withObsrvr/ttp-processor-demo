package main

import (
	"database/sql"
	"fmt"
	"log"
)

// InitSilverSchema creates all Hubble-compatible silver layer tables
func InitSilverSchema(db *sql.DB) error {
	log.Println("📋 Initializing Silver schema (Hubble-compatible)...")

	// Phase 1: Current State Tables (Query API pattern)
	log.Println("Creating current state table schemas...")

	if err := createAccountsCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create accounts_current: %w", err)
	}

	if err := createAccountSignersCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create account_signers_current: %w", err)
	}

	if err := createTrustlinesCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create trustlines_current: %w", err)
	}

	if err := createOffersCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create offers_current: %w", err)
	}

	if err := createClaimableBalancesCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create claimable_balances_current: %w", err)
	}

	if err := createLiquidityPoolsCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create liquidity_pools_current: %w", err)
	}

	if err := createContractDataCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create contract_data_current: %w", err)
	}

	if err := createContractCodeCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create contract_code_current: %w", err)
	}

	if err := createConfigSettingsCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create config_settings_current: %w", err)
	}

	if err := createTTLCurrentTable(db); err != nil {
		return fmt.Errorf("failed to create ttl_current: %w", err)
	}

	// TODO: Add 5 snapshot tables
	// TODO: Add enriched_history_operations

	log.Println("✅ Silver schema ready (10 current state tables)")
	return nil
}

// createEnrichedHistoryOperationsTable creates the Hubble-standard enriched_history_operations table
func createEnrichedHistoryOperationsTable(db *sql.DB) error {
	// Field mapping from bronze (operations_row_v2) to silver (enriched_history_operations):
	// This table denormalizes transaction and ledger data into operations
	// for easier querying

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS enriched_history_operations (
			id BIGINT PRIMARY KEY,
			transaction_id VARCHAR NOT NULL,
			transaction_hash VARCHAR NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			application_order INTEGER NOT NULL,
			type INTEGER NOT NULL,
			type_string VARCHAR NOT NULL,
			details VARCHAR,
			source_account VARCHAR,
			source_account_muxed VARCHAR,
			-- Denormalized transaction fields
			tx_successful BOOLEAN NOT NULL,
			tx_application_order INTEGER NOT NULL,
			tx_account VARCHAR NOT NULL,
			tx_account_sequence BIGINT NOT NULL,
			tx_max_fee BIGINT NOT NULL,
			tx_fee_charged BIGINT NOT NULL,
			tx_operation_count INTEGER NOT NULL,
			tx_created_at TIMESTAMP NOT NULL,
			tx_memo_type VARCHAR,
			tx_memo VARCHAR,
			tx_time_bounds VARCHAR,
			tx_signatures VARCHAR,
			tx_fee_account VARCHAR,
			-- Denormalized ledger fields
			ledger_closed_at TIMESTAMP NOT NULL,
			ledger_successful_transaction_count INTEGER,
			ledger_failed_transaction_count INTEGER,
			ledger_operation_count INTEGER,
			ledger_tx_set_operation_count INTEGER,
			-- Metadata
			is_payment BOOLEAN DEFAULT false,
			imported BOOLEAN DEFAULT false,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			batch_insert_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			closed_at_date DATE GENERATED ALWAYS AS (CAST(tx_created_at AS DATE))
		)
	`)

	return err
}

// ============================================================================
// PHASE 1: CURRENT STATE TABLES (10 tables)
// These tables show the latest state only by filtering for max(closed_at)
// ============================================================================

func createAccountsCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching accounts_snapshot_v1
	// Will be populated via Query API
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS accounts_current (
			account_id VARCHAR PRIMARY KEY,
			balance BIGINT,
			buying_liabilities BIGINT,
			selling_liabilities BIGINT,
			sequence_number BIGINT,
			num_subentries INTEGER,
			num_sponsoring INTEGER,
			num_sponsored INTEGER,
			flags INTEGER,
			home_domain VARCHAR,
			master_weight INTEGER,
			threshold_low INTEGER,
			threshold_medium INTEGER,
			threshold_high INTEGER,
			last_modified_ledger BIGINT,
			ledger_entry_change INTEGER,
			deleted BOOLEAN,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP,
			closed_at TIMESTAMP,
			ledger_sequence BIGINT
		)
	`)
	return err
}

func createAccountSignersCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching account_signers_snapshot_v1
	// Will be populated via Query API
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS account_signers_current (
			account_id VARCHAR,
			signer VARCHAR,
			ledger_sequence BIGINT,
			weight INTEGER,
			sponsor VARCHAR,
			deleted BOOLEAN,
			closed_at TIMESTAMP,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP,
			PRIMARY KEY (account_id, signer)
		)
	`)
	return err
}

func createTrustlinesCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching trustlines_snapshot_v1
	// Will be populated via Query API
	// Note: Trustlines have buying/selling liabilities (unlike accounts)
	// Note: Trustlines use ledger_sequence (no closed_at column)
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS trustlines_current (
			account_id VARCHAR,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			asset_type INTEGER,
			balance BIGINT,
			trust_limit BIGINT,
			buying_liabilities BIGINT,
			selling_liabilities BIGINT,
			authorized BOOLEAN,
			authorized_to_maintain_liabilities BOOLEAN,
			clawback_enabled BOOLEAN,
			ledger_sequence BIGINT,
			flags INTEGER,
			last_modified_ledger BIGINT,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP,
			PRIMARY KEY (account_id, asset_code, asset_issuer)
		)
	`)
	return err
}

func createOffersCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching offers_snapshot_v1
	// Will be populated via Query API
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS offers_current (
			offer_id BIGINT PRIMARY KEY,
			seller_account VARCHAR,
			ledger_sequence BIGINT,
			closed_at TIMESTAMP,
			selling_asset_type INTEGER,
			selling_asset_code VARCHAR,
			selling_asset_issuer VARCHAR,
			buying_asset_type INTEGER,
			buying_asset_code VARCHAR,
			buying_asset_issuer VARCHAR,
			amount BIGINT,
			price VARCHAR,
			flags INTEGER,
			sponsor VARCHAR,
			last_modified_ledger BIGINT,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP
		)
	`)
	return err
}

func createClaimableBalancesCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching claimable_balances_snapshot_v1
	// Will be populated via Query API
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS claimable_balances_current (
			balance_id VARCHAR PRIMARY KEY,
			sponsor VARCHAR,
			ledger_sequence BIGINT,
			closed_at TIMESTAMP,
			asset_type INTEGER,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			amount BIGINT,
			claimants_count INTEGER,
			flags INTEGER,
			claimants VARCHAR,
			last_modified_ledger BIGINT,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP
		)
	`)
	return err
}

func createLiquidityPoolsCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching liquidity_pools_snapshot_v1
	// Will be populated via Query API
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS liquidity_pools_current (
			liquidity_pool_id VARCHAR PRIMARY KEY,
			ledger_sequence BIGINT,
			closed_at TIMESTAMP,
			pool_type INTEGER,
			fee INTEGER,
			trustline_count BIGINT,
			total_pool_shares BIGINT,
			asset_a_type INTEGER,
			asset_a_code VARCHAR,
			asset_a_issuer VARCHAR,
			asset_a_amount BIGINT,
			asset_b_type INTEGER,
			asset_b_code VARCHAR,
			asset_b_issuer VARCHAR,
			asset_b_amount BIGINT,
			last_modified_ledger BIGINT,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP
		)
	`)
	return err
}

func createContractDataCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching contract_data_snapshot_v1
	// Will be populated via Query API
	// Note: Includes SAC denormalized fields and must filter deleted = FALSE
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS contract_data_current (
			contract_id VARCHAR,
			ledger_sequence BIGINT,
			ledger_key_hash VARCHAR,
			contract_key_type INTEGER,
			contract_durability INTEGER,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			asset_type INTEGER,
			balance_holder VARCHAR,
			balance BIGINT,
			last_modified_ledger BIGINT,
			ledger_entry_change INTEGER,
			deleted BOOLEAN,
			closed_at TIMESTAMP,
			contract_data_xdr VARCHAR,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP,
			PRIMARY KEY (contract_id, ledger_key_hash)
		)
	`)
	return err
}

func createContractCodeCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching contract_code_snapshot_v1
	// Will be populated via Query API
	// Note: Includes rich WASM metadata fields
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS contract_code_current (
			contract_code_hash VARCHAR PRIMARY KEY,
			ledger_key_hash VARCHAR,
			contract_code_ext_v INTEGER,
			last_modified_ledger BIGINT,
			ledger_entry_change INTEGER,
			deleted BOOLEAN,
			closed_at TIMESTAMP,
			ledger_sequence BIGINT,
			n_instructions BIGINT,
			n_functions BIGINT,
			n_globals BIGINT,
			n_table_entries BIGINT,
			n_types BIGINT,
			n_data_segments BIGINT,
			n_elem_segments BIGINT,
			n_imports BIGINT,
			n_exports BIGINT,
			n_data_segment_bytes BIGINT,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP
		)
	`)
	return err
}

func createConfigSettingsCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching config_settings_snapshot_v1
	// Will be populated via Query API
	// Note: Very stable table (protocol limits don't change often)
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS config_settings_current (
			config_setting_id INTEGER PRIMARY KEY,
			ledger_sequence BIGINT,
			last_modified_ledger BIGINT,
			deleted BOOLEAN,
			closed_at TIMESTAMP,
			ledger_max_instructions BIGINT,
			tx_max_instructions BIGINT,
			fee_rate_per_instructions_increment BIGINT,
			tx_memory_limit BIGINT,
			ledger_max_read_ledger_entries BIGINT,
			ledger_max_read_bytes BIGINT,
			ledger_max_write_ledger_entries BIGINT,
			ledger_max_write_bytes BIGINT,
			tx_max_read_ledger_entries BIGINT,
			tx_max_read_bytes BIGINT,
			tx_max_write_ledger_entries BIGINT,
			tx_max_write_bytes BIGINT,
			contract_max_size_bytes BIGINT,
			config_setting_xdr VARCHAR,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP
		)
	`)
	return err
}

func createTTLCurrentTable(db *sql.DB) error {
	// Create empty table with schema matching ttl_snapshot_v1
	// Will be populated via Query API
	// Note: Only includes active (non-deleted, non-expired) TTL entries
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS ttl_current (
			key_hash VARCHAR PRIMARY KEY,
			ledger_sequence BIGINT,
			live_until_ledger_seq BIGINT,
			ttl_remaining BIGINT,
			expired BOOLEAN,
			last_modified_ledger BIGINT,
			deleted BOOLEAN,
			closed_at TIMESTAMP,
			batch_id VARCHAR,
			batch_run_date TIMESTAMP,
			batch_insert_ts TIMESTAMP
		)
	`)
	return err
}

// ============================================================================
// PHASE 2: HISTORICAL SNAPSHOT TABLES WITH SCD TYPE 2 (5 tables)
// These tables track state changes over time with valid_from/valid_to timestamps
// ============================================================================

func createAccountsSnapshotTable(db *sql.DB) error {
	// Add SCD Type 2 tracking with valid_from and valid_to timestamps
	_, err := db.Exec(`
		CREATE OR REPLACE TABLE accounts_snapshot AS
		SELECT
			*,
			closed_at AS valid_from,
			LEAD(closed_at) OVER (
				PARTITION BY account_id
				ORDER BY closed_at
			) AS valid_to
		FROM bronze.testnet.accounts_snapshot_v1
	`)
	return err
}

func createTrustlinesSnapshotTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE OR REPLACE TABLE trustlines_snapshot AS
		SELECT
			*,
			closed_at AS valid_from,
			LEAD(closed_at) OVER (
				PARTITION BY account_id, asset_code, asset_issuer
				ORDER BY closed_at
			) AS valid_to
		FROM bronze.testnet.trustlines_snapshot_v1
	`)
	return err
}

func createLiquidityPoolsSnapshotTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE OR REPLACE TABLE liquidity_pools_snapshot AS
		SELECT
			*,
			closed_at AS valid_from,
			LEAD(closed_at) OVER (
				PARTITION BY liquidity_pool_id
				ORDER BY closed_at
			) AS valid_to
		FROM bronze.testnet.liquidity_pools_snapshot_v1
	`)
	return err
}

func createContractDataSnapshotTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE OR REPLACE TABLE contract_data_snapshot AS
		SELECT
			*,
			closed_at AS valid_from,
			LEAD(closed_at) OVER (
				PARTITION BY contract_id, contract_key_type
				ORDER BY closed_at
			) AS valid_to
		FROM bronze.testnet.contract_data_snapshot_v1
	`)
	return err
}

func createEvictedKeysSnapshotTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE OR REPLACE TABLE evicted_keys_snapshot AS
		SELECT
			*,
			closed_at AS valid_from,
			LEAD(closed_at) OVER (
				PARTITION BY contract_id, contract_key
				ORDER BY closed_at
			) AS valid_to
		FROM bronze.testnet.evicted_keys_state_v1
	`)
	return err
}

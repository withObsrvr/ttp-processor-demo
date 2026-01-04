-- PostgreSQL schema from V3 (UNLOGGED tables for hot buffer)

-- account_signers_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS account_signers_snapshot_v1 (
			-- Identity (3 fields)
			account_id TEXT NOT NULL,
			signer TEXT NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- Signer details (2 fields)
			weight INTEGER NOT NULL,
			sponsor TEXT,

			-- Status (1 field)
			deleted BOOLEAN NOT NULL,

			-- Metadata (3 fields)
			closed_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- accounts_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS accounts_snapshot_v1 (
			-- Identity (3 fields) - PK
			account_id TEXT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,

			-- Balance (1 field)
			balance TEXT NOT NULL,

			-- Account Settings (5 fields)
			sequence_number BIGINT NOT NULL,
			num_subentries INT NOT NULL,
			num_sponsoring INT NOT NULL,
			num_sponsored INT NOT NULL,
			home_domain TEXT,

			-- Thresholds (4 fields)
			master_weight INT NOT NULL,
			low_threshold INT NOT NULL,
			med_threshold INT NOT NULL,
			high_threshold INT NOT NULL,

			-- Flags (5 fields)
			flags INT NOT NULL,
			auth_required BOOLEAN NOT NULL,
			auth_revocable BOOLEAN NOT NULL,
			auth_immutable BOOLEAN NOT NULL,
			auth_clawback_enabled BOOLEAN NOT NULL,

			-- Signers (1 field) - JSON array
			signers TEXT,

			-- Sponsorship (1 field)
			sponsor_account TEXT,

			-- Metadata (3 fields)
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- claimable_balances_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS claimable_balances_snapshot_v1 (
			-- Identity (4 fields)
			balance_id TEXT NOT NULL,
			sponsor TEXT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,

			-- Asset & Amount (4 fields)
			asset_type TEXT NOT NULL,
			asset_code TEXT,
			asset_issuer TEXT,
			amount BIGINT NOT NULL,

			-- Claimants (1 field)
			claimants_count INT NOT NULL,

			-- Flags (1 field)
			flags INT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- config_settings_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS config_settings_snapshot_v1 (
			-- Identity (2 fields)
			config_setting_id INT NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- Ledger metadata (3 fields)
			last_modified_ledger INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,

			-- Soroban compute settings (4 fields, nullable)
			ledger_max_instructions BIGINT,
			tx_max_instructions BIGINT,
			fee_rate_per_instructions_increment BIGINT,
			tx_memory_limit BIGINT,

			-- Soroban ledger cost settings (8 fields, nullable)
			ledger_max_read_ledger_entries BIGINT,
			ledger_max_read_bytes BIGINT,
			ledger_max_write_ledger_entries BIGINT,
			ledger_max_write_bytes BIGINT,
			tx_max_read_ledger_entries BIGINT,
			tx_max_read_bytes BIGINT,
			tx_max_write_ledger_entries BIGINT,
			tx_max_write_bytes BIGINT,

			-- Contract size limit (1 field, nullable)
			contract_max_size_bytes BIGINT,

			-- Raw XDR (1 field)
			config_setting_xdr TEXT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- contract_code_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS contract_code_snapshot_v1 (
			-- Identity (2 fields)
			contract_code_hash TEXT NOT NULL,
			ledger_key_hash TEXT NOT NULL,

			-- Extension (1 field)
			contract_code_ext_v INT NOT NULL,

			-- Ledger metadata (4 fields)
			last_modified_ledger INT NOT NULL,
			ledger_entry_change INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,

			-- Ledger tracking (1 field)
			ledger_sequence BIGINT NOT NULL,

			-- WASM metadata (10 fields, nullable)
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

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- contract_data_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS contract_data_snapshot_v1 (
			-- Identity (3 fields)
			contract_id TEXT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			ledger_key_hash TEXT NOT NULL,

			-- Contract metadata (2 fields)
			contract_key_type TEXT NOT NULL,
			contract_durability TEXT NOT NULL,

			-- Asset information (3 fields, nullable)
			asset_code TEXT,
			asset_issuer TEXT,
			asset_type TEXT,

			-- Balance information (2 fields, nullable)
			balance_holder TEXT,
			balance TEXT,

			-- Ledger metadata (4 fields)
			last_modified_ledger INT NOT NULL,
			ledger_entry_change INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,

			-- XDR data (1 field)
			contract_data_xdr TEXT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- contract_events_stream_v1
CREATE UNLOGGED TABLE IF NOT EXISTS contract_events_stream_v1 (
			-- Identity (5 fields)
			event_id TEXT NOT NULL,
			contract_id TEXT,
			ledger_sequence BIGINT NOT NULL,
			transaction_hash TEXT NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,

			-- Event Type (2 fields)
			event_type TEXT NOT NULL,
			in_successful_contract_call BOOLEAN NOT NULL,

			-- Event Data (5 fields - Hubble compatible with decoded versions)
			topics_json TEXT NOT NULL,
			topics_decoded TEXT NOT NULL,
			data_xdr TEXT NOT NULL,
			data_decoded TEXT NOT NULL,
			topic_count INT NOT NULL,

			-- Context (2 fields)
			operation_index INT NOT NULL,
			event_index INT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- effects_row_v1
CREATE UNLOGGED TABLE IF NOT EXISTS effects_row_v1 (
			-- Identity (4 fields)
			ledger_sequence BIGINT NOT NULL,
			transaction_hash TEXT NOT NULL,
			operation_index INT NOT NULL,
			effect_index INT NOT NULL,

			-- Effect type (2 fields)
			effect_type INT NOT NULL,
			effect_type_string TEXT NOT NULL,

			-- Account affected (1 field)
			account_id TEXT,

			-- Amount changes (4 fields) - Nullable
			amount TEXT,
			asset_code TEXT,
			asset_issuer TEXT,
			asset_type TEXT,

			-- Trustline effects (3 fields) - Nullable
			trustline_limit TEXT,
			authorize_flag BOOLEAN,
			clawback_flag BOOLEAN,

			-- Signer effects (2 fields) - Nullable
			signer_account TEXT,
			signer_weight INT,

			-- Offer effects (2 fields) - Nullable
			offer_id BIGINT,
			seller_account TEXT,

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- evicted_keys_state_v1
CREATE UNLOGGED TABLE IF NOT EXISTS evicted_keys_state_v1 (
			-- Identity (2 fields)
			key_hash TEXT NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- Eviction details (3 fields)
			contract_id TEXT NOT NULL,
			key_type TEXT NOT NULL,
			durability TEXT NOT NULL,

			-- Metadata (3 fields)
			closed_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- ledgers_row_v2
CREATE UNLOGGED TABLE IF NOT EXISTS ledgers_row_v2 (
			sequence BIGINT NOT NULL,
			ledger_hash TEXT NOT NULL,
			previous_ledger_hash TEXT NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,
			protocol_version INT NOT NULL,
			total_coins BIGINT NOT NULL,
			fee_pool BIGINT NOT NULL,
			base_fee INT NOT NULL,
			base_reserve INT NOT NULL,
			max_tx_set_size INT NOT NULL,
			successful_tx_count INT NOT NULL,
			failed_tx_count INT NOT NULL,
			ingestion_timestamp TIMESTAMPTZ,
			ledger_range BIGINT,

			-- NEW: Operation counts (stellar-etl alignment)
			transaction_count INT,
			operation_count INT,
			tx_set_operation_count INT,

			-- NEW: Soroban (Protocol 20+)
			soroban_fee_write1kb BIGINT,

			-- NEW: Consensus metadata
			node_id TEXT,
			signature TEXT,
			ledger_header TEXT,

			-- NEW: State tracking
			bucket_list_size BIGINT,
			live_soroban_state_size BIGINT,

			-- NEW: Protocol 23 (CAP-62) - Hot Archive
			evicted_keys_count INT,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- liquidity_pools_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS liquidity_pools_snapshot_v1 (
			-- Identity (3 fields)
			liquidity_pool_id TEXT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,

			-- Pool Type (1 field)
			pool_type TEXT NOT NULL,

			-- Fee (1 field)
			fee INT NOT NULL,

			-- Pool Shares (2 fields)
			trustline_count INT NOT NULL,
			total_pool_shares BIGINT NOT NULL,

			-- Asset A (4 fields)
			asset_a_type TEXT NOT NULL,
			asset_a_code TEXT,
			asset_a_issuer TEXT,
			asset_a_amount BIGINT NOT NULL,

			-- Asset B (4 fields)
			asset_b_type TEXT NOT NULL,
			asset_b_code TEXT,
			asset_b_issuer TEXT,
			asset_b_amount BIGINT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- native_balances_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS native_balances_snapshot_v1 (
			account_id TEXT NOT NULL,
			balance BIGINT NOT NULL,
			buying_liabilities BIGINT NOT NULL,
			selling_liabilities BIGINT NOT NULL,
			num_subentries INT NOT NULL,
			num_sponsoring INT NOT NULL,
			num_sponsored INT NOT NULL,
			sequence_number BIGINT,
			last_modified_ledger BIGINT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			ledger_range BIGINT,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- offers_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS offers_snapshot_v1 (
			-- Identity (4 fields)
			offer_id BIGINT NOT NULL,
			seller_account TEXT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,

			-- Selling Asset (3 fields)
			selling_asset_type TEXT NOT NULL,
			selling_asset_code TEXT,
			selling_asset_issuer TEXT,

			-- Buying Asset (3 fields)
			buying_asset_type TEXT NOT NULL,
			buying_asset_code TEXT,
			buying_asset_issuer TEXT,

			-- Offer Details (2 fields)
			amount TEXT NOT NULL,
			price TEXT NOT NULL,

			-- Flags (1 field)
			flags INT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- operations_row_v2
CREATE UNLOGGED TABLE IF NOT EXISTS operations_row_v2 (
			-- Core fields (12)
			transaction_hash TEXT NOT NULL,
			transaction_index INT NOT NULL,
			operation_index INT NOT NULL,
			ledger_sequence BIGINT NOT NULL,
			source_account TEXT NOT NULL,
			type INT NOT NULL,
			type_string TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			transaction_successful BOOLEAN NOT NULL,
			operation_result_code TEXT,
			operation_trace_code TEXT,
			ledger_range BIGINT,

			-- Muxed accounts (1)
			source_account_muxed TEXT,

			-- Asset fields (8)
			asset TEXT,
			asset_type TEXT,
			asset_code TEXT,
			asset_issuer TEXT,
			source_asset TEXT,
			source_asset_type TEXT,
			source_asset_code TEXT,
			source_asset_issuer TEXT,

			-- Amount/price fields (4)
			amount BIGINT,
			source_amount BIGINT,
			destination_min BIGINT,
			starting_balance BIGINT,

			-- Destination (1)
			destination TEXT,

			-- Trustline (5)
			trustline_limit BIGINT,
			trustor TEXT,
			authorize BOOLEAN,
			authorize_to_maintain_liabilities BOOLEAN,
			trust_line_flags INT,

			-- Claimable balance (2)
			balance_id TEXT,
			claimants_count INT,

			-- Sponsorship (1)
			sponsored_id TEXT,

			-- DEX fields (11)
			offer_id BIGINT,
			price TEXT,
			price_r TEXT,
			buying_asset TEXT,
			buying_asset_type TEXT,
			buying_asset_code TEXT,
			buying_asset_issuer TEXT,
			selling_asset TEXT,
			selling_asset_type TEXT,
			selling_asset_code TEXT,
			selling_asset_issuer TEXT,

			-- Soroban (5)
			soroban_operation TEXT,
			soroban_function TEXT,
			soroban_contract_id TEXT,
			soroban_auth_required BOOLEAN,
			soroban_arguments_json TEXT,

			-- Call Graph (3) - Cross-contract call tracking
			contract_calls_json JSONB,        -- Array of {from, to, function, depth, order}
			contracts_involved TEXT[],        -- All contracts in the call chain
			max_call_depth INT,               -- Maximum depth of nested calls

			-- Account operations (8)
			bump_to BIGINT,
			set_flags INT,
			clear_flags INT,
			home_domain TEXT,
			master_weight INT,
			low_threshold INT,
			medium_threshold INT,
			high_threshold INT,

			-- Other (2)
			data_name TEXT,
			data_value TEXT,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- restored_keys_state_v1
CREATE UNLOGGED TABLE IF NOT EXISTS restored_keys_state_v1 (
			-- Identity (2 fields)
			key_hash TEXT NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- Restoration details (4 fields)
			contract_id TEXT NOT NULL,
			key_type TEXT NOT NULL,
			durability TEXT NOT NULL,
			restored_from_ledger BIGINT NOT NULL,

			-- Metadata (3 fields)
			closed_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- trades_row_v1
CREATE UNLOGGED TABLE IF NOT EXISTS trades_row_v1 (
			-- Identity (4 fields)
			ledger_sequence BIGINT NOT NULL,
			transaction_hash TEXT NOT NULL,
			operation_index INT NOT NULL,
			trade_index INT NOT NULL,

			-- Trade details (2 fields)
			trade_type TEXT NOT NULL,
			trade_timestamp TIMESTAMPTZ NOT NULL,

			-- Seller side (4 fields)
			seller_account TEXT NOT NULL,
			selling_asset_code TEXT,
			selling_asset_issuer TEXT,
			selling_amount TEXT NOT NULL,

			-- Buyer side (4 fields)
			buyer_account TEXT NOT NULL,
			buying_asset_code TEXT,
			buying_asset_issuer TEXT,
			buying_amount TEXT NOT NULL,

			-- Price (1 field)
			price TEXT NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- transactions_row_v2
CREATE UNLOGGED TABLE IF NOT EXISTS transactions_row_v2 (
			-- Core fields (13)
			ledger_sequence BIGINT NOT NULL,
			transaction_hash TEXT NOT NULL,
			source_account TEXT NOT NULL,
			fee_charged BIGINT NOT NULL,
			max_fee BIGINT NOT NULL,
			successful BOOLEAN NOT NULL,
			transaction_result_code TEXT NOT NULL,
			operation_count INT NOT NULL,
			memo_type TEXT,
			memo TEXT,
			created_at TIMESTAMPTZ NOT NULL,
			account_sequence BIGINT,
			ledger_range BIGINT,

			-- Muxed accounts (2) - CAP-27
			source_account_muxed TEXT,
			fee_account_muxed TEXT,

			-- Fee bump transactions (4) - CAP-15
			inner_transaction_hash TEXT,
			fee_bump_fee BIGINT,
			max_fee_bid BIGINT,
			inner_source_account TEXT,

			-- Preconditions (6) - CAP-21
			timebounds_min_time BIGINT,
			timebounds_max_time BIGINT,
			ledgerbounds_min BIGINT,
			ledgerbounds_max BIGINT,
			min_sequence_number BIGINT,
			min_sequence_age BIGINT,

			-- Soroban fields (13) - CAP-46/CAP-47
			soroban_resources_instructions BIGINT,
			soroban_resources_read_bytes BIGINT,
			soroban_resources_write_bytes BIGINT,
			soroban_data_size_bytes INT,
			soroban_data_resources TEXT,
			soroban_fee_base BIGINT,
			soroban_fee_resources BIGINT,
			soroban_fee_refund BIGINT,
			soroban_fee_charged BIGINT,
			soroban_fee_wasted BIGINT,
			soroban_host_function_type TEXT,
			soroban_contract_id TEXT,
			soroban_contract_events_count INT,

			-- Metadata (2)
			signatures_count INT NOT NULL,
			new_account BOOLEAN NOT NULL,

			-- Cycle 6: XDR fields (4) - Complete transaction reconstruction
			tx_envelope TEXT,
			tx_result TEXT,
			tx_meta TEXT,
			tx_fee_meta TEXT,

			-- Cycle 6: Signer fields (2) - Multi-sig analysis
			tx_signers TEXT,
			extra_signers TEXT,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- trustlines_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS trustlines_snapshot_v1 (
			-- Identity (4 fields) - PK
			account_id TEXT NOT NULL,
			asset_code TEXT NOT NULL,
			asset_issuer TEXT NOT NULL,
			asset_type TEXT NOT NULL,

			-- Trust & Balance (4 fields)
			balance TEXT NOT NULL,
			trust_limit TEXT NOT NULL,
			buying_liabilities TEXT NOT NULL,
			selling_liabilities TEXT NOT NULL,

			-- Authorization (3 fields)
			authorized BOOLEAN NOT NULL,
			authorized_to_maintain_liabilities BOOLEAN NOT NULL,
			clawback_enabled BOOLEAN NOT NULL,

			-- Metadata (3 fields)
			ledger_sequence BIGINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);

-- ttl_snapshot_v1
CREATE UNLOGGED TABLE IF NOT EXISTS ttl_snapshot_v1 (
			-- Identity (2 fields)
			key_hash TEXT NOT NULL,
			ledger_sequence BIGINT NOT NULL,

			-- TTL tracking (3 fields)
			live_until_ledger_seq BIGINT NOT NULL,
			ttl_remaining BIGINT NOT NULL,
			expired BOOLEAN NOT NULL,

			-- Ledger metadata (3 fields)
			last_modified_ledger INT NOT NULL,
			deleted BOOLEAN NOT NULL,
			closed_at TIMESTAMPTZ NOT NULL,

			-- Metadata (2 fields)
			created_at TIMESTAMPTZ NOT NULL,
			ledger_range BIGINT NOT NULL,

			-- CYCLE 14: Incremental Versioning Support
			era_id TEXT,
			version_label TEXT
		);


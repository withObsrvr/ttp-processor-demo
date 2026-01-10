
-- account_signers_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.account_signers_current (
     account_id TEXT NOT NULL,
     signer TEXT NOT NULL,
     weight INTEGER NOT NULL,
     sponsor TEXT,
     last_modified_ledger BIGINT NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT
);

-- account_signers_snapshot
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.account_signers_snapshot (
     account_id TEXT NOT NULL,
     signer TEXT NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     weight INTEGER NOT NULL,
     sponsor TEXT,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT,
     valid_to TIMESTAMP,
     closed_at TIMESTAMP NOT NULL
);

-- accounts_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.accounts_current (
     account_id TEXT NOT NULL,
     balance TEXT NOT NULL,
     sequence_number BIGINT NOT NULL,
     num_subentries INTEGER NOT NULL,
     num_sponsoring INTEGER NOT NULL,
     num_sponsored INTEGER NOT NULL,
     home_domain TEXT,
     master_weight INTEGER NOT NULL,
     low_threshold INTEGER NOT NULL,
     med_threshold INTEGER NOT NULL,
     high_threshold INTEGER NOT NULL,
     flags INTEGER NOT NULL,
     auth_required BOOLEAN NOT NULL,
     auth_revocable BOOLEAN NOT NULL,
     auth_immutable BOOLEAN NOT NULL,
     auth_clawback_enabled BOOLEAN NOT NULL,
     signers TEXT,
     sponsor_account TEXT,
     created_at TIMESTAMP NOT NULL,
     updated_at TIMESTAMP NOT NULL,
     last_modified_ledger BIGINT NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT
);

-- accounts_snapshot
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.accounts_snapshot (
     account_id TEXT NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     closed_at TIMESTAMP NOT NULL,
     balance TEXT NOT NULL,
     sequence_number BIGINT NOT NULL,
     num_subentries INTEGER NOT NULL,
     num_sponsoring INTEGER NOT NULL,
     num_sponsored INTEGER NOT NULL,
     home_domain TEXT,
     master_weight INTEGER NOT NULL,
     low_threshold INTEGER NOT NULL,
     med_threshold INTEGER NOT NULL,
     high_threshold INTEGER NOT NULL,
     flags INTEGER NOT NULL,
     auth_required BOOLEAN NOT NULL,
     auth_revocable BOOLEAN NOT NULL,
     auth_immutable BOOLEAN NOT NULL,
     auth_clawback_enabled BOOLEAN NOT NULL,
     signers TEXT,
     sponsor_account TEXT,
     created_at TIMESTAMP NOT NULL,
     updated_at TIMESTAMP NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT,
     valid_to TIMESTAMP
);

-- claimable_balances_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.claimable_balances_current (
     balance_id VARCHAR NOT NULL,
     claimants TEXT,
     asset_type VARCHAR,
     asset_code VARCHAR,
     asset_issuer VARCHAR,
     asset VARCHAR,
     amount BIGINT,
     sponsor VARCHAR,
     flags INTEGER,
     last_modified_ledger BIGINT NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     created_at TIMESTAMP,
     ledger_range BIGINT,
     inserted_at TIMESTAMP,
     updated_at TIMESTAMP
);

-- claimable_balances_snapshot
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.claimable_balances_snapshot (
     balance_id TEXT NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     asset_type TEXT NOT NULL,
     asset_code TEXT NOT NULL,
     asset_issuer TEXT NOT NULL,
     amount TEXT NOT NULL,
     claimants TEXT NOT NULL,
     flags INTEGER NOT NULL,
     sponsor_account TEXT,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT,
     valid_to TIMESTAMP
);

-- config_settings_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.config_settings_current (
     config_setting_id TEXT NOT NULL,
     config_setting TEXT NOT NULL,
     last_modified_ledger BIGINT NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT
);

-- contract_code_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.contract_code_current (
     contract_code_hash TEXT NOT NULL,
     contract_code TEXT NOT NULL,
     expiration_ledger BIGINT,
     last_modified_ledger BIGINT NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT
);

-- contract_data_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.contract_data_current (
     contract_id VARCHAR NOT NULL,
     contract_key_type VARCHAR NOT NULL,
     contract_durability VARCHAR,
     asset VARCHAR,
     asset_code VARCHAR,
     asset_issuer VARCHAR,
     asset_type VARCHAR,
     balance_holder VARCHAR,
     contract_data_xdr TEXT,
     last_modified_ledger BIGINT NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     created_at TIMESTAMP,
     ledger_range BIGINT,
     inserted_at TIMESTAMP,
     updated_at TIMESTAMP
);

-- enriched_history_operations
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.enriched_history_operations (
     transaction_hash VARCHAR NOT NULL,
     operation_index INTEGER NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     source_account VARCHAR,
     type INTEGER,
     type_string VARCHAR,
     created_at TIMESTAMP,
     transaction_successful BOOLEAN,
     operation_result_code VARCHAR,
     operation_trace_code VARCHAR,
     ledger_range BIGINT,
     source_account_muxed VARCHAR,
     asset VARCHAR,
     asset_type VARCHAR,
     asset_code VARCHAR,
     asset_issuer VARCHAR,
     source_asset VARCHAR,
     source_asset_type VARCHAR,
     source_asset_code VARCHAR,
     source_asset_issuer VARCHAR,
     destination VARCHAR,
     destination_muxed VARCHAR,
     amount BIGINT,
     source_amount BIGINT,
     from_account VARCHAR,
     from_muxed VARCHAR,
     to_address VARCHAR,
     to_muxed VARCHAR,
     limit_amount BIGINT,
     offer_id BIGINT,
     selling_asset VARCHAR,
     selling_asset_type VARCHAR,
     selling_asset_code VARCHAR,
     selling_asset_issuer VARCHAR,
     buying_asset VARCHAR,
     buying_asset_type VARCHAR,
     buying_asset_code VARCHAR,
     buying_asset_issuer VARCHAR,
     price_n INTEGER,
     price_d INTEGER,
     price NUMERIC,
     starting_balance BIGINT,
     home_domain VARCHAR,
     inflation_dest VARCHAR,
     set_flags INTEGER,
     set_flags_s VARCHAR[],
     clear_flags INTEGER,
     clear_flags_s VARCHAR[],
     master_key_weight INTEGER,
     low_threshold INTEGER,
     med_threshold INTEGER,
     high_threshold INTEGER,
     signer_account_id VARCHAR,
     signer_key VARCHAR,
     signer_weight INTEGER,
     data_name VARCHAR,
     data_value TEXT,
     host_function_type VARCHAR,
     parameters TEXT,
     address VARCHAR,
     contract_id VARCHAR,
     function_name VARCHAR,
     balance_id VARCHAR,
     claimant VARCHAR,
     claimant_muxed VARCHAR,
     predicate TEXT,
     liquidity_pool_id VARCHAR,
     reserve_a_asset VARCHAR,
     reserve_a_amount BIGINT,
     reserve_b_asset VARCHAR,
     reserve_b_amount BIGINT,
     shares BIGINT,
     shares_received BIGINT,
     into_account VARCHAR,
     into_muxed VARCHAR,
     sponsor VARCHAR,
     sponsored_id VARCHAR,
     begin_sponsor VARCHAR,
     tx_successful BOOLEAN,
     tx_fee_charged BIGINT,
     tx_max_fee BIGINT,
     tx_operation_count INTEGER,
     tx_memo_type VARCHAR,
     tx_memo TEXT,
     ledger_closed_at TIMESTAMP,
     ledger_total_coins BIGINT,
     ledger_fee_pool BIGINT,
     ledger_base_fee INTEGER,
     ledger_base_reserve INTEGER,
     ledger_transaction_count INTEGER,
     ledger_operation_count INTEGER,
     ledger_successful_tx_count INTEGER,
     ledger_failed_tx_count INTEGER,
     is_payment_op BOOLEAN,
     is_soroban_op BOOLEAN,
     inserted_at TIMESTAMP
);

-- liquidity_pools_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.liquidity_pools_current (
     liquidity_pool_id TEXT NOT NULL,
     pool_type TEXT NOT NULL,
     fee INTEGER NOT NULL,
     asset_a_type TEXT NOT NULL,
     asset_a_code TEXT NOT NULL,
     asset_a_issuer TEXT NOT NULL,
     asset_a_amount TEXT NOT NULL,
     asset_b_type TEXT NOT NULL,
     asset_b_code TEXT NOT NULL,
     asset_b_issuer TEXT NOT NULL,
     asset_b_amount TEXT NOT NULL,
     total_pool_shares TEXT NOT NULL,
     pool_share_count BIGINT NOT NULL,
     last_modified_ledger BIGINT NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT
);

-- offers_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.offers_current (
     offer_id BIGINT NOT NULL,
     seller_account TEXT NOT NULL,
     closed_at TIMESTAMP NOT NULL,
     selling_asset_type TEXT NOT NULL,
     selling_asset_code TEXT,
     selling_asset_issuer TEXT,
     buying_asset_type TEXT NOT NULL,
     buying_asset_code TEXT,
     buying_asset_issuer TEXT,
     amount TEXT NOT NULL,
     price TEXT NOT NULL,
     flags INTEGER NOT NULL,
     created_at TIMESTAMP NOT NULL,
     last_modified_ledger BIGINT NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT
);

-- offers_snapshot
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.offers_snapshot (
     offer_id BIGINT NOT NULL,
     seller_account TEXT NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     closed_at TIMESTAMP NOT NULL,
     selling_asset_type TEXT NOT NULL,
     selling_asset_code TEXT,
     selling_asset_issuer TEXT,
     buying_asset_type TEXT NOT NULL,
     buying_asset_code TEXT,
     buying_asset_issuer TEXT,
     amount TEXT NOT NULL,
     price TEXT NOT NULL,
     flags INTEGER NOT NULL,
     created_at TIMESTAMP NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT,
     valid_to TIMESTAMP
);

-- token_transfers_raw
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.token_transfers_raw (
     timestamp TIMESTAMP NOT NULL,
     transaction_hash VARCHAR NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     source_type VARCHAR NOT NULL,
     from_account VARCHAR,
     to_account VARCHAR,
     asset_code VARCHAR,
     asset_issuer VARCHAR,
     amount BIGINT,
     token_contract_id VARCHAR,
     operation_type INTEGER,
     transaction_successful BOOLEAN,
     inserted_at TIMESTAMP
);

-- trustlines_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.trustlines_current (
     account_id VARCHAR NOT NULL,
     asset_type VARCHAR NOT NULL,
     asset_issuer VARCHAR,
     asset_code VARCHAR,
     liquidity_pool_id VARCHAR,
     balance BIGINT,
     trust_line_limit BIGINT,
     buying_liabilities BIGINT,
     selling_liabilities BIGINT,
     flags INTEGER,
     last_modified_ledger BIGINT NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     created_at TIMESTAMP,
     sponsor VARCHAR,
     ledger_range BIGINT,
     inserted_at TIMESTAMP,
     updated_at TIMESTAMP
);

-- trustlines_snapshot
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.trustlines_snapshot (
     account_id TEXT NOT NULL,
     asset_code TEXT NOT NULL,
     asset_issuer TEXT NOT NULL,
     asset_type TEXT NOT NULL,
     balance TEXT NOT NULL,
     trust_limit TEXT NOT NULL,
     buying_liabilities TEXT NOT NULL,
     selling_liabilities TEXT NOT NULL,
     authorized BOOLEAN NOT NULL,
     authorized_to_maintain_liabilities BOOLEAN NOT NULL,
     clawback_enabled BOOLEAN NOT NULL,
     ledger_sequence BIGINT NOT NULL,
     created_at TIMESTAMP NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT,
     valid_to TIMESTAMP
);

-- ttl_current
CREATE TABLE IF NOT EXISTS testnet_catalog.silver.ttl_current (
     key_hash TEXT NOT NULL,
     live_until_ledger BIGINT NOT NULL,
     last_modified_ledger BIGINT NOT NULL,
     ledger_range BIGINT NOT NULL,
     era_id TEXT,
     version_label TEXT
);


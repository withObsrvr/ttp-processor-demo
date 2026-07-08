-- Migration 009: OpenZeppelin smart-account current authorization state
-- Purpose: materialize context rules, signers, and policies from contract events
-- so credential/address reverse lookups do not scan raw events.

ALTER TABLE semantic_entities_contracts ADD COLUMN IF NOT EXISTS wallet_type TEXT;
ALTER TABLE semantic_entities_contracts ADD COLUMN IF NOT EXISTS wallet_signers JSONB;
CREATE INDEX IF NOT EXISTS idx_sem_entities_wallet ON semantic_entities_contracts(wallet_type) WHERE wallet_type IS NOT NULL;

CREATE TABLE IF NOT EXISTS smart_account_context_rules (
    contract_id TEXT NOT NULL,
    context_rule_id BIGINT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    metadata JSONB,
    event_type TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    transaction_hash TEXT,
    operation_index INT,
    event_index INT,
    event_order BIGINT NOT NULL,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (contract_id, context_rule_id)
);

CREATE INDEX IF NOT EXISTS idx_smart_account_rules_contract ON smart_account_context_rules(contract_id);
CREATE INDEX IF NOT EXISTS idx_smart_account_rules_active ON smart_account_context_rules(contract_id, active) WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_smart_account_rules_ledger ON smart_account_context_rules(last_modified_ledger);

CREATE TABLE IF NOT EXISTS smart_account_signers (
    contract_id TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'rule',
    context_rule_id BIGINT NOT NULL DEFAULT -1,
    signer_key TEXT NOT NULL,
    signer_id BIGINT,
    signer_type TEXT,
    signer_address TEXT,
    credential_id TEXT,
    raw_bytes TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    event_type TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    transaction_hash TEXT,
    operation_index INT,
    event_index INT,
    event_order BIGINT NOT NULL,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (contract_id, scope, context_rule_id, signer_key)
);

CREATE INDEX IF NOT EXISTS idx_smart_account_signers_contract ON smart_account_signers(contract_id);
CREATE INDEX IF NOT EXISTS idx_smart_account_signers_rule ON smart_account_signers(contract_id, context_rule_id);
CREATE INDEX IF NOT EXISTS idx_smart_account_signers_credential ON smart_account_signers(credential_id) WHERE credential_id IS NOT NULL AND active = TRUE;
CREATE INDEX IF NOT EXISTS idx_smart_account_signers_address ON smart_account_signers(signer_address) WHERE signer_address IS NOT NULL AND active = TRUE;
CREATE INDEX IF NOT EXISTS idx_smart_account_signers_id ON smart_account_signers(contract_id, signer_id) WHERE signer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_smart_account_signers_ledger ON smart_account_signers(last_modified_ledger);

CREATE TABLE IF NOT EXISTS smart_account_policies (
    contract_id TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'rule',
    context_rule_id BIGINT NOT NULL DEFAULT -1,
    policy_key TEXT NOT NULL,
    policy_id BIGINT,
    policy_address TEXT,
    install_params JSONB,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    event_type TEXT NOT NULL,
    ledger_sequence BIGINT NOT NULL,
    last_modified_ledger BIGINT NOT NULL,
    transaction_hash TEXT,
    operation_index INT,
    event_index INT,
    event_order BIGINT NOT NULL,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (contract_id, scope, context_rule_id, policy_key)
);

CREATE INDEX IF NOT EXISTS idx_smart_account_policies_contract ON smart_account_policies(contract_id);
CREATE INDEX IF NOT EXISTS idx_smart_account_policies_rule ON smart_account_policies(contract_id, context_rule_id);
CREATE INDEX IF NOT EXISTS idx_smart_account_policies_address ON smart_account_policies(policy_address) WHERE policy_address IS NOT NULL AND active = TRUE;
CREATE INDEX IF NOT EXISTS idx_smart_account_policies_id ON smart_account_policies(contract_id, policy_id) WHERE policy_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_smart_account_policies_ledger ON smart_account_policies(last_modified_ledger);

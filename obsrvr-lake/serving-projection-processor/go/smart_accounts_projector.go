package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type SmartAccountsProjector struct {
	network    string
	sourcePool *pgxpool.Pool
	targetPool *pgxpool.Pool
}

func NewSmartAccountsProjector(network string, sourcePool, targetPool *pgxpool.Pool) *SmartAccountsProjector {
	return &SmartAccountsProjector{network: network, sourcePool: sourcePool, targetPool: targetPool}
}

func (p *SmartAccountsProjector) Name() string { return "smart_accounts" }

func (p *SmartAccountsProjector) RunOnce(ctx context.Context) (RunStats, error) {
	var completeThru int64
	if err := p.sourcePool.QueryRow(ctx, `
		SELECT GREATEST(
			COALESCE((SELECT MAX(last_modified_ledger) FROM smart_account_context_rules), 0),
			COALESCE((SELECT MAX(last_modified_ledger) FROM smart_account_signers), 0),
			COALESCE((SELECT MAX(last_modified_ledger) FROM smart_account_policies), 0)
		)
	`).Scan(&completeThru); err != nil {
		return RunStats{}, fmt.Errorf("resolve smart account watermark: %w", err)
	}

	tx, err := p.targetPool.Begin(ctx)
	if err != nil {
		return RunStats{}, fmt.Errorf("begin smart accounts tx: %w", err)
	}
	defer tx.Rollback(ctx)

	deleted := int64(0)
	for _, table := range []string{
		"serving.sv_smart_account_signers",
		"serving.sv_smart_account_contracts",
		"serving.sv_smart_account_signers_by_credential",
		"serving.sv_smart_account_signers_by_address",
		"serving.sv_smart_account_rules_current",
		"serving.sv_smart_account_contracts_current",
	} {
		tag, err := tx.Exec(ctx, "DELETE FROM "+table)
		if err != nil {
			return RunStats{}, fmt.Errorf("clear %s: %w", table, err)
		}
		deleted += tag.RowsAffected()
	}

	contractsTag, err := tx.Exec(ctx, `
		WITH active_rules AS (
			SELECT contract_id, context_rule_id, ledger_sequence, last_modified_ledger
			FROM smart_account_context_rules
			WHERE active = TRUE
		), all_contracts AS (
			SELECT
				contract_id,
				MIN(ledger_sequence) AS first_seen_ledger,
				MAX(last_modified_ledger) AS state_last_modified
			FROM (
				SELECT contract_id, ledger_sequence, last_modified_ledger FROM smart_account_context_rules
				UNION ALL
				SELECT contract_id, ledger_sequence, last_modified_ledger FROM smart_account_signers
				UNION ALL
				SELECT contract_id, ledger_sequence, last_modified_ledger FROM smart_account_policies
			) state_rows
			GROUP BY contract_id
		), active_signers AS (
			SELECT
				s.contract_id,
				s.context_rule_id,
				s.signer_key,
				COALESCE(s.signer_address, reg.signer_address) AS signer_address,
				LOWER(COALESCE(s.credential_id, reg.credential_id)) AS credential_id,
				GREATEST(s.last_modified_ledger, COALESCE(reg.last_modified_ledger, s.last_modified_ledger)) AS last_modified_ledger
			FROM smart_account_signers s
			JOIN active_rules r
			  ON r.contract_id = s.contract_id
			 AND r.context_rule_id = s.context_rule_id
			LEFT JOIN smart_account_signers reg
			  ON reg.contract_id = s.contract_id
			 AND reg.scope = 'registry'
			 AND reg.signer_id = s.signer_id
			 AND reg.active = TRUE
			WHERE s.scope = 'rule'
			  AND s.active = TRUE
		), active_policies AS (
			SELECT
				p.contract_id,
				p.context_rule_id,
				p.policy_key,
				GREATEST(p.last_modified_ledger, COALESCE(reg.last_modified_ledger, p.last_modified_ledger)) AS last_modified_ledger
			FROM smart_account_policies p
			JOIN active_rules r
			  ON r.contract_id = p.contract_id
			 AND r.context_rule_id = p.context_rule_id
			LEFT JOIN smart_account_policies reg
			  ON reg.contract_id = p.contract_id
			 AND reg.scope = 'registry'
			 AND reg.policy_id = p.policy_id
			 AND reg.active = TRUE
			WHERE p.scope = 'rule'
			  AND p.active = TRUE
		), rule_rollup AS (
			SELECT
				contract_id,
				COUNT(*)::int AS context_rule_count,
				ARRAY_AGG(context_rule_id ORDER BY context_rule_id) AS context_rule_ids,
				MIN(ledger_sequence) AS first_seen_ledger,
				MAX(last_modified_ledger) AS rule_last_modified
			FROM active_rules
			GROUP BY contract_id
		), signer_rollup AS (
			SELECT
				contract_id,
				COUNT(DISTINCT signer_key)::int AS active_signer_count,
				(COUNT(DISTINCT credential_id) FILTER (WHERE credential_id IS NOT NULL))::int AS credential_signer_count,
				(COUNT(DISTINCT signer_address) FILTER (WHERE signer_address IS NOT NULL))::int AS address_signer_count,
				MAX(last_modified_ledger) AS signer_last_modified
			FROM active_signers
			GROUP BY contract_id
		), policy_rollup AS (
			SELECT
				contract_id,
				COUNT(DISTINCT policy_key)::int AS active_policy_count,
				MAX(last_modified_ledger) AS policy_last_modified
			FROM active_policies
			GROUP BY contract_id
		)
		INSERT INTO serving.sv_smart_account_contracts_current (
			contract_id, wallet_type, context_rule_count, active_signer_count,
			credential_signer_count, address_signer_count, active_policy_count,
			context_rule_ids, first_seen_ledger, last_modified_ledger, updated_at
		)
		SELECT
			ac.contract_id,
			COALESCE(sec.wallet_type, 'openzeppelin') AS wallet_type,
			COALESCE(rr.context_rule_count, 0),
			COALESCE(sr.active_signer_count, 0),
			COALESCE(sr.credential_signer_count, 0),
			COALESCE(sr.address_signer_count, 0),
			COALESCE(pr.active_policy_count, 0),
			COALESCE(rr.context_rule_ids, ARRAY[]::BIGINT[]),
			ac.first_seen_ledger,
			GREATEST(COALESCE(ac.state_last_modified, 0), COALESCE(rr.rule_last_modified, 0), COALESCE(sr.signer_last_modified, 0), COALESCE(pr.policy_last_modified, 0)) AS last_modified_ledger,
			NOW()
		FROM all_contracts ac
		LEFT JOIN rule_rollup rr ON rr.contract_id = ac.contract_id
		LEFT JOIN signer_rollup sr ON sr.contract_id = ac.contract_id
		LEFT JOIN policy_rollup pr ON pr.contract_id = ac.contract_id
		LEFT JOIN semantic_entities_contracts sec ON sec.contract_id = ac.contract_id
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate smart account contracts: %w", err)
	}

	rulesTag, err := tx.Exec(ctx, `
		WITH active_rules AS (
			SELECT *
			FROM smart_account_context_rules
			WHERE active = TRUE
		), active_signers AS (
			SELECT
				s.contract_id,
				s.context_rule_id,
				s.signer_key,
				s.signer_id,
				COALESCE(s.signer_type, reg.signer_type) AS signer_type,
				COALESCE(s.signer_address, reg.signer_address) AS signer_address,
				LOWER(COALESCE(s.credential_id, reg.credential_id)) AS credential_id,
				COALESCE(s.raw_bytes, reg.raw_bytes) AS raw_bytes,
				COALESCE(s.transaction_hash, reg.transaction_hash) AS transaction_hash,
				(reg.signer_key IS NOT NULL) AS registry_resolved,
				GREATEST(s.last_modified_ledger, COALESCE(reg.last_modified_ledger, s.last_modified_ledger)) AS last_modified_ledger
			FROM smart_account_signers s
			JOIN active_rules r
			  ON r.contract_id = s.contract_id
			 AND r.context_rule_id = s.context_rule_id
			LEFT JOIN smart_account_signers reg
			  ON reg.contract_id = s.contract_id
			 AND reg.scope = 'registry'
			 AND reg.signer_id = s.signer_id
			 AND reg.active = TRUE
			WHERE s.scope = 'rule'
			  AND s.active = TRUE
		), active_policies AS (
			SELECT
				p.contract_id,
				p.context_rule_id,
				p.policy_key,
				p.policy_id,
				COALESCE(p.policy_address, reg.policy_address) AS policy_address,
				COALESCE(p.install_params, reg.install_params) AS install_params,
				COALESCE(p.transaction_hash, reg.transaction_hash) AS transaction_hash,
				(reg.policy_key IS NOT NULL) AS registry_resolved,
				GREATEST(p.last_modified_ledger, COALESCE(reg.last_modified_ledger, p.last_modified_ledger)) AS last_modified_ledger
			FROM smart_account_policies p
			JOIN active_rules r
			  ON r.contract_id = p.contract_id
			 AND r.context_rule_id = p.context_rule_id
			LEFT JOIN smart_account_policies reg
			  ON reg.contract_id = p.contract_id
			 AND reg.scope = 'registry'
			 AND reg.policy_id = p.policy_id
			 AND reg.active = TRUE
			WHERE p.scope = 'rule'
			  AND p.active = TRUE
		), signer_json AS (
			SELECT
				contract_id,
				context_rule_id,
				COUNT(DISTINCT signer_key)::int AS signer_count,
				MAX(last_modified_ledger) AS signer_last_modified,
				COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
					'signer_id', signer_id,
					'signer_type', signer_type,
					'signer_address', signer_address,
					'credential_id', credential_id,
					'raw_bytes', raw_bytes,
					'last_modified_ledger', last_modified_ledger,
					'transaction_hash', transaction_hash,
					'registry_resolved', registry_resolved
				)) FILTER (WHERE signer_key IS NOT NULL), '[]'::jsonb) AS signers_json
			FROM active_signers
			GROUP BY contract_id, context_rule_id
		), policy_json AS (
			SELECT
				contract_id,
				context_rule_id,
				COUNT(DISTINCT policy_key)::int AS policy_count,
				MAX(last_modified_ledger) AS policy_last_modified,
				COALESCE(jsonb_agg(DISTINCT jsonb_build_object(
					'policy_id', policy_id,
					'policy_address', policy_address,
					'install_params', install_params,
					'last_modified_ledger', last_modified_ledger,
					'transaction_hash', transaction_hash,
					'registry_resolved', registry_resolved
				)) FILTER (WHERE policy_key IS NOT NULL), '[]'::jsonb) AS policies_json
			FROM active_policies
			GROUP BY contract_id, context_rule_id
		)
		INSERT INTO serving.sv_smart_account_rules_current (
			contract_id, context_rule_id, wallet_type, metadata, event_type,
			signer_count, policy_count, signers_json, policies_json,
			last_modified_ledger, transaction_hash, closed_at, updated_at
		)
		SELECT
			r.contract_id,
			r.context_rule_id,
			COALESCE(sec.wallet_type, 'openzeppelin') AS wallet_type,
			r.metadata,
			r.event_type,
			COALESCE(sj.signer_count, 0),
			COALESCE(pj.policy_count, 0),
			COALESCE(sj.signers_json, '[]'::jsonb),
			COALESCE(pj.policies_json, '[]'::jsonb),
			GREATEST(COALESCE(r.last_modified_ledger, 0), COALESCE(sj.signer_last_modified, 0), COALESCE(pj.policy_last_modified, 0)) AS last_modified_ledger,
			r.transaction_hash,
			r.closed_at,
			NOW()
		FROM active_rules r
		LEFT JOIN signer_json sj
		  ON sj.contract_id = r.contract_id
		 AND sj.context_rule_id = r.context_rule_id
		LEFT JOIN policy_json pj
		  ON pj.contract_id = r.contract_id
		 AND pj.context_rule_id = r.context_rule_id
		LEFT JOIN semantic_entities_contracts sec ON sec.contract_id = r.contract_id
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate smart account rules: %w", err)
	}

	credentialTag, err := tx.Exec(ctx, `
		WITH active_rules AS (
			SELECT contract_id, context_rule_id, ledger_sequence
			FROM smart_account_context_rules
			WHERE active = TRUE
		), active_signers AS (
			SELECT
				s.contract_id,
				s.context_rule_id,
				s.signer_key,
				s.signer_id,
				LOWER(COALESCE(s.credential_id, reg.credential_id)) AS credential_id,
				GREATEST(s.last_modified_ledger, COALESCE(reg.last_modified_ledger, s.last_modified_ledger)) AS last_modified_ledger
			FROM smart_account_signers s
			JOIN active_rules r
			  ON r.contract_id = s.contract_id
			 AND r.context_rule_id = s.context_rule_id
			LEFT JOIN smart_account_signers reg
			  ON reg.contract_id = s.contract_id
			 AND reg.scope = 'registry'
			 AND reg.signer_id = s.signer_id
			 AND reg.active = TRUE
			WHERE s.scope = 'rule'
			  AND s.active = TRUE
		), policy_rollup AS (
			SELECT p.contract_id, COUNT(DISTINCT p.policy_key)::int AS active_policy_count
			FROM smart_account_policies p
			JOIN active_rules r
			  ON r.contract_id = p.contract_id
			 AND r.context_rule_id = p.context_rule_id
			WHERE p.scope = 'rule'
			  AND p.active = TRUE
			GROUP BY p.contract_id
		)
		INSERT INTO serving.sv_smart_account_signers_by_credential (
			credential_id, contract_id, wallet_type, context_rule_ids, signer_ids,
			signer_count, active_policy_count, first_seen_ledger,
			last_modified_ledger, updated_at
		)
		SELECT
			s.credential_id,
			s.contract_id,
			COALESCE(sec.wallet_type, 'openzeppelin') AS wallet_type,
			ARRAY_AGG(DISTINCT s.context_rule_id ORDER BY s.context_rule_id) AS context_rule_ids,
			COALESCE(ARRAY_AGG(DISTINCT s.signer_id ORDER BY s.signer_id) FILTER (WHERE s.signer_id IS NOT NULL), ARRAY[]::BIGINT[]) AS signer_ids,
			COUNT(DISTINCT s.signer_key)::int AS signer_count,
			COALESCE(pr.active_policy_count, 0),
			MIN(r.ledger_sequence) AS first_seen_ledger,
			MAX(s.last_modified_ledger) AS last_modified_ledger,
			NOW()
		FROM active_signers s
		JOIN active_rules r
		  ON r.contract_id = s.contract_id
		 AND r.context_rule_id = s.context_rule_id
		LEFT JOIN policy_rollup pr ON pr.contract_id = s.contract_id
		LEFT JOIN semantic_entities_contracts sec ON sec.contract_id = s.contract_id
		WHERE s.credential_id IS NOT NULL
		GROUP BY s.credential_id, s.contract_id, COALESCE(sec.wallet_type, 'openzeppelin'), COALESCE(pr.active_policy_count, 0)
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate smart account credential lookup: %w", err)
	}

	addressTag, err := tx.Exec(ctx, `
		WITH active_rules AS (
			SELECT contract_id, context_rule_id, ledger_sequence
			FROM smart_account_context_rules
			WHERE active = TRUE
		), active_signers AS (
			SELECT
				s.contract_id,
				s.context_rule_id,
				s.signer_key,
				s.signer_id,
				COALESCE(s.signer_address, reg.signer_address) AS signer_address,
				GREATEST(s.last_modified_ledger, COALESCE(reg.last_modified_ledger, s.last_modified_ledger)) AS last_modified_ledger
			FROM smart_account_signers s
			JOIN active_rules r
			  ON r.contract_id = s.contract_id
			 AND r.context_rule_id = s.context_rule_id
			LEFT JOIN smart_account_signers reg
			  ON reg.contract_id = s.contract_id
			 AND reg.scope = 'registry'
			 AND reg.signer_id = s.signer_id
			 AND reg.active = TRUE
			WHERE s.scope = 'rule'
			  AND s.active = TRUE
		), policy_rollup AS (
			SELECT p.contract_id, COUNT(DISTINCT p.policy_key)::int AS active_policy_count
			FROM smart_account_policies p
			JOIN active_rules r
			  ON r.contract_id = p.contract_id
			 AND r.context_rule_id = p.context_rule_id
			WHERE p.scope = 'rule'
			  AND p.active = TRUE
			GROUP BY p.contract_id
		)
		INSERT INTO serving.sv_smart_account_signers_by_address (
			signer_address, contract_id, wallet_type, context_rule_ids, signer_ids,
			signer_count, active_policy_count, first_seen_ledger,
			last_modified_ledger, updated_at
		)
		SELECT
			s.signer_address,
			s.contract_id,
			COALESCE(sec.wallet_type, 'openzeppelin') AS wallet_type,
			ARRAY_AGG(DISTINCT s.context_rule_id ORDER BY s.context_rule_id) AS context_rule_ids,
			COALESCE(ARRAY_AGG(DISTINCT s.signer_id ORDER BY s.signer_id) FILTER (WHERE s.signer_id IS NOT NULL), ARRAY[]::BIGINT[]) AS signer_ids,
			COUNT(DISTINCT s.signer_key)::int AS signer_count,
			COALESCE(pr.active_policy_count, 0),
			MIN(r.ledger_sequence) AS first_seen_ledger,
			MAX(s.last_modified_ledger) AS last_modified_ledger,
			NOW()
		FROM active_signers s
		JOIN active_rules r
		  ON r.contract_id = s.contract_id
		 AND r.context_rule_id = s.context_rule_id
		LEFT JOIN policy_rollup pr ON pr.contract_id = s.contract_id
		LEFT JOIN semantic_entities_contracts sec ON sec.contract_id = s.contract_id
		WHERE s.signer_address IS NOT NULL
		GROUP BY s.signer_address, s.contract_id, COALESCE(sec.wallet_type, 'openzeppelin'), COALESCE(pr.active_policy_count, 0)
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate smart account address lookup: %w", err)
	}

	appContractsTag, err := tx.Exec(ctx, `
		INSERT INTO serving.sv_smart_account_contracts (
			contract_id, wallet_type, context_rule_count, active_signer_count,
			credential_signer_count, address_signer_count, active_policy_count,
			context_rule_ids, first_seen_ledger, last_modified_ledger, updated_at
		)
		SELECT
			contract_id, wallet_type, context_rule_count, active_signer_count,
			credential_signer_count, address_signer_count, active_policy_count,
			context_rule_ids, first_seen_ledger, last_modified_ledger, updated_at
		FROM serving.sv_smart_account_contracts_current
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate smart account app contracts: %w", err)
	}

	appSignersTag, err := tx.Exec(ctx, `
		INSERT INTO serving.sv_smart_account_signers (
			identity_type, identity_value, contract_id, wallet_type, credential_id,
			signer_address, context_rule_ids, signer_ids, signer_count,
			active_policy_count, first_seen_ledger, last_modified_ledger, updated_at
		)
		SELECT
			'credential' AS identity_type,
			credential_id AS identity_value,
			contract_id,
			wallet_type,
			credential_id,
			NULL::text AS signer_address,
			context_rule_ids,
			signer_ids,
			signer_count,
			active_policy_count,
			first_seen_ledger,
			last_modified_ledger,
			updated_at
		FROM serving.sv_smart_account_signers_by_credential
		UNION ALL
		SELECT
			'address' AS identity_type,
			signer_address AS identity_value,
			contract_id,
			wallet_type,
			NULL::text AS credential_id,
			signer_address,
			context_rule_ids,
			signer_ids,
			signer_count,
			active_policy_count,
			first_seen_ledger,
			last_modified_ledger,
			updated_at
		FROM serving.sv_smart_account_signers_by_address
	`)
	if err != nil {
		return RunStats{}, fmt.Errorf("populate smart account app signers: %w", err)
	}

	for _, table := range []string{
		"serving.sv_smart_account_contracts_current",
		"serving.sv_smart_account_rules_current",
		"serving.sv_smart_account_signers_by_credential",
		"serving.sv_smart_account_signers_by_address",
		"serving.sv_smart_account_contracts",
		"serving.sv_smart_account_signers",
	} {
		if err := saveServingWatermark(ctx, tx, table, 0, completeThru); err != nil {
			return RunStats{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return RunStats{}, fmt.Errorf("commit smart accounts tx: %w", err)
	}

	applied := contractsTag.RowsAffected() + rulesTag.RowsAffected() + credentialTag.RowsAffected() + addressTag.RowsAffected() + appContractsTag.RowsAffected() + appSignersTag.RowsAffected()
	log.Printf("projector=%s network=%s contracts=%d rules=%d credential_lookup=%d address_lookup=%d app_contracts=%d app_signers=%d deleted=%d watermark=%d rebuilt smart-account serving tables",
		p.Name(), p.network, contractsTag.RowsAffected(), rulesTag.RowsAffected(), credentialTag.RowsAffected(), addressTag.RowsAffected(), appContractsTag.RowsAffected(), appSignersTag.RowsAffected(), deleted, completeThru)
	return RunStats{RowsApplied: applied, RowsDeleted: deleted, Checkpoint: completeThru}, nil
}

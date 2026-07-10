package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

// SmartAccountLookupResponse is the Obsrvr-native reverse lookup response for
// smart-account authorization state.
type SmartAccountLookupResponse struct {
	LookupType string                        `json:"lookup_type"`
	Lookup     string                        `json:"lookup"`
	Normalized string                        `json:"normalized"`
	Source     string                        `json:"source"`
	Coverage   *ServingCoverageMetadata      `json:"coverage,omitempty"`
	Contracts  []SmartAccountContractSummary `json:"contracts"`
	Count      int                           `json:"count"`
}

// SmartAccountContractSummary summarizes the active authorization state for a
// single smart-account contract.
type SmartAccountContractSummary struct {
	ContractID            string  `json:"contract_id"`
	WalletType            string  `json:"wallet_type,omitempty"`
	ContextRuleCount      int     `json:"context_rule_count"`
	ActiveSignerCount     int     `json:"active_signer_count"`
	CredentialSignerCount int     `json:"credential_signer_count"`
	AddressSignerCount    int     `json:"address_signer_count"`
	ActivePolicyCount     int     `json:"active_policy_count"`
	ContextRuleIDs        []int64 `json:"context_rule_ids,omitempty"`
	FirstSeenLedger       *int64  `json:"first_seen_ledger,omitempty"`
	LastModifiedLedger    *int64  `json:"last_modified_ledger,omitempty"`
}

// SmartAccountStateResponse returns contract-local active rule/signature state.
type SmartAccountStateResponse struct {
	ContractID    string                       `json:"contract_id"`
	Source        string                       `json:"source"`
	Coverage      *ServingCoverageMetadata     `json:"coverage,omitempty"`
	Summary       SmartAccountContractSummary  `json:"summary"`
	ContextRules  []SmartAccountContextRuleRow `json:"context_rules"`
	ContextRuleID *int64                       `json:"context_rule_id,omitempty"`
	Count         int                          `json:"count"`
}

type SmartAccountContextRuleRow struct {
	ContextRuleID      int64                   `json:"context_rule_id"`
	Active             bool                    `json:"active"`
	Metadata           *json.RawMessage        `json:"metadata,omitempty"`
	EventType          string                  `json:"event_type,omitempty"`
	LastModifiedLedger int64                   `json:"last_modified_ledger"`
	TransactionHash    string                  `json:"transaction_hash,omitempty"`
	ClosedAt           *time.Time              `json:"closed_at,omitempty"`
	Signers            []SmartAccountSignerRow `json:"signers"`
	Policies           []SmartAccountPolicyRow `json:"policies"`
}

type SmartAccountSignerRow struct {
	SignerID           *int64 `json:"signer_id,omitempty"`
	SignerType         string `json:"signer_type,omitempty"`
	SignerAddress      string `json:"signer_address,omitempty"`
	CredentialID       string `json:"credential_id,omitempty"`
	RawBytes           string `json:"raw_bytes,omitempty"`
	LastModifiedLedger int64  `json:"last_modified_ledger"`
	TransactionHash    string `json:"transaction_hash,omitempty"`
	RegistryResolved   bool   `json:"registry_resolved"`
}

type SmartAccountPolicyRow struct {
	PolicyID           *int64           `json:"policy_id,omitempty"`
	PolicyAddress      string           `json:"policy_address,omitempty"`
	InstallParams      *json.RawMessage `json:"install_params,omitempty"`
	LastModifiedLedger int64            `json:"last_modified_ledger"`
	TransactionHash    string           `json:"transaction_hash,omitempty"`
	RegistryResolved   bool             `json:"registry_resolved"`
}

type SmartAccountStatsResponse struct {
	Source             string                   `json:"source"`
	Coverage           *ServingCoverageMetadata `json:"coverage,omitempty"`
	ContractCount      int64                    `json:"contract_count"`
	ActiveRuleCount    int64                    `json:"active_rule_count"`
	ActiveSignerCount  int64                    `json:"active_signer_count"`
	CredentialCount    int64                    `json:"credential_count"`
	AddressSignerCount int64                    `json:"address_signer_count"`
	ActivePolicyCount  int64                    `json:"active_policy_count"`
	LastModifiedLedger *int64                   `json:"last_modified_ledger,omitempty"`
}

// HandleSmartAccountLookupByCredential serves GET
// /api/v1/silver/smart-accounts/lookup/credential/{credential_id}.
func (h *SmartWalletHandlers) HandleSmartAccountLookupByCredential(w http.ResponseWriter, r *http.Request) {
	if h.hotReader == nil {
		respondError(w, "silver_hot reader not available", http.StatusServiceUnavailable)
		return
	}
	raw := mux.Vars(r)["credential_id"]
	credential := normalizeSmartAccountCredential(raw)
	if credential == "" {
		respondError(w, "credential_id required", http.StatusBadRequest)
		return
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()

	limit := parseIntParam(r, "limit", 100, 1, 500)
	if contracts, coverage, err := h.queryServingSmartAccountSummaries(ctx, "credential", credential, limit); err == nil && (coverage != nil || len(contracts) > 0) {
		respondJSON(w, SmartAccountLookupResponse{
			LookupType: "credential",
			Lookup:     raw,
			Normalized: credential,
			Source:     "serving.sv_smart_account_signers",
			Coverage:   coverage,
			Contracts:  contracts,
			Count:      len(contracts),
		})
		return
	}

	contracts, err := h.querySmartAccountSummaries(ctx, "credential_id = $1", []any{credential}, limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, SmartAccountLookupResponse{
		LookupType: "credential",
		Lookup:     raw,
		Normalized: credential,
		Source:     "silver.smart_account_state",
		Contracts:  contracts,
		Count:      len(contracts),
	})
}

// HandleSmartAccountLookupByAddress serves GET
// /api/v1/silver/smart-accounts/lookup/address/{address}.
func (h *SmartWalletHandlers) HandleSmartAccountLookupByAddress(w http.ResponseWriter, r *http.Request) {
	if h.hotReader == nil {
		respondError(w, "silver_hot reader not available", http.StatusServiceUnavailable)
		return
	}
	raw := strings.TrimSpace(mux.Vars(r)["address"])
	if raw == "" {
		respondError(w, "address required", http.StatusBadRequest)
		return
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()

	limit := parseIntParam(r, "limit", 100, 1, 500)
	if contracts, coverage, err := h.queryServingSmartAccountSummaries(ctx, "address", raw, limit); err == nil && (coverage != nil || len(contracts) > 0) {
		respondJSON(w, SmartAccountLookupResponse{
			LookupType: "address",
			Lookup:     raw,
			Normalized: raw,
			Source:     "serving.sv_smart_account_signers",
			Coverage:   coverage,
			Contracts:  contracts,
			Count:      len(contracts),
		})
		return
	}

	contracts, err := h.querySmartAccountSummaries(ctx, "LOWER(signer_address) = LOWER($1)", []any{raw}, limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, SmartAccountLookupResponse{
		LookupType: "address",
		Lookup:     raw,
		Normalized: raw,
		Source:     "silver.smart_account_state",
		Contracts:  contracts,
		Count:      len(contracts),
	})
}

// HandleSmartAccountState serves GET /api/v1/silver/smart-accounts/{contract_id}/rules.
func (h *SmartWalletHandlers) HandleSmartAccountState(w http.ResponseWriter, r *http.Request) {
	if h.hotReader == nil {
		respondError(w, "silver_hot reader not available", http.StatusServiceUnavailable)
		return
	}
	contractID := strings.TrimSpace(mux.Vars(r)["contract_id"])
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()

	var ruleFilter *int64
	if raw := strings.TrimSpace(r.URL.Query().Get("context_rule_id")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			respondError(w, "invalid context_rule_id", http.StatusBadRequest)
			return
		}
		ruleFilter = &parsed
	}

	if state, err := h.queryServingSmartAccountState(ctx, contractID, ruleFilter); err == nil && state != nil {
		respondJSON(w, state)
		return
	}

	state, err := h.querySmartAccountState(ctx, contractID, ruleFilter)
	if err != nil {
		if err == sql.ErrNoRows {
			respondError(w, "smart account state not found", http.StatusNotFound)
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, state)
}

// HandleSmartAccountStats serves GET /api/v1/silver/smart-accounts/stats.
func (h *SmartWalletHandlers) HandleSmartAccountStats(w http.ResponseWriter, r *http.Request) {
	if h.hotReader == nil {
		respondError(w, "silver_hot reader not available", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()

	if stats, err := h.queryServingSmartAccountStats(ctx); err == nil && stats != nil {
		respondJSON(w, stats)
		return
	}

	var stats SmartAccountStatsResponse
	stats.Source = "silver.smart_account_state"
	var last sql.NullInt64
	err := h.hotReader.db.QueryRowContext(ctx, `
		WITH active_rules AS (
			SELECT contract_id, context_rule_id, last_modified_ledger
			FROM smart_account_context_rules
			WHERE active = TRUE
		), active_signers AS (
			SELECT
				s.contract_id,
				s.context_rule_id,
				COALESCE(s.credential_id, reg.credential_id) AS credential_id,
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
		), active_policies AS (
			SELECT p.contract_id, p.context_rule_id, p.last_modified_ledger
			FROM smart_account_policies p
			JOIN active_rules r
			  ON r.contract_id = p.contract_id
			 AND r.context_rule_id = p.context_rule_id
			WHERE p.scope = 'rule'
			  AND p.active = TRUE
		), contracts AS (
			SELECT contract_id FROM smart_account_context_rules
			UNION
			SELECT contract_id FROM smart_account_signers
			UNION
			SELECT contract_id FROM smart_account_policies
		)
		SELECT
			(SELECT COUNT(*) FROM contracts),
			(SELECT COUNT(*) FROM active_rules),
			(SELECT COUNT(*) FROM active_signers),
			(SELECT COUNT(DISTINCT credential_id) FROM active_signers WHERE credential_id IS NOT NULL),
			(SELECT COUNT(DISTINCT signer_address) FROM active_signers WHERE signer_address IS NOT NULL),
			(SELECT COUNT(*) FROM active_policies),
			(SELECT MAX(last_modified_ledger) FROM (
				SELECT last_modified_ledger FROM smart_account_context_rules
				UNION ALL SELECT last_modified_ledger FROM smart_account_signers
				UNION ALL SELECT last_modified_ledger FROM smart_account_policies
			) m)
	`).Scan(&stats.ContractCount, &stats.ActiveRuleCount, &stats.ActiveSignerCount, &stats.CredentialCount, &stats.AddressSignerCount, &stats.ActivePolicyCount, &last)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if last.Valid {
		stats.LastModifiedLedger = &last.Int64
	}
	respondJSON(w, stats)
}

func (h *SmartWalletHandlers) queryServingSmartAccountSummaries(ctx context.Context, identityType, identityValue string, limit int) ([]SmartAccountContractSummary, *ServingCoverageMetadata, error) {
	coverage, err := h.hotReader.GetServingWatermark(ctx, "serving.sv_smart_account_signers")
	if err != nil {
		return nil, nil, err
	}
	predicate := "s.identity_type = $1 AND s.identity_value = $2"
	args := []any{identityType, identityValue, limit}
	if identityType == "address" {
		predicate = "s.identity_type = $1 AND LOWER(s.identity_value) = LOWER($2)"
	}
	rows, err := h.hotReader.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			c.contract_id,
			c.wallet_type,
			c.context_rule_count,
			c.active_signer_count,
			c.credential_signer_count,
			c.address_signer_count,
			c.active_policy_count,
			c.context_rule_ids,
			c.first_seen_ledger,
			c.last_modified_ledger
		FROM serving.sv_smart_account_signers s
		JOIN serving.sv_smart_account_contracts c ON c.contract_id = s.contract_id
		WHERE %s
		ORDER BY c.last_modified_ledger DESC NULLS LAST, c.contract_id
		LIMIT $3
	`, predicate), args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	out := make([]SmartAccountContractSummary, 0, limit)
	for rows.Next() {
		summary, err := scanSmartAccountContractSummary(rows)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return out, coverage, nil
}

func (h *SmartWalletHandlers) queryServingSmartAccountStats(ctx context.Context) (*SmartAccountStatsResponse, error) {
	coverage, err := h.hotReader.GetServingWatermark(ctx, "serving.sv_smart_account_contracts")
	if err != nil {
		return nil, err
	}
	if coverage == nil {
		return nil, nil
	}
	var stats SmartAccountStatsResponse
	stats.Source = "serving.sv_smart_account_contracts"
	stats.Coverage = coverage
	var last sql.NullInt64
	err = h.hotReader.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*)::bigint,
			COALESCE(SUM(context_rule_count), 0)::bigint,
			COALESCE(SUM(active_signer_count), 0)::bigint,
			(SELECT COUNT(*)::bigint FROM serving.sv_smart_account_signers WHERE identity_type = 'credential'),
			(SELECT COUNT(*)::bigint FROM serving.sv_smart_account_signers WHERE identity_type = 'address'),
			COALESCE(SUM(active_policy_count), 0)::bigint,
			MAX(last_modified_ledger)
		FROM serving.sv_smart_account_contracts
	`).Scan(&stats.ContractCount, &stats.ActiveRuleCount, &stats.ActiveSignerCount, &stats.CredentialCount, &stats.AddressSignerCount, &stats.ActivePolicyCount, &last)
	if err != nil {
		return nil, err
	}
	if last.Valid {
		stats.LastModifiedLedger = &last.Int64
	}
	return &stats, nil
}

func (h *SmartWalletHandlers) queryServingSmartAccountState(ctx context.Context, contractID string, ruleFilter *int64) (*SmartAccountStateResponse, error) {
	coverage, err := h.hotReader.GetServingWatermark(ctx, "serving.sv_smart_account_rules_current")
	if err != nil {
		return nil, err
	}
	if coverage == nil {
		return nil, nil
	}
	summary, err := h.queryServingSmartAccountSummaryByContract(ctx, contractID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	args := []any{contractID}
	ruleWhere := ""
	if ruleFilter != nil {
		args = append(args, *ruleFilter)
		ruleWhere = " AND context_rule_id = $2"
	}
	rows, err := h.hotReader.db.QueryContext(ctx, `
		SELECT
			context_rule_id,
			COALESCE(metadata::text, ''),
			COALESCE(event_type, ''),
			last_modified_ledger,
			COALESCE(transaction_hash, ''),
			closed_at,
			signers_json::text,
			policies_json::text
		FROM serving.sv_smart_account_rules_current
		WHERE contract_id = $1`+ruleWhere+`
		ORDER BY context_rule_id
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rules := []SmartAccountContextRuleRow{}
	for rows.Next() {
		var rule SmartAccountContextRuleRow
		var metadata sql.NullString
		var closedAt sql.NullTime
		var signersJSON, policiesJSON string
		if err := rows.Scan(&rule.ContextRuleID, &metadata, &rule.EventType, &rule.LastModifiedLedger, &rule.TransactionHash, &closedAt, &signersJSON, &policiesJSON); err != nil {
			return nil, err
		}
		rule.Active = true
		rule.Metadata = smartAccountRawJSON(metadata)
		if closedAt.Valid {
			rule.ClosedAt = &closedAt.Time
		}
		rule.Signers = parseServingSmartAccountSigners(signersJSON)
		rule.Policies = parseServingSmartAccountPolicies(policiesJSON)
		rules = append(rules, rule)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(rules) == 0 && ruleFilter == nil {
		return nil, nil
	}
	return &SmartAccountStateResponse{
		ContractID:    contractID,
		Source:        "serving.sv_smart_account_rules_current",
		Coverage:      coverage,
		Summary:       summary,
		ContextRules:  rules,
		ContextRuleID: ruleFilter,
		Count:         len(rules),
	}, nil
}

func (h *SmartWalletHandlers) queryServingSmartAccountSummaryByContract(ctx context.Context, contractID string) (SmartAccountContractSummary, error) {
	row := h.hotReader.db.QueryRowContext(ctx, `
		SELECT
			contract_id,
			wallet_type,
			context_rule_count,
			active_signer_count,
			credential_signer_count,
			address_signer_count,
			active_policy_count,
			context_rule_ids,
			first_seen_ledger,
			last_modified_ledger
		FROM serving.sv_smart_account_contracts
		WHERE contract_id = $1
	`, contractID)
	return scanSmartAccountContractSummary(row)
}

type smartAccountSummaryScanner interface {
	Scan(dest ...any) error
}

func scanSmartAccountContractSummary(scanner smartAccountSummaryScanner) (SmartAccountContractSummary, error) {
	var summary SmartAccountContractSummary
	var ruleIDs pq.Int64Array
	var first sql.NullInt64
	var last sql.NullInt64
	if err := scanner.Scan(
		&summary.ContractID,
		&summary.WalletType,
		&summary.ContextRuleCount,
		&summary.ActiveSignerCount,
		&summary.CredentialSignerCount,
		&summary.AddressSignerCount,
		&summary.ActivePolicyCount,
		&ruleIDs,
		&first,
		&last,
	); err != nil {
		return summary, err
	}
	summary.ContextRuleIDs = []int64(ruleIDs)
	if first.Valid {
		summary.FirstSeenLedger = &first.Int64
	}
	if last.Valid && last.Int64 > 0 {
		summary.LastModifiedLedger = &last.Int64
	}
	return summary, nil
}

func parseServingSmartAccountSigners(raw string) []SmartAccountSignerRow {
	var rows []SmartAccountSignerRow
	type signerJSON struct {
		SignerID           *int64 `json:"signer_id"`
		SignerType         string `json:"signer_type"`
		SignerAddress      string `json:"signer_address"`
		CredentialID       string `json:"credential_id"`
		RawBytes           string `json:"raw_bytes"`
		LastModifiedLedger int64  `json:"last_modified_ledger"`
		TransactionHash    string `json:"transaction_hash"`
		RegistryResolved   bool   `json:"registry_resolved"`
	}
	var parsed []signerJSON
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return rows
	}
	for _, p := range parsed {
		rows = append(rows, SmartAccountSignerRow{
			SignerID:           p.SignerID,
			SignerType:         p.SignerType,
			SignerAddress:      p.SignerAddress,
			CredentialID:       p.CredentialID,
			RawBytes:           p.RawBytes,
			LastModifiedLedger: p.LastModifiedLedger,
			TransactionHash:    p.TransactionHash,
			RegistryResolved:   p.RegistryResolved,
		})
	}
	return rows
}

func parseServingSmartAccountPolicies(raw string) []SmartAccountPolicyRow {
	var rows []SmartAccountPolicyRow
	type policyJSON struct {
		PolicyID           *int64           `json:"policy_id"`
		PolicyAddress      string           `json:"policy_address"`
		InstallParams      *json.RawMessage `json:"install_params"`
		LastModifiedLedger int64            `json:"last_modified_ledger"`
		TransactionHash    string           `json:"transaction_hash"`
		RegistryResolved   bool             `json:"registry_resolved"`
	}
	var parsed []policyJSON
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return rows
	}
	for _, p := range parsed {
		rows = append(rows, SmartAccountPolicyRow{
			PolicyID:           p.PolicyID,
			PolicyAddress:      p.PolicyAddress,
			InstallParams:      p.InstallParams,
			LastModifiedLedger: p.LastModifiedLedger,
			TransactionHash:    p.TransactionHash,
			RegistryResolved:   p.RegistryResolved,
		})
	}
	return rows
}

func (h *SmartWalletHandlers) querySmartAccountSummaries(ctx context.Context, matchPredicate string, args []any, limit int) ([]SmartAccountContractSummary, error) {
	limitArg := len(args) + 1
	query := fmt.Sprintf(`
		WITH active_rules AS (
			SELECT contract_id, context_rule_id, ledger_sequence, last_modified_ledger
			FROM smart_account_context_rules
			WHERE active = TRUE
		), active_signers AS (
			SELECT
				s.contract_id,
				s.context_rule_id,
				s.signer_key,
				COALESCE(s.signer_type, reg.signer_type) AS signer_type,
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
		), matched_contracts AS (
			SELECT DISTINCT contract_id
			FROM active_signers
			WHERE %s
		), rule_rollup AS (
			SELECT
				r.contract_id,
				COUNT(*) AS context_rule_count,
				ARRAY_AGG(r.context_rule_id ORDER BY r.context_rule_id) AS context_rule_ids,
				MIN(r.ledger_sequence) AS first_seen_ledger,
				MAX(r.last_modified_ledger) AS rule_last_modified
			FROM active_rules r
			WHERE r.contract_id IN (SELECT contract_id FROM matched_contracts)
			GROUP BY r.contract_id
		), signer_rollup AS (
			SELECT
				s.contract_id,
				COUNT(DISTINCT s.signer_key) AS active_signer_count,
				COUNT(DISTINCT s.credential_id) FILTER (WHERE s.credential_id IS NOT NULL) AS credential_signer_count,
				COUNT(DISTINCT s.signer_address) FILTER (WHERE s.signer_address IS NOT NULL) AS address_signer_count,
				MAX(s.last_modified_ledger) AS signer_last_modified
			FROM active_signers s
			WHERE s.contract_id IN (SELECT contract_id FROM matched_contracts)
			GROUP BY s.contract_id
		), active_policies AS (
			SELECT p.contract_id, p.policy_key, p.last_modified_ledger
			FROM smart_account_policies p
			JOIN active_rules r
			  ON r.contract_id = p.contract_id
			 AND r.context_rule_id = p.context_rule_id
			WHERE p.scope = 'rule'
			  AND p.active = TRUE
			  AND p.contract_id IN (SELECT contract_id FROM matched_contracts)
		), policy_rollup AS (
			SELECT
				contract_id,
				COUNT(DISTINCT policy_key) AS active_policy_count,
				MAX(last_modified_ledger) AS policy_last_modified
			FROM active_policies
			GROUP BY contract_id
		)
		SELECT
			m.contract_id,
			COALESCE(sec.wallet_type, 'openzeppelin') AS wallet_type,
			COALESCE(rr.context_rule_count, 0),
			COALESCE(sr.active_signer_count, 0),
			COALESCE(sr.credential_signer_count, 0),
			COALESCE(sr.address_signer_count, 0),
			COALESCE(pr.active_policy_count, 0),
			COALESCE(rr.context_rule_ids, ARRAY[]::BIGINT[]),
			rr.first_seen_ledger,
			GREATEST(COALESCE(rr.rule_last_modified, 0), COALESCE(sr.signer_last_modified, 0), COALESCE(pr.policy_last_modified, 0)) AS last_modified_ledger
		FROM matched_contracts m
		LEFT JOIN rule_rollup rr ON rr.contract_id = m.contract_id
		LEFT JOIN signer_rollup sr ON sr.contract_id = m.contract_id
		LEFT JOIN policy_rollup pr ON pr.contract_id = m.contract_id
		LEFT JOIN semantic_entities_contracts sec ON sec.contract_id = m.contract_id
		ORDER BY last_modified_ledger DESC, m.contract_id
		LIMIT $%d
	`, matchPredicate, limitArg)
	args = append(args, limit)

	rows, err := h.hotReader.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]SmartAccountContractSummary, 0, limit)
	for rows.Next() {
		var summary SmartAccountContractSummary
		var ruleIDs pq.Int64Array
		var first sql.NullInt64
		var last sql.NullInt64
		if err := rows.Scan(
			&summary.ContractID,
			&summary.WalletType,
			&summary.ContextRuleCount,
			&summary.ActiveSignerCount,
			&summary.CredentialSignerCount,
			&summary.AddressSignerCount,
			&summary.ActivePolicyCount,
			&ruleIDs,
			&first,
			&last,
		); err != nil {
			return nil, err
		}
		summary.ContextRuleIDs = []int64(ruleIDs)
		if first.Valid {
			summary.FirstSeenLedger = &first.Int64
		}
		if last.Valid && last.Int64 > 0 {
			summary.LastModifiedLedger = &last.Int64
		}
		out = append(out, summary)
	}
	return out, rows.Err()
}

func (h *SmartWalletHandlers) querySmartAccountState(ctx context.Context, contractID string, ruleFilter *int64) (*SmartAccountStateResponse, error) {
	summary, err := h.querySmartAccountSummaryByContract(ctx, contractID)
	if err != nil {
		return nil, err
	}

	args := []any{contractID}
	ruleWhere := ""
	if ruleFilter != nil {
		args = append(args, *ruleFilter)
		ruleWhere = " AND r.context_rule_id = $2"
	}

	rows, err := h.hotReader.db.QueryContext(ctx, `
		SELECT
			r.context_rule_id,
			r.active,
			COALESCE(r.metadata::text, ''),
			COALESCE(r.event_type, ''),
			r.last_modified_ledger,
			COALESCE(r.transaction_hash, ''),
			r.closed_at
		FROM smart_account_context_rules r
		WHERE r.contract_id = $1
		  AND r.active = TRUE`+ruleWhere+`
		ORDER BY r.context_rule_id
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byRule := map[int64]*SmartAccountContextRuleRow{}
	rulePtrs := []*SmartAccountContextRuleRow{}
	for rows.Next() {
		rule := &SmartAccountContextRuleRow{}
		var metadata sql.NullString
		var closedAt sql.NullTime
		if err := rows.Scan(&rule.ContextRuleID, &rule.Active, &metadata, &rule.EventType, &rule.LastModifiedLedger, &rule.TransactionHash, &closedAt); err != nil {
			return nil, err
		}
		rule.Metadata = smartAccountRawJSON(metadata)
		if closedAt.Valid {
			rule.ClosedAt = &closedAt.Time
		}
		rule.Signers = []SmartAccountSignerRow{}
		rule.Policies = []SmartAccountPolicyRow{}
		rulePtrs = append(rulePtrs, rule)
		byRule[rule.ContextRuleID] = rule
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(rulePtrs) == 0 {
		if ruleFilter != nil {
			return &SmartAccountStateResponse{
				ContractID:    contractID,
				Source:        "silver.smart_account_state",
				Summary:       summary,
				ContextRules:  []SmartAccountContextRuleRow{},
				ContextRuleID: ruleFilter,
				Count:         0,
			}, nil
		}
		return nil, sql.ErrNoRows
	}

	if err := h.loadSmartAccountStateSigners(ctx, contractID, ruleFilter, byRule); err != nil {
		return nil, err
	}
	if err := h.loadSmartAccountStatePolicies(ctx, contractID, ruleFilter, byRule); err != nil {
		return nil, err
	}

	rules := make([]SmartAccountContextRuleRow, 0, len(rulePtrs))
	for _, rule := range rulePtrs {
		rules = append(rules, *rule)
	}

	return &SmartAccountStateResponse{
		ContractID:    contractID,
		Source:        "silver.smart_account_state",
		Summary:       summary,
		ContextRules:  rules,
		ContextRuleID: ruleFilter,
		Count:         len(rules),
	}, nil
}

func (h *SmartWalletHandlers) querySmartAccountSummaryByContract(ctx context.Context, contractID string) (SmartAccountContractSummary, error) {
	var summary SmartAccountContractSummary
	summary.ContractID = contractID
	var ruleIDs pq.Int64Array
	var first sql.NullInt64
	var last sql.NullInt64
	err := h.hotReader.db.QueryRowContext(ctx, `
		WITH contract_exists AS (
			SELECT $1::TEXT AS contract_id
			WHERE EXISTS (SELECT 1 FROM smart_account_context_rules WHERE contract_id = $1)
			   OR EXISTS (SELECT 1 FROM smart_account_signers WHERE contract_id = $1)
			   OR EXISTS (SELECT 1 FROM smart_account_policies WHERE contract_id = $1)
		), active_rules AS (
			SELECT contract_id, context_rule_id, ledger_sequence, last_modified_ledger
			FROM smart_account_context_rules
			WHERE contract_id = $1 AND active = TRUE
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
			WHERE s.contract_id = $1
			  AND s.scope = 'rule'
			  AND s.active = TRUE
		), active_policies AS (
			SELECT p.contract_id, p.policy_key, p.last_modified_ledger
			FROM smart_account_policies p
			JOIN active_rules r
			  ON r.contract_id = p.contract_id
			 AND r.context_rule_id = p.context_rule_id
			WHERE p.contract_id = $1
			  AND p.scope = 'rule'
			  AND p.active = TRUE
		)
		SELECT
			ce.contract_id,
			COALESCE(sec.wallet_type, 'openzeppelin') AS wallet_type,
			(SELECT COUNT(*) FROM active_rules),
			(SELECT COUNT(DISTINCT signer_key) FROM active_signers),
			(SELECT COUNT(DISTINCT credential_id) FROM active_signers WHERE credential_id IS NOT NULL),
			(SELECT COUNT(DISTINCT signer_address) FROM active_signers WHERE signer_address IS NOT NULL),
			(SELECT COUNT(DISTINCT policy_key) FROM active_policies),
			COALESCE((SELECT ARRAY_AGG(context_rule_id ORDER BY context_rule_id) FROM active_rules), ARRAY[]::BIGINT[]),
			(SELECT MIN(ledger_sequence) FROM active_rules),
			GREATEST(
				COALESCE((SELECT MAX(last_modified_ledger) FROM active_rules), 0),
				COALESCE((SELECT MAX(last_modified_ledger) FROM active_signers), 0),
				COALESCE((SELECT MAX(last_modified_ledger) FROM active_policies), 0)
			) AS last_modified_ledger
		FROM contract_exists ce
		LEFT JOIN semantic_entities_contracts sec ON sec.contract_id = ce.contract_id
	`, contractID).Scan(
		&summary.ContractID,
		&summary.WalletType,
		&summary.ContextRuleCount,
		&summary.ActiveSignerCount,
		&summary.CredentialSignerCount,
		&summary.AddressSignerCount,
		&summary.ActivePolicyCount,
		&ruleIDs,
		&first,
		&last,
	)
	if err != nil {
		return summary, err
	}
	summary.ContextRuleIDs = []int64(ruleIDs)
	if first.Valid {
		summary.FirstSeenLedger = &first.Int64
	}
	if last.Valid && last.Int64 > 0 {
		summary.LastModifiedLedger = &last.Int64
	}
	return summary, nil
}

func (h *SmartWalletHandlers) loadSmartAccountStateSigners(ctx context.Context, contractID string, ruleFilter *int64, byRule map[int64]*SmartAccountContextRuleRow) error {
	args := []any{contractID}
	ruleWhere := ""
	if ruleFilter != nil {
		args = append(args, *ruleFilter)
		ruleWhere = " AND s.context_rule_id = $2"
	}

	rows, err := h.hotReader.db.QueryContext(ctx, `
		SELECT
			s.context_rule_id,
			s.signer_id,
			COALESCE(s.signer_type, reg.signer_type, ''),
			COALESCE(s.signer_address, reg.signer_address, ''),
			COALESCE(s.credential_id, reg.credential_id, ''),
			COALESCE(s.raw_bytes, reg.raw_bytes, ''),
			GREATEST(s.last_modified_ledger, COALESCE(reg.last_modified_ledger, s.last_modified_ledger)) AS last_modified_ledger,
			COALESCE(s.transaction_hash, ''),
			(reg.signer_id IS NOT NULL) AS registry_resolved
		FROM smart_account_signers s
		JOIN smart_account_context_rules r
		  ON r.contract_id = s.contract_id
		 AND r.context_rule_id = s.context_rule_id
		 AND r.active = TRUE
		LEFT JOIN smart_account_signers reg
		  ON reg.contract_id = s.contract_id
		 AND reg.scope = 'registry'
		 AND reg.signer_id = s.signer_id
		 AND reg.active = TRUE
		WHERE s.contract_id = $1
		  AND s.scope = 'rule'
		  AND s.active = TRUE`+ruleWhere+`
		ORDER BY s.context_rule_id, COALESCE(s.signer_id, 9223372036854775807), s.signer_key
	`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ruleID int64
		var signer SmartAccountSignerRow
		var signerID sql.NullInt64
		if err := rows.Scan(
			&ruleID,
			&signerID,
			&signer.SignerType,
			&signer.SignerAddress,
			&signer.CredentialID,
			&signer.RawBytes,
			&signer.LastModifiedLedger,
			&signer.TransactionHash,
			&signer.RegistryResolved,
		); err != nil {
			return err
		}
		if signerID.Valid {
			signer.SignerID = &signerID.Int64
		}
		if rule := byRule[ruleID]; rule != nil {
			rule.Signers = append(rule.Signers, signer)
		}
	}
	return rows.Err()
}

func (h *SmartWalletHandlers) loadSmartAccountStatePolicies(ctx context.Context, contractID string, ruleFilter *int64, byRule map[int64]*SmartAccountContextRuleRow) error {
	args := []any{contractID}
	ruleWhere := ""
	if ruleFilter != nil {
		args = append(args, *ruleFilter)
		ruleWhere = " AND p.context_rule_id = $2"
	}

	rows, err := h.hotReader.db.QueryContext(ctx, `
		SELECT
			p.context_rule_id,
			p.policy_id,
			COALESCE(p.policy_address, reg.policy_address, ''),
			COALESCE(p.install_params::text, reg.install_params::text, ''),
			GREATEST(p.last_modified_ledger, COALESCE(reg.last_modified_ledger, p.last_modified_ledger)) AS last_modified_ledger,
			COALESCE(p.transaction_hash, ''),
			(reg.policy_id IS NOT NULL) AS registry_resolved
		FROM smart_account_policies p
		JOIN smart_account_context_rules r
		  ON r.contract_id = p.contract_id
		 AND r.context_rule_id = p.context_rule_id
		 AND r.active = TRUE
		LEFT JOIN smart_account_policies reg
		  ON reg.contract_id = p.contract_id
		 AND reg.scope = 'registry'
		 AND reg.policy_id = p.policy_id
		 AND reg.active = TRUE
		WHERE p.contract_id = $1
		  AND p.scope = 'rule'
		  AND p.active = TRUE`+ruleWhere+`
		ORDER BY p.context_rule_id, COALESCE(p.policy_id, 9223372036854775807), p.policy_key
	`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ruleID int64
		var policy SmartAccountPolicyRow
		var policyID sql.NullInt64
		var installParams sql.NullString
		if err := rows.Scan(
			&ruleID,
			&policyID,
			&policy.PolicyAddress,
			&installParams,
			&policy.LastModifiedLedger,
			&policy.TransactionHash,
			&policy.RegistryResolved,
		); err != nil {
			return err
		}
		if policyID.Valid {
			policy.PolicyID = &policyID.Int64
		}
		policy.InstallParams = smartAccountRawJSON(installParams)
		if rule := byRule[ruleID]; rule != nil {
			rule.Policies = append(rule.Policies, policy)
		}
	}
	return rows.Err()
}

func (h *SmartWalletHandlers) getSmartWalletFromSmartAccountState(ctx context.Context, contractID string) (*SmartWalletInfo, error) {
	if h.hotReader == nil {
		return nil, fmt.Errorf("no hot reader")
	}
	summary, err := h.querySmartAccountSummaryByContract(ctx, contractID)
	if err != nil {
		return nil, err
	}
	info := &SmartWalletInfo{
		ContractID:     contractID,
		IsSmartWallet:  true,
		WalletType:     "openzeppelin",
		Implementation: "openzeppelin",
		Confidence:     0.95,
		SignerCount:    summary.ActiveSignerCount,
	}

	rows, err := h.hotReader.db.QueryContext(ctx, `
		SELECT DISTINCT
			COALESCE(s.credential_id, reg.credential_id, s.signer_address, reg.signer_address, s.signer_key) AS id,
			LOWER(COALESCE(s.signer_type, reg.signer_type, 'unknown')) AS key_type,
			COALESCE(s.raw_bytes, reg.raw_bytes, s.signer_address, reg.signer_address, '') AS raw_value
		FROM smart_account_signers s
		JOIN smart_account_context_rules r
		  ON r.contract_id = s.contract_id
		 AND r.context_rule_id = s.context_rule_id
		 AND r.active = TRUE
		LEFT JOIN smart_account_signers reg
		  ON reg.contract_id = s.contract_id
		 AND reg.scope = 'registry'
		 AND reg.signer_id = s.signer_id
		 AND reg.active = TRUE
		WHERE s.contract_id = $1
		  AND s.scope = 'rule'
		  AND s.active = TRUE
		ORDER BY id
	`, contractID)
	if err != nil {
		return info, nil
	}
	defer rows.Close()

	for rows.Next() {
		var signer WalletSignerInfo
		if err := rows.Scan(&signer.ID, &signer.KeyType, &signer.RawValue); err != nil {
			continue
		}
		info.Signers = append(info.Signers, signer)
	}
	if len(info.Signers) > 0 {
		info.SignerCount = len(info.Signers)
	}
	return info, nil
}

func normalizeSmartAccountCredential(raw string) string {
	candidate := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(raw, "0x")))
	if candidate == "" {
		return ""
	}
	if _, err := hex.DecodeString(candidate); err == nil {
		return candidate
	}

	for _, enc := range []*base64.Encoding{
		base64.RawURLEncoding,
		base64.URLEncoding,
		base64.RawStdEncoding,
		base64.StdEncoding,
	} {
		if decoded, err := enc.DecodeString(raw); err == nil && len(decoded) > 0 {
			return strings.ToLower(hex.EncodeToString(decoded))
		}
	}
	return candidate
}

func smartAccountRawJSON(value sql.NullString) *json.RawMessage {
	if !value.Valid {
		return nil
	}
	trimmed := strings.TrimSpace(value.String)
	if trimmed == "" || trimmed == "null" {
		return nil
	}
	raw := json.RawMessage(trimmed)
	return &raw
}

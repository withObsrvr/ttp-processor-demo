package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/lib/pq"
)

// SmartWalletListEntry is a single row in the smart-wallets list response.
type SmartWalletListEntry struct {
	ContractID        string            `json:"contract_id"`
	Source            string            `json:"source"` // "classified" | "heuristic"
	Tier              string            `json:"tier"`   // "passkey" | "oz_or_generic" | "crossmint" | "openzeppelin" | "sep50_generic"
	WalletType        *string           `json:"wallet_type,omitempty"`
	Confidence        float64           `json:"confidence"`
	AdminCalls        int64             `json:"admin_calls"`
	FirstAdminLedger  *int64            `json:"first_admin_ledger,omitempty"`
	LastAdminLedger   *int64            `json:"last_admin_ledger,omitempty"`
	TotalInvocations  int64             `json:"total_invocations"`
	UniqueCallers     int64             `json:"unique_callers"`
	DeployerAccount   *string           `json:"deployer_account,omitempty"`
	WalletSigners     *json.RawMessage  `json:"wallet_signers,omitempty"`
	ObservedFunctions []string          `json:"observed_functions,omitempty"`
}

// smart-wallet detection knobs.
//
// adminFuncsPasskey are admin-functions used by passkey-kit / webauthn-style
// smart wallets. These are strong per-contract wallet signals.
var adminFuncsPasskey = []string{
	"set_webauthn_verifier",
	"allow_signing_key",
	"add_passkey",
}

// adminFuncsOZ are admin-functions used by OpenZeppelin / generic SEP-50-style
// smart wallets. Each individually is a weaker signal than the passkey set but
// the narrow observed_functions surface filter rules out most false positives.
var adminFuncsOZ = []string{
	"add_signer",
	"remove_signer",
	"set_signer",
	"get_signers",
	"add_guardian",
	"remove_guardian",
	"recover",
	"set_threshold",
}

// dappBackendFuncs are observed_functions that rule a contract out — these
// belong to trust-session / game-coordinator contracts that happen to expose
// admin-looking setters but are not per-user custom accounts.
//
// Expand this list as more false positives surface. Drop individual entries
// only if you're confident the function is used by a real wallet implementation.
var dappBackendFuncs = []string{
	"set_game_hub",
	"register_country",
	"set_country_admin",
	"set_country_allowed",
	"create_trust_session",
	"create_trust_group",
	"list_my_trust_groups",
	"list_allowed_countries",
	"get_country_info",
	"get_country_admin",
	"resolve_match",
	"join_session",
	"create_session",
	"admin_delete_session",
	"set_live_session_id",
	"update_location",
}

// HandleSmartWalletsList serves GET /api/v1/silver/smart-wallets
//
// Returns smart-account candidates from two sources, unioned:
//
//  1. CLASSIFIED: semantic_entities_contracts where wallet_type IS NOT NULL
//     (populated by silver-realtime-transformer's transformWalletClassification)
//
//  2. HEURISTIC:  contracts in contract_invocations_raw that called any of
//     adminFuncsPasskey / adminFuncsOZ, excluding any contract whose
//     observed_functions match dappBackendFuncs markers.
//
// Classified rows take precedence — if a contract appears in both, the
// classified row is emitted and the heuristic duplicate is suppressed.
//
// Query params:
//
//	tier=passkey|oz_or_generic|crossmint|openzeppelin|sep50_generic
//	source=classified|heuristic
//	limit=1..500 (default 100)
func (h *SmartWalletHandlers) HandleSmartWalletsList(w http.ResponseWriter, r *http.Request) {
	if h.hotReader == nil {
		respondError(w, "silver_hot reader not available", http.StatusServiceUnavailable)
		return
	}
	ctx := r.Context()

	limit := parseIntParam(r, "limit", 100, 1, 500)
	tier := r.URL.Query().Get("tier")
	source := r.URL.Query().Get("source")

	classified, err := h.listClassifiedSmartWallets(ctx, tier, source, limit)
	if err != nil {
		respondError(w, "classified query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Build a set of already-returned contract_ids so we don't double-count
	// heuristic candidates that the classifier already got to.
	seen := make(map[string]struct{}, len(classified))
	for _, e := range classified {
		seen[e.ContractID] = struct{}{}
	}

	var heuristic []SmartWalletListEntry
	if source == "" || source == "heuristic" {
		heuristic, err = h.listHeuristicSmartWallets(ctx, tier, seen, limit)
		if err != nil {
			respondError(w, "heuristic query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	combined := append(classified, heuristic...)
	if len(combined) > limit {
		combined = combined[:limit]
	}

	respondJSON(w, map[string]any{
		"wallets":          combined,
		"count":            len(combined),
		"classified_count": len(classified),
		"heuristic_count":  len(heuristic),
	})
}

// listClassifiedSmartWallets returns wallets with a populated wallet_type.
func (h *SmartWalletHandlers) listClassifiedSmartWallets(
	ctx context.Context, tier, source string, limit int,
) ([]SmartWalletListEntry, error) {
	if source == "heuristic" {
		return nil, nil
	}

	q := `
		SELECT contract_id, wallet_type, wallet_signers,
		       total_invocations, unique_callers, deployer_account,
		       observed_functions
		FROM semantic_entities_contracts
		WHERE wallet_type IS NOT NULL
	`
	args := []any{}
	argIdx := 1

	// tier filter: "passkey" and "oz_or_generic" are heuristic-only labels;
	// the classifier emits specific wallet_type strings (crossmint/openzeppelin/
	// sep50_generic). Map as best we can.
	switch tier {
	case "":
		// no filter
	case "passkey":
		// Passkey-family is not a distinct classifier output; skip classified results.
		return nil, nil
	case "oz_or_generic":
		q += " AND wallet_type IN ('openzeppelin', 'sep50_generic')"
	case "crossmint", "openzeppelin", "sep50_generic":
		q += " AND wallet_type = $1"
		args = append(args, tier)
		argIdx = 2
	default:
		// Unknown tier — return empty rather than error so caller can keep
		// narrowing via other filters.
		return nil, nil
	}

	q += " ORDER BY total_invocations DESC LIMIT $" + strconv.Itoa(argIdx)
	args = append(args, limit)

	rows, err := h.hotReader.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SmartWalletListEntry
	for rows.Next() {
		var (
			e        SmartWalletListEntry
			wt       sql.NullString
			ws       sql.NullString
			deployer sql.NullString
			funcs    []string
		)
		if err := rows.Scan(
			&e.ContractID, &wt, &ws,
			&e.TotalInvocations, &e.UniqueCallers, &deployer,
			pq.Array(&funcs),
		); err != nil {
			return nil, err
		}
		e.Source = "classified"
		e.Confidence = 0.9
		if wt.Valid {
			e.WalletType = &wt.String
			e.Tier = wt.String
		}
		if ws.Valid && ws.String != "" && ws.String != "null" {
			raw := json.RawMessage(ws.String)
			e.WalletSigners = &raw
		}
		if deployer.Valid {
			e.DeployerAccount = &deployer.String
		}
		if len(funcs) > 0 {
			e.ObservedFunctions = funcs
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// listHeuristicSmartWallets returns candidates derived from the admin-function
// signal on contract_invocations_raw, excluding dapp-backend false positives.
//
// We pass `skip` so classified rows (already returned) aren't duplicated.
func (h *SmartWalletHandlers) listHeuristicSmartWallets(
	ctx context.Context, tier string, skip map[string]struct{}, limit int,
) ([]SmartWalletListEntry, error) {
	// Build the admin-function list based on tier filter.
	var adminFuncs []string
	switch tier {
	case "", "sep50_generic":
		adminFuncs = append(append([]string{}, adminFuncsPasskey...), adminFuncsOZ...)
	case "passkey", "crossmint":
		adminFuncs = adminFuncsPasskey
	case "oz_or_generic", "openzeppelin":
		adminFuncs = adminFuncsOZ
	default:
		return nil, nil
	}

	// Single SQL: aggregate admin-function calls per contract, classify tier,
	// enrich with semantic metadata, exclude dapp backends. Over-fetch and
	// filter `skip` in Go to avoid building a huge NOT IN clause.
	q := `
		WITH signals AS (
			SELECT contract_id,
			       COUNT(*) AS admin_calls,
			       MIN(ledger_sequence) AS first_ls,
			       MAX(ledger_sequence) AS last_ls,
			       BOOL_OR(function_name = ANY($1)) AS has_passkey,
			       BOOL_OR(function_name = ANY($2)) AS has_oz
			FROM contract_invocations_raw
			WHERE function_name = ANY($3)
			GROUP BY contract_id
		)
		SELECT s.contract_id,
		       CASE WHEN s.has_passkey THEN 'passkey' ELSE 'oz_or_generic' END AS tier,
		       s.admin_calls, s.first_ls, s.last_ls,
		       COALESCE(sec.total_invocations, 0),
		       COALESCE(sec.unique_callers, 0),
		       sec.deployer_account,
		       COALESCE(sec.observed_functions, ARRAY[]::text[])
		FROM signals s
		LEFT JOIN semantic_entities_contracts sec USING (contract_id)
		WHERE NOT (COALESCE(sec.observed_functions, ARRAY[]::text[]) && $4)
		ORDER BY s.admin_calls DESC, s.contract_id
		LIMIT $5
	`

	// Over-fetch so that after removing `skip` entries we can still fill `limit`.
	fetchLimit := limit + len(skip)
	if fetchLimit > 2000 {
		fetchLimit = 2000
	}

	rows, err := h.hotReader.db.QueryContext(ctx, q,
		pq.Array(adminFuncsPasskey),
		pq.Array(adminFuncsOZ),
		pq.Array(adminFuncs),
		pq.Array(dappBackendFuncs),
		fetchLimit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]SmartWalletListEntry, 0, limit)
	for rows.Next() {
		var (
			e        SmartWalletListEntry
			tierOut  string
			first    sql.NullInt64
			last     sql.NullInt64
			deployer sql.NullString
			funcs    []string
		)
		if err := rows.Scan(
			&e.ContractID, &tierOut,
			&e.AdminCalls, &first, &last,
			&e.TotalInvocations, &e.UniqueCallers,
			&deployer,
			pq.Array(&funcs),
		); err != nil {
			return nil, err
		}
		if _, dup := skip[e.ContractID]; dup {
			continue
		}
		e.Source = "heuristic"
		e.Tier = tierOut
		// Passkey signals are highly specific; generic add_signer is weaker.
		if tierOut == "passkey" {
			e.Confidence = 0.85
		} else {
			e.Confidence = 0.7
		}
		if first.Valid {
			e.FirstAdminLedger = &first.Int64
		}
		if last.Valid {
			e.LastAdminLedger = &last.Int64
		}
		if deployer.Valid && deployer.String != "" {
			e.DeployerAccount = &deployer.String
		}
		if len(funcs) > 0 {
			e.ObservedFunctions = funcs
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	return out, rows.Err()
}


package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// SemanticHandlers provides HTTP handlers for the semantic layer API
type SemanticHandlers struct {
	hotDB *sql.DB
}

// NewSemanticHandlers creates semantic handlers from a SilverHotReader
func NewSemanticHandlers(hotReader *SilverHotReader) *SemanticHandlers {
	return &SemanticHandlers{hotDB: hotReader.db}
}

// ============================================
// Response Types
// ============================================

type SemanticActivity struct {
	ID                 string  `json:"id"`
	LedgerSequence     int64   `json:"ledger_sequence"`
	Timestamp          string  `json:"timestamp"`
	ActivityType       string  `json:"activity_type"`
	Description        *string `json:"description,omitempty"`
	SourceAccount      *string `json:"source_account,omitempty"`
	DestinationAccount *string `json:"destination_account,omitempty"`
	ContractID         *string `json:"contract_id,omitempty"`
	AssetCode          *string `json:"asset_code,omitempty"`
	AssetIssuer        *string `json:"asset_issuer,omitempty"`
	Amount             *string `json:"amount,omitempty"`
	IsSoroban          bool    `json:"is_soroban"`
	SorobanFunction    *string `json:"soroban_function_name,omitempty"`
	TransactionHash    string  `json:"transaction_hash"`
	OperationIndex     *int    `json:"operation_index,omitempty"`
	Successful         bool    `json:"successful"`
	FeeCharged         *int64  `json:"fee_charged,omitempty"`
}

type SemanticContract struct {
	ContractID       string   `json:"contract_id"`
	ContractType     string   `json:"contract_type"`
	TokenName        *string  `json:"token_name,omitempty"`
	TokenSymbol      *string  `json:"token_symbol,omitempty"`
	TokenDecimals    *int     `json:"token_decimals,omitempty"`
	DeployerAccount  *string  `json:"deployer_account,omitempty"`
	DeployedAt       *string  `json:"deployed_at,omitempty"`
	DeployedLedger   *int64   `json:"deployed_ledger,omitempty"`
	TotalInvocations int64    `json:"total_invocations"`
	LastActivity     *string  `json:"last_activity,omitempty"`
	UniqueCallers    int64    `json:"unique_callers"`
	ObservedFunctions []string `json:"observed_functions,omitempty"`
}

type SemanticFlow struct {
	ID              string  `json:"id"`
	LedgerSequence  int64   `json:"ledger_sequence"`
	Timestamp       string  `json:"timestamp"`
	FlowType        string  `json:"flow_type"`
	FromAccount     *string `json:"from_account,omitempty"`
	ToAccount       *string `json:"to_account,omitempty"`
	ContractID      *string `json:"contract_id,omitempty"`
	AssetCode       *string `json:"asset_code,omitempty"`
	AssetIssuer     *string `json:"asset_issuer,omitempty"`
	AssetType       *string `json:"asset_type,omitempty"`
	Amount          string  `json:"amount"`
	TransactionHash string  `json:"transaction_hash"`
	OperationIndex  *int    `json:"operation_index,omitempty"`
	Successful      bool    `json:"successful"`
}

// ============================================
// Activities Endpoint
// ============================================

// HandleSemanticActivities serves GET /api/v1/semantic/activities
func (h *SemanticHandlers) HandleSemanticActivities(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	limit := parseIntParam(r, "limit", 50, 1, 200)
	account := r.URL.Query().Get("account")
	contractID := r.URL.Query().Get("contract_id")
	activityType := r.URL.Query().Get("activity_type")
	before := r.URL.Query().Get("before")
	after := r.URL.Query().Get("after")

	query := `SELECT id, ledger_sequence, timestamp, activity_type, description,
		source_account, destination_account, contract_id,
		asset_code, asset_issuer, amount,
		is_soroban, soroban_function_name,
		transaction_hash, operation_index, successful, fee_charged
		FROM semantic_activities WHERE 1=1`

	args := []interface{}{}
	argIdx := 1

	if account != "" {
		query += fmt.Sprintf(" AND (source_account = $%d OR destination_account = $%d)", argIdx, argIdx)
		args = append(args, account)
		argIdx++
	}
	if contractID != "" {
		query += fmt.Sprintf(" AND contract_id = $%d", argIdx)
		args = append(args, contractID)
		argIdx++
	}
	if activityType != "" {
		query += fmt.Sprintf(" AND activity_type = $%d", argIdx)
		args = append(args, activityType)
		argIdx++
	}
	if before != "" {
		query += fmt.Sprintf(" AND timestamp < $%d", argIdx)
		args = append(args, before)
		argIdx++
	}
	if after != "" {
		query += fmt.Sprintf(" AND timestamp > $%d", argIdx)
		args = append(args, after)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY timestamp DESC LIMIT $%d", argIdx)
	args = append(args, limit+1) // +1 to detect has_more

	rows, err := h.hotDB.QueryContext(ctx, query, args...)
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	activities, err := scanActivities(rows)
	if err != nil {
		respondSemanticError(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasMore := len(activities) > limit
	if hasMore {
		activities = activities[:limit]
	}

	respondSemanticJSON(w, map[string]interface{}{
		"activities": activities,
		"count":      len(activities),
		"has_more":   hasMore,
	})
}

func scanActivities(rows *sql.Rows) ([]SemanticActivity, error) {
	var results []SemanticActivity
	for rows.Next() {
		var a SemanticActivity
		var amount sql.NullString
		var ts, desc, src, dst, cid, ac, ai, sfn, txh sql.NullString
		var opIdx sql.NullInt32
		var fee sql.NullInt64

		err := rows.Scan(
			&a.ID, &a.LedgerSequence, &ts, &a.ActivityType, &desc,
			&src, &dst, &cid,
			&ac, &ai, &amount,
			&a.IsSoroban, &sfn,
			&txh, &opIdx, &a.Successful, &fee,
		)
		if err != nil {
			return nil, err
		}

		a.Timestamp = ts.String
		a.TransactionHash = txh.String
		if desc.Valid { a.Description = &desc.String }
		if src.Valid { a.SourceAccount = &src.String }
		if dst.Valid { a.DestinationAccount = &dst.String }
		if cid.Valid { a.ContractID = &cid.String }
		if ac.Valid { a.AssetCode = &ac.String }
		if ai.Valid { a.AssetIssuer = &ai.String }
		if amount.Valid { a.Amount = &amount.String }
		if sfn.Valid { a.SorobanFunction = &sfn.String }
		if opIdx.Valid { v := int(opIdx.Int32); a.OperationIndex = &v }
		if fee.Valid { a.FeeCharged = &fee.Int64 }

		results = append(results, a)
	}
	return results, rows.Err()
}

// ============================================
// Contracts Endpoint
// ============================================

// HandleSemanticContracts serves GET /api/v1/semantic/contracts
func (h *SemanticHandlers) HandleSemanticContracts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	limit := parseIntParam(r, "limit", 50, 1, 200)
	contractType := r.URL.Query().Get("contract_type")
	deployer := r.URL.Query().Get("deployer")

	query := `SELECT contract_id, contract_type,
		token_name, token_symbol, token_decimals,
		deployer_account, deployed_at, deployed_ledger,
		total_invocations, last_activity, unique_callers,
		observed_functions
		FROM semantic_entities_contracts WHERE 1=1`

	args := []interface{}{}
	argIdx := 1

	if contractType != "" {
		query += fmt.Sprintf(" AND contract_type = $%d", argIdx)
		args = append(args, contractType)
		argIdx++
	}
	if deployer != "" {
		query += fmt.Sprintf(" AND deployer_account = $%d", argIdx)
		args = append(args, deployer)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY total_invocations DESC LIMIT $%d", argIdx)
	args = append(args, limit+1)

	rows, err := h.hotDB.QueryContext(ctx, query, args...)
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	contracts, err := scanContracts(rows)
	if err != nil {
		respondSemanticError(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasMore := len(contracts) > limit
	if hasMore {
		contracts = contracts[:limit]
	}

	respondSemanticJSON(w, map[string]interface{}{
		"contracts": contracts,
		"count":     len(contracts),
		"has_more":  hasMore,
	})
}

func scanContracts(rows *sql.Rows) ([]SemanticContract, error) {
	var results []SemanticContract
	for rows.Next() {
		var c SemanticContract
		var tn, ts, da, dat sql.NullString
		var td sql.NullInt32
		var dl sql.NullInt64
		var la sql.NullString
		var funcsRaw interface{}

		err := rows.Scan(
			&c.ContractID, &c.ContractType,
			&tn, &ts, &td,
			&da, &dat, &dl,
			&c.TotalInvocations, &la, &c.UniqueCallers,
			&funcsRaw,
		)
		if err != nil {
			return nil, err
		}

		if tn.Valid { c.TokenName = &tn.String }
		if ts.Valid { c.TokenSymbol = &ts.String }
		if td.Valid { v := int(td.Int32); c.TokenDecimals = &v }
		if da.Valid { c.DeployerAccount = &da.String }
		if dat.Valid { c.DeployedAt = &dat.String }
		if dl.Valid { c.DeployedLedger = &dl.Int64 }
		if la.Valid { c.LastActivity = &la.String }

		// Parse PostgreSQL text[] array
		if funcsRaw != nil {
			if raw, ok := funcsRaw.([]byte); ok {
				c.ObservedFunctions = parsePgTextArray(string(raw))
			}
		}

		results = append(results, c)
	}
	return results, rows.Err()
}

// ============================================
// Flows Endpoint
// ============================================

// HandleSemanticFlows serves GET /api/v1/semantic/flows
func (h *SemanticHandlers) HandleSemanticFlows(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	limit := parseIntParam(r, "limit", 50, 1, 200)
	account := r.URL.Query().Get("account")
	assetCode := r.URL.Query().Get("asset_code")
	flowType := r.URL.Query().Get("flow_type")
	before := r.URL.Query().Get("before")
	after := r.URL.Query().Get("after")

	query := `SELECT id, ledger_sequence, timestamp, flow_type,
		from_account, to_account, contract_id,
		asset_code, asset_issuer, asset_type,
		amount, transaction_hash, operation_index, successful
		FROM semantic_flows_value WHERE 1=1`

	args := []interface{}{}
	argIdx := 1

	if account != "" {
		query += fmt.Sprintf(" AND (from_account = $%d OR to_account = $%d)", argIdx, argIdx)
		args = append(args, account)
		argIdx++
	}
	if assetCode != "" {
		query += fmt.Sprintf(" AND asset_code = $%d", argIdx)
		args = append(args, assetCode)
		argIdx++
	}
	if flowType != "" {
		query += fmt.Sprintf(" AND flow_type = $%d", argIdx)
		args = append(args, flowType)
		argIdx++
	}
	if before != "" {
		query += fmt.Sprintf(" AND timestamp < $%d", argIdx)
		args = append(args, before)
		argIdx++
	}
	if after != "" {
		query += fmt.Sprintf(" AND timestamp > $%d", argIdx)
		args = append(args, after)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY timestamp DESC LIMIT $%d", argIdx)
	args = append(args, limit+1)

	rows, err := h.hotDB.QueryContext(ctx, query, args...)
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	flows, err := scanFlows(rows)
	if err != nil {
		respondSemanticError(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasMore := len(flows) > limit
	if hasMore {
		flows = flows[:limit]
	}

	respondSemanticJSON(w, map[string]interface{}{
		"flows":    flows,
		"count":    len(flows),
		"has_more": hasMore,
	})
}

func scanFlows(rows *sql.Rows) ([]SemanticFlow, error) {
	var results []SemanticFlow
	for rows.Next() {
		var f SemanticFlow
		var ts, from, to, cid, ac, ai, at sql.NullString
		var opIdx sql.NullInt32

		err := rows.Scan(
			&f.ID, &f.LedgerSequence, &ts, &f.FlowType,
			&from, &to, &cid,
			&ac, &ai, &at,
			&f.Amount, &f.TransactionHash, &opIdx, &f.Successful,
		)
		if err != nil {
			return nil, err
		}

		f.Timestamp = ts.String
		if from.Valid { f.FromAccount = &from.String }
		if to.Valid { f.ToAccount = &to.String }
		if cid.Valid { f.ContractID = &cid.String }
		if ac.Valid { f.AssetCode = &ac.String }
		if ai.Valid { f.AssetIssuer = &ai.String }
		if at.Valid { f.AssetType = &at.String }
		if opIdx.Valid { v := int(opIdx.Int32); f.OperationIndex = &v }

		results = append(results, f)
	}
	return results, rows.Err()
}

// ============================================
// Helpers
// ============================================

func parseIntParam(r *http.Request, name string, defaultVal, min, max int) int {
	val := r.URL.Query().Get(name)
	if val == "" {
		return defaultVal
	}
	parsed, err := strconv.Atoi(val)
	if err != nil || parsed < min {
		return defaultVal
	}
	if parsed > max {
		return max
	}
	return parsed
}

// parsePgTextArray parses a PostgreSQL text[] literal like {foo,bar,baz}
func parsePgTextArray(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" || s == "{}" || s == "NULL" {
		return nil
	}
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "\"")
		if p != "" && p != "NULL" {
			result = append(result, p)
		}
	}
	return result
}

func respondSemanticJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func respondSemanticError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}


package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

// SemanticHandlers provides HTTP handlers for the semantic layer API
type SemanticHandlers struct {
	unified *UnifiedSilverReader
	duckdb  *UnifiedDuckDBReader
}

// NewSemanticHandlers creates semantic handlers from a UnifiedSilverReader
func NewSemanticHandlers(unified *UnifiedSilverReader) *SemanticHandlers {
	return &SemanticHandlers{unified: unified}
}

// SetDuckDBReader adds the UnifiedDuckDBReader for endpoints that need it
func (h *SemanticHandlers) SetDuckDBReader(reader *UnifiedDuckDBReader) {
	h.duckdb = reader
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
	ContractID        string   `json:"contract_id"`
	ContractType      string   `json:"contract_type"`
	TokenName         *string  `json:"token_name,omitempty"`
	TokenSymbol       *string  `json:"token_symbol,omitempty"`
	TokenDecimals     *int     `json:"token_decimals,omitempty"`
	DeployerAccount   *string  `json:"deployer_account,omitempty"`
	DeployedAt        *string  `json:"deployed_at,omitempty"`
	DeployedLedger    *int64   `json:"deployed_ledger,omitempty"`
	TotalInvocations  int64    `json:"total_invocations"`
	LastActivity      *string  `json:"last_activity,omitempty"`
	UniqueCallers     int64    `json:"unique_callers"`
	ObservedFunctions []string `json:"observed_functions,omitempty"`
	WalletType        *string  `json:"wallet_type,omitempty"`
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
	OperationType   *int    `json:"operation_type,omitempty"`
	Successful      bool    `json:"successful"`
}

// SemanticContractFunction represents a single function's call stats
type SemanticContractFunction struct {
	ContractID      string  `json:"contract_id"`
	FunctionName    string  `json:"function_name"`
	TotalCalls      int64   `json:"total_calls"`
	SuccessfulCalls int64   `json:"successful_calls"`
	FailedCalls     int64   `json:"failed_calls"`
	SuccessRate     float64 `json:"success_rate"`
	UniqueCallers   int64   `json:"unique_callers"`
	FirstCalled     *string `json:"first_called,omitempty"`
	LastCalled      *string `json:"last_called,omitempty"`
}

// SemanticAssetStats represents asset directory stats
type SemanticAssetStats struct {
	AssetKey          string  `json:"asset_key"`
	AssetCode         *string `json:"asset_code,omitempty"`
	AssetIssuer       *string `json:"asset_issuer,omitempty"`
	AssetType         string  `json:"asset_type"`
	TokenName         *string `json:"token_name,omitempty"`
	TokenSymbol       *string `json:"token_symbol,omitempty"`
	TokenDecimals     *int    `json:"token_decimals,omitempty"`
	ContractID        *string `json:"contract_id,omitempty"`
	HolderCount       int64   `json:"holder_count"`
	TransferCount24h  int64   `json:"transfer_count_24h"`
	TransferVolume24h string  `json:"transfer_volume_24h"`
	TransferCount7d   int64   `json:"transfer_count_7d"`
	TransferVolume7d  string  `json:"transfer_volume_7d"`
	MintCount24h      int64   `json:"mint_count_24h"`
	BurnCount24h      int64   `json:"burn_count_24h"`
	FirstSeen         *string `json:"first_seen,omitempty"`
	LastTransfer      *string `json:"last_transfer,omitempty"`
}

// SemanticDexPair represents a DEX trading pair
type SemanticDexPair struct {
	PairKey            string  `json:"pair_key"`
	SellingAssetCode   *string `json:"selling_asset_code,omitempty"`
	SellingAssetIssuer *string `json:"selling_asset_issuer,omitempty"`
	BuyingAssetCode    *string `json:"buying_asset_code,omitempty"`
	BuyingAssetIssuer  *string `json:"buying_asset_issuer,omitempty"`
	TradeCount         int64   `json:"trade_count"`
	TradeCount24h      int64   `json:"trade_count_24h"`
	TradeCount7d       int64   `json:"trade_count_7d"`
	SellingVolume      string  `json:"selling_volume"`
	BuyingVolume       string  `json:"buying_volume"`
	SellingVolume24h   string  `json:"selling_volume_24h"`
	BuyingVolume24h    string  `json:"buying_volume_24h"`
	LastPrice          *string `json:"last_price,omitempty"`
	UniqueSellers      int64   `json:"unique_sellers"`
	UniqueBuyers       int64   `json:"unique_buyers"`
	FirstTrade         *string `json:"first_trade,omitempty"`
	LastTrade          *string `json:"last_trade,omitempty"`
}

// SemanticAccountSummary represents an account's activity summary
type SemanticAccountSummary struct {
	AccountID            string  `json:"account_id"`
	TotalOperations      int64   `json:"total_operations"`
	TotalPaymentsSent    int64   `json:"total_payments_sent"`
	TotalPaymentsRecvd   int64   `json:"total_payments_received"`
	TotalContractCalls   int64   `json:"total_contract_calls"`
	UniqueContractsCalled int64  `json:"unique_contracts_called"`
	TopContractID        *string `json:"top_contract_id,omitempty"`
	TopContractFunction  *string `json:"top_contract_function,omitempty"`
	IsContractDeployer   bool    `json:"is_contract_deployer"`
	ContractsDeployed    int     `json:"contracts_deployed"`
	FirstActivity        *string `json:"first_activity,omitempty"`
	LastActivity         *string `json:"last_activity,omitempty"`
}

// SemanticActivityFilters holds filter params for activities queries
type SemanticActivityFilters struct {
	Account      string
	ContractID   string
	ActivityType string
	Before       *time.Time
	After        *time.Time
	Limit        int
}

// SemanticFlowFilters holds filter params for flows queries
type SemanticFlowFilters struct {
	Account   string
	AssetCode string
	FlowType  string
	Before    *time.Time
	After     *time.Time
	Limit     int
}

// ============================================
// Activities Endpoint
// ============================================

// HandleSemanticActivities serves GET /api/v1/semantic/activities
func (h *SemanticHandlers) HandleSemanticActivities(w http.ResponseWriter, r *http.Request) {
	limit := parseIntParam(r, "limit", 50, 1, 200)

	filters := SemanticActivityFilters{
		Account:      r.URL.Query().Get("account"),
		ContractID:   r.URL.Query().Get("contract_id"),
		ActivityType: r.URL.Query().Get("activity_type"),
		Limit:        limit,
	}

	if before := r.URL.Query().Get("before"); before != "" {
		t, err := time.Parse(time.RFC3339, before)
		if err != nil {
			respondSemanticError(w, "invalid 'before' timestamp: must be RFC3339 format (e.g., 2026-01-01T00:00:00Z)", http.StatusBadRequest)
			return
		}
		filters.Before = &t
	}
	if after := r.URL.Query().Get("after"); after != "" {
		t, err := time.Parse(time.RFC3339, after)
		if err != nil {
			respondSemanticError(w, "invalid 'after' timestamp: must be RFC3339 format (e.g., 2026-01-01T00:00:00Z)", http.StatusBadRequest)
			return
		}
		filters.After = &t
	}

	activities, hasMore, err := h.unified.GetSemanticActivities(r.Context(), filters)
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	respondSemanticJSON(w, map[string]any{
		"activities": activities,
		"count":      len(activities),
		"has_more":   hasMore,
	})
}

// ============================================
// Contracts Endpoint
// ============================================

// HandleSemanticContracts serves GET /api/v1/semantic/contracts
// Note: contracts are UPSERT-only and not flushed to cold — hot-only query
func (h *SemanticHandlers) HandleSemanticContracts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	limit := parseIntParam(r, "limit", 50, 1, 200)
	contractType := r.URL.Query().Get("contract_type")
	deployer := r.URL.Query().Get("deployer")

	query := `SELECT contract_id, contract_type,
		token_name, token_symbol, token_decimals,
		deployer_account, deployed_at, deployed_ledger,
		total_invocations, last_activity, unique_callers,
		observed_functions, wallet_type
		FROM semantic_entities_contracts WHERE 1=1`

	args := []any{}
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

	rows, err := h.unified.hot.db.QueryContext(ctx, query, args...)
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

	respondSemanticJSON(w, map[string]any{
		"contracts": contracts,
		"count":     len(contracts),
		"has_more":  hasMore,
	})
}

// ============================================
// Flows Endpoint
// ============================================

// HandleSemanticFlows serves GET /api/v1/semantic/flows
func (h *SemanticHandlers) HandleSemanticFlows(w http.ResponseWriter, r *http.Request) {
	limit := parseIntParam(r, "limit", 50, 1, 200)

	filters := SemanticFlowFilters{
		Account:   r.URL.Query().Get("account"),
		AssetCode: r.URL.Query().Get("asset_code"),
		FlowType:  r.URL.Query().Get("flow_type"),
		Limit:     limit,
	}

	if before := r.URL.Query().Get("before"); before != "" {
		t, err := time.Parse(time.RFC3339, before)
		if err != nil {
			respondSemanticError(w, "invalid 'before' timestamp: must be RFC3339 format (e.g., 2026-01-01T00:00:00Z)", http.StatusBadRequest)
			return
		}
		filters.Before = &t
	}
	if after := r.URL.Query().Get("after"); after != "" {
		t, err := time.Parse(time.RFC3339, after)
		if err != nil {
			respondSemanticError(w, "invalid 'after' timestamp: must be RFC3339 format (e.g., 2026-01-01T00:00:00Z)", http.StatusBadRequest)
			return
		}
		filters.After = &t
	}

	flows, hasMore, err := h.unified.GetSemanticFlows(r.Context(), filters)
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	respondSemanticJSON(w, map[string]any{
		"flows":    flows,
		"count":    len(flows),
		"has_more": hasMore,
	})
}

// ============================================
// Contract Functions Endpoint
// ============================================

// HandleSemanticContractFunctions serves GET /api/v1/semantic/contracts/functions
// Supports optional ?period=24h|7d|30d to query contract_invocations_raw with time filter
// instead of the materialized semantic_contract_functions table.
func (h *SemanticHandlers) HandleSemanticContractFunctions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	contractID := r.URL.Query().Get("contract_id")
	if contractID == "" {
		respondSemanticError(w, "contract_id parameter is required", http.StatusBadRequest)
		return
	}

	limit := parseIntParam(r, "limit", 50, 1, 200)
	period := r.URL.Query().Get("period")

	// When period is set, query contract_invocations_raw directly with time filter
	if period != "" {
		validPeriods := map[string]string{
			"24h": "24 hours",
			"7d":  "7 days",
			"30d": "30 days",
		}
		interval, ok := validPeriods[period]
		if !ok {
			respondSemanticError(w, "invalid period: must be 24h, 7d, or 30d", http.StatusBadRequest)
			return
		}

		query := fmt.Sprintf(`
			SELECT function_name, COUNT(*) as total_calls,
				COUNT(*) FILTER (WHERE successful = true) as successful_calls,
				COUNT(*) FILTER (WHERE successful = false) as failed_calls,
				COUNT(DISTINCT source_account) as unique_callers,
				MIN(closed_at)::text as first_called,
				MAX(closed_at)::text as last_called
			FROM contract_invocations_raw
			WHERE contract_id = $1 AND closed_at > NOW() - INTERVAL '%s'
			GROUP BY function_name
			ORDER BY total_calls DESC
			LIMIT $2
		`, interval)

		rows, err := h.unified.hot.db.QueryContext(ctx, query, contractID, limit+1)
		if err != nil {
			respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var results []SemanticContractFunction
		for rows.Next() {
			var f SemanticContractFunction
			f.ContractID = contractID
			var fc, lc sql.NullString
			err := rows.Scan(
				&f.FunctionName, &f.TotalCalls, &f.SuccessfulCalls, &f.FailedCalls,
				&f.UniqueCallers, &fc, &lc,
			)
			if err != nil {
				respondSemanticError(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			if fc.Valid {
				f.FirstCalled = &fc.String
			}
			if lc.Valid {
				f.LastCalled = &lc.String
			}
			if f.TotalCalls > 0 {
				f.SuccessRate = float64(f.SuccessfulCalls) / float64(f.TotalCalls)
			}
			results = append(results, f)
		}
		if err := rows.Err(); err != nil {
			respondSemanticError(w, "rows error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		hasMore := len(results) > limit
		if hasMore {
			results = results[:limit]
		}

		respondSemanticJSON(w, map[string]any{
			"functions": results,
			"count":     len(results),
			"has_more":  hasMore,
			"period":    period,
		})
		return
	}

	// Default: use materialized semantic_contract_functions table
	query := `SELECT contract_id, function_name,
		total_calls, successful_calls, failed_calls, unique_callers,
		first_called, last_called
		FROM semantic_contract_functions
		WHERE contract_id = $1
		ORDER BY total_calls DESC
		LIMIT $2`

	rows, err := h.unified.hot.db.QueryContext(ctx, query, contractID, limit+1)
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []SemanticContractFunction
	for rows.Next() {
		var f SemanticContractFunction
		var fc, lc sql.NullString
		err := rows.Scan(
			&f.ContractID, &f.FunctionName,
			&f.TotalCalls, &f.SuccessfulCalls, &f.FailedCalls, &f.UniqueCallers,
			&fc, &lc,
		)
		if err != nil {
			respondSemanticError(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if fc.Valid {
			f.FirstCalled = &fc.String
		}
		if lc.Valid {
			f.LastCalled = &lc.String
		}
		if f.TotalCalls > 0 {
			f.SuccessRate = float64(f.SuccessfulCalls) / float64(f.TotalCalls)
		}
		results = append(results, f)
	}
	if err := rows.Err(); err != nil {
		respondSemanticError(w, "rows error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}

	respondSemanticJSON(w, map[string]any{
		"functions": results,
		"count":     len(results),
		"has_more":  hasMore,
	})
}

// ============================================
// Asset Stats Endpoint
// ============================================

// HandleSemanticAssets serves GET /api/v1/semantic/assets
func (h *SemanticHandlers) HandleSemanticAssets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit := parseIntParam(r, "limit", 50, 1, 200)
	assetType := r.URL.Query().Get("asset_type")
	sort := r.URL.Query().Get("sort")

	query := `SELECT asset_key, asset_code, asset_issuer, asset_type,
		token_name, token_symbol, token_decimals, contract_id,
		holder_count, transfer_count_24h, COALESCE(transfer_volume_24h, 0)::TEXT,
		transfer_count_7d, COALESCE(transfer_volume_7d, 0)::TEXT,
		mint_count_24h, burn_count_24h,
		first_seen, last_transfer
		FROM semantic_asset_stats WHERE 1=1`

	args := []any{}
	argIdx := 1

	if assetType != "" {
		query += fmt.Sprintf(" AND asset_type = $%d", argIdx)
		args = append(args, assetType)
		argIdx++
	}

	switch sort {
	case "volume":
		query += " ORDER BY transfer_volume_24h DESC NULLS LAST"
	case "transfers":
		query += " ORDER BY transfer_count_24h DESC"
	default:
		query += " ORDER BY holder_count DESC"
	}

	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, limit+1)

	rows, err := h.unified.hot.db.QueryContext(ctx, query, args...)
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []SemanticAssetStats
	for rows.Next() {
		var a SemanticAssetStats
		var ac, ai, tn, ts, cid, fs, lt sql.NullString
		var td sql.NullInt32
		err := rows.Scan(
			&a.AssetKey, &ac, &ai, &a.AssetType,
			&tn, &ts, &td, &cid,
			&a.HolderCount, &a.TransferCount24h, &a.TransferVolume24h,
			&a.TransferCount7d, &a.TransferVolume7d,
			&a.MintCount24h, &a.BurnCount24h,
			&fs, &lt,
		)
		if err != nil {
			respondSemanticError(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if ac.Valid {
			a.AssetCode = &ac.String
		}
		if ai.Valid {
			a.AssetIssuer = &ai.String
		}
		if tn.Valid {
			a.TokenName = &tn.String
		}
		if ts.Valid {
			a.TokenSymbol = &ts.String
		}
		if td.Valid {
			v := int(td.Int32)
			a.TokenDecimals = &v
		}
		if cid.Valid {
			a.ContractID = &cid.String
		}
		if fs.Valid {
			a.FirstSeen = &fs.String
		}
		if lt.Valid {
			a.LastTransfer = &lt.String
		}
		results = append(results, a)
	}
	if err := rows.Err(); err != nil {
		respondSemanticError(w, "rows error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}

	respondSemanticJSON(w, map[string]any{
		"assets":   results,
		"count":    len(results),
		"has_more": hasMore,
	})
}

// ============================================
// DEX Pairs Endpoint
// ============================================

// HandleSemanticDexPairs serves GET /api/v1/semantic/dex/pairs
func (h *SemanticHandlers) HandleSemanticDexPairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit := parseIntParam(r, "limit", 50, 1, 200)
	assetCode := r.URL.Query().Get("asset_code")
	sort := r.URL.Query().Get("sort")

	query := `SELECT pair_key, selling_asset_code, selling_asset_issuer,
		buying_asset_code, buying_asset_issuer,
		trade_count, trade_count_24h, trade_count_7d,
		COALESCE(selling_volume, 0)::TEXT, COALESCE(buying_volume, 0)::TEXT,
		COALESCE(selling_volume_24h, 0)::TEXT, COALESCE(buying_volume_24h, 0)::TEXT,
		last_price::TEXT, unique_sellers, unique_buyers,
		first_trade, last_trade
		FROM semantic_dex_pairs WHERE 1=1`

	args := []any{}
	argIdx := 1

	if assetCode != "" {
		if strings.EqualFold(assetCode, "XLM") {
			// Native XLM pairs have NULL asset_code, so match both NULL and 'XLM'
			query += fmt.Sprintf(" AND (selling_asset_code = $%d OR buying_asset_code = $%d OR selling_asset_code IS NULL OR buying_asset_code IS NULL)", argIdx, argIdx)
		} else {
			query += fmt.Sprintf(" AND (selling_asset_code = $%d OR buying_asset_code = $%d)", argIdx, argIdx)
		}
		args = append(args, assetCode)
		argIdx++
	}

	switch sort {
	case "trades":
		query += " ORDER BY trade_count_24h DESC"
	default:
		query += " ORDER BY selling_volume_24h DESC NULLS LAST"
	}

	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, limit+1)

	rows, err := h.unified.hot.db.QueryContext(ctx, query, args...)
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []SemanticDexPair
	for rows.Next() {
		var d SemanticDexPair
		var sac, sai, bac, bai, lp, ft, lt sql.NullString
		err := rows.Scan(
			&d.PairKey, &sac, &sai, &bac, &bai,
			&d.TradeCount, &d.TradeCount24h, &d.TradeCount7d,
			&d.SellingVolume, &d.BuyingVolume,
			&d.SellingVolume24h, &d.BuyingVolume24h,
			&lp, &d.UniqueSellers, &d.UniqueBuyers,
			&ft, &lt,
		)
		if err != nil {
			respondSemanticError(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if sac.Valid {
			d.SellingAssetCode = &sac.String
		}
		if sai.Valid {
			d.SellingAssetIssuer = &sai.String
		}
		if bac.Valid {
			d.BuyingAssetCode = &bac.String
		}
		if bai.Valid {
			d.BuyingAssetIssuer = &bai.String
		}
		if lp.Valid {
			d.LastPrice = &lp.String
		}
		if ft.Valid {
			d.FirstTrade = &ft.String
		}
		if lt.Valid {
			d.LastTrade = &lt.String
		}
		results = append(results, d)
	}
	if err := rows.Err(); err != nil {
		respondSemanticError(w, "rows error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}

	respondSemanticJSON(w, map[string]any{
		"pairs":    results,
		"count":    len(results),
		"has_more": hasMore,
	})
}

// ============================================
// Account Summary Endpoint
// ============================================

// HandleSemanticAccountSummary serves GET /api/v1/semantic/accounts/summary
func (h *SemanticHandlers) HandleSemanticAccountSummary(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	accountID := r.URL.Query().Get("account_id")
	if accountID == "" {
		respondSemanticError(w, "account_id parameter is required", http.StatusBadRequest)
		return
	}

	query := `SELECT account_id, total_operations,
		total_payments_sent, total_payments_received,
		total_contract_calls, unique_contracts_called,
		top_contract_id, top_contract_function,
		is_contract_deployer, contracts_deployed,
		first_activity, last_activity
		FROM semantic_account_summary
		WHERE account_id = $1`

	var s SemanticAccountSummary
	var tcid, tcf, fa, la sql.NullString
	var isDeployer sql.NullBool
	err := h.unified.hot.db.QueryRowContext(ctx, query, accountID).Scan(
		&s.AccountID, &s.TotalOperations,
		&s.TotalPaymentsSent, &s.TotalPaymentsRecvd,
		&s.TotalContractCalls, &s.UniqueContractsCalled,
		&tcid, &tcf,
		&isDeployer, &s.ContractsDeployed,
		&fa, &la,
	)
	if err == sql.ErrNoRows {
		respondSemanticJSON(w, map[string]any{
			"account": nil,
			"found":   false,
		})
		return
	}
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	s.IsContractDeployer = isDeployer.Valid && isDeployer.Bool
	if tcid.Valid {
		s.TopContractID = &tcid.String
	}
	if tcf.Valid {
		s.TopContractFunction = &tcf.String
	}
	if fa.Valid {
		s.FirstActivity = &fa.String
	}
	if la.Valid {
		s.LastActivity = &la.String
	}

	respondSemanticJSON(w, map[string]any{
		"account": s,
		"found":   true,
	})
}

// ============================================
// Token Summary Endpoint
// ============================================

// SemanticTokenSummary represents a combined token metadata + optional balance response
type SemanticTokenSummary struct {
	ContractID    string  `json:"contract_id"`
	TokenName     *string `json:"token_name,omitempty"`
	TokenSymbol   *string `json:"token_symbol,omitempty"`
	TokenDecimals *int    `json:"token_decimals,omitempty"`
	TokenType     *string `json:"token_type,omitempty"`
	Balance       *string `json:"balance,omitempty"`
	HolderCount   *int64  `json:"holder_count,omitempty"`
	TransferCount *int64  `json:"transfer_count,omitempty"`
	FirstSeen     *string `json:"first_seen,omitempty"`
	LastActivity  *string `json:"last_activity,omitempty"`
}

// HandleSemanticTokenSummary serves GET /api/v1/semantic/tokens/{contract_id}
// Returns combined token metadata and optional balance in a single call.
// Use ?address=G... to include balance for a specific address.
func (h *SemanticHandlers) HandleSemanticTokenSummary(w http.ResponseWriter, r *http.Request) {
	if h.duckdb == nil {
		respondSemanticError(w, "token summary requires DuckDB unified reader (unified-duckdb) to be configured", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	contractID := vars["contract_id"]
	if contractID == "" {
		respondSemanticError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	address := r.URL.Query().Get("address")
	ctx := r.Context()

	// Get token metadata (reuses existing reader method)
	meta, err := h.duckdb.GetSEP41TokenMetadata(ctx, contractID)
	if err != nil {
		respondSemanticError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Token is "found" if it has registry data (name/symbol) or transfer history
	found := meta.Name != nil || meta.Symbol != nil || meta.TransferCount > 0
	if !found {
		respondSemanticJSON(w, map[string]any{
			"token": nil,
			"found": false,
		})
		return
	}

	// Build response from metadata
	result := SemanticTokenSummary{
		ContractID:    contractID,
		TokenName:     meta.Name,
		TokenSymbol:   meta.Symbol,
		HolderCount:   &meta.HolderCount,
		TransferCount: &meta.TransferCount,
	}
	if meta.Decimals >= 0 {
		result.TokenDecimals = &meta.Decimals
	}
	if meta.TokenType != "" {
		result.TokenType = &meta.TokenType
	}
	if meta.FirstSeen != "" {
		result.FirstSeen = &meta.FirstSeen
	}
	if meta.LastActivity != "" {
		result.LastActivity = &meta.LastActivity
	}

	// If address provided, get balance too
	if address != "" {
		balance, err := h.duckdb.GetSEP41SingleBalance(ctx, contractID, address)
		if err == nil {
			result.Balance = &balance.Balance
		} else if errors.Is(err, sql.ErrNoRows) {
			zero := "0.0000000"
			result.Balance = &zero
		} else {
			respondSemanticError(w, "balance query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	respondSemanticJSON(w, map[string]any{
		"token": result,
		"found": true,
	})
}

// ============================================
// Scanners (shared between hot and cold)
// ============================================

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

func scanContracts(rows *sql.Rows) ([]SemanticContract, error) {
	var results []SemanticContract
	for rows.Next() {
		var c SemanticContract
		var tn, ts, da, dat sql.NullString
		var td sql.NullInt32
		var dl sql.NullInt64
		var la sql.NullString
		var funcs []string

		var wt sql.NullString
		err := rows.Scan(
			&c.ContractID, &c.ContractType,
			&tn, &ts, &td,
			&da, &dat, &dl,
			&c.TotalInvocations, &la, &c.UniqueCallers,
			pq.Array(&funcs), &wt,
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
		if len(funcs) > 0 {
			c.ObservedFunctions = funcs
		}
		if wt.Valid { c.WalletType = &wt.String }

		results = append(results, c)
	}
	return results, rows.Err()
}

func scanFlows(rows *sql.Rows) ([]SemanticFlow, error) {
	var results []SemanticFlow
	for rows.Next() {
		var f SemanticFlow
		var ts, from, to, cid, ac, ai, at sql.NullString
		var opType sql.NullInt32

		err := rows.Scan(
			&f.ID, &f.LedgerSequence, &ts, &f.FlowType,
			&from, &to, &cid,
			&ac, &ai, &at,
			&f.Amount, &f.TransactionHash, &opType, &f.Successful,
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
		if opType.Valid { v := int(opType.Int32); f.OperationType = &v }

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

func respondSemanticJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func respondSemanticError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

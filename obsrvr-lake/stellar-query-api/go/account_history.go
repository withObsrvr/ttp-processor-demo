package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/stellar/go-stellar-sdk/strkey"
)

type AccountTransaction struct {
	LedgerSequence  int64    `json:"ledger_sequence"`
	ClosedAt        string   `json:"closed_at"`
	TransactionHash string   `json:"transaction_hash"`
	PagingToken     string   `json:"-"`
	Successful      *bool    `json:"successful,omitempty"`
	SourceAccount   *string  `json:"source_account,omitempty"`
	FeeCharged      *string  `json:"fee_charged,omitempty"`
	MemoType        *string  `json:"memo_type,omitempty"`
	Memo            *string  `json:"memo,omitempty"`
	ActivityTypes   []string `json:"activity_types"`
	SourceTables    []string `json:"source_tables"`
	Summary         string   `json:"summary"`
}

type AccountTransactionsResponse struct {
	AccountID    string               `json:"account_id"`
	Transactions []AccountTransaction `json:"transactions"`
	Count        int                  `json:"count"`
	Cursor       string               `json:"cursor,omitempty"`
	HasMore      bool                 `json:"has_more"`
	Coverage     HistoryCoverage      `json:"coverage"`
	Partial      bool                 `json:"partial,omitempty"`
	Warnings     []string             `json:"warnings,omitempty"`
}

type BalanceHistoryPoint struct {
	LedgerSequence  int64   `json:"ledger_sequence"`
	ClosedAt        string  `json:"closed_at"`
	AssetType       string  `json:"asset_type"`
	AssetCode       *string `json:"asset_code,omitempty"`
	AssetIssuer     *string `json:"asset_issuer,omitempty"`
	ContractID      *string `json:"contract_id,omitempty"`
	Balance         string  `json:"balance"`
	Delta           *string `json:"delta,omitempty"`
	TransactionHash *string `json:"transaction_hash,omitempty"`
	SourceTable     string  `json:"source_table"`
}

type BalanceHistoryResponse struct {
	Address    string                `json:"address"`
	Asset      string                `json:"asset,omitempty"`
	ContractID string                `json:"contract_id,omitempty"`
	History    []BalanceHistoryPoint `json:"history"`
	Count      int                   `json:"count"`
	Cursor     string                `json:"cursor,omitempty"`
	HasMore    bool                  `json:"has_more"`
	Coverage   HistoryCoverage       `json:"coverage"`
}

type HistoryCoverage struct {
	Version      string                      `json:"version"`
	Source       string                      `json:"source,omitempty"`
	Includes     []string                    `json:"includes"`
	Limitations  []string                    `json:"limitations"`
	AccountIndex *AccountLedgerIndexCoverage `json:"account_index,omitempty"`
}

type HistoryCursor struct {
	LedgerSequence  int64
	ClosedAt        time.Time
	TransactionHash string
	TieBreaker      string
	Order           string
}

func (c HistoryCursor) Encode() string {
	payload := struct {
		LedgerSequence  int64  `json:"l"`
		ClosedAt        string `json:"t"`
		TransactionHash string `json:"h,omitempty"`
		TieBreaker      string `json:"b,omitempty"`
		Order           string `json:"o"`
	}{c.LedgerSequence, c.ClosedAt.Format(time.RFC3339Nano), c.TransactionHash, c.TieBreaker, c.Order}
	raw, _ := json.Marshal(payload)
	return base64.URLEncoding.EncodeToString(raw)
}

func DecodeHistoryCursor(cursor string) (*HistoryCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	var payload struct {
		LedgerSequence  int64  `json:"l"`
		ClosedAt        string `json:"t"`
		TransactionHash string `json:"h"`
		TieBreaker      string `json:"b"`
		Order           string `json:"o"`
	}
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, fmt.Errorf("invalid cursor format: %w", err)
	}
	ts, err := time.Parse(time.RFC3339Nano, payload.ClosedAt)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp in cursor: %w", err)
	}
	order := payload.Order
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		return nil, fmt.Errorf("invalid order in cursor")
	}
	return &HistoryCursor{LedgerSequence: payload.LedgerSequence, ClosedAt: ts, TransactionHash: payload.TransactionHash, TieBreaker: payload.TieBreaker, Order: order}, nil
}

type AccountTransactionsFilters struct {
	AccountID   string
	Limit       int
	Cursor      *HistoryCursor
	StartLedger int64
	EndLedger   int64
	StartTime   string
	EndTime     string
	Successful  *bool
	Order       string
}

type BalanceHistoryFilters struct {
	Address     string
	Asset       string
	ContractID  string
	Limit       int
	Cursor      *HistoryCursor
	StartLedger int64
	EndLedger   int64
	Order       string
}

func historyCoverage(accountIndex *AccountLedgerIndexCoverage) HistoryCoverage {
	includes := []string{
		"hot+cold query-layer federation with hot-over-cold de-duplication",
		"classic operations from enriched_history_operations",
		"Soroban token transfers from token_transfers_raw",
		"Soroban invocations from contract_invocations_raw",
	}
	limitations := []string{
		"v1 is query-layer first; very high-cardinality accounts may require a future materialized account_transactions index",
		"historical Soroban token balances are reconstructed from observed token_transfers_raw deltas",
	}
	if accountIndex != nil && accountIndex.Enabled {
		includes = append(includes, "account_ledger_index coverage metadata is exposed for cold account-history routing")
		if accountIndex.Used {
			includes = append(includes, "cold account transaction scans were pruned by account_ledger_index ledger ranges")
		}
		if accountIndex.Uncovered {
			limitations = append(limitations, "account_ledger_index coverage does not cover the full queried span; cold query fell back to the unpruned path")
		}
	}
	return HistoryCoverage{Version: "v1", Source: "federated", Includes: includes, Limitations: limitations, AccountIndex: accountIndex}
}

func accountTransactionFeedEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(envOrDefault("ACCOUNT_TX_FEED_ENABLED", "false")))
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func validAccountAddress(address string) bool {
	_, err := strkey.Decode(strkey.VersionByteAccountID, address)
	return err == nil
}

// HandleAccountTransactions returns full-history account-indexed transactions.
// @Router /api/v1/silver/accounts/{id}/transactions [get]
func (h *SilverHandlers) HandleAccountTransactions(w http.ResponseWriter, r *http.Request) {
	accountID := mux.Vars(r)["id"]
	if !validAccountAddress(accountID) {
		respondError(w, "id must be a valid Stellar account (G...) address", http.StatusBadRequest)
		return
	}
	if h.unifiedReader == nil {
		respondError(w, "account transactions endpoint requires unified reader", http.StatusServiceUnavailable)
		return
	}
	filters := AccountTransactionsFilters{AccountID: accountID, Limit: parseLimit(r, 100, 500), Order: strings.ToLower(r.URL.Query().Get("order"))}
	if filters.Order == "" {
		filters.Order = "desc"
	}
	if filters.Order != "asc" && filters.Order != "desc" {
		respondError(w, "order must be asc or desc", http.StatusBadRequest)
		return
	}
	var err error
	filters.Cursor, err = DecodeHistoryCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if filters.Cursor != nil && filters.Cursor.Order != "" {
		filters.Order = filters.Cursor.Order
	}
	if v := r.URL.Query().Get("start_ledger"); v != "" {
		filters.StartLedger, err = strconv.ParseInt(v, 10, 64)
		if err != nil || filters.StartLedger < 0 {
			respondError(w, "invalid start_ledger", http.StatusBadRequest)
			return
		}
	}
	if v := r.URL.Query().Get("end_ledger"); v != "" {
		filters.EndLedger, err = strconv.ParseInt(v, 10, 64)
		if err != nil || filters.EndLedger < 0 {
			respondError(w, "invalid end_ledger", http.StatusBadRequest)
			return
		}
	}
	filters.StartTime = r.URL.Query().Get("start_time")
	if filters.StartTime != "" {
		if _, err := time.Parse(time.RFC3339, filters.StartTime); err != nil {
			respondError(w, "invalid start_time; expected RFC3339 timestamp", http.StatusBadRequest)
			return
		}
	}
	filters.EndTime = r.URL.Query().Get("end_time")
	if filters.EndTime != "" {
		if _, err := time.Parse(time.RFC3339, filters.EndTime); err != nil {
			respondError(w, "invalid end_time; expected RFC3339 timestamp", http.StatusBadRequest)
			return
		}
	}
	if v := r.URL.Query().Get("success"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			respondError(w, "invalid success", http.StatusBadRequest)
			return
		}
		filters.Successful = &b
	}
	ctx, cancel := withInteractiveQueryTimeout(r.Context())
	defer cancel()
	forceSource := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("source")))
	if forceSource != "" && forceSource != "feed" && forceSource != "federated" {
		respondError(w, "source must be feed or federated", http.StatusBadRequest)
		return
	}
	if forceSource == "feed" && !accountTransactionFeedEnabled() {
		respondError(w, "source=feed requires ACCOUNT_TX_FEED_ENABLED=true", http.StatusServiceUnavailable)
		return
	}
	tryFeed := accountTransactionFeedEnabled() && forceSource != "federated"
	if tryFeed && h.legacyReader != nil && h.legacyReader.hot != nil {
		txs, next, hasMore, covered, err := h.legacyReader.hot.GetServingAccountTransactions(ctx, filters)
		if err == nil && covered {
			coverage := historyCoverage(nil)
			coverage.Source = "feed"
			coverage.Includes = append(coverage.Includes, "serving.sv_transactions_by_account feed fast path")
			respondJSON(w, AccountTransactionsResponse{
				AccountID:    accountID,
				Transactions: txs,
				Count:        len(txs),
				Cursor:       next,
				HasMore:      hasMore,
				Coverage:     coverage,
			})
			return
		}
		if forceSource == "feed" && err == nil && !covered {
			respondError(w, "serving account transaction feed does not cover requested span", http.StatusServiceUnavailable)
			return
		}
		if err != nil {
			if forceSource == "feed" {
				respondError(w, err.Error(), http.StatusInternalServerError)
				return
			}
			log.Printf("account transaction feed unavailable account=%s err=%v; falling back to federated path", accountID, err)
		}
	}
	txs, next, hasMore, accountIndexCoverage, err := h.unifiedReader.GetAccountTransactions(ctx, filters)
	if err != nil {
		if isQueryTimeout(err) {
			respondQueryTimeout(w, "account transactions")
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := AccountTransactionsResponse{
		AccountID:    accountID,
		Transactions: txs,
		Count:        len(txs),
		Cursor:       next,
		HasMore:      hasMore,
		Coverage:     historyCoverage(&accountIndexCoverage),
	}
	if accountIndexCoverage.Uncovered {
		resp.Partial = true
		resp.Warnings = append(resp.Warnings, "account_ledger_index coverage does not cover the full queried span; cold account history used the timeout-bounded unpruned path")
	}
	if accountIndexCoverage.LookupFailed {
		resp.Partial = true
		resp.Warnings = append(resp.Warnings, "account_ledger_index lookup timed out or failed; cold account history was skipped to avoid an unpruned cold scan")
	}
	respondJSON(w, resp)
}

// HandleAddressBalanceHistory returns historical balances for a classic asset or Soroban token contract.
// @Router /api/v1/silver/addresses/{addr}/balances/history [get]
func (h *SilverHandlers) HandleAddressBalanceHistory(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["addr"]
	if !isValidStellarAddress(addr) {
		respondError(w, "addr must be a valid Stellar account (G...) or contract (C...) address", http.StatusBadRequest)
		return
	}
	if h.unifiedReader == nil {
		respondError(w, "balance history endpoint requires unified reader", http.StatusServiceUnavailable)
		return
	}
	filters := BalanceHistoryFilters{Address: addr, Asset: r.URL.Query().Get("asset"), ContractID: r.URL.Query().Get("contract_id"), Limit: parseLimit(r, 100, 500), Order: strings.ToLower(r.URL.Query().Get("order"))}
	if filters.Order == "" {
		filters.Order = "desc"
	}
	if filters.Order != "asc" && filters.Order != "desc" {
		respondError(w, "order must be asc or desc", http.StatusBadRequest)
		return
	}
	if (filters.Asset == "") == (filters.ContractID == "") {
		respondError(w, "provide exactly one of asset or contract_id", http.StatusBadRequest)
		return
	}
	if filters.ContractID != "" {
		if _, err := strkey.Decode(strkey.VersionByteContract, filters.ContractID); err != nil {
			respondError(w, "contract_id must be a valid Stellar contract (C...) address", http.StatusBadRequest)
			return
		}
	}
	var err error
	filters.Cursor, err = DecodeHistoryCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if filters.Cursor != nil && filters.Cursor.Order != "" {
		filters.Order = filters.Cursor.Order
	}
	if v := r.URL.Query().Get("start_ledger"); v != "" {
		filters.StartLedger, err = strconv.ParseInt(v, 10, 64)
		if err != nil || filters.StartLedger < 0 {
			respondError(w, "invalid start_ledger", http.StatusBadRequest)
			return
		}
	}
	if v := r.URL.Query().Get("end_ledger"); v != "" {
		filters.EndLedger, err = strconv.ParseInt(v, 10, 64)
		if err != nil || filters.EndLedger < 0 {
			respondError(w, "invalid end_ledger", http.StatusBadRequest)
			return
		}
	}
	history, next, hasMore, err := h.unifiedReader.GetAddressBalanceHistory(r.Context(), filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, BalanceHistoryResponse{Address: addr, Asset: filters.Asset, ContractID: filters.ContractID, History: history, Count: len(history), Cursor: next, HasMore: hasMore, Coverage: historyCoverage(nil)})
}

func (r *UnifiedDuckDBReader) GetAccountTransactions(ctx context.Context, filters AccountTransactionsFilters) ([]AccountTransaction, string, bool, AccountLedgerIndexCoverage, error) {
	if strings.TrimSpace(r.hotSchema) == "" && strings.TrimSpace(r.coldSchema) == "" {
		return nil, "", false, AccountLedgerIndexCoverage{}, fmt.Errorf("GetAccountTransactions: no hot or cold schema configured")
	}
	accountIndexCoverage := initialAccountIndexCoverage(r.accountIndex)
	requestLimit := filters.Limit + 1
	orderDir, cursorOp := "DESC", "<"
	if filters.Order == "asc" {
		orderDir, cursorOp = "ASC", ">"
	}
	where := []string{"1=1"}
	args := []interface{}{filters.AccountID}
	arg := 2
	if filters.StartLedger > 0 {
		where = append(where, fmt.Sprintf("ledger_sequence >= $%d", arg))
		args = append(args, filters.StartLedger)
		arg++
	}
	if filters.EndLedger > 0 {
		where = append(where, fmt.Sprintf("ledger_sequence <= $%d", arg))
		args = append(args, filters.EndLedger)
		arg++
	}
	if filters.StartTime != "" {
		where = append(where, fmt.Sprintf("closed_at >= CAST($%d AS TIMESTAMP)", arg))
		args = append(args, filters.StartTime)
		arg++
	}
	if filters.EndTime != "" {
		where = append(where, fmt.Sprintf("closed_at <= CAST($%d AS TIMESTAMP)", arg))
		args = append(args, filters.EndTime)
		arg++
	}
	if filters.Successful != nil {
		where = append(where, fmt.Sprintf("COALESCE(successful, false) = $%d", arg))
		args = append(args, *filters.Successful)
		arg++
	}
	if filters.Cursor != nil {
		where = append(where, fmt.Sprintf(`(ledger_sequence %s $%d OR (ledger_sequence = $%d AND closed_at %s $%d) OR (ledger_sequence = $%d AND closed_at = $%d AND transaction_hash %s $%d))`, cursorOp, arg, arg, cursorOp, arg+1, arg, arg+1, cursorOp, arg+2))
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.ClosedAt, filters.Cursor.TransactionHash)
		arg += 3
	}
	coldLedgerRangeClause := ""
	if strings.TrimSpace(r.coldSchema) != "" && r.accountIndex.CanPrune() {
		accountIndexCoverage = r.accountIndex.LoadCoverage(ctx)
		if accountIndexCoverage.Covers(filters.StartLedger, filters.EndLedger) {
			lookupCtx, cancel := context.WithTimeout(ctx, accountLedgerIndexLookupTimeout())
			ranges, err := r.accountIndex.LookupLedgerRanges(lookupCtx, filters.AccountID, filters.StartLedger, filters.EndLedger)
			cancel()
			if err != nil {
				log.Printf("account ledger index lookup failed account=%s err=%v; skipping cold account transaction scan to avoid unpruned fallback", filters.AccountID, err)
				coldLedgerRangeClause = " AND false"
				accountIndexCoverage.Used = true
				accountIndexCoverage.SkippedCold = true
				accountIndexCoverage.LookupFailed = true
			} else if len(ranges) > 0 {
				placeholders := make([]string, 0, len(ranges))
				for _, ledgerRange := range ranges {
					placeholders = append(placeholders, fmt.Sprintf("$%d", arg))
					args = append(args, ledgerRange)
					arg++
				}
				coldLedgerRangeClause = fmt.Sprintf(" AND ledger_range IN (%s)", strings.Join(placeholders, ", "))
				accountIndexCoverage.Used = true
				accountIndexCoverage.PrunedRanges = ranges
			} else {
				coldLedgerRangeClause = " AND false"
				accountIndexCoverage.Used = true
				accountIndexCoverage.SkippedCold = true
			}
		} else {
			accountIndexCoverage.Uncovered = true
		}
	}
	args = append(args, requestLimit) // $arg : per-arm distinct-transaction bound AND final page limit
	query := buildAccountTransactionsQuery(r.hotSchema, r.coldSchema, strings.Join(where, " AND "), coldLedgerRangeClause, orderDir, arg)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, accountIndexCoverage, fmt.Errorf("GetAccountTransactions: %w", err)
	}
	defer rows.Close()
	var out []AccountTransaction
	for rows.Next() {
		var tx AccountTransaction
		var closed time.Time
		var successful sql.NullBool
		var source, fee, memoType, memo sql.NullString
		var types, sources string
		if err := rows.Scan(&tx.LedgerSequence, &closed, &tx.TransactionHash, &successful, &source, &fee, &memoType, &memo, &types, &sources, &tx.Summary); err != nil {
			return nil, "", false, accountIndexCoverage, err
		}
		tx.ClosedAt = closed.UTC().Format(time.RFC3339Nano)
		if successful.Valid {
			tx.Successful = &successful.Bool
		}
		tx.SourceAccount = nullStringPtr(source)
		tx.FeeCharged = nullStringPtr(fee)
		tx.MemoType = nullStringPtr(memoType)
		tx.Memo = nullStringPtr(memo)
		tx.ActivityTypes = splitCSV(types)
		tx.SourceTables = splitCSV(sources)
		out = append(out, tx)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, accountIndexCoverage, err
	}
	hasMore := len(out) > filters.Limit
	if hasMore {
		out = out[:filters.Limit]
	}
	next := ""
	if hasMore && len(out) > 0 {
		last := out[len(out)-1]
		ts, _ := time.Parse(time.RFC3339Nano, last.ClosedAt)
		next = HistoryCursor{LedgerSequence: last.LedgerSequence, ClosedAt: ts, TransactionHash: last.TransactionHash, Order: filters.Order}.Encode()
	}
	return out, next, hasMore, accountIndexCoverage, nil
}

func splitCSV(s string) []string {
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func buildAccountTransactionsQuery(hotSchema, coldSchema, whereClause, coldLedgerRangeClause, orderDir string, limitArg int) string {
	rows := func(schema string, rank int, isCold bool) string {
		// Hot enriched_history_operations has all five participant columns as VARCHAR (and indexed);
		// silver COLD typed from_account/to_address/address as int32 because the backfill left them
		// all-NULL, so comparing them to an account string errors. Drop them from the cold filter —
		// they carry no data there, and source_account (senders) + destination (receivers) cover it.
		ehoWhere := "source_account = $1 OR destination = $1 OR from_account = $1 OR to_address = $1 OR address = $1"
		if isCold {
			ehoWhere = "source_account = $1 OR destination = $1"
		}
		tablePrune := ""
		if isCold {
			tablePrune = coldLedgerRangeClause
		}
		return fmt.Sprintf(`
		SELECT ledger_sequence, ledger_closed_at AS closed_at, transaction_hash, tx_successful AS successful, source_account,
		       CAST(tx_fee_charged AS VARCHAR) AS fee_charged, tx_memo_type AS memo_type, tx_memo AS memo,
		       CASE WHEN is_payment_op THEN 'classic_payment' WHEN is_soroban_op THEN 'soroban_operation' ELSE COALESCE(type_string, 'operation') END AS activity_type,
		       'enriched_history_operations' AS source_table, %d AS source_rank
		FROM %s.enriched_history_operations
		WHERE (%s)%s AND (%s)
		UNION ALL
		SELECT ledger_sequence, timestamp AS closed_at, transaction_hash, transaction_successful, NULL,
		       NULL, NULL, NULL,
		       CASE WHEN token_contract_id IS NOT NULL THEN 'soroban_token_transfer' ELSE 'token_transfer' END,
		       'token_transfers_raw', %d
		FROM %s.token_transfers_raw
		WHERE (from_account = $1 OR to_account = $1)%s AND (%s)
		UNION ALL
		SELECT ledger_sequence, closed_at, transaction_hash, successful, source_account,
		       NULL, NULL, NULL, 'contract_invocation', 'contract_invocations_raw', %d
		FROM %s.contract_invocations_raw
		WHERE (source_account = $1 OR contract_id = $1)%s AND (%s)`, rank, schema, ehoWhere, tablePrune, whereClause, rank, schema, tablePrune, whereClause, rank, schema, tablePrune, whereClause)
	}
	// Bounded arm: keep only the rows of the top (limit+1) DISTINCT transactions in page order
	// BEFORE the union/dedup/group. DENSE_RANK ties every op/event row of a transaction to one rank
	// — a transaction is uniquely (ledger_sequence, transaction_hash); closed_at is constant within a
	// ledger so it's omitted — so a single high-fanout transaction (e.g. a Soroban call emitting
	// hundreds of token_transfers_raw rows) can never consume the bound and hide older transactions,
	// which a per-row LIMIT could. NOTE: this materializes the arm's matching rows to rank them;
	// pruning the COLD scan for sparse accounts is the job of the account index-plane, not this bound.
	arm := func(schema string, rank int, isCold bool) string {
		return fmt.Sprintf(`(SELECT * EXCLUDE (txn_rank) FROM (
			SELECT *, DENSE_RANK() OVER (ORDER BY ledger_sequence %s, transaction_hash %s) AS txn_rank
			FROM (%s) raw WHERE %s
		) bounded WHERE txn_rank <= $%d)`,
			orderDir, orderDir, rows(schema, rank, isCold), whereClause, limitArg)
	}
	parts := []string{}
	if strings.TrimSpace(hotSchema) != "" {
		parts = append(parts, arm(hotSchema, 1, false))
	}
	if strings.TrimSpace(coldSchema) != "" {
		parts = append(parts, arm(coldSchema, 2, true))
	}
	return fmt.Sprintf(`WITH combined AS (%s), ranked AS (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY ledger_sequence, transaction_hash, activity_type, source_table ORDER BY source_rank ASC) AS rn FROM combined
	), per_tx AS (
		SELECT ledger_sequence, closed_at, transaction_hash,
		       bool_or(COALESCE(successful, false)) AS successful,
		       any_value(source_account) AS source_account, any_value(fee_charged) AS fee_charged, any_value(memo_type) AS memo_type, any_value(memo) AS memo,
		       string_agg(DISTINCT activity_type, ',' ORDER BY activity_type) AS activity_types,
		       string_agg(DISTINCT source_table, ',' ORDER BY source_table) AS source_tables,
		       'Observed ' || CAST(count(*) AS VARCHAR) || ' account-related event(s): ' || string_agg(DISTINCT activity_type, ', ' ORDER BY activity_type) AS summary
		FROM ranked WHERE rn = 1 GROUP BY ledger_sequence, closed_at, transaction_hash
	)
	SELECT ledger_sequence, closed_at, transaction_hash, successful, source_account, fee_charged, memo_type, memo, activity_types, source_tables, summary
	FROM per_tx ORDER BY ledger_sequence %s, closed_at %s, transaction_hash %s LIMIT $%d`, strings.Join(parts, " UNION ALL "), orderDir, orderDir, orderDir, limitArg)
}

func (r *UnifiedDuckDBReader) GetAddressBalanceHistory(ctx context.Context, filters BalanceHistoryFilters) ([]BalanceHistoryPoint, string, bool, error) {
	if strings.TrimSpace(r.hotSchema) == "" && strings.TrimSpace(r.coldSchema) == "" {
		return nil, "", false, fmt.Errorf("GetAddressBalanceHistory: no hot or cold schema configured")
	}
	if filters.ContractID == "" && strings.TrimSpace(r.coldSchema) == "" {
		return nil, "", false, fmt.Errorf("GetAddressBalanceHistory: classic balance history requires cold balance_changes")
	}
	requestLimit := filters.Limit + 1
	orderDir, cursorOp := "DESC", "<"
	if filters.Order == "asc" {
		orderDir, cursorOp = "ASC", ">"
	}
	where := []string{"1=1"}
	args := []interface{}{filters.Address}
	arg := 2
	if filters.Asset != "" {
		args = append(args, filters.Asset)
		arg++
	} else {
		args = append(args, filters.ContractID)
		arg++
	}
	if filters.StartLedger > 0 {
		where = append(where, fmt.Sprintf("ledger_sequence >= $%d", arg))
		args = append(args, filters.StartLedger)
		arg++
	}
	if filters.EndLedger > 0 {
		where = append(where, fmt.Sprintf("ledger_sequence <= $%d", arg))
		args = append(args, filters.EndLedger)
		arg++
	}
	if filters.Cursor != nil {
		where = append(where, fmt.Sprintf(`(ledger_sequence %s $%d OR (ledger_sequence = $%d AND closed_at %s $%d) OR (ledger_sequence = $%d AND closed_at = $%d AND tie_breaker %s $%d))`, cursorOp, arg, arg, cursorOp, arg+1, arg, arg+1, cursorOp, arg+2))
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.ClosedAt, filters.Cursor.TieBreaker)
		arg += 3
	}
	args = append(args, requestLimit)
	query := buildBalanceHistoryQuery(r.hotSchema, r.coldSchema, filters.ContractID != "", strings.Join(where, " AND "), orderDir, arg)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("GetAddressBalanceHistory: %w", err)
	}
	defer rows.Close()
	var out []BalanceHistoryPoint
	var ties []string
	for rows.Next() {
		var p BalanceHistoryPoint
		var closed time.Time
		var assetCode, issuer, contractID, delta, tx sql.NullString
		var tie string
		if err := rows.Scan(&p.LedgerSequence, &closed, &p.AssetType, &assetCode, &issuer, &contractID, &p.Balance, &delta, &tx, &p.SourceTable, &tie); err != nil {
			return nil, "", false, err
		}
		p.ClosedAt = closed.UTC().Format(time.RFC3339Nano)
		p.AssetCode = nullStringPtr(assetCode)
		p.AssetIssuer = nullStringPtr(issuer)
		p.ContractID = nullStringPtr(contractID)
		p.Delta = nullStringPtr(delta)
		p.TransactionHash = nullStringPtr(tx)
		out = append(out, p)
		ties = append(ties, tie)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}
	hasMore := len(out) > filters.Limit
	if hasMore {
		out = out[:filters.Limit]
	}
	next := ""
	if hasMore && len(out) > 0 {
		last := out[len(out)-1]
		ts, _ := time.Parse(time.RFC3339Nano, last.ClosedAt)
		next = HistoryCursor{LedgerSequence: last.LedgerSequence, ClosedAt: ts, TieBreaker: ties[len(out)-1], Order: filters.Order}.Encode()
	}
	return out, next, hasMore, nil
}

func buildBalanceHistoryQuery(hotSchema, coldSchema string, contractMode bool, whereClause, orderDir string, limitArg int) string {
	if contractMode {
		oneContract := func(schema string, rank int) string {
			return fmt.Sprintf(`SELECT ledger_sequence, timestamp AS closed_at, 'contract' AS asset_type, NULL AS asset_code, NULL AS asset_issuer, token_contract_id AS contract_id, CASE WHEN to_account = $1 THEN TRY_CAST(amount AS DECIMAL(38,7)) ELSE -TRY_CAST(amount AS DECIMAL(38,7)) END AS delta_amount, transaction_hash, 'token_transfers_raw' AS source_table, CAST(ledger_sequence AS VARCHAR) || '|token_transfers_raw|' || transaction_hash || '|' || COALESCE(CAST(event_index AS VARCHAR),'') AS tie_breaker, %d AS source_rank FROM %s.token_transfers_raw WHERE token_contract_id = $2 AND transaction_successful = true AND (from_account = $1 OR to_account = $1)`, rank, schema)
		}
		parts := []string{}
		if strings.TrimSpace(hotSchema) != "" {
			parts = append(parts, oneContract(hotSchema, 1))
		}
		if strings.TrimSpace(coldSchema) != "" {
			parts = append(parts, oneContract(coldSchema, 2))
		}
		return fmt.Sprintf(`WITH combined AS (%s), deduped AS (SELECT * EXCLUDE (rn, source_rank) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tie_breaker ORDER BY source_rank ASC) AS rn FROM combined) ranked WHERE rn = 1), with_balances AS (SELECT ledger_sequence, closed_at, asset_type, asset_code, asset_issuer, contract_id, CAST(SUM(delta_amount) OVER (ORDER BY ledger_sequence ASC, closed_at ASC, transaction_hash ASC, tie_breaker ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS VARCHAR) AS balance, CAST(delta_amount AS VARCHAR) AS delta, transaction_hash, source_table, tie_breaker FROM deduped), filtered AS (SELECT * FROM with_balances WHERE %s) SELECT ledger_sequence, closed_at, asset_type, asset_code, asset_issuer, contract_id, balance, delta, transaction_hash, source_table, tie_breaker FROM filtered ORDER BY ledger_sequence %s, closed_at %s, tie_breaker %s LIMIT $%d`, strings.Join(parts, " UNION ALL "), whereClause, orderDir, orderDir, orderDir, limitArg)
	}

	oneClassic := func(schema string, rank int) string {
		return fmt.Sprintf(`SELECT ledger_sequence, ledger_closed_at AS closed_at, asset_type, asset_code, asset_issuer, NULL AS contract_id, CAST(balance AS VARCHAR) AS balance, NULL AS delta, NULL AS transaction_hash, 'balance_changes' AS source_table, CAST(ledger_sequence AS VARCHAR) || '|balance_changes|' || COALESCE(asset_code,'') || '|' || COALESCE(asset_issuer,'') AS tie_breaker, %d AS source_rank FROM %s.balance_changes WHERE address = $1 AND (($2 = 'XLM' AND (asset_type = 'native' OR asset_code = 'XLM')) OR asset_code || COALESCE(':' || asset_issuer, '') = $2 OR asset_code = $2) AND COALESCE(deleted, false) = false`, rank, schema)
	}
	parts := []string{}
	// balance_changes is produced by the cold silver-history-loader, not the
	// realtime hot schema. Do not emit a hot balance_changes branch in the normal
	// unified-reader setup: DuckDB validates every UNION branch up front, so a
	// missing hot table would fail the whole request instead of returning cold
	// history.
	if strings.TrimSpace(coldSchema) != "" {
		parts = append(parts, oneClassic(coldSchema, 2))
	}
	return fmt.Sprintf(`WITH combined AS (%s), filtered AS (SELECT * FROM combined WHERE %s), deduped AS (SELECT * EXCLUDE (rn, source_rank) FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY tie_breaker ORDER BY source_rank ASC) AS rn FROM filtered) ranked WHERE rn = 1) SELECT ledger_sequence, closed_at, asset_type, asset_code, asset_issuer, contract_id, balance, delta, transaction_hash, source_table, tie_breaker FROM deduped ORDER BY ledger_sequence %s, closed_at %s, tie_breaker %s LIMIT $%d`, strings.Join(parts, " UNION ALL "), whereClause, orderDir, orderDir, orderDir, limitArg)
}

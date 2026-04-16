package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/stellar/go/xdr"
)

// SilverHotReader queries PostgreSQL silver_hot for recent analytics data
type ServingRecentTransaction struct {
	TxHash         string    `json:"tx_hash"`
	LedgerSequence int64     `json:"ledger_sequence"`
	ClosedAt       string    `json:"closed_at"`
	Successful     bool      `json:"successful"`
	Fee            int64     `json:"fee"`
	OperationCount int       `json:"operation_count"`
	SourceAccount  string    `json:"source_account"`
	Summary        TxSummary `json:"summary"`
	CreatedAt      time.Time `json:"-"`
}

type ServingRecentLedger struct {
	LedgerSequence     int64  `json:"ledger_sequence"`
	ClosedAt           string `json:"closed_at"`
	LedgerHash         string `json:"ledger_hash,omitempty"`
	PreviousLedgerHash string `json:"previous_ledger_hash,omitempty"`
	ProtocolVersion    int    `json:"protocol_version,omitempty"`
	BaseFeeStroops     int64  `json:"base_fee_stroops,omitempty"`
	SuccessfulTxCount  int    `json:"successful_tx_count"`
	FailedTxCount      int    `json:"failed_tx_count,omitempty"`
	OperationCount     int    `json:"operation_count"`
}

type SilverHotReader struct {
	db          *sql.DB
	bronzeHotPG *sql.DB
}

// NewSilverHotReader creates a new Silver hot layer reader
func NewSilverHotReader(config PostgresConfig) (*SilverHotReader, error) {
	dsn := config.DSN()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL connection: %w", err)
	}

	if config.MaxConnections > 0 {
		db.SetMaxOpenConns(config.MaxConnections)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}

	return &SilverHotReader{db: db}, nil
}

func (h *SilverHotReader) SetBronzeHotPG(db *sql.DB) {
	h.bronzeHotPG = db
}

// DB returns the underlying database connection
func (h *SilverHotReader) DB() *sql.DB {
	return h.db
}

// GetServingAccountCurrent returns current account state from serving schema.
// This is faster than the legacy/unified paths because it reads the compact
// serving projection instead of federating or scanning broader silver tables.
func (h *SilverHotReader) GetServingAccountCurrent(ctx context.Context, accountID string) (*AccountCurrent, error) {
	query := `
		SELECT
			account_id,
			balance_stroops,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at,
			home_domain,
			created_at
		FROM serving.sv_accounts_current
		WHERE account_id = $1
	`

	var acc AccountCurrent
	var balance sql.NullInt64
	var seq sql.NullInt64
	var updatedAt sql.NullTime
	var homeDomain, createdAt sql.NullString
	if err := h.db.QueryRowContext(ctx, query, accountID).Scan(
		&acc.AccountID,
		&balance,
		&seq,
		&acc.NumSubentries,
		&acc.LastModifiedLedger,
		&updatedAt,
		&homeDomain,
		&createdAt,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	if balance.Valid {
		acc.Balance = strconv.FormatInt(balance.Int64, 10)
	}
	if seq.Valid {
		acc.SequenceNumber = strconv.FormatInt(seq.Int64, 10)
	}
	if updatedAt.Valid {
		acc.UpdatedAt = updatedAt.Time.UTC().Format(time.RFC3339)
	}
	if homeDomain.Valid && homeDomain.String != "" {
		acc.HomeDomain = &homeDomain.String
	}
	if createdAt.Valid && createdAt.String != "" {
		acc.CreatedAt = &createdAt.String
	}
	return &acc, nil
}

// GetServingTopAccounts returns top accounts from serving schema.
func (h *SilverHotReader) GetServingTopAccounts(ctx context.Context, limit int) ([]AccountCurrent, error) {
	rows, err := h.db.QueryContext(ctx, `
		SELECT
			account_id,
			balance_stroops,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM serving.sv_accounts_current
		ORDER BY balance_stroops DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []AccountCurrent
	for rows.Next() {
		var acc AccountCurrent
		var balance, seq sql.NullInt64
		var updatedAt sql.NullTime
		if err := rows.Scan(&acc.AccountID, &balance, &seq, &acc.NumSubentries, &acc.LastModifiedLedger, &updatedAt); err != nil {
			return nil, err
		}
		if balance.Valid {
			acc.Balance = strconv.FormatInt(balance.Int64, 10)
		}
		if seq.Valid {
			acc.SequenceNumber = strconv.FormatInt(seq.Int64, 10)
		}
		if updatedAt.Valid {
			acc.UpdatedAt = updatedAt.Time.UTC().Format(time.RFC3339)
		}
		accounts = append(accounts, acc)
	}
	return accounts, rows.Err()
}

// GetServingAccountBalances returns all balances for an account from serving schema.
func (h *SilverHotReader) GetServingAccountBalances(ctx context.Context, accountID string) (*AccountBalancesResponse, error) {
	rows, err := h.db.QueryContext(ctx, `
		SELECT
			asset_type,
			asset_code,
			asset_issuer,
			balance_stroops,
			limit_stroops,
			is_authorized
		FROM serving.sv_account_balances_current
		WHERE account_id = $1
		ORDER BY balance_stroops DESC, asset_code ASC
	`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resp := &AccountBalancesResponse{AccountID: accountID}
	for rows.Next() {
		var bal Balance
		var issuer sql.NullString
		var balanceStroops sql.NullInt64
		var limitStroops sql.NullInt64
		var authorized sql.NullBool
		if err := rows.Scan(&bal.AssetType, &bal.AssetCode, &issuer, &balanceStroops, &limitStroops, &authorized); err != nil {
			return nil, err
		}
		if issuer.Valid {
			bal.AssetIssuer = &issuer.String
		}
		if balanceStroops.Valid {
			bal.BalanceStroops = balanceStroops.Int64
			bal.Balance = formatStroopsToDecimal(balanceStroops.Int64)
		}
		if limitStroops.Valid {
			ls := formatStroopsToDecimal(limitStroops.Int64)
			bal.Limit = &ls
		}
		if authorized.Valid {
			v := authorized.Bool
			bal.IsAuthorized = &v
		}
		resp.Balances = append(resp.Balances, bal)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(resp.Balances) == 0 {
		return nil, fmt.Errorf("account not found: %s", accountID)
	}
	resp.TotalBalances = len(resp.Balances)
	return resp, nil
}

type AddressBalanceCurrent struct {
	OwnerAddress      string
	AssetKey          string
	AssetType         string
	TokenContractID   *string
	AssetCode         *string
	AssetIssuer       *string
	Symbol            *string
	Decimals          *int
	BalanceRaw        string
	BalanceDisplay    string
	BalanceSource     string
	LastUpdatedLedger *int64
	LastUpdatedAt     *string
}

func (h *SilverHotReader) GetAddressBalancesCurrent(ctx context.Context, ownerAddress string) ([]AddressBalanceCurrent, error) {
	rows, err := h.db.QueryContext(ctx, `
		SELECT owner_address, asset_key, asset_type, token_contract_id,
		       asset_code, asset_issuer, symbol, decimals,
		       CAST(balance_raw AS TEXT), balance_display, balance_source,
		       last_updated_ledger, last_updated_at::text
		FROM address_balances_current
		WHERE owner_address = $1
		ORDER BY CAST(balance_raw AS NUMERIC) DESC, asset_key ASC
	`, ownerAddress)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AddressBalanceCurrent
	for rows.Next() {
		var rec AddressBalanceCurrent
		var tokenContractID, assetCode, assetIssuer, symbol, lastUpdatedAt sql.NullString
		var decimals sql.NullInt64
		var lastUpdatedLedger sql.NullInt64
		if err := rows.Scan(&rec.OwnerAddress, &rec.AssetKey, &rec.AssetType, &tokenContractID, &assetCode, &assetIssuer, &symbol, &decimals, &rec.BalanceRaw, &rec.BalanceDisplay, &rec.BalanceSource, &lastUpdatedLedger, &lastUpdatedAt); err != nil {
			return nil, err
		}
		if tokenContractID.Valid {
			rec.TokenContractID = &tokenContractID.String
		}
		if assetCode.Valid {
			rec.AssetCode = &assetCode.String
		}
		if assetIssuer.Valid {
			rec.AssetIssuer = &assetIssuer.String
		}
		if symbol.Valid {
			rec.Symbol = &symbol.String
		}
		if decimals.Valid {
			d := int(decimals.Int64)
			rec.Decimals = &d
		}
		if lastUpdatedLedger.Valid {
			v := lastUpdatedLedger.Int64
			rec.LastUpdatedLedger = &v
		}
		if lastUpdatedAt.Valid {
			rec.LastUpdatedAt = &lastUpdatedAt.String
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// GetServingNetworkStats returns compact headline network stats from serving schema.
func (h *SilverHotReader) GetServingNetworkStats(ctx context.Context) (*NetworkStats, error) {
	stats := &NetworkStats{DataFreshness: "real-time"}
	var generatedAt, latestClosedAt sql.NullTime
	var avgClose sql.NullFloat64
	var proto sql.NullInt64
	var txTotal, txFailed, activeAccounts, createdAccounts sql.NullInt64
	if err := h.db.QueryRowContext(ctx, `
		SELECT generated_at, latest_ledger, latest_ledger_closed_at, avg_close_time_seconds,
		       protocol_version, tx_24h_total, tx_24h_failed, active_accounts_24h, created_accounts_24h
		FROM serving.sv_network_stats_current
		WHERE network = 'testnet'
		LIMIT 1
	`).Scan(&generatedAt, &stats.Ledger.CurrentSequence, &latestClosedAt, &avgClose, &proto, &txTotal, &txFailed, &activeAccounts, &createdAccounts); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if generatedAt.Valid {
		stats.GeneratedAt = generatedAt.Time.UTC().Format(time.RFC3339)
	} else {
		stats.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	}
	if avgClose.Valid {
		stats.Ledger.AvgCloseTimeSeconds = avgClose.Float64
	}
	if proto.Valid {
		stats.Ledger.ProtocolVersion = int(proto.Int64)
	}
	if txTotal.Valid {
		stats.Transactions24h = &TransactionStats24h{Total: txTotal.Int64, Failed: txFailed.Int64}
		if txTotal.Int64 > 0 {
			stats.Transactions24h.FailureRate = float64(txFailed.Int64) / float64(txTotal.Int64)
		}
	}
	if activeAccounts.Valid {
		stats.Accounts.Active24h = activeAccounts.Int64
	}
	if createdAccounts.Valid {
		stats.Accounts.Created24h = createdAccounts.Int64
	}
	_ = h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM serving.sv_accounts_current`).Scan(&stats.Accounts.Total)
	return stats, nil
}

// GetServingTokenHolders returns token holders from serving schema.
func (h *SilverHotReader) GetServingTokenHolders(ctx context.Context, filters TokenHoldersFilters) (*TokenHoldersResponse, error) {
	assetKey := filters.AssetCode
	isNative := filters.AssetCode == "XLM" || filters.AssetCode == "native"
	if isNative {
		assetKey = "XLM"
	}
	if !isNative {
		assetKey = filters.AssetCode + ":" + filters.AssetIssuer
	}

	query := `
		SELECT account_id, balance_stroops
		FROM serving.sv_account_balances_current
		WHERE asset_key = $1
	`
	args := []any{assetKey}
	argPos := 2
	if filters.MinBalance != nil {
		query += fmt.Sprintf(" AND balance_stroops >= $%d", argPos)
		args = append(args, *filters.MinBalance)
		argPos++
	}
	if filters.Cursor != nil {
		query += fmt.Sprintf(" AND (balance_stroops < $%d OR (balance_stroops = $%d AND account_id > $%d))", argPos, argPos, argPos+1)
		args = append(args, filters.Cursor.Balance, filters.Cursor.AccountID)
		argPos += 2
	}
	query += fmt.Sprintf(" ORDER BY balance_stroops DESC, account_id ASC LIMIT $%d", argPos)
	args = append(args, filters.Limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var holders []TokenHolder
	for rows.Next() {
		var accountID string
		var balance int64
		if err := rows.Scan(&accountID, &balance); err != nil {
			return nil, err
		}
		holders = append(holders, TokenHolder{
			AccountID:      accountID,
			Balance:        formatStroopsToDecimal(balance),
			BalanceStroops: balance,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range holders {
		if filters.Cursor == nil {
			holders[i].Rank = i + 1
		}
	}
	response := &TokenHoldersResponse{Asset: AssetInfo{Code: filters.AssetCode}, Holders: holders}
	if isNative {
		response.Asset.Type = "native"
	} else {
		response.Asset.Type = "credit_alphanum4"
		if len(filters.AssetCode) > 4 {
			response.Asset.Type = "credit_alphanum12"
		}
		response.Asset.Issuer = &filters.AssetIssuer
	}
	response.HasMore = len(response.Holders) > filters.Limit
	if response.HasMore {
		response.Holders = response.Holders[:filters.Limit]
		last := response.Holders[len(response.Holders)-1]
		response.Cursor = TokenHoldersCursor{Balance: last.BalanceStroops, AccountID: last.AccountID}.Encode()
	}
	response.TotalHolders = len(response.Holders)
	return response, nil
}

// GetServingTokenStats returns token stats from serving schema.
func (h *SilverHotReader) GetServingTokenStats(ctx context.Context, assetCode, assetIssuer string) (*TokenStatsResponse, error) {
	assetKey := assetCode
	isNative := assetCode == "XLM" || assetCode == "native"
	if isNative {
		assetKey = "XLM"
	} else {
		assetKey = assetCode + ":" + assetIssuer
	}

	resp := &TokenStatsResponse{Asset: AssetInfo{Code: assetCode}, GeneratedAt: time.Now().UTC().Format(time.RFC3339)}
	if isNative {
		resp.Asset.Type = "native"
	} else {
		resp.Asset.Issuer = &assetIssuer
		resp.Asset.Type = "credit_alphanum4"
		if len(assetCode) > 4 {
			resp.Asset.Type = "credit_alphanum12"
		}
	}

	var supply sql.NullFloat64
	var vol sql.NullFloat64
	if err := h.db.QueryRowContext(ctx, `
		SELECT holder_count, trustline_count, circulating_supply, volume_24h, transfers_24h, unique_accounts_24h, top10_concentration
		FROM serving.sv_asset_stats_current
		WHERE asset_key = $1
	`, assetKey).Scan(
		&resp.Stats.TotalHolders,
		&resp.Stats.TotalTrustlines,
		&supply,
		&vol,
		&resp.Stats.Transfers24h,
		&resp.Stats.UniqueAccounts24h,
		&resp.Stats.Top10Concentration,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if supply.Valid {
		resp.Stats.CirculatingSupply = fmt.Sprintf("%.7f", supply.Float64)
		if vol.Valid {
			resp.Stats.Volume24h = fmt.Sprintf("%.7f", vol.Float64)
		}
	}
	if resp.Stats.Volume24h == "" {
		resp.Stats.Volume24h = "0.0000000"
	}
	if resp.Stats.CirculatingSupply == "" {
		resp.Stats.CirculatingSupply = "0.0000000"
	}
	return resp, nil
}

// GetServingAssetList returns asset directory data from serving schema.
func (h *SilverHotReader) GetServingAssetList(ctx context.Context, filters AssetListFilters) (*AssetListResponse, error) {
	requestLimit := filters.Limit + 1
	query := `
		SELECT a.asset_code, COALESCE(a.asset_issuer, ''), a.asset_type,
		       s.holder_count, COALESCE(s.circulating_supply, 0), COALESCE(s.volume_24h, 0),
		       COALESCE(s.transfers_24h, 0), COALESCE(s.top10_concentration, 0)
		FROM serving.sv_assets_current a
		JOIN serving.sv_asset_stats_current s USING (asset_key)
		WHERE 1=1
	`
	args := []any{}
	argPos := 1
	if filters.AssetType != "" {
		query += fmt.Sprintf(" AND a.asset_type = $%d", argPos)
		args = append(args, filters.AssetType)
		argPos++
	}
	if filters.Search != "" {
		query += fmt.Sprintf(" AND a.asset_code ILIKE $%d", argPos)
		args = append(args, filters.Search+"%")
		argPos++
	}
	if filters.MinHolders != nil {
		query += fmt.Sprintf(" AND s.holder_count >= $%d", argPos)
		args = append(args, *filters.MinHolders)
		argPos++
	}
	orderCol := "s.holder_count"
	if filters.SortBy == "volume_24h" {
		orderCol = "s.volume_24h"
		if filters.Cursor != nil {
			query += fmt.Sprintf(" AND (s.volume_24h < $%d OR (s.volume_24h = $%d AND a.asset_code > $%d))", argPos, argPos, argPos+1)
			args = append(args, float64(filters.Cursor.Volume24h)/10000000.0, filters.Cursor.AssetCode)
			argPos += 2
		}
	} else {
		if filters.Cursor != nil {
			query += fmt.Sprintf(" AND (s.holder_count < $%d OR (s.holder_count = $%d AND a.asset_code > $%d))", argPos, argPos, argPos+1)
			args = append(args, filters.Cursor.HolderCount, filters.Cursor.AssetCode)
			argPos += 2
		}
	}
	query += fmt.Sprintf(" ORDER BY %s DESC, a.asset_code ASC LIMIT $%d", orderCol, argPos)
	args = append(args, requestLimit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resp := &AssetListResponse{GeneratedAt: time.Now().UTC().Format(time.RFC3339)}
	for rows.Next() {
		var a AssetSummary
		var issuer string
		var supply, volume float64
		if err := rows.Scan(&a.AssetCode, &issuer, &a.AssetType, &a.HolderCount, &supply, &volume, &a.Transfers24h, &a.Top10Concentration); err != nil {
			return nil, err
		}
		a.AssetIssuer = issuer
		a.CirculatingSupply = fmt.Sprintf("%.7f", supply)
		a.Volume24h = fmt.Sprintf("%.7f", volume)
		resp.Assets = append(resp.Assets, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	resp.HasMore = len(resp.Assets) > filters.Limit
	if resp.HasMore {
		resp.Assets = resp.Assets[:filters.Limit]
		last := resp.Assets[len(resp.Assets)-1]
		resp.Cursor = (&AssetListCursor{HolderCount: last.HolderCount, AssetCode: last.AssetCode, AssetIssuer: last.AssetIssuer, SortBy: filters.SortBy, SortOrder: filters.SortOrder}).Encode()
	}
	_ = h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM serving.sv_assets_current`).Scan(&resp.TotalAssets)
	return resp, nil
}

func (h *SilverHotReader) GetServingLatestLedgerSequence(ctx context.Context) (int64, error) {
	var latestSequence int64
	if err := h.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(ledger_sequence), 0) FROM serving.sv_ledger_stats_recent`).Scan(&latestSequence); err != nil {
		return 0, err
	}
	if latestSequence > 0 {
		return latestSequence, nil
	}
	if err := h.db.QueryRowContext(ctx, `
		SELECT COALESCE(latest_ledger, 0)
		FROM serving.sv_network_stats_current
		WHERE network = 'testnet'
		LIMIT 1
	`).Scan(&latestSequence); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return latestSequence, nil
}

// GetServingRecentTransactions returns the most recent transactions from serving projections.
func (h *SilverHotReader) GetServingRecentTransactions(ctx context.Context, limit int) (int64, []ServingRecentTransaction, error) {
	latestSequence, err := h.GetServingLatestLedgerSequence(ctx)
	if err != nil {
		return 0, nil, err
	}

	rows, err := h.db.QueryContext(ctx, `
		SELECT tx_hash, ledger_sequence, created_at, COALESCE(source_account, ''), successful,
		       COALESCE(fee_charged_stroops, 0), COALESCE(operation_count, 0),
		       tx_type, summary_text, summary_json, primary_contract_id
		FROM serving.sv_transactions_recent
		ORDER BY ledger_sequence DESC, created_at DESC, tx_hash DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	var out []ServingRecentTransaction
	for rows.Next() {
		var item ServingRecentTransaction
		var txType sql.NullString
		var summaryText sql.NullString
		var summaryJSON sql.NullString
		var primaryContract sql.NullString
		if err := rows.Scan(&item.TxHash, &item.LedgerSequence, &item.CreatedAt, &item.SourceAccount, &item.Successful, &item.Fee, &item.OperationCount, &txType, &summaryText, &summaryJSON, &primaryContract); err != nil {
			return 0, nil, err
		}
		item.ClosedAt = item.CreatedAt.UTC().Format(time.RFC3339)
		if summaryJSON.Valid && summaryJSON.String != "" {
			if err := json.Unmarshal([]byte(summaryJSON.String), &item.Summary); err != nil {
				item.Summary = buildFallbackRecentTxSummary(txType.String, summaryText.String, primaryContract)
			}
		} else {
			item.Summary = buildFallbackRecentTxSummary(txType.String, summaryText.String, primaryContract)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	return latestSequence, out, nil
}

func buildFallbackRecentTxSummary(txType, summaryText string, primaryContract sql.NullString) TxSummary {
	summary := TxSummary{Type: txType, Description: summaryText}
	if summary.Type == "" {
		summary.Type = "classic"
	}
	if summary.Description == "" {
		summary.Description = "Transaction"
	}
	if primaryContract.Valid && primaryContract.String != "" {
		summary.InvolvedContracts = []string{primaryContract.String}
	} else {
		summary.InvolvedContracts = []string{}
	}
	return summary
}

func (h *SilverHotReader) GetServingRecentLedgers(ctx context.Context, limit int) (int64, []ServingRecentLedger, error) {
	latestSequence, err := h.GetServingLatestLedgerSequence(ctx)
	if err != nil {
		return 0, nil, err
	}
	rows, err := h.db.QueryContext(ctx, `
		SELECT ledger_sequence, closed_at, COALESCE(ledger_hash, ''), COALESCE(prev_hash, ''),
		       COALESCE(protocol_version, 0), COALESCE(base_fee_stroops, 0),
		       COALESCE(successful_tx_count, 0), COALESCE(failed_tx_count, 0), COALESCE(operation_count, 0)
		FROM serving.sv_ledger_stats_recent
		ORDER BY ledger_sequence DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	var out []ServingRecentLedger
	for rows.Next() {
		var item ServingRecentLedger
		var closedAt time.Time
		if err := rows.Scan(&item.LedgerSequence, &closedAt, &item.LedgerHash, &item.PreviousLedgerHash, &item.ProtocolVersion, &item.BaseFeeStroops, &item.SuccessfulTxCount, &item.FailedTxCount, &item.OperationCount); err != nil {
			return 0, nil, err
		}
		item.ClosedAt = closedAt.UTC().Format(time.RFC3339)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	return latestSequence, out, nil
}

// GetServingExplorerEvents returns recent explorer events from serving projections.
func (h *SilverHotReader) GetServingExplorerEvents(ctx context.Context, filters ExplorerEventFilters, classifier *EventClassifier) ([]ExplorerEvent, *ExplorerEventMeta, string, bool, error) {
	if events, meta, nextCursor, hasMore, err := h.getProjectedServingExplorerEvents(ctx, filters); err == nil {
		return events, meta, nextCursor, hasMore, nil
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	requestLimit := limit + 1
	orderDir := "DESC"
	cursorOp := "<"
	if filters.Order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}

	conditions := []string{"1=1"}
	args := []any{}
	argPos := 1

	if len(filters.Types) > 0 {
		topic0Values, includesCatchAll := classifier.Topic0ValuesForTypes(filters.Types)
		if includesCatchAll && len(topic0Values) > 0 {
			known := classifier.AllKnownTopic0Values()
			includePH := make([]string, len(topic0Values))
			for i, val := range topic0Values {
				includePH[i] = fmt.Sprintf("$%d", argPos)
				args = append(args, val)
				argPos++
			}
			excludePH := make([]string, len(known))
			for i, val := range known {
				excludePH[i] = fmt.Sprintf("$%d", argPos)
				args = append(args, val)
				argPos++
			}
			conditions = append(conditions, fmt.Sprintf("(topic0 IN (%s) OR topic0 IS NULL OR topic0 NOT IN (%s))", strings.Join(includePH, ","), strings.Join(excludePH, ",")))
		} else if includesCatchAll {
			known := classifier.AllKnownTopic0Values()
			if len(known) > 0 {
				excludePH := make([]string, len(known))
				for i, val := range known {
					excludePH[i] = fmt.Sprintf("$%d", argPos)
					args = append(args, val)
					argPos++
				}
				conditions = append(conditions, fmt.Sprintf("(topic0 IS NULL OR topic0 NOT IN (%s))", strings.Join(excludePH, ",")))
			}
		} else if len(topic0Values) > 0 {
			placeholders := make([]string, len(topic0Values))
			for i, val := range topic0Values {
				placeholders[i] = fmt.Sprintf("$%d", argPos)
				args = append(args, val)
				argPos++
			}
			conditions = append(conditions, fmt.Sprintf("topic0 IN (%s)", strings.Join(placeholders, ",")))
		}
	}
	if filters.ContractID != nil && *filters.ContractID != "" {
		hexID, err := normalizeContractID(*filters.ContractID)
		if err != nil {
			return nil, nil, "", false, fmt.Errorf("invalid contract_id %q: must be C... address or 64-char hex hash", *filters.ContractID)
		}
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argPos))
		args = append(args, hexID)
		argPos++
	}
	if filters.ContractName != nil && *filters.ContractName != "" {
		hexIDs, err := h.resolveContractNameToHexHot(ctx, *filters.ContractName)
		if err != nil {
			return nil, nil, "", false, err
		}
		if len(hexIDs) == 0 {
			return []ExplorerEvent{}, &ExplorerEventMeta{}, "", false, nil
		}
		placeholders := make([]string, len(hexIDs))
		for i, hexID := range hexIDs {
			placeholders[i] = fmt.Sprintf("$%d", argPos)
			args = append(args, hexID)
			argPos++
		}
		conditions = append(conditions, fmt.Sprintf("contract_id IN (%s)", strings.Join(placeholders, ",")))
	}
	if filters.TxHash != nil && *filters.TxHash != "" {
		conditions = append(conditions, fmt.Sprintf("tx_hash = $%d", argPos))
		args = append(args, *filters.TxHash)
		argPos++
	}
	if filters.TopicMatch != nil && *filters.TopicMatch != "" {
		conditions = append(conditions, fmt.Sprintf("COALESCE(raw_event_json->>'topics_decoded','') ILIKE '%%' || $%d || '%%'", argPos))
		args = append(args, *filters.TopicMatch)
		argPos++
	}
	if filters.Topic0 != nil && *filters.Topic0 != "" {
		conditions = append(conditions, fmt.Sprintf("topic0 = $%d", argPos))
		args = append(args, *filters.Topic0)
		argPos++
	}
	if filters.Topic1 != nil && *filters.Topic1 != "" {
		conditions = append(conditions, fmt.Sprintf("topic1 = $%d", argPos))
		args = append(args, *filters.Topic1)
		argPos++
	}
	if filters.Topic2 != nil && *filters.Topic2 != "" {
		conditions = append(conditions, fmt.Sprintf("topic2 = $%d", argPos))
		args = append(args, *filters.Topic2)
		argPos++
	}
	if filters.Topic3 != nil && *filters.Topic3 != "" {
		conditions = append(conditions, fmt.Sprintf("topic3 = $%d", argPos))
		args = append(args, *filters.Topic3)
		argPos++
	}
	if filters.Cursor != nil && *filters.Cursor != "" {
		parts := strings.SplitN(*filters.Cursor, ":", 2)
		if len(parts) != 2 {
			return nil, nil, "", false, fmt.Errorf("invalid cursor format")
		}
		cursorLedger, err1 := strconv.ParseInt(parts[0], 10, 64)
		cursorEvent, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil {
			return nil, nil, "", false, fmt.Errorf("invalid cursor values")
		}
		conditions = append(conditions, fmt.Sprintf("(ledger_sequence %s $%d OR (ledger_sequence = $%d AND COALESCE(event_index,0) %s $%d))", cursorOp, argPos, argPos, cursorOp, argPos+1))
		args = append(args, cursorLedger, cursorEvent)
		argPos += 2
	}

	whereClause := strings.Join(conditions, " AND ")
	query := fmt.Sprintf(`
		SELECT event_id, contract_id, ledger_sequence, tx_hash, created_at,
		       COALESCE((raw_event_json->>'in_successful_contract_call')::boolean, true),
		       topic0, topic1, topic2, topic3,
		       raw_event_json->>'topics_decoded', raw_event_json->>'data_decoded',
		       COALESCE(event_index, 0), COALESCE((raw_event_json->>'operation_index')::int, 0)
		FROM serving.sv_events_recent
		WHERE %s
		ORDER BY ledger_sequence %s, COALESCE(event_index,0) %s
		LIMIT $%d
	`, whereClause, orderDir, orderDir, argPos)
	queryArgs := append(append([]any{}, args...), requestLimit)

	rows, err := h.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, nil, "", false, err
	}
	defer rows.Close()

	var events []ExplorerEvent
	contractSet := map[string]bool{}
	var contractIDs []string
	typeSet := map[string]bool{}
	for _, t := range filters.Types {
		typeSet[t] = true
	}

	for rows.Next() {
		var e ExplorerEvent
		var createdAt time.Time
		var successful bool
		if err := rows.Scan(&e.EventID, &e.ContractID, &e.LedgerSequence, &e.TxHash, &createdAt, &successful, &e.Topic0, &e.Topic1, &e.Topic2, &e.Topic3, &e.TopicsDecoded, &e.DataDecoded, &e.EventIndex, &e.OpIndex); err != nil {
			return nil, nil, "", false, err
		}
		e.ClosedAt = createdAt.UTC().Format(time.RFC3339)
		e.Successful = successful
		e.Data = e.DataDecoded
		if e.ContractID != nil {
			if strKeyID, err := hexToStrKey(*e.ContractID); err == nil {
				e.ContractID = &strKeyID
			}
		}
		classification := classifier.Classify(e.ContractID, e.Topic0, e.TopicsDecoded)
		e.Type = classification.EventType
		e.Protocol = classification.Protocol
		if len(typeSet) > 0 && !typeSet[e.Type] {
			continue
		}
		events = append(events, e)
		if e.ContractID != nil && !contractSet[*e.ContractID] {
			contractSet[*e.ContractID] = true
			contractIDs = append(contractIDs, *e.ContractID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, "", false, err
	}

	if len(contractIDs) > 0 {
		nameMap := h.batchResolveContractNames(ctx, contractIDs)
		for i := range events {
			if events[i].ContractID != nil {
				if info, ok := nameMap[*events[i].ContractID]; ok {
					events[i].ContractName = info.name
					events[i].ContractSymbol = info.symbol
					events[i].ContractCategory = info.category
				}
			}
		}
	}

	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}
	var nextCursor string
	if len(events) > 0 && hasMore {
		last := events[len(events)-1]
		nextCursor = fmt.Sprintf("%d:%d", last.LedgerSequence, last.EventIndex)
	}

	metaQuery := fmt.Sprintf(`
		WITH filtered AS (
			SELECT ledger_sequence FROM serving.sv_events_recent WHERE %s
		), capped AS (
			SELECT ledger_sequence FROM filtered LIMIT 10001
		)
		SELECT (SELECT COUNT(*) FROM capped),
		       (SELECT MIN(ledger_sequence) FROM filtered),
		       (SELECT MAX(ledger_sequence) FROM filtered)
	`, whereClause)
	var count int64
	var minLedger, maxLedger sql.NullInt64
	if err := h.db.QueryRowContext(ctx, metaQuery, args...).Scan(&count, &minLedger, &maxLedger); err != nil {
		return events, &ExplorerEventMeta{}, nextCursor, hasMore, nil
	}
	meta := &ExplorerEventMeta{MatchedCount: count, CountCapped: count > 10000}
	if meta.CountCapped {
		meta.MatchedCount = 10000
	}
	if minLedger.Valid {
		meta.LedgerRange.Min = minLedger.Int64
	}
	if maxLedger.Valid {
		meta.LedgerRange.Max = maxLedger.Int64
	}
	return events, meta, nextCursor, hasMore, nil
}

func (h *SilverHotReader) getProjectedServingExplorerEvents(ctx context.Context, filters ExplorerEventFilters) ([]ExplorerEvent, *ExplorerEventMeta, string, bool, error) {
	limit := filters.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	requestLimit := limit + 1
	orderDir := "DESC"
	cursorOp := "<"
	if filters.Order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}
	conditions := []string{"1=1"}
	args := []any{}
	argPos := 1
	if len(filters.Types) > 0 {
		placeholders := make([]string, len(filters.Types))
		for i, t := range filters.Types {
			placeholders[i] = fmt.Sprintf("$%d", argPos)
			args = append(args, t)
			argPos++
		}
		conditions = append(conditions, fmt.Sprintf("explorer_type IN (%s)", strings.Join(placeholders, ",")))
	}
	if filters.ContractID != nil && *filters.ContractID != "" {
		normalized, err := normalizeContractID(*filters.ContractID)
		if err != nil {
			return nil, nil, "", false, err
		}
		conditions = append(conditions, fmt.Sprintf("(contract_id = $%d OR contract_address = $%d)", argPos, argPos+1))
		args = append(args, normalized, *filters.ContractID)
		argPos += 2
	}
	if filters.ContractName != nil && *filters.ContractName != "" {
		conditions = append(conditions, fmt.Sprintf("COALESCE(contract_name,'') ILIKE '%%' || $%d || '%%'", argPos))
		args = append(args, *filters.ContractName)
		argPos++
	}
	if filters.TxHash != nil && *filters.TxHash != "" {
		conditions = append(conditions, fmt.Sprintf("tx_hash = $%d", argPos))
		args = append(args, *filters.TxHash)
		argPos++
	}
	if filters.TopicMatch != nil && *filters.TopicMatch != "" {
		conditions = append(conditions, fmt.Sprintf("COALESCE(topics_decoded,'') ILIKE '%%' || $%d || '%%'", argPos))
		args = append(args, *filters.TopicMatch)
		argPos++
	}
	for _, pair := range []struct {
		v   *string
		col string
	}{{filters.Topic0, "topic0"}, {filters.Topic1, "topic1"}, {filters.Topic2, "topic2"}, {filters.Topic3, "topic3"}} {
		if pair.v != nil && *pair.v != "" {
			conditions = append(conditions, fmt.Sprintf("%s = $%d", pair.col, argPos))
			args = append(args, *pair.v)
			argPos++
		}
	}
	if filters.Cursor != nil && *filters.Cursor != "" {
		parts := strings.SplitN(*filters.Cursor, ":", 2)
		if len(parts) != 2 {
			return nil, nil, "", false, fmt.Errorf("invalid cursor format")
		}
		cursorLedger, err1 := strconv.ParseInt(parts[0], 10, 64)
		cursorEvent, err2 := strconv.ParseInt(parts[1], 10, 64)
		if err1 != nil || err2 != nil {
			return nil, nil, "", false, fmt.Errorf("invalid cursor values")
		}
		conditions = append(conditions, fmt.Sprintf("(ledger_sequence %s $%d OR (ledger_sequence = $%d AND COALESCE(event_index,0) %s $%d))", cursorOp, argPos, argPos, cursorOp, argPos+1))
		args = append(args, cursorLedger, cursorEvent)
		argPos += 2
	}
	where := strings.Join(conditions, " AND ")
	query := fmt.Sprintf(`
		SELECT event_id, contract_address, ledger_sequence, tx_hash, created_at, successful,
		       topic0, topic1, topic2, topic3, topics_decoded, data_decoded,
		       COALESCE(event_index,0), COALESCE(operation_index,0), explorer_type, protocol,
		       contract_name, contract_symbol, contract_category
		FROM serving.sv_explorer_events_recent
		WHERE %s
		ORDER BY ledger_sequence %s, COALESCE(event_index,0) %s
		LIMIT $%d
	`, where, orderDir, orderDir, argPos)
	queryArgs := append(append([]any{}, args...), requestLimit)
	rows, err := h.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, nil, "", false, err
	}
	defer rows.Close()
	var events []ExplorerEvent
	for rows.Next() {
		var e ExplorerEvent
		var createdAt time.Time
		if err := rows.Scan(&e.EventID, &e.ContractID, &e.LedgerSequence, &e.TxHash, &createdAt, &e.Successful, &e.Topic0, &e.Topic1, &e.Topic2, &e.Topic3, &e.TopicsDecoded, &e.DataDecoded, &e.EventIndex, &e.OpIndex, &e.Type, &e.Protocol, &e.ContractName, &e.ContractSymbol, &e.ContractCategory); err != nil {
			return nil, nil, "", false, err
		}
		e.ClosedAt = createdAt.UTC().Format(time.RFC3339)
		e.Data = e.DataDecoded
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, "", false, err
	}
	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}
	var nextCursor string
	if len(events) > 0 && hasMore {
		last := events[len(events)-1]
		nextCursor = fmt.Sprintf("%d:%d", last.LedgerSequence, last.EventIndex)
	}
	metaQuery := fmt.Sprintf(`WITH filtered AS (SELECT ledger_sequence FROM serving.sv_explorer_events_recent WHERE %s), capped AS (SELECT ledger_sequence FROM filtered LIMIT 10001) SELECT (SELECT COUNT(*) FROM capped), (SELECT MIN(ledger_sequence) FROM filtered), (SELECT MAX(ledger_sequence) FROM filtered)`, where)
	var count int64
	var minLedger, maxLedger sql.NullInt64
	if err := h.db.QueryRowContext(ctx, metaQuery, args...).Scan(&count, &minLedger, &maxLedger); err != nil {
		return events, &ExplorerEventMeta{}, nextCursor, hasMore, nil
	}
	meta := &ExplorerEventMeta{MatchedCount: count, CountCapped: count > 10000}
	if meta.CountCapped {
		meta.MatchedCount = 10000
	}
	if minLedger.Valid {
		meta.LedgerRange.Min = minLedger.Int64
	}
	if maxLedger.Valid {
		meta.LedgerRange.Max = maxLedger.Int64
	}
	return events, meta, nextCursor, hasMore, nil
}

func (h *SilverHotReader) resolveContractNameToHexHot(ctx context.Context, name string) ([]string, error) {
	rows, err := h.db.QueryContext(ctx, `
		SELECT DISTINCT contract_id FROM (
			SELECT contract_id FROM contract_registry WHERE display_name ILIKE '%' || $1 || '%'
			UNION
			SELECT contract_id FROM token_registry WHERE token_name ILIKE '%' || $1 || '%'
		) matches
	`, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hexIDs []string
	for rows.Next() {
		var contractID string
		if err := rows.Scan(&contractID); err != nil {
			return nil, err
		}
		if contractID == "" {
			continue
		}
		if normalized, err := normalizeContractID(contractID); err == nil {
			hexIDs = append(hexIDs, normalized)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return hexIDs, nil
}

func (h *SilverHotReader) batchResolveContractNames(ctx context.Context, contractIDs []string) map[string]contractNameInfo {
	result := make(map[string]contractNameInfo)
	if len(contractIDs) == 0 {
		return result
	}
	placeholders := make([]string, len(contractIDs))
	args := make([]any, len(contractIDs))
	for i, id := range contractIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf(`
		SELECT cr.contract_id, cr.display_name, tr.token_symbol, cr.category
		FROM contract_registry cr
		LEFT JOIN token_registry tr ON cr.contract_id = tr.contract_id
		WHERE cr.contract_id IN (%s)
	`, strings.Join(placeholders, ","))
	rows, err := h.db.QueryContext(ctx, query, args...)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var contractID string
			var name, symbol, category sql.NullString
			if err := rows.Scan(&contractID, &name, &symbol, &category); err != nil {
				continue
			}
			strKeyID := contractID
			if converted, err := hexToStrKey(contractID); err == nil {
				strKeyID = converted
			}
			info := contractNameInfo{}
			if name.Valid && name.String != "" {
				s := name.String
				info.name = &s
			}
			if symbol.Valid && symbol.String != "" {
				s := symbol.String
				info.symbol = &s
			}
			if category.Valid && category.String != "" {
				s := category.String
				info.category = &s
			}
			result[strKeyID] = info
		}
	}
	if len(result) == len(contractIDs) {
		return result
	}

	missingHex := make([]string, 0)
	for _, id := range contractIDs {
		if _, ok := result[id]; !ok {
			if hexID, err := normalizeContractID(id); err == nil {
				missingHex = append(missingHex, hexID)
			}
		}
	}
	if len(missingHex) == 0 {
		return result
	}
	placeholders = make([]string, len(missingHex))
	args = make([]any, len(missingHex))
	for i, id := range missingHex {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	fallbackQuery := fmt.Sprintf(`SELECT contract_id, token_name, token_symbol FROM token_registry WHERE contract_id IN (%s)`, strings.Join(placeholders, ","))
	rows, err = h.db.QueryContext(ctx, fallbackQuery, args...)
	if err != nil {
		return result
	}
	defer rows.Close()
	for rows.Next() {
		var contractID string
		var name, symbol sql.NullString
		if err := rows.Scan(&contractID, &name, &symbol); err != nil {
			continue
		}
		strKeyID := contractID
		if converted, err := hexToStrKey(contractID); err == nil {
			strKeyID = converted
		}
		info := contractNameInfo{}
		if name.Valid && name.String != "" {
			s := name.String
			info.name = &s
		}
		if symbol.Valid && symbol.String != "" {
			s := symbol.String
			info.symbol = &s
		}
		tokenCat := "token"
		info.category = &tokenCat
		result[strKeyID] = info
	}
	return result
}

// GetServingTopContracts returns top contracts from serving stats tables.
func (h *SilverHotReader) GetServingTopContracts(ctx context.Context, period string, limit int) ([]TopContract, error) {
	if period == "all" {
		return nil, fmt.Errorf("period=all not supported by serving fast path")
	}
	orderCol := "total_calls_24h"
	uniqueCol := "unique_callers_24h"
	if period == "7d" {
		orderCol = "total_calls_7d"
		uniqueCol = "unique_callers_7d"
	} else if period == "30d" {
		orderCol = "total_calls_30d"
		uniqueCol = "unique_callers_7d"
	}
	query := fmt.Sprintf(`
		SELECT contract_id, %s as total_calls, COALESCE(%s, 0) as unique_callers, COALESCE(top_function, ''), last_activity_at
		FROM serving.sv_contract_stats_current
		WHERE %s > 0
		ORDER BY %s DESC, contract_id ASC
		LIMIT $1
	`, orderCol, uniqueCol, orderCol, orderCol)
	rows, err := h.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TopContract
	for rows.Next() {
		var tc TopContract
		var totalCalls, uniqueCallers int64
		var lastActivity sql.NullTime
		if err := rows.Scan(&tc.ContractID, &totalCalls, &uniqueCallers, &tc.TopFunction, &lastActivity); err != nil {
			return nil, err
		}
		tc.TotalCalls = int(totalCalls)
		tc.UniqueCallers = int(uniqueCallers)
		if lastActivity.Valid {
			tc.LastActivity = lastActivity.Time.UTC().Format(time.RFC3339)
		}
		out = append(out, tc)
	}
	return out, rows.Err()
}

// GetServingGenericEvents returns recent generic contract events from serving projections.
func (h *SilverHotReader) GetServingGenericEvents(ctx context.Context, filters GenericEventFilters) ([]GenericEvent, string, bool, error) {
	limit := filters.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	requestLimit := limit + 1
	orderDir := "DESC"
	cursorOp := "<"
	if filters.Order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}

	query := `
		SELECT
			event_id,
			contract_id,
			ledger_sequence,
			tx_hash,
			created_at,
			event_type,
			COALESCE((raw_event_json->>'in_successful_contract_call')::boolean, true),
			raw_event_json->>'topics_json',
			raw_event_json->>'topics_decoded',
			raw_event_json->>'data_decoded',
			COALESCE((raw_event_json->>'topic_count')::int,
				CASE WHEN topic3 IS NOT NULL THEN 4 WHEN topic2 IS NOT NULL THEN 3 WHEN topic1 IS NOT NULL THEN 2 WHEN topic0 IS NOT NULL THEN 1 ELSE 0 END),
			COALESCE((raw_event_json->>'operation_index')::int, 0),
			COALESCE(event_index, 0),
			topic0,
			topic1,
			topic2,
			topic3
		FROM serving.sv_events_recent
		WHERE 1=1
	`
	args := []any{}
	argPos := 1
	if filters.ContractID != nil && *filters.ContractID != "" {
		query += fmt.Sprintf(" AND contract_id = $%d", argPos)
		args = append(args, *filters.ContractID)
		argPos++
	}
	if filters.TxHash != nil && *filters.TxHash != "" {
		query += fmt.Sprintf(" AND tx_hash = $%d", argPos)
		args = append(args, *filters.TxHash)
		argPos++
	}
	if filters.EventType != nil && *filters.EventType != "" {
		query += fmt.Sprintf(" AND event_type = $%d", argPos)
		args = append(args, *filters.EventType)
		argPos++
	}
	if filters.TopicMatch != nil && *filters.TopicMatch != "" {
		query += fmt.Sprintf(" AND COALESCE(raw_event_json->>'topics_decoded','') ILIKE '%%' || $%d || '%%'", argPos)
		args = append(args, *filters.TopicMatch)
		argPos++
	}
	if filters.Topic0 != nil && *filters.Topic0 != "" {
		query += fmt.Sprintf(" AND topic0 = $%d", argPos)
		args = append(args, *filters.Topic0)
		argPos++
	}
	if filters.Topic1 != nil && *filters.Topic1 != "" {
		query += fmt.Sprintf(" AND topic1 = $%d", argPos)
		args = append(args, *filters.Topic1)
		argPos++
	}
	if filters.Topic2 != nil && *filters.Topic2 != "" {
		query += fmt.Sprintf(" AND topic2 = $%d", argPos)
		args = append(args, *filters.Topic2)
		argPos++
	}
	if filters.Topic3 != nil && *filters.Topic3 != "" {
		query += fmt.Sprintf(" AND topic3 = $%d", argPos)
		args = append(args, *filters.Topic3)
		argPos++
	}
	if filters.StartLedger != nil {
		query += fmt.Sprintf(" AND ledger_sequence >= $%d", argPos)
		args = append(args, *filters.StartLedger)
		argPos++
	}
	if filters.EndLedger != nil {
		query += fmt.Sprintf(" AND ledger_sequence <= $%d", argPos)
		args = append(args, *filters.EndLedger)
		argPos++
	}
	if filters.Cursor != nil && *filters.Cursor != "" {
		parts := strings.SplitN(*filters.Cursor, ":", 2)
		if len(parts) == 2 {
			cursorLedger, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return nil, "", false, fmt.Errorf("invalid cursor ledger value %q: %w", parts[0], err)
			}
			cursorEvent, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, "", false, fmt.Errorf("invalid cursor event_index value %q: %w", parts[1], err)
			}
			query += fmt.Sprintf(" AND (ledger_sequence %s $%d OR (ledger_sequence = $%d AND COALESCE(event_index,0) %s $%d))", cursorOp, argPos, argPos, cursorOp, argPos+1)
			args = append(args, cursorLedger, cursorEvent)
			argPos += 2
		}
	}
	query += fmt.Sprintf(" ORDER BY ledger_sequence %s, event_index %s LIMIT $%d", orderDir, orderDir, argPos)
	args = append(args, requestLimit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var events []GenericEvent
	for rows.Next() {
		var e GenericEvent
		var contractID sql.NullString
		var createdAt sql.NullTime
		if err := rows.Scan(&e.EventID, &contractID, &e.LedgerSeq, &e.TxHash, &createdAt, &e.EventType, &e.Successful, &e.TopicsJSON, &e.TopicsDecoded, &e.DataDecoded, &e.TopicCount, &e.OpIndex, &e.EventIndex, &e.Topic0Decoded, &e.Topic1Decoded, &e.Topic2Decoded, &e.Topic3Decoded); err != nil {
			return nil, "", false, err
		}
		if contractID.Valid {
			e.ContractID = &contractID.String
		}
		if createdAt.Valid {
			e.ClosedAt = createdAt.Time.UTC().Format(time.RFC3339)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}

	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}
	var nextCursor string
	if len(events) > 0 && hasMore {
		last := events[len(events)-1]
		nextCursor = fmt.Sprintf("%d:%d", last.LedgerSeq, last.EventIndex)
	}
	return events, nextCursor, hasMore, nil
}

// GetServingContractRecentCalls returns recent contract calls from serving projections.
func (h *SilverHotReader) GetServingContractRecentCalls(ctx context.Context, contractID string, limit int, cursor *OperationCursor, order string) ([]ContractCallRecord, string, bool, error) {
	requestLimit := limit + 1
	orderDir := "DESC"
	cursorOp := "<"
	if order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}

	query := `
		SELECT
			tx_hash,
			ledger_sequence,
			created_at,
			contract_id,
			function_name,
			COALESCE(caller_account, ''),
			COALESCE(successful, false),
			COALESCE(NULLIF(split_part(call_id, ':', 3), ''), '0')::bigint as operation_index
		FROM serving.sv_contract_calls_recent
		WHERE contract_id = $1
	`
	args := []any{contractID}
	argPos := 2
	if cursor != nil {
		query += fmt.Sprintf(" AND (ledger_sequence %s $%d OR (ledger_sequence = $%d AND COALESCE(NULLIF(split_part(call_id, ':', 3), ''), '0')::bigint %s $%d))", cursorOp, argPos, argPos, cursorOp, argPos+1)
		args = append(args, cursor.LedgerSequence, cursor.OperationIndex)
		argPos += 2
	}
	query += fmt.Sprintf(" ORDER BY ledger_sequence %s, operation_index %s LIMIT $%d", orderDir, orderDir, argPos)
	args = append(args, requestLimit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var calls []ContractCallRecord
	for rows.Next() {
		var c ContractCallRecord
		var createdAt sql.NullTime
		var contractIDVal sql.NullString
		var functionName sql.NullString
		if err := rows.Scan(&c.TransactionHash, &c.LedgerSequence, &createdAt, &contractIDVal, &functionName, &c.SourceAccount, &c.Successful, &c.OperationIndex); err != nil {
			return nil, "", false, err
		}
		if createdAt.Valid {
			c.ClosedAt = createdAt.Time.UTC().Format(time.RFC3339)
		}
		if contractIDVal.Valid {
			c.ContractID = &contractIDVal.String
		}
		if functionName.Valid {
			c.FunctionName = &functionName.String
		}
		calls = append(calls, c)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}

	hasMore := len(calls) > limit
	if hasMore {
		calls = calls[:limit]
	}
	var nextCursor string
	if len(calls) > 0 && hasMore {
		last := calls[len(calls)-1]
		nextCursor = (OperationCursor{LedgerSequence: last.LedgerSequence, OperationIndex: last.OperationIndex, Order: order}).Encode()
	}
	return calls, nextCursor, hasMore, nil
}

// GetServingContractMetadata returns contract metadata from serving projections.
func (h *SilverHotReader) GetServingContractMetadata(ctx context.Context, contractID string) (*ContractMetadataResponse, error) {
	resp := &ContractMetadataResponse{ContractID: contractID}
	var name, symbol, contractType sql.NullString
	var creator, wasmHash sql.NullString
	var deployLedger, wasmSizeBytes, totalStateSizeBytes, estimatedMonthlyRent sql.NullInt64
	var deployTime sql.NullTime
	var persistentEntries, temporaryEntries, instanceEntries sql.NullInt32
	if err := h.db.QueryRowContext(ctx, `
		SELECT name, symbol, contract_type, creator_account, wasm_hash, deploy_ledger, deploy_timestamp,
		       wasm_size_bytes, persistent_entries, temporary_entries, instance_entries,
		       total_state_size_bytes, estimated_monthly_rent_stroops
		FROM serving.sv_contracts_current
		WHERE contract_id = $1
	`, contractID).Scan(
		&name, &symbol, &contractType, &creator, &wasmHash, &deployLedger, &deployTime,
		&wasmSizeBytes, &persistentEntries, &temporaryEntries, &instanceEntries,
		&totalStateSizeBytes, &estimatedMonthlyRent,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if name.Valid {
		resp.DisplayName = &name.String
	}
	if contractType.Valid {
		resp.ContractType = &contractType.String
	}
	if creator.Valid {
		resp.CreatorAddress = &creator.String
	}
	if wasmHash.Valid {
		resp.WasmHash = &wasmHash.String
	}
	if wasmSizeBytes.Valid {
		resp.WasmSizeBytes = &wasmSizeBytes.Int64
	}
	if deployLedger.Valid {
		resp.CreatedLedger = &deployLedger.Int64
	}
	if deployTime.Valid {
		s := deployTime.Time.UTC().Format(time.RFC3339)
		resp.CreatedAt = &s
	}
	if persistentEntries.Valid {
		resp.PersistentEntries = int(persistentEntries.Int32)
	}
	if temporaryEntries.Valid {
		resp.TemporaryEntries = int(temporaryEntries.Int32)
	}
	if instanceEntries.Valid {
		resp.InstanceEntries = int(instanceEntries.Int32)
	}
	resp.TotalEntries = resp.PersistentEntries + resp.TemporaryEntries + resp.InstanceEntries
	if totalStateSizeBytes.Valid {
		resp.TotalStateSizeBytes = &totalStateSizeBytes.Int64
	}
	if estimatedMonthlyRent.Valid {
		resp.EstimatedMonthlyRentStroops = &estimatedMonthlyRent.Int64
	}
	rows, err := h.db.QueryContext(ctx, `
		SELECT function_name, COUNT(*) as call_count
		FROM contract_invocations_raw
		WHERE contract_id = $1 AND function_name <> ''
		GROUP BY function_name
		ORDER BY call_count DESC, function_name ASC
	`, contractID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var fc FunctionCallCount
			if err := rows.Scan(&fc.Name, &fc.CallCount); err != nil {
				break
			}
			resp.ExportedFunctions = append(resp.ExportedFunctions, fc)
		}
	}
	h.enrichContractRegistry(ctx, resp)
	if resp.EstimatedMonthlyRentStroops == nil {
		h.enrichContractRentEstimate(ctx, resp)
	}
	_ = symbol
	return resp, nil
}

// Close closes the database connection
func (h *SilverHotReader) Close() error {
	if h.db != nil {
		return h.db.Close()
	}
	return nil
}

// ============================================
// ACCOUNT QUERIES (Current State + History)
// ============================================

// GetAccountCurrent returns the current state of an account from hot buffer
func (h *SilverHotReader) GetAccountCurrent(ctx context.Context, accountID string) (*AccountCurrent, error) {
	query := `
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at,
			home_domain
		FROM accounts_current
		WHERE account_id = $1
	`

	var acc AccountCurrent
	var homeDomain sql.NullString
	err := h.db.QueryRowContext(ctx, query, accountID).Scan(
		&acc.AccountID, &acc.Balance, &acc.SequenceNumber,
		&acc.NumSubentries, &acc.LastModifiedLedger, &acc.UpdatedAt,
		&homeDomain,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if homeDomain.Valid && homeDomain.String != "" {
		acc.HomeDomain = &homeDomain.String
	}

	return &acc, nil
}

// GetAccountCreatedAt returns the earliest closed_at for an account from accounts_snapshot
func (h *SilverHotReader) GetAccountCreatedAt(ctx context.Context, accountID string) (*string, error) {
	query := `
		SELECT MIN(closed_at)::text
		FROM accounts_snapshot
		WHERE account_id = $1
	`
	var createdAt sql.NullString
	err := h.db.QueryRowContext(ctx, query, accountID).Scan(&createdAt)
	if err != nil {
		return nil, err
	}
	if !createdAt.Valid {
		return nil, sql.ErrNoRows
	}
	return &createdAt.String, nil
}

// GetAccountHistory returns historical snapshots from hot buffer
func (h *SilverHotReader) GetAccountHistory(ctx context.Context, accountID string, limit int, cursor *AccountCursor) ([]AccountSnapshot, error) {
	query := `
		SELECT
			account_id,
			balance,
			sequence_number,
			ledger_sequence,
			closed_at,
			valid_to
		FROM accounts_snapshot
		WHERE account_id = $1
	`

	args := []interface{}{accountID}

	// Cursor-based pagination: filter for records before the cursor position
	if cursor != nil {
		query += " AND ledger_sequence < $2"
		args = append(args, cursor.LedgerSequence)
	}

	query += " ORDER BY ledger_sequence DESC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snapshots []AccountSnapshot
	for rows.Next() {
		var snap AccountSnapshot
		if err := rows.Scan(&snap.AccountID, &snap.Balance, &snap.SequenceNumber,
			&snap.LedgerSequence, &snap.ClosedAt, &snap.ValidTo); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snap)
	}

	return snapshots, nil
}

// GetTopAccounts returns top accounts by balance from hot buffer
func (h *SilverHotReader) GetTopAccounts(ctx context.Context, limit int) ([]AccountCurrent, error) {
	query := `
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM accounts_current
		ORDER BY CAST(balance AS DECIMAL) DESC
		LIMIT $1
	`

	rows, err := h.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []AccountCurrent
	for rows.Next() {
		var acc AccountCurrent
		if err := rows.Scan(&acc.AccountID, &acc.Balance, &acc.SequenceNumber,
			&acc.NumSubentries, &acc.LastModifiedLedger, &acc.UpdatedAt); err != nil {
			return nil, err
		}
		accounts = append(accounts, acc)
	}

	return accounts, nil
}

// AccountListFilters contains filters for listing accounts
type AccountListFilters struct {
	SortBy     string             // "balance", "last_modified", "account_id"
	SortOrder  string             // "asc" or "desc"
	MinBalance *int64             // Optional minimum balance filter
	Limit      int                // Max results to return
	Cursor     *AccountListCursor // Cursor for pagination
}

// GetAccountsList returns a paginated list of all accounts
func (h *SilverHotReader) GetAccountsList(ctx context.Context, filters AccountListFilters) ([]AccountCurrent, error) {
	// Build base query
	query := `
		SELECT
			account_id,
			balance,
			sequence_number,
			num_subentries,
			last_modified_ledger,
			updated_at
		FROM accounts_current
		WHERE 1=1
	`

	args := []interface{}{}

	// Apply minimum balance filter (balance is stored as decimal string in XLM)
	if filters.MinBalance != nil {
		// Convert stroops to XLM for comparison (divide by 10^7)
		minBalXLM := float64(*filters.MinBalance) / 10000000.0
		query += " AND CAST(balance AS DECIMAL) >= $" + fmt.Sprint(len(args)+1)
		args = append(args, minBalXLM)
	}

	// Cursor-based pagination - respects current sort field and order
	if filters.Cursor != nil {
		// Determine effective sort settings
		sortBy := filters.SortBy
		if sortBy == "" {
			sortBy = "balance"
		}
		sortOrder := filters.SortOrder
		if sortOrder == "" {
			sortOrder = "desc"
		}
		isAsc := sortOrder == "asc"

		switch sortBy {
		case "last_modified":
			// Paginate based on last_modified_ledger, tie-break by account_id
			if isAsc {
				query += " AND (last_modified_ledger > $" + fmt.Sprint(len(args)+1) +
					" OR (last_modified_ledger = $" + fmt.Sprint(len(args)+2) +
					" AND account_id > $" + fmt.Sprint(len(args)+3) + "))"
			} else {
				query += " AND (last_modified_ledger < $" + fmt.Sprint(len(args)+1) +
					" OR (last_modified_ledger = $" + fmt.Sprint(len(args)+2) +
					" AND account_id > $" + fmt.Sprint(len(args)+3) + "))"
			}
			args = append(args, filters.Cursor.LastModifiedLedger, filters.Cursor.LastModifiedLedger, filters.Cursor.AccountID)

		case "account_id":
			// Paginate directly on account_id
			if isAsc {
				query += " AND account_id > $" + fmt.Sprint(len(args)+1)
			} else {
				query += " AND account_id < $" + fmt.Sprint(len(args)+1)
			}
			args = append(args, filters.Cursor.AccountID)

		default: // "balance" or empty
			// Paginate based on balance, tie-break by account_id
			cursorBalXLM := float64(filters.Cursor.Balance) / 10000000.0
			if isAsc {
				query += " AND (CAST(balance AS DECIMAL) > $" + fmt.Sprint(len(args)+1) +
					" OR (CAST(balance AS DECIMAL) = $" + fmt.Sprint(len(args)+2) +
					" AND account_id > $" + fmt.Sprint(len(args)+3) + "))"
			} else {
				query += " AND (CAST(balance AS DECIMAL) < $" + fmt.Sprint(len(args)+1) +
					" OR (CAST(balance AS DECIMAL) = $" + fmt.Sprint(len(args)+2) +
					" AND account_id > $" + fmt.Sprint(len(args)+3) + "))"
			}
			args = append(args, cursorBalXLM, cursorBalXLM, filters.Cursor.AccountID)
		}
	}

	// Default sort by balance descending (balance is stored as decimal string)
	sortBy := "CAST(balance AS DECIMAL)"
	sortOrder := "DESC"

	switch filters.SortBy {
	case "last_modified":
		sortBy = "last_modified_ledger"
	case "account_id":
		sortBy = "account_id"
	}

	if filters.SortOrder == "asc" {
		sortOrder = "ASC"
	}

	// Add secondary sort by account_id for stable ordering
	query += fmt.Sprintf(" ORDER BY %s %s, account_id ASC LIMIT $%d", sortBy, sortOrder, len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []AccountCurrent
	for rows.Next() {
		var acc AccountCurrent
		if err := rows.Scan(&acc.AccountID, &acc.Balance, &acc.SequenceNumber,
			&acc.NumSubentries, &acc.LastModifiedLedger, &acc.UpdatedAt); err != nil {
			return nil, err
		}
		accounts = append(accounts, acc)
	}

	return accounts, nil
}

// ============================================
// ENRICHED OPERATIONS QUERIES
// ============================================

// GetEnrichedOperations returns enriched operations from hot buffer
func (h *SilverHotReader) GetEnrichedOperations(ctx context.Context, filters OperationFilters) ([]EnrichedOperation, error) {
	query := `
		SELECT
			transaction_hash,
			operation_index,
			ledger_sequence,
			ledger_closed_at,
			source_account,
			type,
			destination,
			asset_code,
			asset_issuer,
			amount,
			tx_successful,
			tx_fee_charged,
			is_payment_op,
			is_soroban_op
		FROM enriched_history_operations
		WHERE 1=1
	`

	args := []interface{}{}

	if filters.AccountID != "" {
		query += " AND (source_account = $" + fmt.Sprint(len(args)+1) + " OR destination = $" + fmt.Sprint(len(args)+2) + ")"
		args = append(args, filters.AccountID, filters.AccountID)
	}

	if filters.TxHash != "" {
		query += " AND transaction_hash = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.TxHash)
	}

	if filters.StartLedger > 0 {
		query += " AND ledger_sequence >= $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.StartLedger)
	}

	if filters.EndLedger > 0 {
		query += " AND ledger_sequence <= $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.EndLedger)
	}

	if filters.PaymentsOnly {
		query += " AND is_payment_op = true"
	}

	if filters.SorobanOnly {
		query += " AND is_soroban_op = true"
	}

	// Cursor-based pagination: filter for records before the cursor position
	if filters.Cursor != nil {
		query += " AND (ledger_sequence < $" + fmt.Sprint(len(args)+1) +
			" OR (ledger_sequence = $" + fmt.Sprint(len(args)+2) +
			" AND operation_index < $" + fmt.Sprint(len(args)+3) + "))"
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.LedgerSequence, filters.Cursor.OperationIndex)
	}

	query += " ORDER BY ledger_sequence DESC, operation_index DESC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var operations []EnrichedOperation
	for rows.Next() {
		var op EnrichedOperation
		if err := rows.Scan(&op.TransactionHash, &op.OperationID, &op.LedgerSequence,
			&op.LedgerClosedAt, &op.SourceAccount, &op.Type,
			&op.Destination, &op.AssetCode, &op.AssetIssuer, &op.Amount,
			&op.TxSuccessful, &op.TxFeeCharged, &op.IsPaymentOp, &op.IsSorobanOp); err != nil {
			return nil, err
		}
		op.TypeName = operationTypeName(op.Type)
		operations = append(operations, op)
	}

	return operations, nil
}

// ============================================
// TOKEN TRANSFERS QUERIES
// ============================================

// GetTokenTransfers returns token transfers from hot buffer
func (h *SilverHotReader) GetTokenTransfers(ctx context.Context, filters TransferFilters) ([]TokenTransfer, error) {
	query := `
		SELECT
			timestamp,
			transaction_hash,
			ledger_sequence,
			source_type,
			from_account,
			to_account,
			asset_code,
			asset_issuer,
			amount,
			token_contract_id,
			transaction_successful
		FROM token_transfers_raw
		WHERE transaction_successful = true
	`

	args := []interface{}{}

	if filters.SourceType != "" {
		query += " AND source_type = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.SourceType)
	}

	if filters.AssetCode != "" {
		query += " AND asset_code = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.AssetCode)
	}

	if filters.FromAccount != "" {
		query += " AND from_account = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.FromAccount)
	}

	if filters.ToAccount != "" {
		query += " AND to_account = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.ToAccount)
	}

	if !filters.StartTime.IsZero() {
		query += " AND timestamp >= $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.StartTime)
	}

	if !filters.EndTime.IsZero() {
		query += " AND timestamp <= $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.EndTime)
	}

	// Cursor-based pagination: filter for records before the cursor position
	if filters.Cursor != nil {
		query += " AND (ledger_sequence < $" + fmt.Sprint(len(args)+1) +
			" OR (ledger_sequence = $" + fmt.Sprint(len(args)+2) +
			" AND timestamp < $" + fmt.Sprint(len(args)+3) + "))"
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.LedgerSequence, filters.Cursor.Timestamp)
	}

	query += " ORDER BY ledger_sequence DESC, timestamp DESC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transfers []TokenTransfer
	for rows.Next() {
		var t TokenTransfer
		if err := rows.Scan(&t.Timestamp, &t.TransactionHash, &t.LedgerSequence,
			&t.SourceType, &t.FromAccount, &t.ToAccount, &t.AssetCode, &t.AssetIssuer,
			&t.Amount, &t.TokenContractID, &t.TransactionSuccessful); err != nil {
			return nil, err
		}
		transfers = append(transfers, t)
	}

	return transfers, nil
}

// ============================================
// CONTRACT CALL QUERIES (Freighter Support)
// ============================================

// GetContractsInvolved returns all unique contracts involved in a transaction
func (h *SilverHotReader) GetContractsInvolved(ctx context.Context, txHash string) ([]string, error) {
	query := `
		SELECT DISTINCT contract_id
		FROM (
			SELECT from_contract AS contract_id FROM contract_invocation_calls WHERE transaction_hash = $1
			UNION
			SELECT to_contract AS contract_id FROM contract_invocation_calls WHERE transaction_hash = $1
		) contracts
		ORDER BY contract_id
	`

	rows, err := h.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query contracts involved: %w", err)
	}
	defer rows.Close()

	var contracts []string
	for rows.Next() {
		var contractID string
		if err := rows.Scan(&contractID); err != nil {
			return nil, err
		}
		contracts = append(contracts, contractID)
	}

	return contracts, nil
}

// GetTransactionCallGraph returns the call graph for a transaction
func (h *SilverHotReader) GetTransactionCallGraph(ctx context.Context, txHash string) ([]ContractCall, error) {
	query := `
		SELECT
			from_contract,
			to_contract,
			function_name,
			call_depth,
			execution_order,
			successful,
			transaction_hash,
			ledger_sequence,
			closed_at
		FROM contract_invocation_calls
		WHERE transaction_hash = $1
		ORDER BY execution_order
	`

	rows, err := h.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query call graph: %w", err)
	}
	defer rows.Close()

	var calls []ContractCall
	for rows.Next() {
		var call ContractCall
		var closedAt sql.NullTime
		if err := rows.Scan(
			&call.FromContract, &call.ToContract, &call.FunctionName,
			&call.CallDepth, &call.ExecutionOrder, &call.Successful,
			&call.TransactionHash, &call.LedgerSequence, &closedAt,
		); err != nil {
			return nil, err
		}
		if closedAt.Valid {
			call.ClosedAt = closedAt.Time.Format("2006-01-02T15:04:05Z")
		}
		calls = append(calls, call)
	}

	return calls, nil
}

// GetTransactionHierarchy returns the contract hierarchy for a transaction
func (h *SilverHotReader) GetTransactionHierarchy(ctx context.Context, txHash string) ([]ContractHierarchy, error) {
	query := `
		SELECT
			root_contract,
			child_contract,
			path_depth,
			full_path
		FROM contract_invocation_hierarchy
		WHERE transaction_hash = $1
		ORDER BY path_depth
	`

	rows, err := h.db.QueryContext(ctx, query, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to query hierarchy: %w", err)
	}
	defer rows.Close()

	var hierarchies []ContractHierarchy
	for rows.Next() {
		var h ContractHierarchy
		var pathArray []string
		if err := rows.Scan(&h.RootContract, &h.ChildContract, &h.PathDepth, pq.Array(&pathArray)); err != nil {
			return nil, err
		}
		h.FullPath = pathArray
		hierarchies = append(hierarchies, h)
	}

	return hierarchies, nil
}

// GetContractRecentCalls returns recent calls for a contract
func (h *SilverHotReader) GetContractRecentCalls(ctx context.Context, contractID string, limit int) ([]ContractCall, int, int, error) {
	query := `
		SELECT
			from_contract,
			to_contract,
			function_name,
			call_depth,
			execution_order,
			successful,
			transaction_hash,
			ledger_sequence,
			closed_at
		FROM contract_invocation_calls
		WHERE from_contract = $1 OR to_contract = $1
		ORDER BY closed_at DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to query recent calls: %w", err)
	}
	defer rows.Close()

	var calls []ContractCall
	asCaller := 0
	asCallee := 0

	for rows.Next() {
		var call ContractCall
		var closedAt sql.NullTime
		if err := rows.Scan(
			&call.FromContract, &call.ToContract, &call.FunctionName,
			&call.CallDepth, &call.ExecutionOrder, &call.Successful,
			&call.TransactionHash, &call.LedgerSequence, &closedAt,
		); err != nil {
			return nil, 0, 0, err
		}
		if closedAt.Valid {
			call.ClosedAt = closedAt.Time.Format("2006-01-02T15:04:05Z")
		}
		calls = append(calls, call)

		if call.FromContract == contractID {
			asCaller++
		}
		if call.ToContract == contractID {
			asCallee++
		}
	}

	return calls, asCaller, asCallee, nil
}

// GetContractCallers returns contracts that call a specific contract
func (h *SilverHotReader) GetContractCallers(ctx context.Context, contractID string, limit int) ([]ContractRelationship, error) {
	query := `
		SELECT
			from_contract AS contract_id,
			COUNT(*) AS call_count,
			array_agg(DISTINCT function_name) AS functions,
			MAX(closed_at) AS last_call
		FROM contract_invocation_calls
		WHERE to_contract = $1
		GROUP BY from_contract
		ORDER BY call_count DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query callers: %w", err)
	}
	defer rows.Close()

	var callers []ContractRelationship
	for rows.Next() {
		var r ContractRelationship
		var functionsArray []string
		var lastCall sql.NullTime
		if err := rows.Scan(&r.ContractID, &r.CallCount, pq.Array(&functionsArray), &lastCall); err != nil {
			return nil, err
		}
		r.Functions = functionsArray
		if lastCall.Valid {
			r.LastCall = lastCall.Time.Format("2006-01-02T15:04:05Z")
		}
		callers = append(callers, r)
	}

	return callers, nil
}

// GetContractCallees returns contracts called by a specific contract
func (h *SilverHotReader) GetContractCallees(ctx context.Context, contractID string, limit int) ([]ContractRelationship, error) {
	query := `
		SELECT
			to_contract AS contract_id,
			COUNT(*) AS call_count,
			array_agg(DISTINCT function_name) AS functions,
			MAX(closed_at) AS last_call
		FROM contract_invocation_calls
		WHERE from_contract = $1
		GROUP BY to_contract
		ORDER BY call_count DESC
		LIMIT $2
	`

	rows, err := h.db.QueryContext(ctx, query, contractID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query callees: %w", err)
	}
	defer rows.Close()

	var callees []ContractRelationship
	for rows.Next() {
		var r ContractRelationship
		var functionsArray []string
		var lastCall sql.NullTime
		if err := rows.Scan(&r.ContractID, &r.CallCount, pq.Array(&functionsArray), &lastCall); err != nil {
			return nil, err
		}
		r.Functions = functionsArray
		if lastCall.Valid {
			r.LastCall = lastCall.Time.Format("2006-01-02T15:04:05Z")
		}
		callees = append(callees, r)
	}

	return callees, nil
}

// GetContractCallSummary returns aggregated call statistics for a contract
func (h *SilverHotReader) GetContractCallSummary(ctx context.Context, contractID string) (*ContractCallSummary, error) {
	query := `
		WITH caller_stats AS (
			SELECT COUNT(*) as total_as_caller, COUNT(DISTINCT to_contract) as unique_callees
			FROM contract_invocation_calls WHERE from_contract = $1
		),
		callee_stats AS (
			SELECT COUNT(*) as total_as_callee, COUNT(DISTINCT from_contract) as unique_callers
			FROM contract_invocation_calls WHERE to_contract = $1
		),
		time_stats AS (
			SELECT MIN(closed_at) as first_seen, MAX(closed_at) as last_seen
			FROM contract_invocation_calls WHERE from_contract = $1 OR to_contract = $1
		)
		SELECT
			$1 as contract_id,
			COALESCE((SELECT total_as_caller FROM caller_stats), 0),
			COALESCE((SELECT total_as_callee FROM callee_stats), 0),
			COALESCE((SELECT unique_callers FROM callee_stats), 0),
			COALESCE((SELECT unique_callees FROM caller_stats), 0),
			(SELECT first_seen FROM time_stats),
			(SELECT last_seen FROM time_stats)
	`

	var summary ContractCallSummary
	var firstSeen, lastSeen sql.NullTime

	err := h.db.QueryRowContext(ctx, query, contractID).Scan(
		&summary.ContractID,
		&summary.TotalCallsAsCaller,
		&summary.TotalCallsAsCallee,
		&summary.UniqueCallers,
		&summary.UniqueCallees,
		&firstSeen,
		&lastSeen,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get call summary: %w", err)
	}

	if firstSeen.Valid {
		summary.FirstSeen = firstSeen.Time.Format("2006-01-02T15:04:05Z")
	}
	if lastSeen.Valid {
		summary.LastSeen = lastSeen.Time.Format("2006-01-02T15:04:05Z")
	}

	return &summary, nil
}

// ============================================
// PHASE 6: STATE TABLE QUERIES (Offers, Liquidity Pools, Claimable Balances)
// ============================================

// GetOffers returns offers with optional filters
func (h *SilverHotReader) GetOffers(ctx context.Context, filters OfferFilters) ([]OfferCurrent, error) {
	query := `
		SELECT
			offer_id,
			seller_id,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			amount,
			price_n,
			price_d,
			price,
			last_modified_ledger,
			sponsor
		FROM offers_current
		WHERE 1=1
	`

	args := []interface{}{}

	if filters.SellerID != "" {
		query += " AND seller_id = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.SellerID)
	}

	if filters.SellingAssetCode != "" {
		if filters.SellingAssetCode == "XLM" {
			query += " AND selling_asset_type = 'native'"
		} else {
			query += " AND selling_asset_code = $" + fmt.Sprint(len(args)+1)
			args = append(args, filters.SellingAssetCode)
			if filters.SellingAssetIssuer != "" {
				query += " AND selling_asset_issuer = $" + fmt.Sprint(len(args)+1)
				args = append(args, filters.SellingAssetIssuer)
			}
		}
	}

	if filters.BuyingAssetCode != "" {
		if filters.BuyingAssetCode == "XLM" {
			query += " AND buying_asset_type = 'native'"
		} else {
			query += " AND buying_asset_code = $" + fmt.Sprint(len(args)+1)
			args = append(args, filters.BuyingAssetCode)
			if filters.BuyingAssetIssuer != "" {
				query += " AND buying_asset_issuer = $" + fmt.Sprint(len(args)+1)
				args = append(args, filters.BuyingAssetIssuer)
			}
		}
	}

	// Cursor-based pagination
	if filters.Cursor != nil {
		query += " AND offer_id > $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.Cursor.OfferID)
	}

	query += " ORDER BY offer_id ASC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query offers: %w", err)
	}
	defer rows.Close()

	var offers []OfferCurrent
	for rows.Next() {
		var o OfferCurrent
		var sellingType, sellingCode, buyingType, buyingCode sql.NullString
		var sellingIssuer, buyingIssuer, sponsor sql.NullString
		var amount int64
		var priceN, priceD int
		var price float64

		if err := rows.Scan(
			&o.OfferID, &o.SellerID,
			&sellingType, &sellingCode, &sellingIssuer,
			&buyingType, &buyingCode, &buyingIssuer,
			&amount, &priceN, &priceD, &price,
			&o.LastModifiedLedger, &sponsor,
		); err != nil {
			return nil, err
		}

		// Build selling asset
		o.Selling = buildAssetInfo(sellingType.String, sellingCode.String, sellingIssuer.String)
		// Build buying asset
		o.Buying = buildAssetInfo(buyingType.String, buyingCode.String, buyingIssuer.String)
		// Format amount from stroops
		o.Amount = formatStroops(amount)
		// Set price
		o.Price = fmt.Sprintf("%.7f", price)
		o.PriceR = PriceR{N: priceN, D: priceD}
		// Set sponsor
		if sponsor.Valid {
			o.Sponsor = &sponsor.String
		}

		offers = append(offers, o)
	}

	return offers, nil
}

// GetOfferByID returns a single offer by ID
func (h *SilverHotReader) GetOfferByID(ctx context.Context, offerID int64) (*OfferCurrent, error) {
	query := `
		SELECT
			offer_id,
			seller_id,
			selling_asset_type,
			selling_asset_code,
			selling_asset_issuer,
			buying_asset_type,
			buying_asset_code,
			buying_asset_issuer,
			amount,
			price_n,
			price_d,
			price,
			last_modified_ledger,
			sponsor
		FROM offers_current
		WHERE offer_id = $1
	`

	var o OfferCurrent
	var sellingType, sellingCode, buyingType, buyingCode sql.NullString
	var sellingIssuer, buyingIssuer, sponsor sql.NullString
	var amount int64
	var priceN, priceD int
	var price float64

	err := h.db.QueryRowContext(ctx, query, offerID).Scan(
		&o.OfferID, &o.SellerID,
		&sellingType, &sellingCode, &sellingIssuer,
		&buyingType, &buyingCode, &buyingIssuer,
		&amount, &priceN, &priceD, &price,
		&o.LastModifiedLedger, &sponsor,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	o.Selling = buildAssetInfo(sellingType.String, sellingCode.String, sellingIssuer.String)
	o.Buying = buildAssetInfo(buyingType.String, buyingCode.String, buyingIssuer.String)
	o.Amount = formatStroops(amount)
	o.Price = fmt.Sprintf("%.7f", price)
	o.PriceR = PriceR{N: priceN, D: priceD}
	if sponsor.Valid {
		o.Sponsor = &sponsor.String
	}

	return &o, nil
}

// GetLiquidityPools returns liquidity pools with optional filters
func (h *SilverHotReader) GetLiquidityPools(ctx context.Context, filters LiquidityPoolFilters) ([]LiquidityPoolCurrent, error) {
	query := `
		SELECT
			liquidity_pool_id,
			pool_type,
			fee,
			trustline_count,
			total_pool_shares,
			asset_a_type,
			asset_a_code,
			asset_a_issuer,
			asset_a_amount,
			asset_b_type,
			asset_b_code,
			asset_b_issuer,
			asset_b_amount,
			last_modified_ledger
		FROM liquidity_pools_current
		WHERE 1=1
	`

	args := []interface{}{}

	// Filter by asset (matches either asset_a or asset_b)
	if filters.AssetCode != "" {
		if filters.AssetCode == "XLM" {
			query += " AND (asset_a_type = 'native' OR asset_b_type = 'native')"
		} else {
			query += " AND ((asset_a_code = $" + fmt.Sprint(len(args)+1) + " AND asset_a_issuer = $" + fmt.Sprint(len(args)+2) + ")"
			query += " OR (asset_b_code = $" + fmt.Sprint(len(args)+1) + " AND asset_b_issuer = $" + fmt.Sprint(len(args)+2) + "))"
			args = append(args, filters.AssetCode, filters.AssetIssuer)
		}
	}

	// Cursor-based pagination
	if filters.Cursor != nil {
		query += " AND liquidity_pool_id > $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.Cursor.PoolID)
	}

	query += " ORDER BY liquidity_pool_id ASC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query liquidity pools: %w", err)
	}
	defer rows.Close()

	var pools []LiquidityPoolCurrent
	for rows.Next() {
		var p LiquidityPoolCurrent
		var assetAType, assetACode, assetBType, assetBCode sql.NullString
		var assetAIssuer, assetBIssuer sql.NullString
		var assetAAmount, assetBAmount, totalShares int64

		if err := rows.Scan(
			&p.PoolID, &p.PoolType, &p.FeeBP, &p.TrustlineCount, &totalShares,
			&assetAType, &assetACode, &assetAIssuer, &assetAAmount,
			&assetBType, &assetBCode, &assetBIssuer, &assetBAmount,
			&p.LastModifiedLedger,
		); err != nil {
			return nil, err
		}

		p.TotalShares = formatStroops(totalShares)
		p.Reserves = []PoolReserve{
			{
				Asset:  buildAssetInfo(assetAType.String, assetACode.String, assetAIssuer.String),
				Amount: formatStroops(assetAAmount),
			},
			{
				Asset:  buildAssetInfo(assetBType.String, assetBCode.String, assetBIssuer.String),
				Amount: formatStroops(assetBAmount),
			},
		}

		pools = append(pools, p)
	}

	return pools, nil
}

// GetLiquidityPoolByID returns a single liquidity pool by ID
func (h *SilverHotReader) GetLiquidityPoolByID(ctx context.Context, poolID string) (*LiquidityPoolCurrent, error) {
	query := `
		SELECT
			liquidity_pool_id,
			pool_type,
			fee,
			trustline_count,
			total_pool_shares,
			asset_a_type,
			asset_a_code,
			asset_a_issuer,
			asset_a_amount,
			asset_b_type,
			asset_b_code,
			asset_b_issuer,
			asset_b_amount,
			last_modified_ledger
		FROM liquidity_pools_current
		WHERE liquidity_pool_id = $1
	`

	var p LiquidityPoolCurrent
	var assetAType, assetACode, assetBType, assetBCode sql.NullString
	var assetAIssuer, assetBIssuer sql.NullString
	var assetAAmount, assetBAmount, totalShares int64

	err := h.db.QueryRowContext(ctx, query, poolID).Scan(
		&p.PoolID, &p.PoolType, &p.FeeBP, &p.TrustlineCount, &totalShares,
		&assetAType, &assetACode, &assetAIssuer, &assetAAmount,
		&assetBType, &assetBCode, &assetBIssuer, &assetBAmount,
		&p.LastModifiedLedger,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	p.TotalShares = formatStroops(totalShares)
	p.Reserves = []PoolReserve{
		{
			Asset:  buildAssetInfo(assetAType.String, assetACode.String, assetAIssuer.String),
			Amount: formatStroops(assetAAmount),
		},
		{
			Asset:  buildAssetInfo(assetBType.String, assetBCode.String, assetBIssuer.String),
			Amount: formatStroops(assetBAmount),
		},
	}

	return &p, nil
}

// GetClaimableBalances returns claimable balances with optional filters
func (h *SilverHotReader) GetClaimableBalances(ctx context.Context, filters ClaimableBalanceFilters) ([]ClaimableBalanceCurrent, error) {
	query := `
		SELECT
			balance_id,
			sponsor,
			asset_type,
			asset_code,
			asset_issuer,
			amount,
			COALESCE(jsonb_array_length(claimants), 0) as claimants_count,
			flags,
			last_modified_ledger
		FROM claimable_balances_current
		WHERE 1=1
	`

	args := []interface{}{}

	if filters.Sponsor != "" {
		query += " AND sponsor = $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.Sponsor)
	}

	if filters.AssetCode != "" {
		if filters.AssetCode == "XLM" {
			query += " AND asset_type = 'native'"
		} else {
			query += " AND asset_code = $" + fmt.Sprint(len(args)+1)
			args = append(args, filters.AssetCode)
			if filters.AssetIssuer != "" {
				query += " AND asset_issuer = $" + fmt.Sprint(len(args)+1)
				args = append(args, filters.AssetIssuer)
			}
		}
	}

	// Cursor-based pagination
	if filters.Cursor != nil {
		query += " AND balance_id > $" + fmt.Sprint(len(args)+1)
		args = append(args, filters.Cursor.BalanceID)
	}

	query += " ORDER BY balance_id ASC LIMIT $" + fmt.Sprint(len(args)+1)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable balances: %w", err)
	}
	defer rows.Close()

	var balances []ClaimableBalanceCurrent
	for rows.Next() {
		var b ClaimableBalanceCurrent
		var sponsor, assetType, assetCode, assetIssuer sql.NullString
		var amount int64

		if err := rows.Scan(
			&b.BalanceID, &sponsor,
			&assetType, &assetCode, &assetIssuer,
			&amount, &b.ClaimantsCount, &b.Flags, &b.LastModifiedLedger,
		); err != nil {
			return nil, err
		}

		if sponsor.Valid {
			b.Sponsor = &sponsor.String
		}
		b.Asset = buildAssetInfo(assetType.String, assetCode.String, assetIssuer.String)
		b.Amount = formatStroops(amount)

		balances = append(balances, b)
	}

	return balances, nil
}

// GetClaimableBalanceByID returns a single claimable balance by ID
func (h *SilverHotReader) GetClaimableBalanceByID(ctx context.Context, balanceID string) (*ClaimableBalanceCurrent, error) {
	query := `
		SELECT
			balance_id,
			sponsor,
			asset_type,
			asset_code,
			asset_issuer,
			amount,
			COALESCE(jsonb_array_length(claimants), 0) as claimants_count,
			flags,
			last_modified_ledger
		FROM claimable_balances_current
		WHERE balance_id = $1
	`

	var b ClaimableBalanceCurrent
	var sponsor, assetType, assetCode, assetIssuer sql.NullString
	var amount int64

	err := h.db.QueryRowContext(ctx, query, balanceID).Scan(
		&b.BalanceID, &sponsor,
		&assetType, &assetCode, &assetIssuer,
		&amount, &b.ClaimantsCount, &b.Flags, &b.LastModifiedLedger,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if sponsor.Valid {
		b.Sponsor = &sponsor.String
	}
	b.Asset = buildAssetInfo(assetType.String, assetCode.String, assetIssuer.String)
	b.Amount = formatStroops(amount)

	return &b, nil
}

// Helper functions for Phase 6

// buildAssetInfo creates an AssetInfo from type, code, and issuer
func buildAssetInfo(assetType, assetCode, assetIssuer string) AssetInfo {
	if assetType == "native" || assetCode == "" {
		return AssetInfo{
			Type: "native",
			Code: "XLM",
		}
	}
	info := AssetInfo{
		Type: assetType,
		Code: assetCode,
	}
	if assetIssuer != "" {
		info.Issuer = &assetIssuer
	}
	return info
}

// formatStroops converts stroops (int64) to XLM string with 7 decimal places
func formatStroops(stroops int64) string {
	return fmt.Sprintf("%.7f", float64(stroops)/10000000.0)
}

// ============================================
// PHASE 7: EVENT TABLE METHODS
// ============================================

// GetTrades returns trades from the hot buffer with filters
func (h *SilverHotReader) GetTrades(ctx context.Context, filters TradeFilters) ([]SilverTrade, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	// Time range filter (default to last 24 hours)
	if filters.StartTime.IsZero() {
		filters.StartTime = time.Now().Add(-24 * time.Hour)
	}
	if filters.EndTime.IsZero() {
		filters.EndTime = time.Now()
	}
	conditions = append(conditions, fmt.Sprintf("trade_timestamp >= $%d AND trade_timestamp <= $%d", argNum, argNum+1))
	args = append(args, filters.StartTime, filters.EndTime)
	argNum += 2

	// Account filters
	if filters.AccountID != "" {
		conditions = append(conditions, fmt.Sprintf("(seller_account = $%d OR buyer_account = $%d)", argNum, argNum))
		args = append(args, filters.AccountID)
		argNum++
	}
	if filters.SellerAccount != "" {
		conditions = append(conditions, fmt.Sprintf("seller_account = $%d", argNum))
		args = append(args, filters.SellerAccount)
		argNum++
	}
	if filters.BuyerAccount != "" {
		conditions = append(conditions, fmt.Sprintf("buyer_account = $%d", argNum))
		args = append(args, filters.BuyerAccount)
		argNum++
	}

	// Asset pair filters
	if filters.SellingAssetCode != "" {
		if filters.SellingAssetCode == "XLM" {
			conditions = append(conditions, "(selling_asset_code IS NULL OR selling_asset_code = '')")
		} else {
			conditions = append(conditions, fmt.Sprintf("selling_asset_code = $%d", argNum))
			args = append(args, filters.SellingAssetCode)
			argNum++
			if filters.SellingAssetIssuer != "" {
				conditions = append(conditions, fmt.Sprintf("selling_asset_issuer = $%d", argNum))
				args = append(args, filters.SellingAssetIssuer)
				argNum++
			}
		}
	}
	if filters.BuyingAssetCode != "" {
		if filters.BuyingAssetCode == "XLM" {
			conditions = append(conditions, "(buying_asset_code IS NULL OR buying_asset_code = '')")
		} else {
			conditions = append(conditions, fmt.Sprintf("buying_asset_code = $%d", argNum))
			args = append(args, filters.BuyingAssetCode)
			argNum++
			if filters.BuyingAssetIssuer != "" {
				conditions = append(conditions, fmt.Sprintf("buying_asset_issuer = $%d", argNum))
				args = append(args, filters.BuyingAssetIssuer)
				argNum++
			}
		}
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf(`
			(ledger_sequence, transaction_hash, operation_index, trade_index) > ($%d, $%d, $%d, $%d)
		`, argNum, argNum+1, argNum+2, argNum+3))
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.TransactionHash,
			filters.Cursor.OperationIndex, filters.Cursor.TradeIndex)
		argNum += 4
	}

	whereClause := "WHERE " + strings.Join(conditions, " AND ")

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT ledger_sequence, transaction_hash, operation_index, trade_index,
			   COALESCE(trade_type, 'orderbook') as trade_type, trade_timestamp,
			   seller_account, selling_asset_code, selling_asset_issuer, selling_amount,
			   buyer_account, buying_asset_code, buying_asset_issuer, buying_amount,
			   price
		FROM trades
		%s
		ORDER BY ledger_sequence ASC, transaction_hash ASC, operation_index ASC, trade_index ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var trades []SilverTrade
	for rows.Next() {
		var t SilverTrade
		var sellingCode, sellingIssuer, buyingCode, buyingIssuer sql.NullString
		var sellingAmount, buyingAmount int64
		var priceDecimal float64

		err := rows.Scan(
			&t.LedgerSequence, &t.TransactionHash, &t.OperationIndex, &t.TradeIndex,
			&t.TradeType, &t.Timestamp,
			&t.Seller.AccountID, &sellingCode, &sellingIssuer, &sellingAmount,
			&t.Buyer.AccountID, &buyingCode, &buyingIssuer, &buyingAmount,
			&priceDecimal,
		)
		if err != nil {
			return nil, "", false, err
		}

		t.Selling.Asset = buildAssetInfo("", sellingCode.String, sellingIssuer.String)
		t.Selling.Amount = formatStroops(sellingAmount)
		t.Buying.Asset = buildAssetInfo("", buyingCode.String, buyingIssuer.String)
		t.Buying.Amount = formatStroops(buyingAmount)
		t.Price = fmt.Sprintf("%.7f", priceDecimal)

		trades = append(trades, t)
	}

	hasMore := len(trades) > limit
	if hasMore {
		trades = trades[:limit]
	}

	var nextCursor string
	if hasMore && len(trades) > 0 {
		last := trades[len(trades)-1]
		nextCursor = TradeCursor{
			LedgerSequence:  last.LedgerSequence,
			TransactionHash: last.TransactionHash,
			OperationIndex:  last.OperationIndex,
			TradeIndex:      last.TradeIndex,
		}.Encode()
	}

	return trades, nextCursor, hasMore, nil
}

// GetTradeStats returns aggregated trade statistics
func (h *SilverHotReader) GetTradeStats(ctx context.Context, groupBy string, startTime, endTime time.Time) ([]TradeStats, error) {
	var groupExpr, selectGroup string
	switch groupBy {
	case "asset_pair":
		groupExpr = "COALESCE(selling_asset_code, 'XLM') || '/' || COALESCE(buying_asset_code, 'XLM')"
		selectGroup = groupExpr + " as group_key"
	case "hour":
		groupExpr = "date_trunc('hour', trade_timestamp)"
		selectGroup = "to_char(" + groupExpr + ", 'YYYY-MM-DD HH24:00') as group_key"
	case "day":
		groupExpr = "date_trunc('day', trade_timestamp)"
		selectGroup = "to_char(" + groupExpr + ", 'YYYY-MM-DD') as group_key"
	default:
		groupExpr = "COALESCE(selling_asset_code, 'XLM') || '/' || COALESCE(buying_asset_code, 'XLM')"
		selectGroup = groupExpr + " as group_key"
	}

	query := fmt.Sprintf(`
		SELECT %s,
			   COUNT(*) as trade_count,
			   SUM(selling_amount) as volume_selling,
			   SUM(buying_amount) as volume_buying,
			   COUNT(DISTINCT seller_account) as unique_sellers,
			   COUNT(DISTINCT buyer_account) as unique_buyers,
			   AVG(price) as avg_price
		FROM trades
		WHERE trade_timestamp >= $1 AND trade_timestamp <= $2
		GROUP BY %s
		ORDER BY trade_count DESC
		LIMIT 100
	`, selectGroup, groupExpr)

	rows, err := h.db.QueryContext(ctx, query, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []TradeStats
	for rows.Next() {
		var s TradeStats
		var volSelling, volBuying int64
		var avgPrice sql.NullFloat64

		err := rows.Scan(&s.Group, &s.TradeCount, &volSelling, &volBuying,
			&s.UniqueSellers, &s.UniqueBuyers, &avgPrice)
		if err != nil {
			return nil, err
		}

		s.VolumeSelling = formatStroops(volSelling)
		s.VolumeBuying = formatStroops(volBuying)
		if avgPrice.Valid {
			avgStr := fmt.Sprintf("%.7f", avgPrice.Float64)
			s.AvgPrice = &avgStr
		}

		stats = append(stats, s)
	}

	return stats, nil
}

// GetEffectsByTransactionFast returns all effects for a transaction using a
// selective hot-path query: first resolve the transaction's ledger/op indexes
// from enriched_history_operations, then fetch matching effects by
// ledger_sequence + operation_index instead of scanning by transaction_hash
// across the large effects table.
func (h *SilverHotReader) GetEffectsByTransactionFast(ctx context.Context, txHash string) ([]SilverEffect, error) {
	rows, err := h.db.QueryContext(ctx, `
		SELECT DISTINCT ledger_sequence, operation_index
		FROM enriched_history_operations
		WHERE transaction_hash = $1
		ORDER BY ledger_sequence, operation_index
	`, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve tx operation indexes: %w", err)
	}
	defer rows.Close()

	var ledgerSequence int64
	var opIndexes []int
	for rows.Next() {
		var ledgerSeq int64
		var opIndex int
		if err := rows.Scan(&ledgerSeq, &opIndex); err != nil {
			return nil, fmt.Errorf("failed to scan tx operation indexes: %w", err)
		}
		if ledgerSequence == 0 {
			ledgerSequence = ledgerSeq
		}
		opIndexes = append(opIndexes, opIndex)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate tx operation indexes: %w", err)
	}
	if len(opIndexes) == 0 {
		return []SilverEffect{}, nil
	}

	query := `
		SELECT ledger_sequence, transaction_hash, operation_index, effect_index,
		       operation_id, effect_type, effect_type_string, account_id,
		       amount, asset_code, asset_issuer, asset_type,
		       details_json,
		       trustline_limit, authorize_flag, clawback_flag,
		       signer_account, signer_weight, offer_id, seller_account,
		       created_at
		FROM effects
		WHERE ledger_sequence = $1
		  AND operation_index = ANY($2)
		ORDER BY operation_index ASC, effect_index ASC
	`

	fxRows, err := h.db.QueryContext(ctx, query, ledgerSequence, pq.Array(opIndexes))
	if err != nil {
		return nil, fmt.Errorf("failed to query tx effects fast path: %w", err)
	}
	defer fxRows.Close()

	var effects []SilverEffect
	for fxRows.Next() {
		var e SilverEffect
		var operationID sql.NullInt64
		var accountID, amount, assetCode, assetIssuer, assetType sql.NullString
		var detailsJSON sql.NullString
		var trustlineLimit, signerAccount, sellerAccount sql.NullString
		var authorizeFlag, clawbackFlag sql.NullBool
		var signerWeight sql.NullInt32
		var offerID sql.NullInt64

		err := fxRows.Scan(
			&e.LedgerSequence, &e.TransactionHash, &e.OperationIndex, &e.EffectIndex,
			&operationID, &e.EffectType, &e.EffectTypeString, &accountID,
			&amount, &assetCode, &assetIssuer, &assetType,
			&detailsJSON,
			&trustlineLimit, &authorizeFlag, &clawbackFlag,
			&signerAccount, &signerWeight, &offerID, &sellerAccount,
			&e.Timestamp,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan tx effects fast path: %w", err)
		}
		if e.TransactionHash != txHash {
			continue
		}
		if operationID.Valid {
			e.OperationID = &operationID.Int64
		}
		if accountID.Valid {
			e.AccountID = &accountID.String
		}
		if amount.Valid {
			e.Amount = &amount.String
		}
		if assetCode.Valid || assetType.Valid {
			asset := buildAssetInfo(assetType.String, assetCode.String, assetIssuer.String)
			e.Asset = &asset
		}
		if detailsJSON.Valid {
			raw := json.RawMessage(detailsJSON.String)
			e.Details = &raw
		}
		if trustlineLimit.Valid {
			e.TrustlineLimit = &trustlineLimit.String
		}
		if authorizeFlag.Valid {
			e.AuthorizeFlag = &authorizeFlag.Bool
		}
		if clawbackFlag.Valid {
			e.ClawbackFlag = &clawbackFlag.Bool
		}
		if signerAccount.Valid {
			e.SignerAccount = &signerAccount.String
		}
		if signerWeight.Valid {
			v := int(signerWeight.Int32)
			e.SignerWeight = &v
		}
		if offerID.Valid {
			e.OfferID = &offerID.Int64
		}
		if sellerAccount.Valid {
			e.SellerAccount = &sellerAccount.String
		}
		effects = append(effects, e)
	}
	if err := fxRows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate tx effects fast path: %w", err)
	}
	return effects, nil
}

// GetEffects returns effects from the hot buffer with filters
func (h *SilverHotReader) GetEffects(ctx context.Context, filters EffectFilters) ([]SilverEffect, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	// Time range filter
	if !filters.StartTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("created_at >= $%d", argNum))
		args = append(args, filters.StartTime)
		argNum++
	}
	if !filters.EndTime.IsZero() {
		conditions = append(conditions, fmt.Sprintf("created_at <= $%d", argNum))
		args = append(args, filters.EndTime)
		argNum++
	}

	// Account filter
	if filters.AccountID != "" {
		conditions = append(conditions, fmt.Sprintf("account_id = $%d", argNum))
		args = append(args, filters.AccountID)
		argNum++
	}

	// Effect type filter (int or string)
	if filters.EffectType != "" {
		if typeInt, err := strconv.Atoi(filters.EffectType); err == nil {
			conditions = append(conditions, fmt.Sprintf("effect_type = $%d", argNum))
			args = append(args, typeInt)
		} else {
			conditions = append(conditions, fmt.Sprintf("effect_type_string = $%d", argNum))
			args = append(args, filters.EffectType)
		}
		argNum++
	}

	// Ledger filter
	if filters.LedgerSequence > 0 {
		conditions = append(conditions, fmt.Sprintf("ledger_sequence = $%d", argNum))
		args = append(args, filters.LedgerSequence)
		argNum++
	}

	// Transaction filter
	if filters.TransactionHash != "" {
		conditions = append(conditions, fmt.Sprintf("transaction_hash = $%d", argNum))
		args = append(args, filters.TransactionHash)
		argNum++
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf(`
			(ledger_sequence, transaction_hash, operation_index, effect_index) > ($%d, $%d, $%d, $%d)
		`, argNum, argNum+1, argNum+2, argNum+3))
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.TransactionHash,
			filters.Cursor.OperationIndex, filters.Cursor.EffectIndex)
		argNum += 4
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	orderDir := "DESC"
	if filters.Order == "asc" {
		orderDir = "ASC"
	}

	query := fmt.Sprintf(`
		SELECT ledger_sequence, transaction_hash, operation_index, effect_index,
			   effect_type, effect_type_string, account_id,
			   amount, asset_code, asset_issuer, asset_type,
			   trustline_limit, authorize_flag, clawback_flag,
			   signer_account, signer_weight, offer_id, seller_account,
			   created_at
		FROM effects
		%s
		ORDER BY ledger_sequence %s, transaction_hash %s, operation_index %s, effect_index %s
		LIMIT $%d
	`, whereClause, orderDir, orderDir, orderDir, orderDir, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var effects []SilverEffect
	for rows.Next() {
		var e SilverEffect
		var accountID, amount, assetCode, assetIssuer, assetType sql.NullString
		var trustlineLimit, signerAccount, sellerAccount sql.NullString
		var authorizeFlag, clawbackFlag sql.NullBool
		var signerWeight sql.NullInt32
		var offerID sql.NullInt64

		err := rows.Scan(
			&e.LedgerSequence, &e.TransactionHash, &e.OperationIndex, &e.EffectIndex,
			&e.EffectType, &e.EffectTypeString, &accountID,
			&amount, &assetCode, &assetIssuer, &assetType,
			&trustlineLimit, &authorizeFlag, &clawbackFlag,
			&signerAccount, &signerWeight, &offerID, &sellerAccount,
			&e.Timestamp,
		)
		if err != nil {
			return nil, "", false, err
		}

		if accountID.Valid {
			e.AccountID = &accountID.String
		}
		if amount.Valid {
			e.Amount = &amount.String
		}
		if assetCode.Valid || assetType.Valid {
			asset := buildAssetInfo(assetType.String, assetCode.String, assetIssuer.String)
			e.Asset = &asset
		}
		if trustlineLimit.Valid {
			e.TrustlineLimit = &trustlineLimit.String
		}
		if authorizeFlag.Valid {
			e.AuthorizeFlag = &authorizeFlag.Bool
		}
		if clawbackFlag.Valid {
			e.ClawbackFlag = &clawbackFlag.Bool
		}
		if signerAccount.Valid {
			e.SignerAccount = &signerAccount.String
		}
		if signerWeight.Valid {
			sw := int(signerWeight.Int32)
			e.SignerWeight = &sw
		}
		if offerID.Valid {
			e.OfferID = &offerID.Int64
		}
		if sellerAccount.Valid {
			e.SellerAccount = &sellerAccount.String
		}

		effects = append(effects, e)
	}

	hasMore := len(effects) > limit
	if hasMore {
		effects = effects[:limit]
	}

	var nextCursor string
	if hasMore && len(effects) > 0 {
		last := effects[len(effects)-1]
		nextCursor = EffectCursor{
			LedgerSequence:  last.LedgerSequence,
			TransactionHash: last.TransactionHash,
			OperationIndex:  last.OperationIndex,
			EffectIndex:     last.EffectIndex,
		}.Encode()
	}

	return effects, nextCursor, hasMore, nil
}

// GetEffectTypes returns counts of each effect type
func (h *SilverHotReader) GetEffectTypes(ctx context.Context) ([]EffectTypeCount, int64, error) {
	query := `
		SELECT effect_type, effect_type_string, COUNT(*) as count
		FROM effects
		GROUP BY effect_type, effect_type_string
		ORDER BY count DESC
	`

	rows, err := h.db.QueryContext(ctx, query)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var types []EffectTypeCount
	var total int64
	for rows.Next() {
		var t EffectTypeCount
		err := rows.Scan(&t.Type, &t.Name, &t.Count)
		if err != nil {
			return nil, 0, err
		}
		total += t.Count
		types = append(types, t)
	}

	return types, total, nil
}

// ============================================
// PHASE 8: SOROBAN TABLE METHODS
// ============================================

// GetContractCode returns contract code metadata by hash
func (h *SilverHotReader) GetContractCode(ctx context.Context, hash string) (*ContractCode, error) {
	query := `
		SELECT contract_code_hash, n_functions, n_instructions, n_data_segments,
			   n_data_segment_bytes, n_elem_segments, n_exports, n_globals,
			   n_imports, n_table_entries, n_types, last_modified_ledger, created_at
		FROM contract_code_current
		WHERE contract_code_hash = $1
	`

	var cc ContractCode
	var nFunctions, nInstructions, nDataSegments, nDataSegmentBytes sql.NullInt32
	var nElemSegments, nExports, nGlobals, nImports, nTableEntries, nTypes sql.NullInt32

	err := h.db.QueryRowContext(ctx, query, hash).Scan(
		&cc.Hash, &nFunctions, &nInstructions, &nDataSegments,
		&nDataSegmentBytes, &nElemSegments, &nExports, &nGlobals,
		&nImports, &nTableEntries, &nTypes, &cc.LastModifiedLedger, &cc.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	cc.Metrics = ContractCodeMetrics{
		NFunctions:        int(nFunctions.Int32),
		NInstructions:     int(nInstructions.Int32),
		NDataSegments:     int(nDataSegments.Int32),
		NDataSegmentBytes: int(nDataSegmentBytes.Int32),
		NElemSegments:     int(nElemSegments.Int32),
		NExports:          int(nExports.Int32),
		NGlobals:          int(nGlobals.Int32),
		NImports:          int(nImports.Int32),
		NTableEntries:     int(nTableEntries.Int32),
		NTypes:            int(nTypes.Int32),
	}

	return &cc, nil
}

// GetTTL returns TTL entry for a specific key
func (h *SilverHotReader) GetTTL(ctx context.Context, keyHash string) (*TTLEntry, error) {
	query := `
		SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
		FROM ttl_current
		WHERE key_hash = $1
	`

	var entry TTLEntry
	err := h.db.QueryRowContext(ctx, query, keyHash).Scan(
		&entry.KeyHash, &entry.LiveUntilLedger, &entry.Expired,
		&entry.LastModifiedLedger, &entry.ClosedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &entry, nil
}

// GetTTLExpiring returns TTL entries expiring within N ledgers
func (h *SilverHotReader) GetTTLExpiring(ctx context.Context, currentLedger int64, filters TTLFilters) ([]TTLEntry, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	// Calculate expiration threshold
	expirationThreshold := currentLedger + filters.WithinLedgers
	conditions = append(conditions, fmt.Sprintf("live_until_ledger_seq <= $%d", argNum))
	args = append(args, expirationThreshold)
	argNum++

	// Only non-expired entries
	conditions = append(conditions, "expired = false")

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(live_until_ledger_seq, key_hash) > ($%d, $%d)", argNum, argNum+1))
		args = append(args, filters.Cursor.LiveUntilLedger, filters.Cursor.KeyHash)
		argNum += 2
	}

	whereClause := strings.Join(conditions, " AND ")

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
		FROM ttl_current
		WHERE %s
		ORDER BY live_until_ledger_seq ASC, key_hash ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var entries []TTLEntry
	for rows.Next() {
		var e TTLEntry
		err := rows.Scan(&e.KeyHash, &e.LiveUntilLedger, &e.Expired,
			&e.LastModifiedLedger, &e.ClosedAt)
		if err != nil {
			return nil, "", false, err
		}
		e.LedgersRemaining = e.LiveUntilLedger - currentLedger
		entries = append(entries, e)
	}

	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	var nextCursor string
	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		nextCursor = TTLCursor{
			LiveUntilLedger: last.LiveUntilLedger,
			KeyHash:         last.KeyHash,
		}.Encode()
	}

	return entries, nextCursor, hasMore, nil
}

// GetTTLExpired returns expired TTL entries
func (h *SilverHotReader) GetTTLExpired(ctx context.Context, filters TTLFilters) ([]TTLEntry, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	conditions = append(conditions, "expired = true")

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(live_until_ledger_seq, key_hash) > ($%d, $%d)", argNum, argNum+1))
		args = append(args, filters.Cursor.LiveUntilLedger, filters.Cursor.KeyHash)
		argNum += 2
	}

	whereClause := strings.Join(conditions, " AND ")

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT key_hash, live_until_ledger_seq, expired, last_modified_ledger, closed_at
		FROM ttl_current
		WHERE %s
		ORDER BY live_until_ledger_seq DESC, key_hash ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var entries []TTLEntry
	for rows.Next() {
		var e TTLEntry
		err := rows.Scan(&e.KeyHash, &e.LiveUntilLedger, &e.Expired,
			&e.LastModifiedLedger, &e.ClosedAt)
		if err != nil {
			return nil, "", false, err
		}
		entries = append(entries, e)
	}

	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	var nextCursor string
	if hasMore && len(entries) > 0 {
		last := entries[len(entries)-1]
		nextCursor = TTLCursor{
			LiveUntilLedger: last.LiveUntilLedger,
			KeyHash:         last.KeyHash,
		}.Encode()
	}

	return entries, nextCursor, hasMore, nil
}

// GetEvictedKeys returns evicted storage keys, optionally filtered by contract
func (h *SilverHotReader) GetEvictedKeys(ctx context.Context, filters EvictionFilters) ([]EvictedKey, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argNum))
		args = append(args, filters.ContractID)
		argNum++
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(contract_id, key_hash, ledger_sequence) > ($%d, $%d, $%d)", argNum, argNum+1, argNum+2))
		args = append(args, filters.Cursor.ContractID, filters.Cursor.KeyHash, filters.Cursor.LedgerSequence)
		argNum += 3
	}

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT contract_id, key_hash, ledger_sequence, closed_at
		FROM evicted_keys
		WHERE %s
		ORDER BY closed_at DESC, contract_id ASC, key_hash ASC, ledger_sequence ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var keys []EvictedKey
	for rows.Next() {
		var k EvictedKey
		err := rows.Scan(&k.ContractID, &k.KeyHash, &k.LedgerSequence, &k.ClosedAt)
		if err != nil {
			return nil, "", false, err
		}
		keys = append(keys, k)
	}

	hasMore := len(keys) > limit
	if hasMore {
		keys = keys[:limit]
	}

	var nextCursor string
	if hasMore && len(keys) > 0 {
		last := keys[len(keys)-1]
		nextCursor = EvictionCursor{
			ContractID:     last.ContractID,
			KeyHash:        last.KeyHash,
			LedgerSequence: last.LedgerSequence,
		}.Encode()
	}

	return keys, nextCursor, hasMore, nil
}

// GetRestoredKeys returns restored storage keys, optionally filtered by contract
func (h *SilverHotReader) GetRestoredKeys(ctx context.Context, filters EvictionFilters) ([]RestoredKey, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argNum))
		args = append(args, filters.ContractID)
		argNum++
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(contract_id, key_hash, ledger_sequence) > ($%d, $%d, $%d)", argNum, argNum+1, argNum+2))
		args = append(args, filters.Cursor.ContractID, filters.Cursor.KeyHash, filters.Cursor.LedgerSequence)
		argNum += 3
	}

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT contract_id, key_hash, ledger_sequence, closed_at
		FROM restored_keys
		WHERE %s
		ORDER BY closed_at DESC, contract_id ASC, key_hash ASC, ledger_sequence ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var keys []RestoredKey
	for rows.Next() {
		var k RestoredKey
		err := rows.Scan(&k.ContractID, &k.KeyHash, &k.LedgerSequence, &k.ClosedAt)
		if err != nil {
			return nil, "", false, err
		}
		keys = append(keys, k)
	}

	hasMore := len(keys) > limit
	if hasMore {
		keys = keys[:limit]
	}

	var nextCursor string
	if hasMore && len(keys) > 0 {
		last := keys[len(keys)-1]
		nextCursor = EvictionCursor{
			ContractID:     last.ContractID,
			KeyHash:        last.KeyHash,
			LedgerSequence: last.LedgerSequence,
		}.Encode()
	}

	return keys, nextCursor, hasMore, nil
}

// GetSorobanConfig returns current Soroban network configuration
func (h *SilverHotReader) GetSorobanConfig(ctx context.Context) (*SorobanConfig, error) {
	query := `
		SELECT ledger_max_instructions, tx_max_instructions, fee_rate_per_instructions_increment,
			   tx_memory_limit, ledger_max_read_ledger_entries, ledger_max_read_bytes,
			   ledger_max_write_ledger_entries, ledger_max_write_bytes,
			   tx_max_read_ledger_entries, tx_max_read_bytes,
			   tx_max_write_ledger_entries, tx_max_write_bytes,
			   contract_max_size_bytes, last_modified_ledger, closed_at
		FROM config_settings_current
		WHERE config_setting_id = 1
	`

	var cfg SorobanConfig
	var ledgerMaxInstr, txMaxInstr, feeRate, txMemLimit sql.NullInt64
	var ledgerMaxReadEntries, ledgerMaxReadBytes, ledgerMaxWriteEntries, ledgerMaxWriteBytes sql.NullInt64
	var txMaxReadEntries, txMaxReadBytes, txMaxWriteEntries, txMaxWriteBytes sql.NullInt64
	var contractMaxSize sql.NullInt64

	err := h.db.QueryRowContext(ctx, query).Scan(
		&ledgerMaxInstr, &txMaxInstr, &feeRate, &txMemLimit,
		&ledgerMaxReadEntries, &ledgerMaxReadBytes, &ledgerMaxWriteEntries, &ledgerMaxWriteBytes,
		&txMaxReadEntries, &txMaxReadBytes, &txMaxWriteEntries, &txMaxWriteBytes,
		&contractMaxSize, &cfg.LastModifiedLedger, &cfg.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	cfg.Instructions = SorobanInstructionLimits{
		LedgerMax:           ledgerMaxInstr.Int64,
		TxMax:               txMaxInstr.Int64,
		FeeRatePerIncrement: feeRate.Int64,
	}
	cfg.Memory = SorobanMemoryLimits{
		TxLimitBytes: txMemLimit.Int64,
	}
	cfg.LedgerLimits = SorobanIOLimits{
		MaxReadEntries:  ledgerMaxReadEntries.Int64,
		MaxReadBytes:    ledgerMaxReadBytes.Int64,
		MaxWriteEntries: ledgerMaxWriteEntries.Int64,
		MaxWriteBytes:   ledgerMaxWriteBytes.Int64,
	}
	cfg.TxLimits = SorobanIOLimits{
		MaxReadEntries:  txMaxReadEntries.Int64,
		MaxReadBytes:    txMaxReadBytes.Int64,
		MaxWriteEntries: txMaxWriteEntries.Int64,
		MaxWriteBytes:   txMaxWriteBytes.Int64,
	}
	cfg.Contract = SorobanContractLimits{
		MaxSizeBytes: contractMaxSize.Int64,
	}

	return &cfg, nil
}

// GetContractData returns contract storage entries
func (h *SilverHotReader) GetContractData(ctx context.Context, filters ContractDataFilters) ([]ContractData, string, bool, error) {
	var conditions []string
	var args []interface{}
	argNum := 1

	// Contract ID is required
	if filters.ContractID != "" {
		conditions = append(conditions, fmt.Sprintf("contract_id = $%d", argNum))
		args = append(args, filters.ContractID)
		argNum++
	}

	// Durability filter
	if filters.Durability != "" {
		conditions = append(conditions, fmt.Sprintf("durability = $%d", argNum))
		args = append(args, filters.Durability)
		argNum++
	}

	// Single key lookup
	if filters.KeyHash != "" {
		conditions = append(conditions, fmt.Sprintf("key_hash = $%d", argNum))
		args = append(args, filters.KeyHash)
		argNum++
	}

	// Cursor pagination
	if filters.Cursor != nil {
		conditions = append(conditions, fmt.Sprintf("(contract_id, key_hash) > ($%d, $%d)", argNum, argNum+1))
		args = append(args, filters.Cursor.ContractID, filters.Cursor.KeyHash)
		argNum += 2
	}

	whereClause := "1=1"
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(`
		SELECT contract_id, key_hash, durability, data_value,
			   asset_type, asset_code, asset_issuer, last_modified_ledger
		FROM contract_data_current
		WHERE %s
		ORDER BY contract_id ASC, key_hash ASC
		LIMIT $%d
	`, whereClause, argNum)
	args = append(args, limit+1)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, err
	}
	defer rows.Close()

	var data []ContractData
	for rows.Next() {
		var d ContractData
		var dataValue, assetType, assetCode, assetIssuer sql.NullString

		err := rows.Scan(&d.ContractID, &d.KeyHash, &d.Durability, &dataValue,
			&assetType, &assetCode, &assetIssuer, &d.LastModifiedLedger)
		if err != nil {
			return nil, "", false, err
		}

		if dataValue.Valid && dataValue.String != "" {
			d.DataValueXDR = &dataValue.String
		}
		if assetCode.Valid && assetCode.String != "" {
			issuer := assetIssuer.String
			d.Asset = &AssetInfo{
				Type:   assetType.String,
				Code:   assetCode.String,
				Issuer: &issuer,
			}
		}

		data = append(data, d)
	}

	hasMore := len(data) > limit
	if hasMore {
		data = data[:limit]
	}

	var nextCursor string
	if hasMore && len(data) > 0 {
		last := data[len(data)-1]
		nextCursor = ContractDataCursor{
			ContractID: last.ContractID,
			KeyHash:    last.KeyHash,
		}.Encode()
	}

	return data, nextCursor, hasMore, nil
}

// GetCurrentLedger returns the current ledger sequence from hot storage
func (h *SilverHotReader) GetCurrentLedger(ctx context.Context) (int64, error) {
	query := `SELECT MAX(ledger_sequence) FROM ttl_current`
	var ledger sql.NullInt64
	err := h.db.QueryRowContext(ctx, query).Scan(&ledger)
	if err != nil {
		return 0, err
	}
	return ledger.Int64, nil
}

func (h *SilverHotReader) GetContractFunctions(ctx context.Context, contractID string) ([]string, error) {
	rows, err := h.db.QueryContext(ctx, `
		SELECT DISTINCT function_name
		FROM enriched_history_operations
		WHERE contract_id = $1 AND function_name IS NOT NULL
		ORDER BY function_name
	`, contractID)
	if err != nil {
		return nil, fmt.Errorf("GetContractFunctions: %w", err)
	}
	defer rows.Close()
	var functions []string
	for rows.Next() {
		var fn string
		if err := rows.Scan(&fn); err != nil {
			return nil, err
		}
		functions = append(functions, fn)
	}
	return functions, nil
}

// enrichContractMetadata adds creator info from contract_metadata table
func (h *SilverHotReader) enrichContractMetadata(ctx context.Context, resp *ContractMetadataResponse) {
	query := `
		SELECT creator_address, wasm_hash, created_ledger, created_at::text
		FROM contract_metadata
		WHERE contract_id = $1
	`
	var creator, wasmHash, createdAt sql.NullString
	var createdLedger sql.NullInt64
	err := h.db.QueryRowContext(ctx, query, resp.ContractID).Scan(
		&creator, &wasmHash, &createdLedger, &createdAt,
	)
	if err != nil {
		return
	}
	if creator.Valid {
		resp.CreatorAddress = &creator.String
	}
	if wasmHash.Valid {
		resp.WasmHash = &wasmHash.String
	}
	if createdLedger.Valid {
		resp.CreatedLedger = &createdLedger.Int64
	}
	if createdAt.Valid {
		resp.CreatedAt = &createdAt.String
	}
}

// enrichContractFunctions adds observed function names from contract_invocations_raw
func (h *SilverHotReader) enrichContractFunctions(ctx context.Context, resp *ContractMetadataResponse) {
	query := `
		SELECT function_name, COUNT(*) as call_count
		FROM contract_invocations_raw
		WHERE contract_id = $1 AND function_name != ''
		GROUP BY function_name
		ORDER BY call_count DESC
	`
	rows, err := h.db.QueryContext(ctx, query, resp.ContractID)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var fc FunctionCallCount
		if err := rows.Scan(&fc.Name, &fc.CallCount); err != nil {
			break
		}
		resp.ExportedFunctions = append(resp.ExportedFunctions, fc)
	}
}

// enrichContractStorage adds storage summary from contract_data_current
func (h *SilverHotReader) enrichContractStorage(ctx context.Context, resp *ContractMetadataResponse) {
	query := `
		SELECT
			COUNT(*) as total_entries,
			COUNT(*) FILTER (WHERE lower(durability) LIKE '%persistent') as persistent_entries,
			COUNT(*) FILTER (WHERE lower(durability) LIKE '%temporary') as temporary_entries,
			COUNT(*) FILTER (WHERE lower(durability) LIKE '%instance') as instance_entries,
			COALESCE(SUM(LENGTH(data_value)), 0) as total_state_size_bytes
		FROM contract_data_current
		WHERE contract_id = $1
	`
	var totalStateSizeBytes sql.NullInt64
	_ = h.db.QueryRowContext(ctx, query, resp.ContractID).Scan(
		&resp.TotalEntries, &resp.PersistentEntries, &resp.TemporaryEntries, &resp.InstanceEntries, &totalStateSizeBytes,
	)
	if totalStateSizeBytes.Valid {
		resp.TotalStateSizeBytes = &totalStateSizeBytes.Int64
	}
	if resp.EstimatedMonthlyRentStroops == nil {
		h.enrichContractRentEstimate(ctx, resp)
	}
}

// enrichContractWasm adds WASM metrics from contract_code_current
func (h *SilverHotReader) enrichContractWasm(ctx context.Context, resp *ContractMetadataResponse) {
	if resp.WasmHash == nil {
		return
	}
	query := `
		SELECT n_data_segment_bytes, n_instructions, n_functions, n_exports
		FROM contract_code_current
		WHERE contract_code_hash = $1
	`
	var wasmSizeBytes sql.NullInt64
	var nInstr, nFunc, nExport sql.NullInt32
	err := h.db.QueryRowContext(ctx, query, *resp.WasmHash).Scan(&wasmSizeBytes, &nInstr, &nFunc, &nExport)
	if err != nil {
		return
	}
	if wasmSizeBytes.Valid {
		resp.WasmSizeBytes = &wasmSizeBytes.Int64
	}
	if nInstr.Valid {
		v := int(nInstr.Int32)
		resp.NInstructions = &v
	}
	if nFunc.Valid {
		v := int(nFunc.Int32)
		resp.NFunctions = &v
	}
	if nExport.Valid {
		v := int(nExport.Int32)
		resp.NExports = &v
	}
}

func (h *SilverHotReader) enrichContractRegistry(ctx context.Context, resp *ContractMetadataResponse) {
	var displayName, registryCategory sql.NullString
	var verified sql.NullBool
	err := h.db.QueryRowContext(ctx, `
		SELECT display_name, category, verified
		FROM contract_registry
		WHERE contract_id = $1
	`, resp.ContractID).Scan(&displayName, &registryCategory, &verified)
	if err != nil {
		return
	}
	if displayName.Valid {
		resp.DisplayName = &displayName.String
	}
	if registryCategory.Valid && resp.ContractType == nil {
		resp.ContractType = &registryCategory.String
	}
	if verified.Valid {
		v := verified.Bool
		resp.Verified = &v
	}
}

type contractRentConfig struct {
	NetworkStateBytes             int64
	FeePerRent1KB                 int64
	StateTargetSizeBytes          int64
	RentFee1KBStateSizeLow        int64
	RentFee1KBStateSizeHigh       int64
	StateSizeRentFeeGrowthFactor  uint32
	PersistentRentRateDenominator int64
	TemporaryRentRateDenominator  int64
}

func (h *SilverHotReader) enrichContractRentEstimate(ctx context.Context, resp *ContractMetadataResponse) {
	var persistentBytes, temporaryBytes, instanceBytes sql.NullInt64
	err := h.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(LENGTH(data_value)) FILTER (WHERE lower(durability) LIKE '%persistent'), 0),
			COALESCE(SUM(LENGTH(data_value)) FILTER (WHERE lower(durability) LIKE '%temporary'), 0),
			COALESCE(SUM(LENGTH(data_value)) FILTER (WHERE lower(durability) LIKE '%instance'), 0)
		FROM contract_data_current
		WHERE contract_id = $1
	`, resp.ContractID).Scan(&persistentBytes, &temporaryBytes, &instanceBytes)
	if err != nil {
		return
	}

	cfg, err := h.loadContractRentConfig(ctx)
	if err != nil || cfg == nil {
		return
	}

	feePerRent1KB := approximateRentFeePer1KB(cfg.NetworkStateBytes, cfg)
	if feePerRent1KB <= 0 {
		return
	}

	const ledgersPerMonth = int64(518400) // ~30 days at 5s/ledger
	persistentTotalBytes := persistentBytes.Int64 + instanceBytes.Int64
	temporaryTotalBytes := temporaryBytes.Int64

	monthly := int64(0)
	if persistentTotalBytes > 0 && cfg.PersistentRentRateDenominator > 0 {
		monthly += estimateMonthlyRentForBytes(persistentTotalBytes, feePerRent1KB, cfg.PersistentRentRateDenominator, ledgersPerMonth)
	}
	if temporaryTotalBytes > 0 && cfg.TemporaryRentRateDenominator > 0 {
		monthly += estimateMonthlyRentForBytes(temporaryTotalBytes, feePerRent1KB, cfg.TemporaryRentRateDenominator, ledgersPerMonth)
	}
	if monthly > 0 {
		resp.EstimatedMonthlyRentStroops = &monthly
	}
}

func estimateMonthlyRentForBytes(totalBytes, feePerRent1KB, denominator, ledgersPerMonth int64) int64 {
	if totalBytes <= 0 || feePerRent1KB <= 0 || denominator <= 0 || ledgersPerMonth <= 0 {
		return 0
	}
	kbRoundedUp := (totalBytes + 1023) / 1024
	return (kbRoundedUp * feePerRent1KB * ledgersPerMonth) / denominator
}

func approximateRentFeePer1KB(networkStateBytes int64, cfg *contractRentConfig) int64 {
	if cfg == nil {
		return 0
	}
	if cfg.FeePerRent1KB > 0 {
		return cfg.FeePerRent1KB
	}
	if cfg.StateTargetSizeBytes <= 0 {
		return 0
	}
	if networkStateBytes <= 0 {
		return cfg.RentFee1KBStateSizeLow
	}
	if networkStateBytes <= cfg.StateTargetSizeBytes {
		delta := cfg.RentFee1KBStateSizeHigh - cfg.RentFee1KBStateSizeLow
		return cfg.RentFee1KBStateSizeLow + (delta*networkStateBytes)/cfg.StateTargetSizeBytes
	}
	// Approximation beyond target size: use the high watermark scaled by the
	// configured growth factor for state beyond the first target window.
	return cfg.RentFee1KBStateSizeHigh * int64(maxUint32(cfg.StateSizeRentFeeGrowthFactor, 1))
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func (h *SilverHotReader) loadContractRentConfig(ctx context.Context) (*contractRentConfig, error) {
	var networkStateBytes sql.NullInt64
	if err := h.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(LENGTH(data_value)), 0) FROM contract_data_current`).Scan(&networkStateBytes); err != nil {
		return nil, err
	}

	rows, err := h.db.QueryContext(ctx, `
		SELECT config_setting_id, config_setting_xdr
		FROM config_settings_current
		WHERE config_setting_id IN (2, 10)
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cfg := &contractRentConfig{NetworkStateBytes: networkStateBytes.Int64}
	for rows.Next() {
		var id int
		var encoded string
		if err := rows.Scan(&id, &encoded); err != nil {
			return nil, err
		}
		blob, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return nil, err
		}
		var entry xdr.ConfigSettingEntry
		if err := entry.UnmarshalBinary(blob); err != nil {
			return nil, err
		}
		switch xdr.ConfigSettingId(id) {
		case xdr.ConfigSettingIdConfigSettingContractLedgerCostV0:
			if entry.ContractLedgerCost != nil {
				cfg.StateTargetSizeBytes = int64(entry.ContractLedgerCost.SorobanStateTargetSizeBytes)
				cfg.RentFee1KBStateSizeLow = int64(entry.ContractLedgerCost.RentFee1KbSorobanStateSizeLow)
				cfg.RentFee1KBStateSizeHigh = int64(entry.ContractLedgerCost.RentFee1KbSorobanStateSizeHigh)
				cfg.StateSizeRentFeeGrowthFactor = uint32(entry.ContractLedgerCost.SorobanStateRentFeeGrowthFactor)
			}
		case xdr.ConfigSettingIdConfigSettingStateArchival:
			if entry.StateArchivalSettings != nil {
				cfg.PersistentRentRateDenominator = int64(entry.StateArchivalSettings.PersistentRentRateDenominator)
				cfg.TemporaryRentRateDenominator = int64(entry.StateArchivalSettings.TempRentRateDenominator)
			}
		}
	}
	if cfg.PersistentRentRateDenominator == 0 {
		cfg.PersistentRentRateDenominator = 1215 // Stellar testnet fallback
	}
	if cfg.TemporaryRentRateDenominator == 0 {
		cfg.TemporaryRentRateDenominator = 2430 // Stellar testnet fallback
	}
	if h.bronzeHotPG != nil {
		var feePerRent1KB, liveStateBytes sql.NullInt64
		if err := h.bronzeHotPG.QueryRowContext(ctx, `
			SELECT soroban_fee_write1kb, live_soroban_state_size
			FROM ledgers_row_v2
			WHERE soroban_fee_write1kb IS NOT NULL
			ORDER BY sequence DESC
			LIMIT 1
		`).Scan(&feePerRent1KB, &liveStateBytes); err == nil {
			if feePerRent1KB.Valid {
				cfg.FeePerRent1KB = feePerRent1KB.Int64
			}
			if liveStateBytes.Valid && liveStateBytes.Int64 > 0 {
				cfg.NetworkStateBytes = liveStateBytes.Int64
			}
		}
	}
	if cfg.FeePerRent1KB == 0 && cfg.StateTargetSizeBytes == 0 {
		return nil, nil
	}
	return cfg, nil
}

// ============================================
// SEMANTIC LAYER QUERIES
// ============================================

// GetSemanticActivities queries semantic_activities from hot storage
func (h *SilverHotReader) GetSemanticActivities(ctx context.Context, filters SemanticActivityFilters) ([]SemanticActivity, error) {
	query := `SELECT id, ledger_sequence, timestamp, activity_type, description,
		source_account, destination_account, contract_id,
		asset_code, asset_issuer, amount,
		is_soroban, soroban_function_name,
		transaction_hash, operation_index, successful, fee_charged
		FROM semantic_activities WHERE 1=1`

	args := []any{}
	argIdx := 1

	if filters.Account != "" {
		query += fmt.Sprintf(" AND (source_account = $%d OR destination_account = $%d)", argIdx, argIdx)
		args = append(args, filters.Account)
		argIdx++
	}
	if filters.ContractID != "" {
		query += fmt.Sprintf(" AND contract_id = $%d", argIdx)
		args = append(args, filters.ContractID)
		argIdx++
	}
	if filters.ActivityType != "" {
		query += fmt.Sprintf(" AND activity_type = $%d", argIdx)
		args = append(args, filters.ActivityType)
		argIdx++
	}
	if filters.Before != nil {
		query += fmt.Sprintf(" AND timestamp < $%d", argIdx)
		args = append(args, *filters.Before)
		argIdx++
	}
	if filters.After != nil {
		query += fmt.Sprintf(" AND timestamp > $%d", argIdx)
		args = append(args, *filters.After)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY timestamp DESC LIMIT $%d", argIdx)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanActivities(rows)
}

// GetSemanticFlows queries semantic_flows_value from hot storage
func (h *SilverHotReader) GetSemanticFlows(ctx context.Context, filters SemanticFlowFilters) ([]SemanticFlow, error) {
	query := `SELECT id, ledger_sequence, timestamp, flow_type,
		from_account, to_account, contract_id,
		asset_code, asset_issuer, asset_type,
		amount, transaction_hash, operation_type, successful
		FROM semantic_flows_value WHERE 1=1`

	args := []any{}
	argIdx := 1

	if filters.Account != "" {
		query += fmt.Sprintf(" AND (from_account = $%d OR to_account = $%d)", argIdx, argIdx)
		args = append(args, filters.Account)
		argIdx++
	}
	if filters.AssetCode != "" {
		query += fmt.Sprintf(" AND asset_code = $%d", argIdx)
		args = append(args, filters.AssetCode)
		argIdx++
	}
	if filters.FlowType != "" {
		query += fmt.Sprintf(" AND flow_type = $%d", argIdx)
		args = append(args, filters.FlowType)
		argIdx++
	}
	if filters.Before != nil {
		query += fmt.Sprintf(" AND timestamp < $%d", argIdx)
		args = append(args, *filters.Before)
		argIdx++
	}
	if filters.After != nil {
		query += fmt.Sprintf(" AND timestamp > $%d", argIdx)
		args = append(args, *filters.After)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY timestamp DESC LIMIT $%d", argIdx)
	args = append(args, filters.Limit)

	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanFlows(rows)
}

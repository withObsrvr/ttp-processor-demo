package main

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/http"
	"time"
)

// ============================================
// NETWORK STATS TYPES
// ============================================

// NetworkStats represents headline network statistics
type NetworkStats struct {
	GeneratedAt   string `json:"generated_at"`
	DataFreshness string `json:"data_freshness"`

	Accounts      AccountStats       `json:"accounts"`
	Ledger        LedgerStats        `json:"ledger"`
	Operations24h OperationStats     `json:"operations_24h"`
	Transactions24h *TransactionStats24h `json:"transactions_24h,omitempty"`
	Fees24h       *FeeStats24h       `json:"fees_24h,omitempty"`
	Soroban       *SorobanNetStats   `json:"soroban,omitempty"`
}

// AccountStats contains account-related statistics
type AccountStats struct {
	Total       int64 `json:"total"`
	Active24h   int64 `json:"active_24h,omitempty"`
	Created24h  int64 `json:"created_24h,omitempty"`
}

// LedgerStats contains ledger-related statistics
type LedgerStats struct {
	CurrentSequence     int64   `json:"current_sequence"`
	AvgCloseTimeSeconds float64 `json:"avg_close_time_seconds,omitempty"`
	ProtocolVersion     int     `json:"protocol_version,omitempty"`
}

// OperationStats contains 24-hour operation statistics
type OperationStats struct {
	Total            int64 `json:"total"`
	Payments         int64 `json:"payments,omitempty"`
	PathPayments     int64 `json:"path_payments,omitempty"`
	CreateAccount    int64 `json:"create_account,omitempty"`
	AccountMerge     int64 `json:"account_merge,omitempty"`
	ChangeTrust      int64 `json:"change_trust,omitempty"`
	ManageOffer      int64 `json:"manage_offer,omitempty"`
	ContractInvoke   int64 `json:"contract_invoke,omitempty"`
	Other            int64 `json:"other,omitempty"`
	PreviousTotal          int64 `json:"previous_total,omitempty"`
	PreviousContractInvoke int64 `json:"previous_contract_invoke,omitempty"`
}

// TransactionStats24h contains 24-hour transaction-level statistics
type TransactionStats24h struct {
	Total       int64   `json:"total"`
	Failed      int64   `json:"failed"`
	FailureRate float64 `json:"failure_rate"`
}

// FeeStats24h contains 24-hour fee statistics
type FeeStats24h struct {
	MedianStroops     int64 `json:"median_stroops"`
	P99Stroops        int64 `json:"p99_stroops"`
	DailyTotalStroops int64 `json:"daily_total_stroops"`
	SurgeActive       bool  `json:"surge_active"`
}

// SorobanNetStats contains Soroban network statistics
type SorobanNetStats struct {
	ActiveContracts24h   int64 `json:"active_contracts_24h"`
	AvgCpuInsns          int64 `json:"avg_cpu_insns,omitempty"`
	RentBurned24hStroops int64 `json:"rent_burned_24h_stroops,omitempty"`
}

// ============================================
// BRONZE NETWORK STATS TYPES
// ============================================

// BronzeNetworkStats represents headline statistics from the bronze layer
type BronzeNetworkStats struct {
	GeneratedAt     string            `json:"generated_at"`
	DataFreshness   string            `json:"data_freshness"`
	Ledger          BronzeLedgerStats `json:"ledger"`
	Transactions24h *BronzeTxStats24h `json:"transactions_24h,omitempty"`
	Operations24h   *BronzeOpStats24h `json:"operations_24h,omitempty"`
}

// BronzeLedgerStats contains latest ledger info from bronze
type BronzeLedgerStats struct {
	LatestSequence      int64   `json:"latest_sequence"`
	LatestHash          string  `json:"latest_hash,omitempty"`
	ClosedAt            string  `json:"closed_at,omitempty"`
	ProtocolVersion     int     `json:"protocol_version,omitempty"`
	AvgCloseTimeSeconds float64 `json:"avg_close_time_seconds,omitempty"`
}

// BronzeTxStats24h contains 24-hour transaction stats from bronze
type BronzeTxStats24h struct {
	Total            int64 `json:"total"`
	Successful       int64 `json:"successful"`
	Failed           int64 `json:"failed"`
	SorobanCount     int64 `json:"soroban_count"`
	TotalFeesCharged int64 `json:"total_fees_charged"`
}

// BronzeOpStats24h contains 24-hour operation stats from bronze
type BronzeOpStats24h struct {
	Total          int64 `json:"total"`
	SorobanOpCount int64 `json:"soroban_op_count"`
}

// ============================================
// HOT READER METHODS
// ============================================

// GetNetworkStats returns aggregated network statistics from hot buffer
func (h *SilverHotReader) GetNetworkStats(ctx context.Context) (*NetworkStats, error) {
	stats := &NetworkStats{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		DataFreshness: "real-time",
	}

	// Query 1: Total accounts
	accountQuery := `SELECT COUNT(*) FROM accounts_current`
	err := h.db.QueryRowContext(ctx, accountQuery).Scan(&stats.Accounts.Total)
	if err != nil {
		return nil, fmt.Errorf("failed to get account count: %w", err)
	}

	// Query 2: Current ledger (from accounts_current)
	ledgerQuery := `SELECT COALESCE(MAX(last_modified_ledger), 0) FROM accounts_current`
	err = h.db.QueryRowContext(ctx, ledgerQuery).Scan(&stats.Ledger.CurrentSequence)
	if err != nil {
		return nil, fmt.Errorf("failed to get current ledger: %w", err)
	}

	// Query 3: Active accounts in 24h (accounts that had operations)
	activeQuery := `
		SELECT COUNT(DISTINCT source_account)
		FROM enriched_history_operations
		WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
	`
	err = h.db.QueryRowContext(ctx, activeQuery).Scan(&stats.Accounts.Active24h)
	if err != nil && err != sql.ErrNoRows {
		// Non-fatal - some deployments may not have this data
		stats.Accounts.Active24h = 0
	}

	// Query 4: 24h operations breakdown by type (using integer type column)
	opsQuery := `
		SELECT
			type,
			COUNT(*) as count
		FROM enriched_history_operations
		WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
		GROUP BY type
	`
	rows, err := h.db.QueryContext(ctx, opsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to get operations breakdown: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var opType int32
		var count int64
		if err := rows.Scan(&opType, &count); err != nil {
			continue
		}

		stats.Operations24h.Total += count

		// Map operation types to categories (see operationTypeName in silver_reader.go)
		switch opType {
		case 1: // PAYMENT
			stats.Operations24h.Payments += count
		case 2, 13: // PATH_PAYMENT_STRICT_RECEIVE, PATH_PAYMENT_STRICT_SEND
			stats.Operations24h.PathPayments += count
		case 0: // CREATE_ACCOUNT
			stats.Operations24h.CreateAccount += count
			stats.Accounts.Created24h += count
		case 8: // ACCOUNT_MERGE
			stats.Operations24h.AccountMerge += count
		case 6, 7, 21: // CHANGE_TRUST, ALLOW_TRUST, SET_TRUST_LINE_FLAGS
			stats.Operations24h.ChangeTrust += count
		case 3, 4, 12: // MANAGE_SELL_OFFER, CREATE_PASSIVE_SELL_OFFER, MANAGE_BUY_OFFER
			stats.Operations24h.ManageOffer += count
		case 24, 25, 26: // INVOKE_HOST_FUNCTION, EXTEND_FOOTPRINT_TTL, RESTORE_FOOTPRINT
			stats.Operations24h.ContractInvoke += count
		default:
			stats.Operations24h.Other += count
		}
	}

	// Query 5: Average ledger close time (last 100 ledgers approximation)
	// This requires a ledgers table which may not exist in silver_hot
	// We'll skip this for now and document it as an enhancement
	stats.Ledger.AvgCloseTimeSeconds = 5.0 // Default Stellar target

	// Query 6: Previous 24h operations (48h-24h window) for trend comparison
	prevOpsQuery := `
		SELECT
			type,
			COUNT(*) as count
		FROM enriched_history_operations
		WHERE ledger_closed_at BETWEEN NOW() - INTERVAL '48 hours' AND NOW() - INTERVAL '24 hours'
		GROUP BY type
	`
	prevRows, err := h.db.QueryContext(ctx, prevOpsQuery)
	if err == nil {
		defer prevRows.Close()
		for prevRows.Next() {
			var opType int32
			var count int64
			if err := prevRows.Scan(&opType, &count); err != nil {
				continue
			}
			stats.Operations24h.PreviousTotal += count
			if opType == 24 || opType == 25 || opType == 26 {
				stats.Operations24h.PreviousContractInvoke += count
			}
		}
	}

	// Query 7: Transaction stats (total, failed)
	txStats := &TransactionStats24h{}
	txStatsQuery := `
		SELECT
			COUNT(DISTINCT transaction_hash) as total,
			COUNT(DISTINCT transaction_hash) FILTER (WHERE NOT tx_successful) as failed
		FROM enriched_history_operations
		WHERE ledger_closed_at > NOW() - INTERVAL '24 hours'
	`
	err = h.db.QueryRowContext(ctx, txStatsQuery).Scan(&txStats.Total, &txStats.Failed)
	if err == nil && txStats.Total > 0 {
		txStats.FailureRate = float64(txStats.Failed) / float64(txStats.Total)
		stats.Transactions24h = txStats
	}

	// Query 8: Active contracts in 24h
	sorobanStats := &SorobanNetStats{}
	activeContractsQuery := `
		SELECT COUNT(DISTINCT contract_id)
		FROM contract_invocations_raw
		WHERE closed_at > NOW() - INTERVAL '24 hours'
	`
	err = h.db.QueryRowContext(ctx, activeContractsQuery).Scan(&sorobanStats.ActiveContracts24h)
	if err == nil && sorobanStats.ActiveContracts24h > 0 {
		stats.Soroban = sorobanStats
	}

	return stats, nil
}

// ============================================
// UNIFIED READER METHODS
// ============================================

// GetNetworkStats returns aggregated network statistics from both hot and cold storage
func (u *UnifiedSilverReader) GetNetworkStats(ctx context.Context) (*NetworkStats, error) {
	stats := &NetworkStats{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		DataFreshness: "real-time",
	}

	// Get total accounts from COLD storage (full history)
	totalAccounts, err := u.cold.GetTotalAccountCount(ctx)
	if err != nil {
		// Fall back to hot if cold fails
		hotStats, hotErr := u.hot.GetNetworkStats(ctx)
		if hotErr != nil {
			return nil, fmt.Errorf("failed to get stats from both hot and cold: cold=%v, hot=%v", err, hotErr)
		}
		return hotStats, nil
	}
	stats.Accounts.Total = totalAccounts

	// Get 24h stats from HOT storage (more real-time)
	hotStats, err := u.hot.GetNetworkStats(ctx)
	if err != nil {
		// Return what we have from cold
		return stats, nil
	}

	// Use hot storage for 24h metrics (more current)
	stats.Accounts.Active24h = hotStats.Accounts.Active24h
	stats.Accounts.Created24h = hotStats.Accounts.Created24h
	stats.Ledger = hotStats.Ledger
	stats.Operations24h = hotStats.Operations24h

	return stats, nil
}

// ============================================
// HTTP HANDLERS
// ============================================

// NetworkStatsHandler handles network statistics requests
type NetworkStatsHandler struct {
	silverReader  *UnifiedSilverReader
	bronzeReader  *ColdReader // Bronze layer for accurate total account count
	unifiedReader *UnifiedDuckDBReader // For fee stats from bronze
}

// NewNetworkStatsHandler creates a new network stats handler
func NewNetworkStatsHandler(silverReader *UnifiedSilverReader, bronzeReader *ColdReader) *NetworkStatsHandler {
	return &NetworkStatsHandler{
		silverReader: silverReader,
		bronzeReader: bronzeReader,
	}
}

// SetUnifiedReader sets the unified reader for fee/resource queries against bronze
func (h *NetworkStatsHandler) SetUnifiedReader(reader *UnifiedDuckDBReader) {
	h.unifiedReader = reader
}

// HandleNetworkStats returns headline network statistics
// GET /api/v1/silver/stats/network
func (h *NetworkStatsHandler) HandleNetworkStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	stats := &NetworkStats{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		DataFreshness: "real-time",
	}

	// Get total accounts from BRONZE storage (full network history)
	if h.bronzeReader != nil {
		totalAccounts, err := h.bronzeReader.GetDistinctAccountCount(ctx)
		if err == nil {
			stats.Accounts.Total = totalAccounts
		}
	}

	// Get 24h stats from Silver hot storage (more current)
	hotStats, err := h.silverReader.hot.GetNetworkStats(ctx)
	if err != nil {
		// If we have a total from Bronze, return partial stats
		if stats.Accounts.Total > 0 {
			respondJSON(w, stats)
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Use hot storage for 24h metrics
	stats.Accounts.Active24h = hotStats.Accounts.Active24h
	stats.Accounts.Created24h = hotStats.Accounts.Created24h
	stats.Ledger = hotStats.Ledger
	stats.Operations24h = hotStats.Operations24h
	stats.Transactions24h = hotStats.Transactions24h
	stats.Soroban = hotStats.Soroban

	// If Bronze failed, fall back to hot/silver total
	if stats.Accounts.Total == 0 {
		stats.Accounts.Total = hotStats.Accounts.Total
	}

	// Fetch protocol_version and avg close time from bronze ledgers
	if h.unifiedReader != nil {
		schemas := []string{}
		if h.unifiedReader.bronzeHotSchema != "" {
			schemas = append(schemas, h.unifiedReader.bronzeHotSchema)
		}
		if h.unifiedReader.bronzeColdSchema != "" {
			schemas = append(schemas, h.unifiedReader.bronzeColdSchema)
		}
		for _, schema := range schemas {
			query := fmt.Sprintf("SELECT protocol_version FROM %s.ledgers_row_v2 ORDER BY sequence DESC LIMIT 1", schema)
			var proto sql.NullInt64
			err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&proto)
			if err != nil {
				continue
			}
			if proto.Valid && proto.Int64 > 0 {
				stats.Ledger.ProtocolVersion = int(proto.Int64)
				break
			}
		}
		for _, schema := range schemas {
			// Compute avg close time: (max_time - min_time) / (count - 1) over recent ledgers
			query := fmt.Sprintf(`
				SELECT EXTRACT(EPOCH FROM MAX(closed_at) - MIN(closed_at)), COUNT(*)
				FROM (SELECT closed_at FROM %s.ledgers_row_v2 ORDER BY sequence DESC LIMIT 100) sub
			`, schema)
			var diffSecs sql.NullFloat64
			var cnt sql.NullInt64
			err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&diffSecs, &cnt)
			if err != nil {
				continue
			}
			if diffSecs.Valid && cnt.Valid && cnt.Int64 > 1 && diffSecs.Float64 > 0 {
				avg := diffSecs.Float64 / float64(cnt.Int64-1)
				stats.Ledger.AvgCloseTimeSeconds = math.Round(avg*100) / 100
				break
			}
		}
	}

	// Fetch fee stats from bronze if unified reader available
	if h.unifiedReader != nil {
		feeStats := h.fetchFeeStats(ctx)
		if feeStats != nil {
			stats.Fees24h = feeStats
		}
		// Fetch avg CPU and rent from bronze for soroban stats
		if stats.Soroban != nil {
			avgCPU, rentBurned := h.fetchBronzeSorobanStats(ctx)
			if avgCPU > 0 {
				stats.Soroban.AvgCpuInsns = avgCPU
			}
			if rentBurned > 0 {
				stats.Soroban.RentBurned24hStroops = rentBurned
			}
		}
	}

	respondJSON(w, stats)
}

// fetchFeeStats gets fee percentiles from bronze transactions_row_v2
func (h *NetworkStatsHandler) fetchFeeStats(ctx context.Context) *FeeStats24h {
	if h.unifiedReader == nil {
		return nil
	}

	// Try bronze hot first, then cold
	schemas := []string{}
	if h.unifiedReader.bronzeHotSchema != "" {
		schemas = append(schemas, h.unifiedReader.bronzeHotSchema)
	}
	if h.unifiedReader.bronzeColdSchema != "" {
		schemas = append(schemas, h.unifiedReader.bronzeColdSchema)
	}

	for _, schema := range schemas {
		query := fmt.Sprintf(`
			SELECT
				PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fee_charged) as median,
				PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY fee_charged) as p99,
				SUM(fee_charged) as total
			FROM %s.transactions_row_v2
			WHERE created_at > NOW() - INTERVAL '24 hours' AND successful = true
		`, schema)

		var median, p99 sql.NullFloat64
		var total sql.NullInt64
		err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&median, &p99, &total)
		if err != nil {
			continue
		}

		feeStats := &FeeStats24h{}
		if median.Valid {
			feeStats.MedianStroops = int64(median.Float64)
		}
		if p99.Valid {
			feeStats.P99Stroops = int64(p99.Float64)
		}
		if total.Valid {
			feeStats.DailyTotalStroops = total.Int64
		}
		// Surge detection: if median > base fee (100 stroops)
		feeStats.SurgeActive = feeStats.MedianStroops > 100
		return feeStats
	}
	return nil
}

// HandleBronzeNetworkStats returns headline network statistics from the bronze layer
// GET /api/v1/bronze/stats/network
func (h *NetworkStatsHandler) HandleBronzeNetworkStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if h.unifiedReader == nil {
		respondError(w, "bronze layer not configured", http.StatusServiceUnavailable)
		return
	}

	stats := &BronzeNetworkStats{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		DataFreshness: "real-time",
	}

	// Build ordered schema list: hot first, cold fallback
	schemas := []string{}
	if h.unifiedReader.bronzeHotSchema != "" {
		schemas = append(schemas, h.unifiedReader.bronzeHotSchema)
	}
	if h.unifiedReader.bronzeColdSchema != "" {
		schemas = append(schemas, h.unifiedReader.bronzeColdSchema)
	}
	if len(schemas) == 0 {
		respondError(w, "no bronze schemas configured", http.StatusServiceUnavailable)
		return
	}

	// Query 1: Latest ledger — try hot first, fall back to cold
	for _, schema := range schemas {
		query := fmt.Sprintf(`SELECT sequence, ledger_hash, closed_at, protocol_version
			FROM %s.ledgers_row_v2 ORDER BY sequence DESC LIMIT 1`, schema)
		var seq sql.NullInt64
		var hash sql.NullString
		var closedAt sql.NullTime
		var proto sql.NullInt64
		err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&seq, &hash, &closedAt, &proto)
		if err != nil {
			continue
		}
		if seq.Valid {
			stats.Ledger.LatestSequence = seq.Int64
		}
		if hash.Valid {
			stats.Ledger.LatestHash = hash.String
		}
		if closedAt.Valid {
			stats.Ledger.ClosedAt = closedAt.Time.UTC().Format(time.RFC3339)
		}
		if proto.Valid {
			stats.Ledger.ProtocolVersion = int(proto.Int64)
		}
		break
	}

	// Query 2: Avg close time from recent 100 ledgers — try hot first
	for _, schema := range schemas {
		query := fmt.Sprintf(`
			SELECT EXTRACT(EPOCH FROM MAX(closed_at) - MIN(closed_at)), COUNT(*)
			FROM (SELECT closed_at FROM %s.ledgers_row_v2 ORDER BY sequence DESC LIMIT 100) sub
		`, schema)
		var diffSecs sql.NullFloat64
		var cnt sql.NullInt64
		err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&diffSecs, &cnt)
		if err != nil {
			continue
		}
		if diffSecs.Valid && cnt.Valid && cnt.Int64 > 1 && diffSecs.Float64 > 0 {
			avg := diffSecs.Float64 / float64(cnt.Int64-1)
			stats.Ledger.AvgCloseTimeSeconds = math.Round(avg*100) / 100
			break
		}
	}

	// Query 3: 24h transaction stats — UNION ALL across hot+cold
	txStats := h.fetchBronzeTxStats24h(ctx, schemas)
	if txStats != nil {
		stats.Transactions24h = txStats
	}

	// Query 4: 24h operation stats — UNION ALL across hot+cold
	opStats := h.fetchBronzeOpStats24h(ctx, schemas)
	if opStats != nil {
		stats.Operations24h = opStats
	}

	respondJSON(w, stats)
}

// fetchBronzeTxStats24h gets 24h transaction stats using UNION ALL across schemas
func (h *NetworkStatsHandler) fetchBronzeTxStats24h(ctx context.Context, schemas []string) *BronzeTxStats24h {
	// Try UNION ALL first if we have both schemas
	if len(schemas) == 2 {
		query := fmt.Sprintf(`
			SELECT COUNT(*),
			       SUM(CASE WHEN successful THEN 1 ELSE 0 END),
			       SUM(fee_charged),
			       COUNT(*) FILTER (WHERE soroban_resources_instructions IS NOT NULL)
			FROM (
				SELECT successful, fee_charged, soroban_resources_instructions
				FROM %s.transactions_row_v2 WHERE created_at > NOW() - INTERVAL '24 hours'
				UNION ALL
				SELECT successful, fee_charged, soroban_resources_instructions
				FROM %s.transactions_row_v2 WHERE created_at > NOW() - INTERVAL '24 hours'
			) combined
		`, schemas[0], schemas[1])
		var total, successful, fees, soroban sql.NullInt64
		err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&total, &successful, &fees, &soroban)
		if err == nil && total.Valid && total.Int64 > 0 {
			return &BronzeTxStats24h{
				Total:            total.Int64,
				Successful:       successful.Int64,
				Failed:           total.Int64 - successful.Int64,
				SorobanCount:     soroban.Int64,
				TotalFeesCharged: fees.Int64,
			}
		}
	}

	// Fallback: try each schema individually
	for _, schema := range schemas {
		query := fmt.Sprintf(`
			SELECT COUNT(*),
			       SUM(CASE WHEN successful THEN 1 ELSE 0 END),
			       SUM(fee_charged),
			       COUNT(*) FILTER (WHERE soroban_resources_instructions IS NOT NULL)
			FROM %s.transactions_row_v2
			WHERE created_at > NOW() - INTERVAL '24 hours'
		`, schema)
		var total, successful, fees, soroban sql.NullInt64
		err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&total, &successful, &fees, &soroban)
		if err != nil {
			continue
		}
		if total.Valid && total.Int64 > 0 {
			return &BronzeTxStats24h{
				Total:            total.Int64,
				Successful:       successful.Int64,
				Failed:           total.Int64 - successful.Int64,
				SorobanCount:     soroban.Int64,
				TotalFeesCharged: fees.Int64,
			}
		}
	}
	return nil
}

// fetchBronzeOpStats24h gets 24h operation stats using UNION ALL across schemas
func (h *NetworkStatsHandler) fetchBronzeOpStats24h(ctx context.Context, schemas []string) *BronzeOpStats24h {
	// Try UNION ALL first if we have both schemas
	if len(schemas) == 2 {
		query := fmt.Sprintf(`
			SELECT SUM(operation_count), SUM(soroban_op_count)
			FROM (
				SELECT operation_count, soroban_op_count FROM %s.ledgers_row_v2 WHERE closed_at > NOW() - INTERVAL '24 hours'
				UNION ALL
				SELECT operation_count, soroban_op_count FROM %s.ledgers_row_v2 WHERE closed_at > NOW() - INTERVAL '24 hours'
			) combined
		`, schemas[0], schemas[1])
		var total, soroban sql.NullInt64
		err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&total, &soroban)
		if err == nil && total.Valid && total.Int64 > 0 {
			return &BronzeOpStats24h{
				Total:          total.Int64,
				SorobanOpCount: soroban.Int64,
			}
		}
	}

	// Fallback: try each schema individually
	for _, schema := range schemas {
		query := fmt.Sprintf(`
			SELECT SUM(operation_count), SUM(soroban_op_count)
			FROM %s.ledgers_row_v2
			WHERE closed_at > NOW() - INTERVAL '24 hours'
		`, schema)
		var total, soroban sql.NullInt64
		err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&total, &soroban)
		if err != nil {
			continue
		}
		if total.Valid && total.Int64 > 0 {
			return &BronzeOpStats24h{
				Total:          total.Int64,
				SorobanOpCount: soroban.Int64,
			}
		}
	}
	return nil
}

// fetchBronzeSorobanStats gets average CPU instructions and rent burned from bronze
func (h *NetworkStatsHandler) fetchBronzeSorobanStats(ctx context.Context) (avgCPU int64, rentBurned int64) {
	if h.unifiedReader == nil {
		return 0, 0
	}

	schemas := []string{}
	if h.unifiedReader.bronzeHotSchema != "" {
		schemas = append(schemas, h.unifiedReader.bronzeHotSchema)
	}
	if h.unifiedReader.bronzeColdSchema != "" {
		schemas = append(schemas, h.unifiedReader.bronzeColdSchema)
	}

	for _, schema := range schemas {
		query := fmt.Sprintf(`
			SELECT AVG(soroban_resources_instructions),
			       COALESCE(SUM(rent_fee_charged), 0)
			FROM %s.transactions_row_v2
			WHERE soroban_resources_instructions IS NOT NULL
			AND created_at > NOW() - INTERVAL '24 hours'
		`, schema)

		var avgCPUVal sql.NullFloat64
		var rentVal sql.NullInt64
		err := h.unifiedReader.db.QueryRowContext(ctx, query).Scan(&avgCPUVal, &rentVal)
		if err == nil {
			if avgCPUVal.Valid {
				avgCPU = int64(avgCPUVal.Float64)
			}
			if rentVal.Valid {
				rentBurned = rentVal.Int64
			}
			return
		}
	}
	return 0, 0
}

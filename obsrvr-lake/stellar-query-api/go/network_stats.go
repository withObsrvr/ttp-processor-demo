package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
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
	bronzeReader  *ColdReader          // Bronze layer for accurate total account count
	unifiedReader *UnifiedDuckDBReader  // For fee stats from bronze
	bronzeHotPG   *sql.DB               // Direct PG handle to bronze hot, bypasses DuckDB
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

// SetBronzeHotPG provides a direct PG connection to bronze hot so the
// HandleBronzeNetworkStats handler can run simple aggregate queries without
// going through DuckDB ATTACH POSTGRES. The unified reader's federation layer
// adds ~1-2s of overhead per query, and this handler runs 4 queries, so
// bypassing it drops the total endpoint latency from ~5-7s to well under 1s.
func (h *NetworkStatsHandler) SetBronzeHotPG(db *sql.DB) {
	h.bronzeHotPG = db
}

// HandleNetworkStats returns headline network statistics
// GET /api/v1/silver/stats/network
func (h *NetworkStatsHandler) HandleNetworkStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Fast path: serving projection in silver_hot. This gives Prism and other
	// explorer clients a single-row read for network stats instead of stitching
	// multiple hot/cold queries at request time.
	if h.silverReader != nil && h.silverReader.hot != nil {
		if servingStats, err := h.silverReader.hot.GetServingNetworkStats(ctx); err == nil && servingStats != nil {
			respondJSON(w, servingStats)
			return
		}
	}

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

	// Fetch bronze-side data (ledger info, fee stats, soroban stats)
	// using the parallel hot+cold fast path. This replaces four earlier
	// implementations that each ran through the DuckDB unified reader's
	// postgres_scan federation layer (~1-2s/query × 5-8 queries = 10-15s
	// just for the bronze portion of this endpoint).
	//
	// wantFees=true + wantSoroban=true makes the helper fetch the extra
	// aggregate queries we need for stats.Fees24h and stats.Soroban.
	if fast := h.fetchBronzeStatsFast(ctx, true, true); fast != nil {
		// Bronze overrides silver for ledger metadata and protocol
		// version — the freshest view is bronze hot.
		if fast.ProtocolVersion > 0 {
			stats.Ledger.ProtocolVersion = fast.ProtocolVersion
		}
		if fast.AvgCloseTimeSeconds > 0 {
			stats.Ledger.AvgCloseTimeSeconds = fast.AvgCloseTimeSeconds
		}

		if fast.Fees24h != nil {
			stats.Fees24h = fast.Fees24h
		}

		// Merge bronze soroban aggregates into the silver-derived
		// stats.Soroban struct if it exists. Only overwrite fields we
		// actually got values for.
		if fast.Soroban != nil && stats.Soroban != nil {
			if fast.Soroban.AvgCPU > 0 {
				stats.Soroban.AvgCpuInsns = fast.Soroban.AvgCPU
			}
			if fast.Soroban.RentBurned > 0 {
				stats.Soroban.RentBurned24hStroops = fast.Soroban.RentBurned
			}
		}
	} else if h.unifiedReader != nil {
		// Slow-path fallback: neither bronzeHotPG nor bronzeReader
		// configured, fall back to the DuckDB unified reader. Kept
		// for backwards compatibility with deployments that don't
		// wire the fast-path handles.
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
		if feeStats := h.fetchFeeStats(ctx); feeStats != nil {
			stats.Fees24h = feeStats
		}
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

// bronzeStatsFast holds the aggregated bronze data shared between
// HandleBronzeNetworkStats and HandleNetworkStats. All fields are populated
// by fetchBronzeStatsFast using a parallel hot+cold split, keyed on the
// flusher watermark so there is no overlap or gap between the two layers.
//
// Ledger fields are kept as raw primitives (rather than a typed LedgerStats
// struct) because the two endpoints use different Ledger types
// (LedgerStats vs BronzeLedgerStats) and we want one helper that feeds
// both.
type bronzeStatsFast struct {
	// Latest ledger (always from hot)
	LatestSequence      int64
	LatestHash          string
	ClosedAt            string
	ProtocolVersion     int
	AvgCloseTimeSeconds float64

	Tx24h   *BronzeTxStats24h
	Ops24h  *BronzeOpStats24h
	Fees24h *FeeStats24h
	Soroban *struct {
		AvgCPU     int64
		RentBurned int64
	}
}

// fetchBronzeStatsFast runs the bronze-layer aggregates needed by both
// /bronze/stats/network and /silver/stats/network.
//
// Splits the work along the flusher watermark:
//   - cold (DuckLake via ColdReader — direct Parquet reads, no postgres_scan)
//     serves everything `ledger_sequence <= watermark`
//   - hot (direct PG — no DuckDB federation overhead) serves everything
//     `ledger_sequence > watermark` plus the "always in hot" queries
//     (latest ledger, avg close time)
//
// Hot and cold run in parallel goroutines. All partial failures are logged
// but never propagated — this endpoint aims to return *something* even when
// one layer is unreachable.
//
// Requires both h.bronzeHotPG and h.bronzeReader to be set. Returns nil if
// either is missing (callers should fall through to a slower DuckDB-based
// path).
func (h *NetworkStatsHandler) fetchBronzeStatsFast(ctx context.Context, wantFees, wantSoroban bool) *bronzeStatsFast {
	if h.bronzeHotPG == nil || h.bronzeReader == nil {
		return nil
	}

	result := &bronzeStatsFast{}

	// Flusher watermark — split point between hot and cold. Everything
	// ledger_sequence<=watermark is authoritative in cold (possibly
	// duplicated in hot until the flusher's next DELETE pass), everything
	// >watermark is only in hot.
	var watermark int64
	_ = h.bronzeHotPG.QueryRowContext(ctx, `
		SELECT last_flushed_watermark FROM cold_flusher_checkpoint WHERE id = 1
	`).Scan(&watermark)

	type txAgg struct {
		total, successful, fees, soroban int64
	}
	type opAgg struct {
		ops, sorobanOps int64
	}
	type feeAgg struct {
		median, p99, total int64
	}
	type sorobanAgg struct {
		avgCPU, rentBurned int64
	}

	var (
		coldTx, hotTx             txAgg
		coldOp, hotOp             opAgg
		coldFee, hotFee           feeAgg
		coldSoroban, hotSoroban   sorobanAgg
	)

	catalog := h.bronzeReader.CatalogName()
	schema := h.bronzeReader.SchemaName()
	coldDB := h.bronzeReader.DB()

	// Launch all cold queries in one goroutine. They share the same
	// DuckDB handle so running them sequentially on a single goroutine is
	// fine — parallelizing further would just thrash the single writer.
	doneCold := make(chan struct{})
	go func() {
		defer close(doneCold)

		// Transactions 24h
		txSQL := fmt.Sprintf(`
			SELECT COUNT(*),
			       SUM(CASE WHEN successful THEN 1 ELSE 0 END),
			       SUM(fee_charged),
			       COUNT(*) FILTER (WHERE soroban_resources_instructions IS NOT NULL)
			FROM %s.%s.transactions_row_v2
			WHERE ledger_sequence <= $1
			  AND created_at > NOW() - INTERVAL '24 hours'
		`, catalog, schema)
		var total, successful, fees, soroban sql.NullInt64
		if err := coldDB.QueryRowContext(ctx, txSQL, watermark).Scan(&total, &successful, &fees, &soroban); err != nil {
			log.Printf("fetchBronzeStatsFast cold tx: %v", err)
		} else {
			coldTx = txAgg{total.Int64, successful.Int64, fees.Int64, soroban.Int64}
		}

		// Operations 24h
		opSQL := fmt.Sprintf(`
			SELECT SUM(operation_count), SUM(soroban_op_count)
			FROM %s.%s.ledgers_row_v2
			WHERE sequence <= $1
			  AND closed_at > NOW() - INTERVAL '24 hours'
		`, catalog, schema)
		var opsTotal, opsSoroban sql.NullInt64
		if err := coldDB.QueryRowContext(ctx, opSQL, watermark).Scan(&opsTotal, &opsSoroban); err != nil {
			log.Printf("fetchBronzeStatsFast cold op: %v", err)
		} else {
			coldOp = opAgg{opsTotal.Int64, opsSoroban.Int64}
		}

		// Fee percentiles (only if requested — skipped for /bronze/stats/network)
		if wantFees {
			feeSQL := fmt.Sprintf(`
				SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fee_charged),
				       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY fee_charged),
				       SUM(fee_charged)
				FROM %s.%s.transactions_row_v2
				WHERE ledger_sequence <= $1
				  AND created_at > NOW() - INTERVAL '24 hours'
				  AND successful = true
			`, catalog, schema)
			var median, p99 sql.NullFloat64
			var sum sql.NullInt64
			if err := coldDB.QueryRowContext(ctx, feeSQL, watermark).Scan(&median, &p99, &sum); err != nil {
				log.Printf("fetchBronzeStatsFast cold fee: %v", err)
			} else {
				if median.Valid {
					coldFee.median = int64(median.Float64)
				}
				if p99.Valid {
					coldFee.p99 = int64(p99.Float64)
				}
				if sum.Valid {
					coldFee.total = sum.Int64
				}
			}
		}

		// Soroban avg CPU + rent (only if requested)
		if wantSoroban {
			sbSQL := fmt.Sprintf(`
				SELECT AVG(soroban_resources_instructions),
				       COALESCE(SUM(rent_fee_charged), 0)
				FROM %s.%s.transactions_row_v2
				WHERE ledger_sequence <= $1
				  AND soroban_resources_instructions IS NOT NULL
				  AND created_at > NOW() - INTERVAL '24 hours'
			`, catalog, schema)
			var avg sql.NullFloat64
			var rent sql.NullInt64
			if err := coldDB.QueryRowContext(ctx, sbSQL, watermark).Scan(&avg, &rent); err != nil {
				log.Printf("fetchBronzeStatsFast cold soroban: %v", err)
			} else {
				if avg.Valid {
					coldSoroban.avgCPU = int64(avg.Float64)
				}
				if rent.Valid {
					coldSoroban.rentBurned = rent.Int64
				}
			}
		}
	}()

	// Hot side runs on the main goroutine.

	// Query 1: Latest ledger (always in hot)
	var seq sql.NullInt64
	var hash sql.NullString
	var closedAt sql.NullTime
	var proto sql.NullInt64
	if err := h.bronzeHotPG.QueryRowContext(ctx, `
		SELECT sequence, ledger_hash, closed_at, protocol_version
		FROM ledgers_row_v2 ORDER BY sequence DESC LIMIT 1
	`).Scan(&seq, &hash, &closedAt, &proto); err != nil {
		log.Printf("fetchBronzeStatsFast latest ledger: %v", err)
	} else {
		if seq.Valid {
			result.LatestSequence = seq.Int64
		}
		if hash.Valid {
			result.LatestHash = hash.String
		}
		if closedAt.Valid {
			result.ClosedAt = closedAt.Time.UTC().Format(time.RFC3339)
		}
		if proto.Valid {
			result.ProtocolVersion = int(proto.Int64)
		}
	}

	// Query 2: Avg close time from the recent 100 ledgers (always in hot)
	var diffSecs sql.NullFloat64
	var cnt sql.NullInt64
	_ = h.bronzeHotPG.QueryRowContext(ctx, `
		SELECT EXTRACT(EPOCH FROM MAX(closed_at) - MIN(closed_at)), COUNT(*)
		FROM (SELECT closed_at FROM ledgers_row_v2 ORDER BY sequence DESC LIMIT 100) sub
	`).Scan(&diffSecs, &cnt)
	if diffSecs.Valid && cnt.Valid && cnt.Int64 > 1 && diffSecs.Float64 > 0 {
		avg := diffSecs.Float64 / float64(cnt.Int64-1)
		result.AvgCloseTimeSeconds = math.Round(avg*100) / 100
	}

	// Query 3: hot-tail 24h tx stats (> watermark)
	var hTotal, hSuccessful, hFees, hSoroban sql.NullInt64
	_ = h.bronzeHotPG.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       SUM(CASE WHEN successful THEN 1 ELSE 0 END),
		       SUM(fee_charged),
		       COUNT(*) FILTER (WHERE soroban_resources_instructions IS NOT NULL)
		FROM transactions_row_v2
		WHERE ledger_sequence > $1
		  AND created_at > NOW() - INTERVAL '24 hours'
	`, watermark).Scan(&hTotal, &hSuccessful, &hFees, &hSoroban)
	hotTx = txAgg{hTotal.Int64, hSuccessful.Int64, hFees.Int64, hSoroban.Int64}

	// Query 4: hot-tail 24h op stats (> watermark)
	var hOpsTotal, hOpsSoroban sql.NullInt64
	_ = h.bronzeHotPG.QueryRowContext(ctx, `
		SELECT SUM(operation_count), SUM(soroban_op_count)
		FROM ledgers_row_v2
		WHERE sequence > $1
		  AND closed_at > NOW() - INTERVAL '24 hours'
	`, watermark).Scan(&hOpsTotal, &hOpsSoroban)
	hotOp = opAgg{hOpsTotal.Int64, hOpsSoroban.Int64}

	// Query 5: hot-tail fee percentiles (optional)
	if wantFees {
		var median, p99 sql.NullFloat64
		var sum sql.NullInt64
		_ = h.bronzeHotPG.QueryRowContext(ctx, `
			SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fee_charged),
			       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY fee_charged),
			       SUM(fee_charged)
			FROM transactions_row_v2
			WHERE ledger_sequence > $1
			  AND created_at > NOW() - INTERVAL '24 hours'
			  AND successful = true
		`, watermark).Scan(&median, &p99, &sum)
		if median.Valid {
			hotFee.median = int64(median.Float64)
		}
		if p99.Valid {
			hotFee.p99 = int64(p99.Float64)
		}
		if sum.Valid {
			hotFee.total = sum.Int64
		}
	}

	// Query 6: hot-tail soroban avg CPU + rent (optional)
	if wantSoroban {
		var avg sql.NullFloat64
		var rent sql.NullInt64
		_ = h.bronzeHotPG.QueryRowContext(ctx, `
			SELECT AVG(soroban_resources_instructions),
			       COALESCE(SUM(rent_fee_charged), 0)
			FROM transactions_row_v2
			WHERE ledger_sequence > $1
			  AND soroban_resources_instructions IS NOT NULL
			  AND created_at > NOW() - INTERVAL '24 hours'
		`, watermark).Scan(&avg, &rent)
		if avg.Valid {
			hotSoroban.avgCPU = int64(avg.Float64)
		}
		if rent.Valid {
			hotSoroban.rentBurned = rent.Int64
		}
	}

	// Wait for cold and merge
	<-doneCold

	totalTx := hotTx.total + coldTx.total
	if totalTx > 0 {
		result.Tx24h = &BronzeTxStats24h{
			Total:            totalTx,
			Successful:       hotTx.successful + coldTx.successful,
			Failed:           totalTx - (hotTx.successful + coldTx.successful),
			SorobanCount:     hotTx.soroban + coldTx.soroban,
			TotalFeesCharged: hotTx.fees + coldTx.fees,
		}
	}
	totalOps := hotOp.ops + coldOp.ops
	if totalOps > 0 {
		result.Ops24h = &BronzeOpStats24h{
			Total:          totalOps,
			SorobanOpCount: hotOp.sorobanOps + coldOp.sorobanOps,
		}
	}

	// Merging percentiles across two samples is statistically incorrect
	// (the p50/p99 of a union isn't the average of each side's p50/p99).
	// The best we can do without re-sorting the combined sample is to
	// take the value from whichever side has more data. In practice cold
	// holds the vast majority of the 24h window (everything except the
	// minutes since the last flush), so cold's percentiles are the
	// authoritative signal. Fall back to hot's values only if cold was
	// empty (either no data in range or query failed).
	if wantFees {
		result.Fees24h = &FeeStats24h{
			DailyTotalStroops: hotFee.total + coldFee.total,
		}
		if coldFee.median > 0 || coldFee.p99 > 0 {
			result.Fees24h.MedianStroops = coldFee.median
			result.Fees24h.P99Stroops = coldFee.p99
		} else {
			result.Fees24h.MedianStroops = hotFee.median
			result.Fees24h.P99Stroops = hotFee.p99
		}
		result.Fees24h.SurgeActive = result.Fees24h.MedianStroops > 100
	}

	if wantSoroban {
		// AVG across two samples is also approximate, but we can do
		// better than for percentiles by weighting: cold has vastly more
		// rows so its average dominates. Use cold when available.
		result.Soroban = &struct {
			AvgCPU     int64
			RentBurned int64
		}{
			RentBurned: hotSoroban.rentBurned + coldSoroban.rentBurned,
		}
		if coldSoroban.avgCPU > 0 {
			result.Soroban.AvgCPU = coldSoroban.avgCPU
		} else {
			result.Soroban.AvgCPU = hotSoroban.avgCPU
		}
	}

	return result
}

// HandleBronzeNetworkStats returns headline network statistics from the bronze layer
// GET /api/v1/bronze/stats/network
func (h *NetworkStatsHandler) HandleBronzeNetworkStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	stats := &BronzeNetworkStats{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		DataFreshness: "real-time",
	}

	// Fast path: use the parallel hot+cold helper. See fetchBronzeStatsFast
	// for the architecture — direct PG for the hot tail, direct DuckLake
	// for everything below the flusher watermark, merged in Go. This
	// endpoint doesn't need fee percentiles or Soroban CPU averages so we
	// pass wantFees=false, wantSoroban=false.
	if fast := h.fetchBronzeStatsFast(ctx, false, false); fast != nil {
		stats.Ledger.LatestSequence = fast.LatestSequence
		stats.Ledger.LatestHash = fast.LatestHash
		stats.Ledger.ClosedAt = fast.ClosedAt
		stats.Ledger.ProtocolVersion = fast.ProtocolVersion
		stats.Ledger.AvgCloseTimeSeconds = fast.AvgCloseTimeSeconds
		stats.Transactions24h = fast.Tx24h
		stats.Operations24h = fast.Ops24h
		if stats.Ledger.LatestSequence == 0 {
			respondError(w, "unable to query bronze ledger data", http.StatusServiceUnavailable)
			return
		}
		respondJSON(w, stats)
		return
	}

	// Kept for compile — the old inline implementation follows. The
	// condition is now unreachable because fetchBronzeStatsFast handles
	// the same (bronzeHotPG != nil && bronzeReader != nil) case, but we
	// leave the slow fallback in place for deployments where one of the
	// two is missing.
	if h.bronzeHotPG != nil && h.bronzeReader != nil {
		// Discover the flush watermark that splits hot from cold.
		// Everything with ledger_sequence <= watermark has been flushed
		// to cold. Everything with ledger_sequence > watermark lives
		// only in hot. Rows with ledger_sequence <= watermark may also
		// exist in hot (the flusher delays deletion until the downstream
		// silver checkpoint catches up), but we won't double-count
		// because we split the hot query to `> watermark`.
		var watermark int64
		_ = h.bronzeHotPG.QueryRowContext(ctx, `
			SELECT last_flushed_watermark FROM cold_flusher_checkpoint WHERE id = 1
		`).Scan(&watermark)

		// Kick off cold and hot queries in parallel.
		type txAgg struct {
			total, successful, fees, soroban int64
		}
		type opAgg struct {
			ops, sorobanOps int64
		}

		var (
			coldTx, hotTx txAgg
			coldOp, hotOp opAgg
			coldErr       error
		)

		doneCold := make(chan struct{})
		go func() {
			defer close(doneCold)
			// Cold: transactions with closed_at in the 24h window,
			// bounded above by the flusher watermark so we never
			// count rows that are also in hot. `closed_at` is the
			// ledger close timestamp preserved through the flusher
			// (INSERT … SELECT), so this is stable regardless of
			// when the row was flushed.
			catalog := h.bronzeReader.CatalogName()
			schema := h.bronzeReader.SchemaName()
			txSQL := fmt.Sprintf(`
				SELECT COUNT(*),
				       SUM(CASE WHEN successful THEN 1 ELSE 0 END),
				       SUM(fee_charged),
				       COUNT(*) FILTER (WHERE soroban_resources_instructions IS NOT NULL)
				FROM %s.%s.transactions_row_v2
				WHERE ledger_sequence <= $1
				  AND created_at > NOW() - INTERVAL '24 hours'
			`, catalog, schema)
			var total, successful, fees, soroban sql.NullInt64
			if err := h.bronzeReader.DB().QueryRowContext(ctx, txSQL, watermark).Scan(&total, &successful, &fees, &soroban); err != nil {
				coldErr = err
				return
			}
			coldTx.total = total.Int64
			coldTx.successful = successful.Int64
			coldTx.fees = fees.Int64
			coldTx.soroban = soroban.Int64

			// Cold: operation stats in the 24h window
			opSQL := fmt.Sprintf(`
				SELECT SUM(operation_count), SUM(soroban_op_count)
				FROM %s.%s.ledgers_row_v2
				WHERE sequence <= $1
				  AND closed_at > NOW() - INTERVAL '24 hours'
			`, catalog, schema)
			var opsTotal, opsSoroban sql.NullInt64
			if err := h.bronzeReader.DB().QueryRowContext(ctx, opSQL, watermark).Scan(&opsTotal, &opsSoroban); err != nil {
				coldErr = err
				return
			}
			coldOp.ops = opsTotal.Int64
			coldOp.sorobanOps = opsSoroban.Int64
		}()

		// Hot queries run on the main goroutine so we can return
		// early on a hard error without a cancel dance. Query 1
		// (latest ledger) is required; the rest are best-effort.

		// Query 1: Latest ledger — always in hot by definition
		var seq sql.NullInt64
		var hash sql.NullString
		var closedAt sql.NullTime
		var proto sql.NullInt64
		if err := h.bronzeHotPG.QueryRowContext(ctx, `
			SELECT sequence, ledger_hash, closed_at, protocol_version
			FROM ledgers_row_v2 ORDER BY sequence DESC LIMIT 1
		`).Scan(&seq, &hash, &closedAt, &proto); err != nil {
			<-doneCold // don't leak the goroutine
			respondError(w, "unable to query bronze ledger data", http.StatusServiceUnavailable)
			return
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

		// Query 2: Avg close time from recent 100 ledgers — always in hot
		var diffSecs sql.NullFloat64
		var cnt sql.NullInt64
		_ = h.bronzeHotPG.QueryRowContext(ctx, `
			SELECT EXTRACT(EPOCH FROM MAX(closed_at) - MIN(closed_at)), COUNT(*)
			FROM (SELECT closed_at FROM ledgers_row_v2 ORDER BY sequence DESC LIMIT 100) sub
		`).Scan(&diffSecs, &cnt)
		if diffSecs.Valid && cnt.Valid && cnt.Int64 > 1 && diffSecs.Float64 > 0 {
			avg := diffSecs.Float64 / float64(cnt.Int64-1)
			stats.Ledger.AvgCloseTimeSeconds = math.Round(avg*100) / 100
		}

		// Query 3 (hot tail): 24h transaction stats strictly above the
		// flusher watermark. Uses idx_transactions_row_v2_created_at
		// combined with the ledger_sequence filter.
		var hTotal, hSuccessful, hFees, hSoroban sql.NullInt64
		_ = h.bronzeHotPG.QueryRowContext(ctx, `
			SELECT COUNT(*),
			       SUM(CASE WHEN successful THEN 1 ELSE 0 END),
			       SUM(fee_charged),
			       COUNT(*) FILTER (WHERE soroban_resources_instructions IS NOT NULL)
			FROM transactions_row_v2
			WHERE ledger_sequence > $1
			  AND created_at > NOW() - INTERVAL '24 hours'
		`, watermark).Scan(&hTotal, &hSuccessful, &hFees, &hSoroban)
		hotTx.total = hTotal.Int64
		hotTx.successful = hSuccessful.Int64
		hotTx.fees = hFees.Int64
		hotTx.soroban = hSoroban.Int64

		// Query 4 (hot tail): 24h operation stats strictly above the
		// flusher watermark.
		var hOpsTotal, hOpsSoroban sql.NullInt64
		_ = h.bronzeHotPG.QueryRowContext(ctx, `
			SELECT SUM(operation_count), SUM(soroban_op_count)
			FROM ledgers_row_v2
			WHERE sequence > $1
			  AND closed_at > NOW() - INTERVAL '24 hours'
		`, watermark).Scan(&hOpsTotal, &hOpsSoroban)
		hotOp.ops = hOpsTotal.Int64
		hotOp.sorobanOps = hOpsSoroban.Int64

		// Wait for cold and merge
		<-doneCold
		if coldErr != nil {
			log.Printf("HandleBronzeNetworkStats: cold query failed, returning hot-only stats: %v", coldErr)
		}

		totalTx := hotTx.total + coldTx.total
		if totalTx > 0 {
			stats.Transactions24h = &BronzeTxStats24h{
				Total:            totalTx,
				Successful:       hotTx.successful + coldTx.successful,
				Failed:           totalTx - (hotTx.successful + coldTx.successful),
				SorobanCount:     hotTx.soroban + coldTx.soroban,
				TotalFeesCharged: hotTx.fees + coldTx.fees,
			}
		}
		totalOps := hotOp.ops + coldOp.ops
		if totalOps > 0 {
			stats.Operations24h = &BronzeOpStats24h{
				Total:          totalOps,
				SorobanOpCount: hotOp.sorobanOps + coldOp.sorobanOps,
			}
		}

		respondJSON(w, stats)
		return
	}

	// Slow path: no direct PG handle, fall back to the unified reader.
	// This is the historical behavior and runs every query through DuckDB.
	if h.unifiedReader == nil {
		respondError(w, "bronze layer not configured", http.StatusServiceUnavailable)
		return
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
	ledgerFound := false
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
			ledgerFound = true
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
	if !ledgerFound {
		respondError(w, "unable to query bronze ledger data", http.StatusServiceUnavailable)
		return
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

// fetchBronzeTxStats24h gets 24h transaction stats, deduplicating across hot+cold by transaction_hash
func (h *NetworkStatsHandler) fetchBronzeTxStats24h(ctx context.Context, schemas []string) *BronzeTxStats24h {
	// UNION ALL with dedup via DISTINCT ON to avoid double-counting during flush overlap
	if len(schemas) == 2 {
		query := fmt.Sprintf(`
			SELECT COUNT(*),
			       SUM(CASE WHEN successful THEN 1 ELSE 0 END),
			       SUM(fee_charged),
			       COUNT(*) FILTER (WHERE soroban_resources_instructions IS NOT NULL)
			FROM (
				SELECT DISTINCT ON (transaction_hash) successful, fee_charged, soroban_resources_instructions
				FROM (
					SELECT transaction_hash, successful, fee_charged, soroban_resources_instructions
					FROM %s.transactions_row_v2 WHERE created_at > NOW() - INTERVAL '24 hours'
					UNION ALL
					SELECT transaction_hash, successful, fee_charged, soroban_resources_instructions
					FROM %s.transactions_row_v2 WHERE created_at > NOW() - INTERVAL '24 hours'
				) raw
			) deduped
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

	// Fallback: single schema, no dedup needed
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

// fetchBronzeOpStats24h gets 24h operation stats, deduplicating across hot+cold by ledger sequence
func (h *NetworkStatsHandler) fetchBronzeOpStats24h(ctx context.Context, schemas []string) *BronzeOpStats24h {
	// UNION ALL with dedup via DISTINCT ON(sequence) to avoid double-counting during flush overlap
	if len(schemas) == 2 {
		query := fmt.Sprintf(`
			SELECT SUM(operation_count), SUM(soroban_op_count)
			FROM (
				SELECT DISTINCT ON (sequence) operation_count, soroban_op_count
				FROM (
					SELECT sequence, operation_count, soroban_op_count FROM %s.ledgers_row_v2 WHERE closed_at > NOW() - INTERVAL '24 hours'
					UNION ALL
					SELECT sequence, operation_count, soroban_op_count FROM %s.ledgers_row_v2 WHERE closed_at > NOW() - INTERVAL '24 hours'
				) raw
			) deduped
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

	// Fallback: single schema, no dedup needed
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

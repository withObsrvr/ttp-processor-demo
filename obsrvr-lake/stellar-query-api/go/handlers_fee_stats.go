package main

import (
	"database/sql"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// FeeStatsHandler handles fee statistics endpoints
type FeeStatsHandler struct {
	reader *UnifiedDuckDBReader
}

// NewFeeStatsHandler creates a new fee stats handler
func NewFeeStatsHandler(reader *UnifiedDuckDBReader) *FeeStatsHandler {
	return &FeeStatsHandler{reader: reader}
}

// FeeStatsResponse represents fee statistics for a time period
type FeeStatsResponse struct {
	Period       string  `json:"period"`
	MedianFee    int64   `json:"median_fee"`
	P75Fee       int64   `json:"p75_fee"`
	P90Fee       int64   `json:"p90_fee"`
	P99Fee       int64   `json:"p99_fee"`
	MinFee       int64   `json:"min_fee"`
	MaxFee       int64   `json:"max_fee"`
	TotalFees    int64   `json:"total_fees"`
	TxCount      int64   `json:"tx_count"`
	SurgeActive  bool    `json:"surge_active"`
	SurgePct     float64 `json:"surge_pct_of_ledgers,omitempty"`
	GeneratedAt  string  `json:"generated_at"`
}

// HandleFeeStats returns fee statistics for a given period
// @Summary Get fee statistics
// @Description Returns fee percentiles, surge detection, and aggregate fee data
// @Tags Statistics
// @Accept json
// @Produce json
// @Param period query string false "Time period: 1h, 24h (default), 7d"
// @Success 200 {object} FeeStatsResponse "Fee statistics"
// @Failure 400 {object} map[string]interface{} "Invalid period"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/stats/fees [get]
func (h *FeeStatsHandler) HandleFeeStats(w http.ResponseWriter, r *http.Request) {
	period := r.URL.Query().Get("period")
	if period == "" {
		period = "24h"
	}

	validPeriods := map[string]string{
		"1h":  "1 hour",
		"24h": "24 hours",
		"7d":  "7 days",
	}
	interval, ok := validPeriods[period]
	if !ok {
		respondError(w, "invalid period: must be 1h, 24h, or 7d", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// Try bronze hot, then cold
	schemas := h.getBronzeSchemas()
	if len(schemas) == 0 {
		respondError(w, "bronze data source not configured", http.StatusInternalServerError)
		return
	}

	for _, schema := range schemas {
		query := fmt.Sprintf(`
			SELECT
				PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fee_charged) as median,
				PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY fee_charged) as p75,
				PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY fee_charged) as p90,
				PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY fee_charged) as p99,
				MIN(fee_charged), MAX(fee_charged),
				SUM(fee_charged), COUNT(*)
			FROM %s.transactions_row_v2
			WHERE created_at > NOW() - INTERVAL '%s' AND successful = true
		`, schema, interval)

		var median, p75, p90, p99 sql.NullFloat64
		var minFee, maxFee, totalFees, txCount sql.NullInt64
		err := h.reader.db.QueryRowContext(ctx, query).Scan(
			&median, &p75, &p90, &p99,
			&minFee, &maxFee, &totalFees, &txCount,
		)
		if err != nil {
			continue
		}

		resp := FeeStatsResponse{
			Period:      period,
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		}
		if median.Valid {
			resp.MedianFee = int64(median.Float64)
		}
		if p75.Valid {
			resp.P75Fee = int64(p75.Float64)
		}
		if p90.Valid {
			resp.P90Fee = int64(p90.Float64)
		}
		if p99.Valid {
			resp.P99Fee = int64(p99.Float64)
		}
		if minFee.Valid {
			resp.MinFee = minFee.Int64
		}
		if maxFee.Valid {
			resp.MaxFee = maxFee.Int64
		}
		if totalFees.Valid {
			resp.TotalFees = totalFees.Int64
		}
		if txCount.Valid {
			resp.TxCount = txCount.Int64
		}

		// Surge detection: median > base fee (100 stroops)
		resp.SurgeActive = resp.MedianFee > 100

		respondJSON(w, resp)
		return
	}

	respondError(w, "failed to query fee stats", http.StatusInternalServerError)
}

// LedgerFeesResponse represents fee distribution for a single ledger
type LedgerFeesResponse struct {
	LedgerSequence int64           `json:"ledger_sequence"`
	TxCount        int             `json:"tx_count"`
	MinFee         int64           `json:"min_fee"`
	MaxFee         int64           `json:"max_fee"`
	MedianFee      int64           `json:"median_fee"`
	P90Fee         int64           `json:"p90_fee"`
	TotalFees      int64           `json:"total_fees"`
	Histogram      []FeeBucket     `json:"histogram"`
	GeneratedAt    string          `json:"generated_at"`
}

// FeeBucket represents a fee histogram bucket
type FeeBucket struct {
	Range string `json:"range"`
	Count int    `json:"count"`
}

// HandleLedgerFees returns fee distribution for a specific ledger
// @Summary Get ledger fee distribution
// @Description Returns fee histogram and percentiles for a specific ledger
// @Tags Statistics
// @Accept json
// @Produce json
// @Param seq path int true "Ledger sequence number"
// @Success 200 {object} LedgerFeesResponse "Ledger fee distribution"
// @Failure 400 {object} map[string]interface{} "Invalid ledger sequence"
// @Failure 404 {object} map[string]interface{} "No transactions in ledger"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/ledgers/{seq}/fees [get]
func (h *FeeStatsHandler) HandleLedgerFees(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	seqStr := vars["seq"]
	ledgerSeq, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil {
		respondError(w, "invalid ledger sequence", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	schemas := h.getBronzeSchemas()
	if len(schemas) == 0 {
		respondError(w, "bronze data source not configured", http.StatusInternalServerError)
		return
	}

	for _, schema := range schemas {
		query := fmt.Sprintf(`
			SELECT fee_charged
			FROM %s.transactions_row_v2
			WHERE ledger_sequence = $1
			ORDER BY fee_charged
		`, schema)

		rows, err := h.reader.db.QueryContext(ctx, query, ledgerSeq)
		if err != nil {
			continue
		}

		var fees []int64
		for rows.Next() {
			var fee int64
			if err := rows.Scan(&fee); err != nil {
				rows.Close()
				continue
			}
			fees = append(fees, fee)
		}
		rows.Close()

		if len(fees) == 0 {
			respondError(w, "no transactions found in ledger", http.StatusNotFound)
			return
		}

		sort.Slice(fees, func(i, j int) bool { return fees[i] < fees[j] })

		var totalFees int64
		for _, f := range fees {
			totalFees += f
		}

		// Compute histogram buckets
		buckets := []struct {
			label string
			min   int64
			max   int64
		}{
			{"100-200", 100, 200},
			{"200-1000", 200, 1000},
			{"1000-10000", 1000, 10000},
			{"10000+", 10000, math.MaxInt64},
		}

		histogram := make([]FeeBucket, 0, len(buckets))
		for _, b := range buckets {
			count := 0
			for _, f := range fees {
				if f >= b.min && f < b.max {
					count++
				}
			}
			if count > 0 {
				histogram = append(histogram, FeeBucket{Range: b.label, Count: count})
			}
		}

		resp := LedgerFeesResponse{
			LedgerSequence: ledgerSeq,
			TxCount:        len(fees),
			MinFee:         fees[0],
			MaxFee:         fees[len(fees)-1],
			MedianFee:      percentile(fees, 0.5),
			P90Fee:         percentile(fees, 0.9),
			TotalFees:      totalFees,
			Histogram:      histogram,
			GeneratedAt:    time.Now().UTC().Format(time.RFC3339),
		}

		respondJSON(w, resp)
		return
	}

	respondError(w, "failed to query ledger fees", http.StatusInternalServerError)
}

// getBronzeSchemas returns available bronze schemas to query
func (h *FeeStatsHandler) getBronzeSchemas() []string {
	schemas := []string{}
	if h.reader.bronzeHotSchema != "" {
		schemas = append(schemas, h.reader.bronzeHotSchema)
	}
	if h.reader.bronzeColdSchema != "" {
		schemas = append(schemas, h.reader.bronzeColdSchema)
	}
	return schemas
}

// SorobanStatsHandler handles Soroban network statistics
type SorobanStatsHandler struct {
	reader    *UnifiedDuckDBReader
	silverDB  *SilverHotReader
}

// NewSorobanStatsHandler creates a new soroban stats handler
func NewSorobanStatsHandler(reader *UnifiedDuckDBReader, silverDB *SilverHotReader) *SorobanStatsHandler {
	return &SorobanStatsHandler{reader: reader, silverDB: silverDB}
}

// SorobanStatsResponse represents Soroban network statistics
type SorobanStatsResponse struct {
	Contracts   SorobanContractStats   `json:"contracts"`
	Execution   SorobanExecutionStats  `json:"execution"`
	State       SorobanStateStats      `json:"state"`
	GeneratedAt string                 `json:"generated_at"`
}

// SorobanContractStats contains contract deployment/activity stats
type SorobanContractStats struct {
	TotalDeployed int64 `json:"total_deployed"`
	Active24h     int64 `json:"active_24h"`
	Active7d      int64 `json:"active_7d"`
}

// SorobanExecutionStats contains execution stats
type SorobanExecutionStats struct {
	TotalInvocations24h  int64 `json:"total_invocations_24h"`
	AvgCPUInsns          int64 `json:"avg_cpu_insns,omitempty"`
	TotalCPUInsns        int64 `json:"total_cpu_insns,omitempty"`
	RentBurned24hStroops int64 `json:"rent_burned_24h_stroops,omitempty"`
}

// SorobanStateStats contains state entry stats
type SorobanStateStats struct {
	PersistentEntries int64 `json:"persistent_entries"`
	TemporaryEntries  int64 `json:"temporary_entries"`
}

// HandleSorobanStats returns comprehensive Soroban network statistics
// @Summary Get Soroban network statistics
// @Description Returns contract, execution, and state statistics for the Soroban runtime
// @Tags Statistics
// @Accept json
// @Produce json
// @Success 200 {object} SorobanStatsResponse "Soroban statistics"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/stats/soroban [get]
func (h *SorobanStatsHandler) HandleSorobanStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	resp := SorobanStatsResponse{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Contract stats from silver contract_invocations_raw
	if h.silverDB != nil {
		contractQuery := `
			SELECT
				COUNT(DISTINCT contract_id) as total_deployed,
				COUNT(DISTINCT contract_id) FILTER (WHERE closed_at > NOW() - INTERVAL '24 hours') as active_24h,
				COUNT(DISTINCT contract_id) FILTER (WHERE closed_at > NOW() - INTERVAL '7 days') as active_7d
			FROM contract_invocations_raw
		`
		_ = h.silverDB.db.QueryRowContext(ctx, contractQuery).Scan(
			&resp.Contracts.TotalDeployed,
			&resp.Contracts.Active24h,
			&resp.Contracts.Active7d,
		)

		// Invocation count
		invocQuery := `
			SELECT COUNT(*)
			FROM contract_invocations_raw
			WHERE closed_at > NOW() - INTERVAL '24 hours'
		`
		_ = h.silverDB.db.QueryRowContext(ctx, invocQuery).Scan(&resp.Execution.TotalInvocations24h)
	}

	// Execution stats from bronze (CPU instructions)
	if h.reader != nil {
		schemas := []string{}
		if h.reader.bronzeHotSchema != "" {
			schemas = append(schemas, h.reader.bronzeHotSchema)
		}
		if h.reader.bronzeColdSchema != "" {
			schemas = append(schemas, h.reader.bronzeColdSchema)
		}
		for _, schema := range schemas {
			query := fmt.Sprintf(`
				SELECT
					AVG(soroban_resources_instructions) as avg_cpu,
					SUM(soroban_resources_instructions) as total_cpu,
					COALESCE(SUM(rent_fee_charged), 0) as rent_burned
				FROM %s.transactions_row_v2
				WHERE soroban_resources_instructions IS NOT NULL
				AND created_at > NOW() - INTERVAL '24 hours'
			`, schema)
			var avgCPU, totalCPU sql.NullFloat64
			var rentBurned sql.NullInt64
			err := h.reader.db.QueryRowContext(ctx, query).Scan(&avgCPU, &totalCPU, &rentBurned)
			if err == nil {
				if avgCPU.Valid {
					resp.Execution.AvgCPUInsns = int64(avgCPU.Float64)
				}
				if totalCPU.Valid {
					resp.Execution.TotalCPUInsns = int64(totalCPU.Float64)
				}
				if rentBurned.Valid {
					resp.Execution.RentBurned24hStroops = rentBurned.Int64
				}
				break
			}
		}
	}

	// State entry counts from silver
	if h.silverDB != nil {
		stateQuery := `
			SELECT
				COUNT(*) FILTER (WHERE durability = 'persistent') as persistent,
				COUNT(*) FILTER (WHERE durability = 'temporary') as temporary
			FROM contract_data_current
		`
		_ = h.silverDB.db.QueryRowContext(ctx, stateQuery).Scan(
			&resp.State.PersistentEntries,
			&resp.State.TemporaryEntries,
		)
	}

	respondJSON(w, resp)
}

// percentile computes the p-th percentile from a sorted slice of int64s
func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := p * float64(len(sorted)-1)
	lower := int(idx)
	if lower >= len(sorted)-1 {
		return sorted[len(sorted)-1]
	}
	return sorted[lower]
}

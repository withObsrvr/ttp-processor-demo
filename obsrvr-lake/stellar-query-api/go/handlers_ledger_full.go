package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// LedgerFullHandler serves /silver/ledger/{seq}/full — a single composite endpoint
// that returns everything Prism needs for the ledger detail page in one call.
// Replaces 6 separate gateway calls with one.
type LedgerFullHandler struct {
	queryService  *QueryService
	silverHot     *SilverHotReader
	silverUnified *UnifiedSilverReader // hot+cold for operations (no DuckDB federation)
}

func NewLedgerFullHandler(qs *QueryService, silverHot *SilverHotReader, silverUnified *UnifiedSilverReader) *LedgerFullHandler {
	return &LedgerFullHandler{
		queryService:  qs,
		silverHot:     silverHot,
		silverUnified: silverUnified,
	}
}

// HandleLedgerFull returns a composite response with ledger header, transactions,
// fee stats, soroban stats, and decoded transaction summaries.
func (h *LedgerFullHandler) HandleLedgerFull(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	seqStr := vars["seq"]
	if seqStr == "" {
		respondError(w, "Missing required path parameter: seq", http.StatusBadRequest)
		return
	}
	seq, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil {
		respondError(w, "Invalid seq parameter", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	type result struct {
		key  string
		data interface{}
		err  error
	}

	ch := make(chan result, 5)
	var wg sync.WaitGroup

	// 1. Ledger header (from bronze hot/cold)
	wg.Add(1)
	go func() {
		defer wg.Done()
		queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := h.queryService.determineSource(seq, seq)
		var ledger map[string]interface{}
		if queryHot {
			rows, err := h.queryService.hot.QueryLedgers(ctx, hotStart, hotEnd, 1, "sequence_asc")
			if err == nil {
				results, scanErr := scanLedgers(rows)
				rows.Close()
				if scanErr == nil && len(results) > 0 {
					ledger = results[0]
				}
			}
		}
		if ledger == nil && queryCold {
			rows, err := h.queryService.cold.QueryLedgers(ctx, coldStart, coldEnd, 1, "sequence_asc")
			if err == nil {
				results, scanErr := scanLedgers(rows)
				rows.Close()
				if scanErr == nil && len(results) > 0 {
					ledger = results[0]
				}
			}
		}
		ch <- result{key: "ledger", data: ledger}
	}()

	// 2. Transactions (from bronze hot/cold)
	wg.Add(1)
	go func() {
		defer wg.Done()
		queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := h.queryService.determineSource(seq, seq)
		var txs []map[string]interface{}
		if queryHot {
			rows, err := h.queryService.hot.QueryTransactions(ctx, hotStart, hotEnd, 50)
			if err == nil {
				txs, _ = scanTransactions(rows)
				rows.Close()
			}
		}
		if len(txs) == 0 && queryCold {
			rows, err := h.queryService.cold.QueryTransactions(ctx, coldStart, coldEnd, 50)
			if err == nil {
				txs, _ = scanTransactions(rows)
				rows.Close()
			}
		}
		ch <- result{key: "transactions", data: txs}
	}()

	// 3. Fee stats (from silver hot PG directly)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if h.silverHot == nil {
			ch <- result{key: "fees"}
			return
		}
		fees, err := h.silverHot.GetLedgerFeeStats(ctx, seq)
		if err != nil {
			log.Printf("ledger/full: fees error: %v", err)
			ch <- result{key: "fees", err: err}
			return
		}
		ch <- result{key: "fees", data: fees}
	}()

	// 4. Soroban stats (from silver hot PG directly)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if h.silverHot == nil {
			ch <- result{key: "soroban"}
			return
		}
		soroban, err := h.silverHot.GetLedgerSorobanStats(ctx, seq)
		if err != nil {
			log.Printf("ledger/full: soroban error: %v", err)
			ch <- result{key: "soroban", err: err}
			return
		}
		ch <- result{key: "soroban", data: soroban}
	}()

	// 5. Operations — silver enriched first (hot+cold), bronze fallback for old ledgers
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Try silver enriched operations (hot PG first, cold DuckLake fallback)
		if h.silverUnified != nil {
			ops, err := h.silverUnified.GetEnrichedOperations(ctx, OperationFilters{
				StartLedger: seq,
				EndLedger:   seq,
				Limit:       200,
			})
			if err == nil && len(ops) > 0 {
				ch <- result{key: "operations", data: ops}
				return
			}
		}
		// Fallback to bronze operations for old ledgers without silver data
		queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := h.queryService.determineSource(seq, seq)
		var bronzeOps []map[string]interface{}
		if queryHot {
			rows, err := h.queryService.hot.QueryOperations(ctx, hotStart, hotEnd, 200)
			if err == nil {
				bronzeOps, _ = scanOperations(rows)
				rows.Close()
			}
		}
		if len(bronzeOps) == 0 && queryCold {
			rows, err := h.queryService.cold.QueryOperations(ctx, coldStart, coldEnd, 200)
			if err == nil {
				bronzeOps, _ = scanOperations(rows)
				rows.Close()
			}
		}
		ch <- result{key: "operations", data: bronzeOps}
	}()

	go func() {
		wg.Wait()
		close(ch)
	}()

	// Collect results
	response := map[string]interface{}{
		"ledger_sequence": seq,
		"generated_at":    time.Now().UTC().Format(time.RFC3339),
	}
	for r := range ch {
		if r.data != nil {
			response[r.key] = r.data
		}
	}

	respondJSON(w, response)
}

// GetLedgerFeeStats queries fee distribution for a ledger directly from silver_hot PG.
func (h *SilverHotReader) GetLedgerFeeStats(ctx context.Context, seq int64) (map[string]interface{}, error) {
	query := `
		SELECT
			COUNT(*) AS tx_count,
			COALESCE(MIN(tx_fee_charged), 0) AS min_fee,
			COALESCE(MAX(tx_fee_charged), 0) AS max_fee,
			COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx_fee_charged), 0) AS median_fee,
			COALESCE(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY tx_fee_charged), 0) AS p90_fee,
			COALESCE(SUM(tx_fee_charged), 0) AS total_fees
		FROM enriched_history_operations
		WHERE ledger_sequence = $1
		  AND operation_index = 0
	`
	var txCount int
	var minFee, maxFee, medianFee, p90Fee, totalFees float64
	err := h.db.QueryRowContext(ctx, query, seq).Scan(&txCount, &minFee, &maxFee, &medianFee, &p90Fee, &totalFees)
	if err != nil {
		return nil, fmt.Errorf("fee stats: %w", err)
	}
	return map[string]interface{}{
		"ledger_sequence": seq,
		"tx_count":        txCount,
		"min_fee":         int64(minFee),
		"max_fee":         int64(maxFee),
		"median_fee":      int64(medianFee),
		"p90_fee":         int64(p90Fee),
		"total_fees":      int64(totalFees),
	}, nil
}

// GetLedgerSorobanStats queries Soroban resource usage for a ledger from silver_hot PG.
func (h *SilverHotReader) GetLedgerSorobanStats(ctx context.Context, seq int64) (map[string]interface{}, error) {
	query := `
		SELECT
			COUNT(*) FILTER (WHERE contract_id IS NOT NULL) AS soroban_tx_count,
			COALESCE(COUNT(DISTINCT contract_id) FILTER (WHERE contract_id IS NOT NULL), 0) AS unique_contracts
		FROM enriched_history_operations
		WHERE ledger_sequence = $1
	`
	var sorobanTxCount, uniqueContracts int
	err := h.db.QueryRowContext(ctx, query, seq).Scan(&sorobanTxCount, &uniqueContracts)
	if err != nil {
		return nil, fmt.Errorf("soroban stats: %w", err)
	}
	return map[string]interface{}{
		"ledger_sequence":  seq,
		"soroban_tx_count": sorobanTxCount,
		"unique_contracts": uniqueContracts,
	}, nil
}

// Ensure respondJSON and respondError are available (they should be in another file)
func init() {
	// Force json import to be used
	_ = json.Marshal
}

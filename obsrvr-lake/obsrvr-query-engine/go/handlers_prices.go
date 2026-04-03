package main

import (
	"log"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
)

// ── Price Handlers ──────────────────────────────────────────

func OHLCCandlesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		base := vars["base"]
		counter := vars["counter"]
		if base == "" || counter == "" {
			respondError(w, http.StatusBadRequest, "base and counter asset codes required")
			return
		}

		interval := r.URL.Query().Get("interval")
		if interval == "" {
			interval = "1h"
		}
		baseIssuer := r.URL.Query().Get("base_issuer")
		counterIssuer := r.URL.Query().Get("counter_issuer")

		endTime := time.Now().UTC()
		startTime := endTime.Add(-24 * time.Hour)
		if s := r.URL.Query().Get("start"); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				startTime = t
			}
		}
		if s := r.URL.Query().Get("end"); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				endTime = t
			}
		}

		candles, err := reader.GetOHLCCandles(r.Context(), base, baseIssuer, counter, counterIssuer, interval, startTime, endTime)
		if err != nil {
			log.Printf("ERROR: OHLC candles: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve price candles")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"base": base, "counter": counter, "interval": interval,
			"candles": candles, "count": len(candles),
		})
	}
}

func LatestPriceHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		base := vars["base"]
		counter := vars["counter"]
		if base == "" || counter == "" {
			respondError(w, http.StatusBadRequest, "base and counter asset codes required")
			return
		}

		baseIssuer := r.URL.Query().Get("base_issuer")
		counterIssuer := r.URL.Query().Get("counter_issuer")

		price, err := reader.GetLatestPrice(r.Context(), base, baseIssuer, counter, counterIssuer)
		if err != nil {
			if err.Error() == "no trades found for this pair" {
				respondError(w, http.StatusNotFound, err.Error())
				return
			}
			log.Printf("ERROR: latest price: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve latest price")
			return
		}
		respondJSON(w, http.StatusOK, price)
	}
}

func TradePairsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pairs, err := reader.GetTradePairs(r.Context())
		if err != nil {
			log.Printf("ERROR: trade pairs: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve trade pairs")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"pairs": pairs, "count": len(pairs)})
	}
}

// ── Ledger Stats Handlers ──────────────────────────────────

func LedgerFeesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		seqStr := vars["seq"]
		ledgerSeq, err := strconv.ParseInt(seqStr, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid ledger sequence")
			return
		}

		fees, err := reader.GetLedgerFees(r.Context(), ledgerSeq)
		if err != nil {
			log.Printf("ERROR: ledger fees: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve ledger fees")
			return
		}
		if len(fees) == 0 {
			respondError(w, http.StatusNotFound, "no transactions found in ledger")
			return
		}

		var totalFees int64
		for _, f := range fees {
			totalFees += f
		}

		// Compute histogram
		type bucket struct {
			label string
			min   int64
			max   int64
		}
		buckets := []bucket{
			{"100-200", 100, 200},
			{"200-1000", 200, 1000},
			{"1000-10000", 1000, 10000},
			{"10000+", 10000, math.MaxInt64},
		}

		type histEntry struct {
			Range string `json:"range"`
			Count int    `json:"count"`
		}
		var histogram []histEntry
		for _, b := range buckets {
			count := 0
			for _, f := range fees {
				if f >= b.min && f < b.max {
					count++
				}
			}
			if count > 0 {
				histogram = append(histogram, histEntry{Range: b.label, Count: count})
			}
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"ledger_sequence": ledgerSeq,
			"tx_count":        len(fees),
			"min_fee":         fees[0],
			"max_fee":         fees[len(fees)-1],
			"median_fee":      percentile(fees, 0.5),
			"p90_fee":         percentile(fees, 0.9),
			"total_fees":      totalFees,
			"histogram":       histogram,
			"generated_at":    time.Now().UTC().Format(time.RFC3339),
		})
	}
}

func LedgerSorobanHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		seqStr := vars["seq"]
		ledgerSeq, err := strconv.ParseInt(seqStr, 10, 64)
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid ledger sequence")
			return
		}

		result, err := reader.GetLedgerSoroban(r.Context(), ledgerSeq)
		if err != nil {
			log.Printf("ERROR: ledger soroban: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve ledger soroban stats")
			return
		}
		if result == nil {
			respondError(w, http.StatusNotFound, "no soroban transactions in ledger")
			return
		}
		result["generated_at"] = time.Now().UTC().Format(time.RFC3339)
		respondJSON(w, http.StatusOK, result)
	}
}

func SorobanStatsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := reader.GetSorobanStats(r.Context())
		if err != nil {
			log.Printf("ERROR: soroban stats: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve soroban stats")
			return
		}
		respondJSON(w, http.StatusOK, stats)
	}
}

// ── Gold Snapshot Handlers ──────────────────────────────────

func GoldAccountSnapshotHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			respondError(w, http.StatusBadRequest, "account_id required")
			return
		}

		ts := time.Now().UTC()
		if s := r.URL.Query().Get("at"); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t
			}
		}

		account, err := reader.GetGoldAccountSnapshot(r.Context(), accountID, ts)
		if err != nil {
			log.Printf("ERROR: gold account snapshot: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account snapshot")
			return
		}
		if account == nil {
			respondError(w, http.StatusNotFound, "account not found at given timestamp")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"account":   account,
			"timestamp": ts.Format(time.RFC3339),
		})
	}
}

func GoldAssetHoldersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		assetCode := r.URL.Query().Get("asset_code")
		assetIssuer := r.URL.Query().Get("asset_issuer")
		if assetCode == "" {
			respondError(w, http.StatusBadRequest, "asset_code required")
			return
		}

		ts := time.Now().UTC()
		if s := r.URL.Query().Get("at"); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t
			}
		}
		limit := int(parseIntParam(r, "limit", 50))

		holders, err := reader.GetGoldAssetHolders(r.Context(), assetCode, assetIssuer, ts, limit)
		if err != nil {
			log.Printf("ERROR: gold asset holders: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve asset holders")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"asset_code": assetCode, "asset_issuer": assetIssuer,
			"timestamp": ts.Format(time.RFC3339),
			"holders": holders, "count": len(holders),
		})
	}
}

func GoldPortfolioHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			respondError(w, http.StatusBadRequest, "account_id required")
			return
		}

		ts := time.Now().UTC()
		if s := r.URL.Query().Get("at"); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t
			}
		}

		portfolio, err := reader.GetGoldPortfolioSnapshot(r.Context(), accountID, ts)
		if err != nil {
			log.Printf("ERROR: gold portfolio: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve portfolio snapshot")
			return
		}
		respondJSON(w, http.StatusOK, portfolio)
	}
}

func GoldBatchAccountsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Accept account_ids as comma-separated query param
		accountIDs := r.URL.Query().Get("account_ids")
		if accountIDs == "" {
			respondError(w, http.StatusBadRequest, "account_ids required (comma-separated)")
			return
		}

		ts := time.Now().UTC()
		if s := r.URL.Query().Get("at"); s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				ts = t
			}
		}

		ids := splitAndTrim(accountIDs)
		if len(ids) > 100 {
			respondError(w, http.StatusBadRequest, "maximum 100 accounts per batch")
			return
		}

		var results []map[string]interface{}
		for _, id := range ids {
			account, err := reader.GetGoldAccountSnapshot(r.Context(), id, ts)
			if err != nil {
				continue
			}
			if account != nil {
				results = append(results, account)
			}
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"accounts":  results,
			"count":     len(results),
			"timestamp": ts.Format(time.RFC3339),
		})
	}
}

// splitAndTrim splits a comma-separated string and trims whitespace.
func splitAndTrim(s string) []string {
	var result []string
	for _, part := range splitComma(s) {
		trimmed := trimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func splitComma(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func trimSpace(s string) string {
	i, j := 0, len(s)
	for i < j && s[i] == ' ' {
		i++
	}
	for j > i && s[j-1] == ' ' {
		j--
	}
	return s[i:j]
}

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"
)

type QueryService struct {
	hot    *HotReader
	cold   *ColdReader
	config QueryConfig
}

func NewQueryService(hot *HotReader, cold *ColdReader, config QueryConfig) *QueryService {
	return &QueryService{
		hot:    hot,
		cold:   cold,
		config: config,
	}
}

// Determine if a ledger range should be queried from hot, cold, or both
func (qs *QueryService) determineSource(start, end int64) (queryHot, queryCold bool, hotStart, hotEnd, coldStart, coldEnd int64) {
	hotLow, err := qs.hot.GetLowWatermark()
	if err != nil {
		log.Printf("Warning: failed to get hot low watermark: %v", err)
		hotLow = 0
	}

	hotHigh, err := qs.hot.GetHighWatermark()
	if err != nil {
		log.Printf("Warning: failed to get hot high watermark: %v", err)
		hotHigh = 0
	}

	// If no data in hot buffer, query cold only
	if hotLow == 0 && hotHigh == 0 {
		return false, true, 0, 0, start, end
	}

	// If requested range is entirely before hot buffer, query cold only
	if end < hotLow {
		return false, true, 0, 0, start, end
	}

	// If requested range is entirely within hot buffer, query hot only
	if start >= hotLow && end <= hotHigh {
		return true, false, start, end, 0, 0
	}

	// If requested range is entirely after hot buffer, query cold only
	// (This shouldn't happen in normal operation, but handle it)
	if start > hotHigh {
		return false, true, 0, 0, start, end
	}

	// Range spans hot and cold
	if start < hotLow && end >= hotLow {
		// Query cold for [start, hotLow-1] and hot for [hotLow, end]
		return true, true, hotLow, end, start, hotLow - 1
	}

	// Default to both (shouldn't reach here, but safe fallback)
	return true, true, start, end, start, end
}

func (qs *QueryService) HandleLedgers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")
	sortParam := r.URL.Query().Get("sort")

	if startStr == "" || endStr == "" {
		http.Error(w, "Missing required parameters: start, end", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid start parameter", http.StatusBadRequest)
		return
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid end parameter", http.StatusBadRequest)
		return
	}

	limit := qs.config.DefaultLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 && parsedLimit <= qs.config.MaxLimit {
			limit = parsedLimit
		}
	}

	// Validate and normalize sort parameter
	validSorts := map[string]bool{
		"sequence_asc":    true,
		"sequence_desc":   true,
		"closed_at_asc":   true,
		"closed_at_desc":  true,
		"tx_count_desc":   true,
	}
	if sortParam == "" {
		sortParam = "sequence_asc" // default
	}
	if !validSorts[sortParam] {
		http.Error(w, "Invalid sort parameter: must be sequence_asc, sequence_desc, closed_at_asc, closed_at_desc, or tx_count_desc", http.StatusBadRequest)
		return
	}

	// Determine which sources to query
	queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := qs.determineSource(start, end)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]interface{}

	// Determine query order based on sort direction
	// For descending sorts, query hot (recent) first to get most recent data
	// For ascending sorts, query cold (historical) first
	isDescending := sortParam == "sequence_desc" || sortParam == "closed_at_desc" || sortParam == "tx_count_desc"

	if isDescending {
		// Query hot storage first (recent data) for descending sorts
		if queryHot {
			hotRows, err := qs.hot.QueryLedgers(ctx, hotStart, hotEnd, limit, sortParam)
			if err != nil {
				log.Printf("Error querying hot storage: %v", err)
			} else {
				hotResults, err := scanLedgers(hotRows)
				hotRows.Close()
				if err != nil {
					log.Printf("Error scanning hot results: %v", err)
				} else {
					results = append(results, hotResults...)
				}
			}
		}

		// Query cold storage if we need more results
		if queryCold && len(results) < limit {
			coldRows, err := qs.cold.QueryLedgers(ctx, coldStart, coldEnd, limit-len(results), sortParam)
			if err != nil {
				log.Printf("Error querying cold storage: %v", err)
			} else {
				coldResults, err := scanLedgers(coldRows)
				coldRows.Close()
				if err != nil {
					log.Printf("Error scanning cold results: %v", err)
				} else {
					results = append(results, coldResults...)
				}
			}
		}
	} else {
		// Query cold storage first (historical data) for ascending sorts
		if queryCold {
			coldRows, err := qs.cold.QueryLedgers(ctx, coldStart, coldEnd, limit, sortParam)
			if err != nil {
				log.Printf("Error querying cold storage: %v", err)
			} else {
				coldResults, err := scanLedgers(coldRows)
				coldRows.Close()
				if err != nil {
					log.Printf("Error scanning cold results: %v", err)
				} else {
					results = append(results, coldResults...)
				}
			}
		}

		// Query hot storage if we need more results
		if queryHot && len(results) < limit {
			hotRows, err := qs.hot.QueryLedgers(ctx, hotStart, hotEnd, limit-len(results), sortParam)
			if err != nil {
				log.Printf("Error querying hot storage: %v", err)
			} else {
				hotResults, err := scanLedgers(hotRows)
				hotRows.Close()
				if err != nil {
					log.Printf("Error scanning hot results: %v", err)
				} else {
					results = append(results, hotResults...)
				}
			}
		}
	}

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ledgers": results,
		"count":   len(results),
		"start":   start,
		"end":     end,
		"sort":    sortParam,
	})
}

func (qs *QueryService) HandleTransactions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")

	if startStr == "" || endStr == "" {
		http.Error(w, "Missing required parameters: start, end", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid start parameter", http.StatusBadRequest)
		return
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid end parameter", http.StatusBadRequest)
		return
	}

	limit := qs.config.DefaultLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 && parsedLimit <= qs.config.MaxLimit {
			limit = parsedLimit
		}
	}

	queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := qs.determineSource(start, end)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]interface{}

	if queryCold {
		coldRows, err := qs.cold.QueryTransactions(ctx, coldStart, coldEnd, limit)
		if err != nil {
			log.Printf("Error querying cold storage: %v", err)
		} else {
			coldResults, err := scanTransactions(coldRows)
			coldRows.Close()
			if err != nil {
				log.Printf("Error scanning cold results: %v", err)
			} else {
				results = append(results, coldResults...)
			}
		}
	}

	if queryHot {
		hotRows, err := qs.hot.QueryTransactions(ctx, hotStart, hotEnd, limit-len(results))
		if err != nil {
			log.Printf("Error querying hot storage: %v", err)
		} else {
			hotResults, err := scanTransactions(hotRows)
			hotRows.Close()
			if err != nil {
				log.Printf("Error scanning hot results: %v", err)
			} else {
				results = append(results, hotResults...)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"transactions": results,
		"count":        len(results),
		"start":        start,
		"end":          end,
	})
}

func (qs *QueryService) HandleOperations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")

	if startStr == "" || endStr == "" {
		http.Error(w, "Missing required parameters: start, end", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid start parameter", http.StatusBadRequest)
		return
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid end parameter", http.StatusBadRequest)
		return
	}

	limit := qs.config.DefaultLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 && parsedLimit <= qs.config.MaxLimit {
			limit = parsedLimit
		}
	}

	queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := qs.determineSource(start, end)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]interface{}

	if queryCold {
		coldRows, err := qs.cold.QueryOperations(ctx, coldStart, coldEnd, limit)
		if err != nil {
			log.Printf("Error querying cold storage: %v", err)
		} else {
			coldResults, err := scanOperations(coldRows)
			coldRows.Close()
			if err != nil {
				log.Printf("Error scanning cold results: %v", err)
			} else {
				results = append(results, coldResults...)
			}
		}
	}

	if queryHot {
		hotRows, err := qs.hot.QueryOperations(ctx, hotStart, hotEnd, limit-len(results))
		if err != nil {
			log.Printf("Error querying hot storage: %v", err)
		} else {
			hotResults, err := scanOperations(hotRows)
			hotRows.Close()
			if err != nil {
				log.Printf("Error scanning hot results: %v", err)
			} else {
				results = append(results, hotResults...)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"operations": results,
		"count":      len(results),
		"start":      start,
		"end":        end,
	})
}

func (qs *QueryService) HandleEffects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")

	if startStr == "" || endStr == "" {
		http.Error(w, "Missing required parameters: start, end", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid start parameter", http.StatusBadRequest)
		return
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid end parameter", http.StatusBadRequest)
		return
	}

	limit := qs.config.DefaultLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 && parsedLimit <= qs.config.MaxLimit {
			limit = parsedLimit
		}
	}

	queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := qs.determineSource(start, end)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]interface{}

	if queryCold {
		coldRows, err := qs.cold.QueryEffects(ctx, coldStart, coldEnd, limit)
		if err != nil {
			log.Printf("Error querying cold storage: %v", err)
		} else {
			coldResults, err := scanEffects(coldRows)
			coldRows.Close()
			if err != nil {
				log.Printf("Error scanning cold results: %v", err)
			} else {
				results = append(results, coldResults...)
			}
		}
	}

	if queryHot {
		hotRows, err := qs.hot.QueryEffects(ctx, hotStart, hotEnd, limit-len(results))
		if err != nil {
			log.Printf("Error querying hot storage: %v", err)
		} else {
			hotResults, err := scanEffects(hotRows)
			hotRows.Close()
			if err != nil {
				log.Printf("Error scanning hot results: %v", err)
			} else {
				results = append(results, hotResults...)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"effects": results,
		"count":   len(results),
		"start":   start,
		"end":     end,
	})
}

func (qs *QueryService) HandleTrades(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")

	if startStr == "" || endStr == "" {
		http.Error(w, "Missing required parameters: start, end", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid start parameter", http.StatusBadRequest)
		return
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid end parameter", http.StatusBadRequest)
		return
	}

	limit := qs.config.DefaultLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 && parsedLimit <= qs.config.MaxLimit {
			limit = parsedLimit
		}
	}

	queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := qs.determineSource(start, end)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]interface{}

	if queryCold {
		coldRows, err := qs.cold.QueryTrades(ctx, coldStart, coldEnd, limit)
		if err != nil {
			log.Printf("Error querying cold storage: %v", err)
		} else {
			coldResults, err := scanTrades(coldRows)
			coldRows.Close()
			if err != nil {
				log.Printf("Error scanning cold results: %v", err)
			} else {
				results = append(results, coldResults...)
			}
		}
	}

	if queryHot {
		hotRows, err := qs.hot.QueryTrades(ctx, hotStart, hotEnd, limit-len(results))
		if err != nil {
			log.Printf("Error querying hot storage: %v", err)
		} else {
			hotResults, err := scanTrades(hotRows)
			hotRows.Close()
			if err != nil {
				log.Printf("Error scanning hot results: %v", err)
			} else {
				results = append(results, hotResults...)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"trades": results,
		"count":  len(results),
		"start":  start,
		"end":    end,
	})
}

func (qs *QueryService) HandleAccounts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	accountID := r.URL.Query().Get("account_id")
	limitStr := r.URL.Query().Get("limit")

	if accountID == "" {
		http.Error(w, "Missing required parameter: account_id", http.StatusBadRequest)
		return
	}

	limit := qs.config.DefaultLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 && parsedLimit <= qs.config.MaxLimit {
			limit = parsedLimit
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]interface{}

	// Query hot first (most recent state)
	hotRows, err := qs.hot.QueryAccounts(ctx, accountID, limit)
	if err != nil {
		log.Printf("Error querying hot storage: %v", err)
	} else {
		hotResults, err := scanAccounts(hotRows)
		hotRows.Close()
		if err != nil {
			log.Printf("Error scanning hot results: %v", err)
		} else {
			results = append(results, hotResults...)
		}
	}

	// Query cold if needed
	if len(results) < limit {
		coldRows, err := qs.cold.QueryAccounts(ctx, accountID, limit-len(results))
		if err != nil {
			log.Printf("Error querying cold storage: %v", err)
		} else {
			coldResults, err := scanAccounts(coldRows)
			coldRows.Close()
			if err != nil {
				log.Printf("Error scanning cold results: %v", err)
			} else {
				results = append(results, coldResults...)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"accounts":   results,
		"count":      len(results),
		"account_id": accountID,
	})
}

func (qs *QueryService) HandleTrustlines(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	accountID := r.URL.Query().Get("account_id")
	limitStr := r.URL.Query().Get("limit")

	if accountID == "" {
		http.Error(w, "Missing required parameter: account_id", http.StatusBadRequest)
		return
	}

	limit := qs.config.DefaultLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 && parsedLimit <= qs.config.MaxLimit {
			limit = parsedLimit
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]interface{}

	hotRows, err := qs.hot.QueryTrustlines(ctx, accountID, limit)
	if err != nil {
		log.Printf("Error querying hot storage: %v", err)
	} else {
		hotResults, err := scanTrustlines(hotRows)
		hotRows.Close()
		if err != nil {
			log.Printf("Error scanning hot results: %v", err)
		} else {
			results = append(results, hotResults...)
		}
	}

	if len(results) < limit {
		coldRows, err := qs.cold.QueryTrustlines(ctx, accountID, limit-len(results))
		if err != nil {
			log.Printf("Error querying cold storage: %v", err)
		} else {
			coldResults, err := scanTrustlines(coldRows)
			coldRows.Close()
			if err != nil {
				log.Printf("Error scanning cold results: %v", err)
			} else {
				results = append(results, coldResults...)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"trustlines": results,
		"count":      len(results),
		"account_id": accountID,
	})
}

func (qs *QueryService) HandleOffers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sellerID := r.URL.Query().Get("seller_id")
	limitStr := r.URL.Query().Get("limit")

	if sellerID == "" {
		http.Error(w, "Missing required parameter: seller_id", http.StatusBadRequest)
		return
	}

	limit := qs.config.DefaultLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 && parsedLimit <= qs.config.MaxLimit {
			limit = parsedLimit
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]interface{}

	hotRows, err := qs.hot.QueryOffers(ctx, sellerID, limit)
	if err != nil {
		log.Printf("Error querying hot storage: %v", err)
	} else {
		hotResults, err := scanOffers(hotRows)
		hotRows.Close()
		if err != nil {
			log.Printf("Error scanning hot results: %v", err)
		} else {
			results = append(results, hotResults...)
		}
	}

	if len(results) < limit {
		coldRows, err := qs.cold.QueryOffers(ctx, sellerID, limit-len(results))
		if err != nil {
			log.Printf("Error querying cold storage: %v", err)
		} else {
			coldResults, err := scanOffers(coldRows)
			coldRows.Close()
			if err != nil {
				log.Printf("Error scanning cold results: %v", err)
			} else {
				results = append(results, coldResults...)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"offers":    results,
		"count":     len(results),
		"seller_id": sellerID,
	})
}

func (qs *QueryService) HandleContractEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")

	if startStr == "" || endStr == "" {
		http.Error(w, "Missing required parameters: start, end", http.StatusBadRequest)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid start parameter", http.StatusBadRequest)
		return
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid end parameter", http.StatusBadRequest)
		return
	}

	limit := qs.config.DefaultLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 && parsedLimit <= qs.config.MaxLimit {
			limit = parsedLimit
		}
	}

	queryHot, queryCold, hotStart, hotEnd, coldStart, coldEnd := qs.determineSource(start, end)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	var results []map[string]interface{}

	if queryCold {
		coldRows, err := qs.cold.QueryContractEvents(ctx, coldStart, coldEnd, limit)
		if err != nil {
			log.Printf("Error querying cold storage: %v", err)
		} else {
			coldResults, err := scanContractEvents(coldRows)
			coldRows.Close()
			if err != nil {
				log.Printf("Error scanning cold results: %v", err)
			} else {
				results = append(results, coldResults...)
			}
		}
	}

	if queryHot {
		hotRows, err := qs.hot.QueryContractEvents(ctx, hotStart, hotEnd, limit-len(results))
		if err != nil {
			log.Printf("Error querying hot storage: %v", err)
		} else {
			hotResults, err := scanContractEvents(hotRows)
			hotRows.Close()
			if err != nil {
				log.Printf("Error scanning hot results: %v", err)
			} else {
				results = append(results, hotResults...)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"contract_events": results,
		"count":           len(results),
		"start":           start,
		"end":             end,
	})
}

// Scanning functions to convert sql.Rows to map[string]interface{}

func scanLedgers(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for rows.Next() {
		var (
			sequence, txCount, opCount, successTxCount, failedTxCount, txSetOpCount int64
			totalCoins, feePool, baseFee, baseReserve, maxTxSetSize, protocolVersion int64
			ledgerRange                                                               int64
			sorobanFeeWrite1kb                                                        sql.NullInt64
			ledgerHash, prevLedgerHash                                                string
			ledgerHeader, nodeID, signature, eraID, versionLabel                      sql.NullString
			closedAt, createdAt                                                       time.Time
		)

		err := rows.Scan(
			&sequence, &ledgerHash, &prevLedgerHash, &txCount, &opCount, &successTxCount, &failedTxCount,
			&txSetOpCount, &closedAt, &totalCoins, &feePool, &baseFee, &baseReserve, &maxTxSetSize,
			&protocolVersion, &ledgerHeader, &sorobanFeeWrite1kb, &nodeID, &signature, &ledgerRange,
			&eraID, &versionLabel, &createdAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan ledger row: %w", err)
		}

		result := map[string]interface{}{
			"sequence":               sequence,
			"ledger_hash":            ledgerHash,
			"previous_ledger_hash":   prevLedgerHash,
			"transaction_count":      txCount,
			"operation_count":        opCount,
			"successful_tx_count":    successTxCount,
			"failed_tx_count":        failedTxCount,
			"tx_set_operation_count": txSetOpCount,
			"closed_at":              closedAt,
			"total_coins":            totalCoins,
			"fee_pool":               feePool,
			"base_fee":               baseFee,
			"base_reserve":           baseReserve,
			"max_tx_set_size":        maxTxSetSize,
			"protocol_version":       protocolVersion,
			"ledger_range":           ledgerRange,
			"created_at":             createdAt,
		}

		if sorobanFeeWrite1kb.Valid {
			result["soroban_fee_write_1kb"] = sorobanFeeWrite1kb.Int64
		}
		if ledgerHeader.Valid {
			result["ledger_header"] = ledgerHeader.String
		}
		if nodeID.Valid {
			result["node_id"] = nodeID.String
		}
		if signature.Valid {
			result["signature"] = signature.String
		}
		if eraID.Valid {
			result["era_id"] = eraID.String
		}
		if versionLabel.Valid {
			result["version_label"] = versionLabel.String
		}

		results = append(results, result)
	}

	return results, rows.Err()
}

func scanTransactions(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for rows.Next() {
		var (
			ledgerSeq, accountSeq, maxFee, opCount, ledgerRange int64
			txHash, sourceAccount                               string
			sourceMuxed, eraID, versionLabel                    sql.NullString
			successful                                          bool
			createdAt                                           time.Time
		)

		err := rows.Scan(
			&ledgerSeq, &txHash, &sourceAccount, &sourceMuxed, &accountSeq,
			&maxFee, &opCount, &createdAt, &ledgerRange, &eraID, &versionLabel, &successful,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction row: %w", err)
		}

		txResult := map[string]interface{}{
			"ledger_sequence":  ledgerSeq,
			"transaction_hash": txHash,
			"source_account":   sourceAccount,
			"account_sequence": accountSeq,
			"max_fee":          maxFee,
			"operation_count":  opCount,
			"successful":       successful,
			"ledger_range":     ledgerRange,
			"created_at":       createdAt,
		}

		if sourceMuxed.Valid {
			txResult["source_account_muxed"] = sourceMuxed.String
		}
		if eraID.Valid {
			txResult["era_id"] = eraID.String
		}
		if versionLabel.Valid {
			txResult["version_label"] = versionLabel.String
		}

		results = append(results, txResult)
	}

	return results, rows.Err()
}

func scanOperations(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for rows.Next() {
		var (
			ledgerSeq, opIndex, opType, ledgerRange          int64
			txHash, sourceAccount                             string
			sourceMuxed, eraID, versionLabel                  sql.NullString
			createdAt                                         time.Time
		)

		err := rows.Scan(
			&ledgerSeq, &txHash, &opIndex, &opType, &sourceAccount, &sourceMuxed,
			&createdAt, &ledgerRange, &eraID, &versionLabel,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan operation row: %w", err)
		}

		opResultMap := map[string]interface{}{
			"ledger_sequence":  ledgerSeq,
			"transaction_hash": txHash,
			"operation_index":  opIndex,
			"type":             opType,
			"source_account":   sourceAccount,
			"ledger_range":     ledgerRange,
			"created_at":       createdAt,
		}

		if sourceMuxed.Valid {
			opResultMap["source_account_muxed"] = sourceMuxed.String
		}
		if eraID.Valid {
			opResultMap["era_id"] = eraID.String
		}
		if versionLabel.Valid {
			opResultMap["version_label"] = versionLabel.String
		}

		results = append(results, opResultMap)
	}

	return results, rows.Err()
}

func scanEffects(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for rows.Next() {
		var (
			ledgerSeq, opIndex, effectIndex, effectType, signerWeight                                                                                  int64
			txHash, effectTypeStr                                                                                                                      string
			accountID, amount, assetCode, assetIssuer, assetType, trustlineLimit, signerAccount, sellerAccount, eraID, versionLabel                    sql.NullString
			authorizeFlag, clawbackFlag                                                                                                                sql.NullBool
			offerID                                                                                                                                    sql.NullInt64
			createdAt                                                                                                                                  time.Time
			ledgerRange                                                                                                                                int64
		)

		err := rows.Scan(
			&ledgerSeq, &txHash, &opIndex, &effectIndex, &effectType, &effectTypeStr,
			&accountID, &amount, &assetCode, &assetIssuer, &assetType, &trustlineLimit,
			&authorizeFlag, &clawbackFlag, &signerAccount, &signerWeight, &offerID, &sellerAccount,
			&createdAt, &ledgerRange, &eraID, &versionLabel,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan effect row: %w", err)
		}

		effectResult := map[string]interface{}{
			"ledger_sequence":    ledgerSeq,
			"transaction_hash":   txHash,
			"operation_index":    opIndex,
			"effect_index":       effectIndex,
			"effect_type":        effectType,
			"effect_type_string": effectTypeStr,
			"ledger_range":       ledgerRange,
		}

		if accountID.Valid {
			effectResult["account_id"] = accountID.String
		}
		if amount.Valid {
			effectResult["amount"] = amount.String
		}
		if assetCode.Valid {
			effectResult["asset_code"] = assetCode.String
		}
		if assetIssuer.Valid {
			effectResult["asset_issuer"] = assetIssuer.String
		}
		if assetType.Valid {
			effectResult["asset_type"] = assetType.String
		}

		results = append(results, effectResult)
	}

	return results, rows.Err()
}

func scanTrades(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for rows.Next() {
		var (
			ledgerSeq, opIndex, tradeIndex, ledgerRange int64
			txHash, sellerAccount, buyerAccount          string
			sellingAssetCode, sellingAssetIssuer         sql.NullString
			buyingAssetCode, buyingAssetIssuer           sql.NullString
			eraID, versionLabel                          sql.NullString
			sellingAmount, buyingAmount, price           sql.NullString
		)

		err := rows.Scan(
			&ledgerSeq, &txHash, &opIndex, &tradeIndex, &sellerAccount,
			&sellingAssetCode, &sellingAssetIssuer, &sellingAmount,
			&buyerAccount, &buyingAssetCode, &buyingAssetIssuer, &buyingAmount,
			&price, &ledgerRange, &eraID, &versionLabel,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan trade row: %w", err)
		}

		tradeResult := map[string]interface{}{
			"ledger_sequence":  ledgerSeq,
			"transaction_hash": txHash,
			"operation_index":  opIndex,
			"trade_index":      tradeIndex,
			"seller_account":   sellerAccount,
			"buyer_account":    buyerAccount,
			"ledger_range":     ledgerRange,
		}

		if sellingAssetCode.Valid {
			tradeResult["selling_asset_code"] = sellingAssetCode.String
		}
		if sellingAssetIssuer.Valid {
			tradeResult["selling_asset_issuer"] = sellingAssetIssuer.String
		}
		if sellingAmount.Valid {
			tradeResult["selling_amount"] = sellingAmount.String
		}
		if buyingAssetCode.Valid {
			tradeResult["buying_asset_code"] = buyingAssetCode.String
		}
		if buyingAssetIssuer.Valid {
			tradeResult["buying_asset_issuer"] = buyingAssetIssuer.String
		}
		if buyingAmount.Valid {
			tradeResult["buying_amount"] = buyingAmount.String
		}
		if price.Valid {
			tradeResult["price"] = price.String
		}
		if eraID.Valid {
			tradeResult["era_id"] = eraID.String
		}
		if versionLabel.Valid {
			tradeResult["version_label"] = versionLabel.String
		}

		results = append(results, tradeResult)
	}

	return results, rows.Err()
}

func scanAccounts(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for rows.Next() {
		var (
			accountID, balance                                           string
			seqNum, numSubentries, ledgerSeq, ledgerRange                int64
			masterWeight, thresholdLow, thresholdMed, thresholdHigh      int64
			numSponsoring, numSponsored, flags                           int64
			homeDomain, sponsor, signers, eraID, versionLabel            sql.NullString
			createdAt                                                    time.Time
		)

		err := rows.Scan(
			&accountID, &balance, &seqNum, &numSubentries, &flags, &homeDomain,
			&masterWeight, &thresholdLow, &thresholdMed, &thresholdHigh, &ledgerSeq,
			&numSponsoring, &numSponsored, &sponsor, &signers, &ledgerRange,
			&eraID, &versionLabel, &createdAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan account row: %w", err)
		}

		accountResult := map[string]interface{}{
			"account_id":       accountID,
			"balance":          balance,
			"sequence_number":  seqNum,
			"num_subentries":   numSubentries,
			"flags":            flags,
			"master_weight":    masterWeight,
			"low_threshold":    thresholdLow,
			"med_threshold":    thresholdMed,
			"high_threshold":   thresholdHigh,
			"ledger_sequence":  ledgerSeq,
			"num_sponsoring":   numSponsoring,
			"num_sponsored":    numSponsored,
			"ledger_range":     ledgerRange,
			"created_at":       createdAt,
		}

		if homeDomain.Valid {
			accountResult["home_domain"] = homeDomain.String
		}
		if sponsor.Valid {
			accountResult["sponsor_account"] = sponsor.String
		}
		if signers.Valid {
			accountResult["signers"] = signers.String
		}
		if eraID.Valid {
			accountResult["era_id"] = eraID.String
		}
		if versionLabel.Valid {
			accountResult["version_label"] = versionLabel.String
		}

		results = append(results, accountResult)
	}

	return results, rows.Err()
}

func scanTrustlines(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for rows.Next() {
		var (
			accountID, assetCode, assetIssuer, assetType, balance, trustLimit string
			buyingLiab, sellingLiab, ledgerSeq, ledgerRange                    int64
			eraID, versionLabel                                                sql.NullString
			createdAt                                                          time.Time
		)

		err := rows.Scan(
			&accountID, &assetCode, &assetIssuer, &assetType, &balance, &trustLimit,
			&buyingLiab, &sellingLiab, &ledgerSeq, &ledgerRange, &eraID, &versionLabel, &createdAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan trustline row: %w", err)
		}

		trustlineResult := map[string]interface{}{
			"account_id":          accountID,
			"asset_code":          assetCode,
			"asset_issuer":        assetIssuer,
			"asset_type":          assetType,
			"balance":             balance,
			"trust_limit":         trustLimit,
			"buying_liabilities":  buyingLiab,
			"selling_liabilities": sellingLiab,
			"ledger_sequence":     ledgerSeq,
			"ledger_range":        ledgerRange,
			"created_at":          createdAt,
		}

		if eraID.Valid {
			trustlineResult["era_id"] = eraID.String
		}
		if versionLabel.Valid {
			trustlineResult["version_label"] = versionLabel.String
		}

		results = append(results, trustlineResult)
	}

	return results, rows.Err()
}

func scanOffers(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for rows.Next() {
		var (
			sellerAccount, amount, price                             string
			offerID, flags, ledgerSeq, ledgerRange                   int64
			sellingAssetCode, sellingAssetIssuer, sellingAssetType   sql.NullString
			buyingAssetCode, buyingAssetIssuer, buyingAssetType      sql.NullString
			eraID, versionLabel                                      sql.NullString
			createdAt                                                time.Time
		)

		err := rows.Scan(
			&sellerAccount, &offerID, &sellingAssetCode, &sellingAssetIssuer, &sellingAssetType,
			&buyingAssetCode, &buyingAssetIssuer, &buyingAssetType, &amount, &price,
			&flags, &ledgerSeq, &ledgerRange, &eraID, &versionLabel, &createdAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan offer row: %w", err)
		}

		offerResult := map[string]interface{}{
			"seller_account":  sellerAccount,
			"offer_id":        offerID,
			"amount":          amount,
			"price":           price,
			"flags":           flags,
			"ledger_sequence": ledgerSeq,
			"ledger_range":    ledgerRange,
			"created_at":      createdAt,
		}

		if sellingAssetCode.Valid {
			offerResult["selling_asset_code"] = sellingAssetCode.String
		}
		if sellingAssetIssuer.Valid {
			offerResult["selling_asset_issuer"] = sellingAssetIssuer.String
		}
		if sellingAssetType.Valid {
			offerResult["selling_asset_type"] = sellingAssetType.String
		}
		if buyingAssetCode.Valid {
			offerResult["buying_asset_code"] = buyingAssetCode.String
		}
		if buyingAssetIssuer.Valid {
			offerResult["buying_asset_issuer"] = buyingAssetIssuer.String
		}
		if buyingAssetType.Valid {
			offerResult["buying_asset_type"] = buyingAssetType.String
		}
		if eraID.Valid {
			offerResult["era_id"] = eraID.String
		}
		if versionLabel.Valid {
			offerResult["version_label"] = versionLabel.String
		}

		results = append(results, offerResult)
	}

	return results, rows.Err()
}

func scanContractEvents(rows *sql.Rows) ([]map[string]interface{}, error) {
	var results []map[string]interface{}

	for rows.Next() {
		var (
			eventID, contractID, txHash                 string
			ledgerSeq, opIndex, eventIndex, eventType   int64
			topicsJSON, dataJSON                        string
			closedAt                                    time.Time
			ledgerRange                                 int64
			eraID, versionLabel                         sql.NullString
		)

		err := rows.Scan(
			&eventID, &contractID, &ledgerSeq, &txHash, &opIndex, &eventIndex,
			&eventType, &topicsJSON, &dataJSON, &closedAt, &ledgerRange, &eraID, &versionLabel,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan contract event row: %w", err)
		}

		eventResult := map[string]interface{}{
			"event_id":         eventID,
			"contract_id":      contractID,
			"ledger_sequence":  ledgerSeq,
			"transaction_hash": txHash,
			"operation_index":  opIndex,
			"event_index":      eventIndex,
			"event_type":       eventType,
			"topics_json":      topicsJSON,
			"data_json":        dataJSON,
			"closed_at":        closedAt,
			"ledger_range":     ledgerRange,
		}

		if eraID.Valid {
			eventResult["era_id"] = eraID.String
		}
		if versionLabel.Valid {
			eventResult["version_label"] = versionLabel.String
		}

		results = append(results, eventResult)
	}

	return results, rows.Err()
}

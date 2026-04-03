package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
)

// respondJSON writes a JSON response with the given status code.
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("ERROR: failed to encode JSON response: %v", err)
	}
}

// respondError writes a JSON error response.
func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}

// parseIntParam extracts an integer query parameter with a default value.
func parseIntParam(r *http.Request, key string, defaultVal int64) int64 {
	s := r.URL.Query().Get(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultVal
	}
	return v
}

// HealthHandler returns service health status.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, HealthResponse{Status: "healthy"})
}

// DataBoundariesHandler returns the available ledger range.
func DataBoundariesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		boundaries, err := reader.GetDataBoundaries(r.Context())
		if err != nil {
			log.Printf("ERROR: data boundaries: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve data boundaries")
			return
		}
		respondJSON(w, http.StatusOK, boundaries)
	}
}

// NetworkStatsHandler returns aggregate network statistics.
func NetworkStatsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := reader.GetNetworkStats(r.Context())
		if err != nil {
			log.Printf("ERROR: network stats: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve network stats")
			return
		}
		respondJSON(w, http.StatusOK, stats)
	}
}

// SilverNetworkStatsHandler returns the full silver network stats with accounts, fees, soroban sections.
func SilverNetworkStatsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := reader.GetSilverNetworkStats(r.Context())
		if err != nil {
			log.Printf("ERROR: silver network stats: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve silver network stats")
			return
		}
		respondJSON(w, http.StatusOK, stats)
	}
}

// TopAccountsHandler returns the top accounts by balance.
func TopAccountsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 20))
		accounts, err := reader.GetTopAccounts(r.Context(), limit)
		if err != nil {
			log.Printf("ERROR: top accounts: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve top accounts")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"accounts": accounts, "count": len(accounts)})
	}
}

// EnrichedOperationsHandler returns enriched operations with optional filters.
func EnrichedOperationsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filters := OperationFilters{
			StartLedger: parseIntParam(r, "start_ledger", 0),
			EndLedger:   parseIntParam(r, "end_ledger", 0),
			OpType:      r.URL.Query().Get("op_type"),
			Account:     r.URL.Query().Get("account"),
			Limit:       int(parseIntParam(r, "limit", 100)),
		}

		ops, err := reader.GetEnrichedOperations(r.Context(), filters)
		if err != nil {
			log.Printf("ERROR: enriched operations: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve enriched operations")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"operations": ops, "count": len(ops)})
	}
}

// LedgersHandler returns raw ledger data from bronze.
func LedgersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, end, limit, errMsg := parseLedgerRangeParams(r)
		if errMsg != "" {
			respondError(w, http.StatusBadRequest, errMsg)
			return
		}
		if end == 0 {
			end = start + int64(limit)
		}

		ledgers, err := reader.GetLedgers(r.Context(), start, end, limit)
		if err != nil {
			log.Printf("ERROR: ledgers: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve ledgers")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"ledgers": ledgers, "count": len(ledgers)})
	}
}

// TransactionsHandler returns raw transaction data from bronze.
func TransactionsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, end, limit, errMsg := parseLedgerRangeParams(r)
		if errMsg != "" {
			respondError(w, http.StatusBadRequest, errMsg)
			return
		}
		if end == 0 {
			end = start + int64(limit)
		}

		txns, err := reader.GetTransactions(r.Context(), start, end, limit)
		if err != nil {
			log.Printf("ERROR: transactions: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve transactions")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"transactions": txns, "count": len(txns)})
	}
}

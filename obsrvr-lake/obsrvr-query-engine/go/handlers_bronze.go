package main

import (
	"log"
	"net/http"
)

// parseLedgerRangeParams extracts start/end/limit from query params for bronze range endpoints.
func parseLedgerRangeParams(r *http.Request) (start, end int64, limit int, err string) {
	start = parseIntParam(r, "start", 0)
	end = parseIntParam(r, "end", 0)
	// Also accept start_ledger/end_ledger for backwards compat
	if start == 0 {
		start = parseIntParam(r, "start_ledger", 0)
	}
	if end == 0 {
		end = parseIntParam(r, "end_ledger", 0)
	}
	limit = int(parseIntParam(r, "limit", 100))

	if start == 0 && end == 0 {
		return 0, 0, 0, "start and end (or start_ledger and end_ledger) are required"
	}
	if end == 0 {
		end = start + int64(limit)
	}
	return start, end, limit, ""
}

// OperationsHandler returns raw operation data from bronze.
func OperationsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, end, limit, errMsg := parseLedgerRangeParams(r)
		if errMsg != "" {
			respondError(w, http.StatusBadRequest, errMsg)
			return
		}

		ops, err := reader.GetOperations(r.Context(), start, end, limit)
		if err != nil {
			log.Printf("ERROR: operations: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve operations")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"operations": ops, "count": len(ops), "start": start, "end": end})
	}
}

// EffectsHandler returns raw effect data from bronze.
func EffectsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, end, limit, errMsg := parseLedgerRangeParams(r)
		if errMsg != "" {
			respondError(w, http.StatusBadRequest, errMsg)
			return
		}

		effects, err := reader.GetEffects(r.Context(), start, end, limit)
		if err != nil {
			log.Printf("ERROR: effects: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve effects")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"effects": effects, "count": len(effects), "start": start, "end": end})
	}
}

// BronzeTradesHandler returns raw trade data from bronze.
func BronzeTradesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, end, limit, errMsg := parseLedgerRangeParams(r)
		if errMsg != "" {
			respondError(w, http.StatusBadRequest, errMsg)
			return
		}

		trades, err := reader.GetTrades(r.Context(), start, end, limit)
		if err != nil {
			log.Printf("ERROR: trades: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve trades")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"trades": trades, "count": len(trades), "start": start, "end": end})
	}
}

// BronzeAccountsHandler returns account snapshots from bronze.
func BronzeAccountsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			respondError(w, http.StatusBadRequest, "account_id is required")
			return
		}
		limit := int(parseIntParam(r, "limit", 100))

		accounts, err := reader.GetBronzeAccounts(r.Context(), accountID, limit)
		if err != nil {
			log.Printf("ERROR: bronze accounts: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve accounts")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"accounts": accounts, "count": len(accounts)})
	}
}

// BronzeTrustlinesHandler returns trustline snapshots from bronze.
func BronzeTrustlinesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			respondError(w, http.StatusBadRequest, "account_id is required")
			return
		}
		limit := int(parseIntParam(r, "limit", 100))

		trustlines, err := reader.GetBronzeTrustlines(r.Context(), accountID, limit)
		if err != nil {
			log.Printf("ERROR: bronze trustlines: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve trustlines")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"trustlines": trustlines, "count": len(trustlines)})
	}
}

// BronzeOffersHandler returns offer snapshots from bronze.
func BronzeOffersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sellerID := r.URL.Query().Get("seller_id")
		if sellerID == "" {
			respondError(w, http.StatusBadRequest, "seller_id is required")
			return
		}
		limit := int(parseIntParam(r, "limit", 100))

		offers, err := reader.GetBronzeOffers(r.Context(), sellerID, limit)
		if err != nil {
			log.Printf("ERROR: bronze offers: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve offers")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"offers": offers, "count": len(offers)})
	}
}

// ContractEventsHandler returns contract events from bronze.
func ContractEventsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, end, limit, errMsg := parseLedgerRangeParams(r)
		if errMsg != "" {
			respondError(w, http.StatusBadRequest, errMsg)
			return
		}

		events, err := reader.GetContractEvents(r.Context(), start, end, limit)
		if err != nil {
			log.Printf("ERROR: contract events: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract events")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"contract_events": events, "count": len(events), "start": start, "end": end})
	}
}

// BronzeNetworkStatsHandler returns network stats computed from bronze data.
func BronzeNetworkStatsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := reader.GetBronzeNetworkStats(r.Context())
		if err != nil {
			log.Printf("ERROR: bronze network stats: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve bronze network stats")
			return
		}
		respondJSON(w, http.StatusOK, stats)
	}
}

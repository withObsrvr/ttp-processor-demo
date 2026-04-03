package main

import (
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// ── Search Handler ──────────────────────────────────────────

func SearchHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if q == "" {
			respondError(w, http.StatusBadRequest, "q (search query) required")
			return
		}
		limit := int(parseIntParam(r, "limit", 5))

		results, err := reader.Search(r.Context(), q, limit)
		if err != nil {
			log.Printf("ERROR: search: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to perform search")
			return
		}
		respondJSON(w, http.StatusOK, results)
	}
}

// ── Fee Stats Handler ──────────────────────────────────────────

func FeeStatsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		period := r.URL.Query().Get("period")
		if period == "" {
			period = "24h"
		}

		stats, err := reader.GetFeeStats(r.Context(), period)
		if err != nil {
			log.Printf("ERROR: fee stats: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve fee stats")
			return
		}
		respondJSON(w, http.StatusOK, stats)
	}
}

// ── Transaction Summaries Handler ──────────────────────────────────

func TransactionSummariesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		limit := int(parseIntParam(r, "limit", 50))

		if startLedger == 0 || endLedger == 0 {
			respondError(w, http.StatusBadRequest, "start_ledger and end_ledger required")
			return
		}

		summaries, err := reader.GetTransactionSummaries(r.Context(), startLedger, endLedger, limit)
		if err != nil {
			log.Printf("ERROR: transaction summaries: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve transaction summaries")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("summaries", summaries, len(summaries)))
	}
}

// ── Unified Event Handlers (transfer/mint/burn) ────────────────

func UnifiedEventsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		contractID := r.URL.Query().Get("contract_id")

		events, err := reader.GetUnifiedEvents(r.Context(), limit, startLedger, endLedger, contractID)
		if err != nil {
			log.Printf("ERROR: unified events: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve events")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("events", events, len(events)))
	}
}

func UnifiedEventsByContractHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		contractID := r.URL.Query().Get("contract_id")
		limit := int(parseIntParam(r, "limit", 50))

		events, err := reader.GetUnifiedEvents(r.Context(), limit, 0, 0, contractID)
		if err != nil {
			log.Printf("ERROR: unified events by contract: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve events")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{
			"contract_id": contractID, "events": events, "count": len(events),
		})
	}
}

// ── Generic Event Handlers (raw contract events) ──────────────

func GenericEventsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		contractID := r.URL.Query().Get("contract_id")

		events, err := reader.GetGenericEvents(r.Context(), limit, startLedger, endLedger, contractID)
		if err != nil {
			log.Printf("ERROR: generic events: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve events")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("events", events, len(events)))
	}
}

func ContractGenericEventsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)

		events, err := reader.GetGenericEvents(r.Context(), limit, startLedger, endLedger, contractID)
		if err != nil {
			log.Printf("ERROR: contract events: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract events")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("events", events, len(events)))
	}
}

func AddressEventsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		address := vars["addr"]
		limit := int(parseIntParam(r, "limit", 50))

		events, err := reader.GetAddressEvents(r.Context(), address, limit)
		if err != nil {
			log.Printf("ERROR: address events: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve address events")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("events", events, len(events)))
	}
}

func TransactionEventsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["hash"]

		events, err := reader.GetTransactionEvents(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: transaction events: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve transaction events")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("events", events, len(events)))
	}
}

// ── Liquidity Pool Handlers ──────────────────────────────────────

func LiquidityPoolsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))

		pools, err := reader.GetLiquidityPools(r.Context(), limit)
		if err != nil {
			log.Printf("ERROR: liquidity pools: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve liquidity pools")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("liquidity_pools", pools, len(pools)))
	}
}

func LiquidityPoolsByAssetHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		assetCode := r.URL.Query().Get("asset_code")
		assetIssuer := r.URL.Query().Get("asset_issuer")
		limit := int(parseIntParam(r, "limit", 50))

		pools, err := reader.GetLiquidityPoolsByAsset(r.Context(), assetCode, assetIssuer, limit)
		if err != nil {
			log.Printf("ERROR: liquidity pools by asset: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve liquidity pools")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("liquidity_pools", pools, len(pools)))
	}
}

func LiquidityPoolByIDHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		poolID := vars["id"]

		pool, err := reader.GetLiquidityPoolByID(r.Context(), poolID)
		if err != nil {
			log.Printf("ERROR: liquidity pool by ID: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve liquidity pool")
			return
		}
		if pool == nil {
			respondError(w, http.StatusNotFound, "liquidity pool not found")
			return
		}
		respondJSON(w, http.StatusOK, pool)
	}
}

// ── TTL Handlers ──────────────────────────────────────────

func TTLHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		keyHash := r.URL.Query().Get("key_hash")
		limit := int(parseIntParam(r, "limit", 50))

		entries, err := reader.GetTTLEntries(r.Context(), keyHash, limit)
		if err != nil {
			log.Printf("ERROR: TTL entries: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve TTL entries")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("entries", entries, len(entries)))
	}
}

func TTLExpiringHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		withinLedgers := parseIntParam(r, "within_ledgers", 1000)
		limit := int(parseIntParam(r, "limit", 50))

		entries, err := reader.GetTTLExpiring(r.Context(), withinLedgers, limit)
		if err != nil {
			log.Printf("ERROR: TTL expiring: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve expiring entries")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("entries", entries, len(entries)))
	}
}

func TTLExpiredHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))

		entries, err := reader.GetTTLExpired(r.Context(), limit)
		if err != nil {
			log.Printf("ERROR: TTL expired: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve expired entries")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("entries", entries, len(entries)))
	}
}

// ── Evicted/Restored Keys Handlers ──────────────────────────────

func EvictedKeysHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		contractID := r.URL.Query().Get("contract_id")
		limit := int(parseIntParam(r, "limit", 50))

		keys, err := reader.GetEvictedKeys(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: evicted keys: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve evicted keys")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("keys", keys, len(keys)))
	}
}

func RestoredKeysHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		contractID := r.URL.Query().Get("contract_id")
		limit := int(parseIntParam(r, "limit", 50))

		keys, err := reader.GetRestoredKeys(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: restored keys: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve restored keys")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("keys", keys, len(keys)))
	}
}

// ── Soroban Config Handler ──────────────────────────────────────

func SorobanConfigHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		config, err := reader.GetSorobanConfig(r.Context())
		if err != nil {
			log.Printf("ERROR: soroban config: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve soroban config")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("config", config, len(config)))
	}
}

// ── Semantic Handlers ──────────────────────────────────────

func SemanticActivitiesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		activityType := r.URL.Query().Get("type")
		account := r.URL.Query().Get("account")
		data, err := reader.GetSemanticActivities(r.Context(), limit, startLedger, endLedger, activityType, account)
		if err != nil {
			log.Printf("ERROR: semantic activities: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve activities")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("data", data, len(data)))
	}
}

func SemanticContractsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		contractType := r.URL.Query().Get("type")
		data, err := reader.GetSemanticContracts(r.Context(), limit, contractType)
		if err != nil {
			log.Printf("ERROR: semantic contracts: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contracts")
			return
		}
		// Split observed_functions from comma-separated string to array
		for _, row := range data {
			if fns, ok := row["observed_functions"].(string); ok && fns != "" {
				parts := strings.Split(fns, ",")
				row["observed_functions"] = parts
			}
		}
		respondJSON(w, http.StatusOK, listResponse("contracts", data, len(data)))
	}
}

func SemanticFlowsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		flowType := r.URL.Query().Get("type")
		account := r.URL.Query().Get("account")
		data, err := reader.GetSemanticFlows(r.Context(), limit, startLedger, endLedger, flowType, account)
		if err != nil {
			log.Printf("ERROR: semantic flows: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve flows")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("data", data, len(data)))
	}
}

func SemanticContractFunctionsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		contractID := r.URL.Query().Get("contract_id")
		limit := int(parseIntParam(r, "limit", 50))
		data, err := reader.GetSemanticContractFunctions(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: semantic contract functions: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract functions")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("data", data, len(data)))
	}
}

func SemanticAssetsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		assetCode := r.URL.Query().Get("asset_code")
		data, err := reader.GetSemanticAssets(r.Context(), limit, assetCode)
		if err != nil {
			log.Printf("ERROR: semantic assets: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve assets")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("data", data, len(data)))
	}
}

func SemanticDexPairsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		data, err := reader.GetSemanticDexPairs(r.Context(), limit)
		if err != nil {
			log.Printf("ERROR: semantic dex pairs: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve dex pairs")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("data", data, len(data)))
	}
}

func SemanticAccountSummaryHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID := r.URL.Query().Get("account_id")
		limit := int(parseIntParam(r, "limit", 50))
		data, err := reader.GetSemanticAccountSummary(r.Context(), accountID, limit)
		if err != nil {
			log.Printf("ERROR: semantic account summary: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account summaries")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("data", data, len(data)))
	}
}

func SemanticTokenSummaryHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]
		data, err := reader.GetSemanticTokenSummary(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: semantic token summary: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve token summary")
			return
		}
		if data == nil {
			respondError(w, http.StatusNotFound, "token not found")
			return
		}
		respondJSON(w, http.StatusOK, data)
	}
}

// ── SEP-41 Token Handlers ──────────────────────────────────

func SEP41TokenMetadataHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]
		data, err := reader.GetSEP41TokenMetadata(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: SEP-41 metadata: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve token metadata")
			return
		}
		if data == nil {
			respondError(w, http.StatusNotFound, "token not found")
			return
		}
		respondJSON(w, http.StatusOK, data)
	}
}

func SEP41TokenBalancesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]
		limit := int(parseIntParam(r, "limit", 50))
		data, err := reader.GetSEP41TokenBalances(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: SEP-41 balances: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve token balances")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("data", data, len(data)))
	}
}

func SEP41SingleBalanceHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]
		address := vars["address"]
		data, err := reader.GetSEP41SingleBalance(r.Context(), contractID, address)
		if err != nil {
			log.Printf("ERROR: SEP-41 single balance: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve balance")
			return
		}
		respondJSON(w, http.StatusOK, data)
	}
}

func SEP41TokenTransfersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]
		limit := int(parseIntParam(r, "limit", 50))
		data, err := reader.GetSEP41TokenTransfers(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: SEP-41 transfers: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve token transfers")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("data", data, len(data)))
	}
}

func SEP41TokenStatsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]
		data, err := reader.GetSEP41TokenStats(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: SEP-41 stats: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve token stats")
			return
		}
		respondJSON(w, http.StatusOK, data)
	}
}

func AddressTokenPortfolioHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		address := vars["addr"]
		limit := int(parseIntParam(r, "limit", 50))
		data, err := reader.GetAddressTokenPortfolio(r.Context(), address, limit)
		if err != nil {
			log.Printf("ERROR: address portfolio: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve token portfolio")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("data", data, len(data)))
	}
}

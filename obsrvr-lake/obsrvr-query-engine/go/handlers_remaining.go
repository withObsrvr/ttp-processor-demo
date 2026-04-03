package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// ── Transaction Diffs Handler ──────────────────────────────────

func TransactionDiffsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["hash"]
		result, err := reader.GetTransactionDiffs(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: tx diffs: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve transaction diffs")
			return
		}
		if result == nil {
			respondError(w, http.StatusNotFound, "transaction not found")
			return
		}
		respondJSON(w, http.StatusOK, result)
	}
}

// ── Smart Wallet Handler ──────────────────────────────────────

func SmartWalletHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]
		result, err := reader.GetSmartWalletInfo(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: smart wallet: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to detect smart wallet")
			return
		}
		respondJSON(w, http.StatusOK, result)
	}
}

// ── Index Plane Handlers ──────────────────────────────────────

func IndexTransactionLookupHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["hash"]
		result, err := reader.GetIndexTransactionLookup(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: index tx lookup: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to lookup transaction")
			return
		}
		if result == nil {
			respondError(w, http.StatusNotFound, "transaction not found")
			return
		}
		respondJSON(w, http.StatusOK, result)
	}
}

func IndexBatchTransactionLookupHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var hashes []string

		if r.Method == "POST" {
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err == nil {
				defer r.Body.Close()
				var req struct {
					Hashes []string `json:"hashes"`
				}
				if json.Unmarshal(body, &req) == nil {
					hashes = req.Hashes
				}
			}
		}
		if len(hashes) == 0 {
			if h := r.URL.Query().Get("hashes"); h != "" {
				hashes = strings.Split(h, ",")
			}
		}
		if len(hashes) == 0 {
			respondError(w, http.StatusBadRequest, "hashes required")
			return
		}

		results, err := reader.GetIndexBatchTransactionLookup(r.Context(), hashes)
		if err != nil {
			log.Printf("ERROR: index batch lookup: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to batch lookup transactions")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"transactions": results, "count": len(results)})
	}
}

func IndexHealthHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		health, err := reader.GetIndexHealth(r.Context())
		if err != nil {
			log.Printf("ERROR: index health: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve index health")
			return
		}
		respondJSON(w, http.StatusOK, health)
	}
}

func ContractIndexHealthHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		health, err := reader.GetContractIndexHealth(r.Context())
		if err != nil {
			log.Printf("ERROR: contract index health: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract index health")
			return
		}
		respondJSON(w, http.StatusOK, health)
	}
}

func ContractIndexLookupHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Batch contract lookup via POST
		var contractIDs []string
		if r.Method == "POST" {
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err == nil {
				defer r.Body.Close()
				var req struct {
					ContractIDs []string `json:"contract_ids"`
				}
				if json.Unmarshal(body, &req) == nil {
					contractIDs = req.ContractIDs
				}
			}
		}
		if len(contractIDs) == 0 {
			respondError(w, http.StatusBadRequest, "contract_ids required (POST JSON body)")
			return
		}

		var results []map[string]interface{}
		for _, id := range contractIDs {
			summary, err := reader.GetContractIndexSummary(r.Context(), id)
			if err == nil && summary != nil {
				results = append(results, summary)
			}
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"contracts": results, "count": len(results)})
	}
}

func ContractIndexLedgersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]
		limit := int(parseIntParam(r, "limit", 50))

		ledgers, err := reader.GetContractIndexLedgers(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: contract index ledgers: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract ledgers")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("ledgers", ledgers, len(ledgers)))
	}
}

func ContractIndexSummaryHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["contract_id"]

		summary, err := reader.GetContractIndexSummary(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: contract index summary: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract summary")
			return
		}
		if summary == nil {
			respondError(w, http.StatusNotFound, "contract not found")
			return
		}
		respondJSON(w, http.StatusOK, summary)
	}
}

// ── Gold Compliance Handlers ──────────────────────────────────

func ComplianceTransactionsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		assetCode := r.URL.Query().Get("asset_code")
		assetIssuer := r.URL.Query().Get("asset_issuer")
		if assetCode == "" {
			respondError(w, http.StatusBadRequest, "asset_code required")
			return
		}
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		limit := int(parseIntParam(r, "limit", 100))

		txs, err := reader.GetComplianceTransactions(r.Context(), assetCode, assetIssuer, startLedger, endLedger, limit)
		if err != nil {
			log.Printf("ERROR: compliance transactions: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve compliance transactions")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("transactions", txs, len(txs)))
	}
}

func ComplianceBalancesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		assetCode := r.URL.Query().Get("asset_code")
		assetIssuer := r.URL.Query().Get("asset_issuer")
		if assetCode == "" {
			respondError(w, http.StatusBadRequest, "asset_code required")
			return
		}
		atLedger := parseIntParam(r, "at_ledger", 0)
		limit := int(parseIntParam(r, "limit", 100))

		balances, err := reader.GetComplianceBalances(r.Context(), assetCode, assetIssuer, atLedger, limit)
		if err != nil {
			log.Printf("ERROR: compliance balances: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve compliance balances")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("balances", balances, len(balances)))
	}
}

func ComplianceSupplyHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		assetCode := r.URL.Query().Get("asset_code")
		assetIssuer := r.URL.Query().Get("asset_issuer")
		if assetCode == "" {
			respondError(w, http.StatusBadRequest, "asset_code required")
			return
		}
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)

		supply, err := reader.GetComplianceSupply(r.Context(), assetCode, assetIssuer, startLedger, endLedger)
		if err != nil {
			log.Printf("ERROR: compliance supply: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve compliance supply")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("supply", supply, len(supply)))
	}
}

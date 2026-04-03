package main

import (
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
)

// parseAssetParam splits "CODE:ISSUER" or "CODE-ISSUER" into code and issuer.
func parseAssetParam(asset string) (string, string) {
	if i := strings.IndexByte(asset, ':'); i >= 0 {
		return asset[:i], asset[i+1:]
	}
	if i := strings.IndexByte(asset, '-'); i >= 0 {
		return asset[:i], asset[i+1:]
	}
	return asset, ""
}

// ── Account Handlers ──────────────────────────────────────────

func ListAccountsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		cursor := r.URL.Query().Get("cursor")
		accounts, err := reader.GetAccountsList(r.Context(), limit, cursor)
		if err != nil {
			log.Printf("ERROR: list accounts: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve accounts")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("accounts", accounts, len(accounts)))
	}
}

func AccountCurrentHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			respondError(w, http.StatusBadRequest, "account_id required")
			return
		}
		account, err := reader.GetAccountCurrentSilver(r.Context(), accountID)
		if err != nil {
			log.Printf("ERROR: account current: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account")
			return
		}
		if account == nil {
			respondError(w, http.StatusNotFound, "account not found")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"account": account})
	}
}

func AccountHistoryHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			respondError(w, http.StatusBadRequest, "account_id required")
			return
		}
		limit := int(parseIntParam(r, "limit", 50))
		cursorLedger := parseIntParam(r, "cursor", 0)
		history, err := reader.GetAccountHistory(r.Context(), accountID, limit, cursorLedger)
		if err != nil {
			log.Printf("ERROR: account history: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account history")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("history", history, len(history)))
	}
}

func AccountSignersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			respondError(w, http.StatusBadRequest, "account_id required")
			return
		}
		signers, err := reader.GetAccountSigners(r.Context(), accountID)
		if err != nil {
			log.Printf("ERROR: account signers: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account signers")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("signers", signers, len(signers)))
	}
}

func AccountBalancesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		accountID := vars["id"]
		balances, err := reader.GetAccountBalances(r.Context(), accountID)
		if err != nil {
			log.Printf("ERROR: account balances: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account balances")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("balances", balances, len(balances)))
	}
}

func AccountOffersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		accountID := vars["id"]
		limit := int(parseIntParam(r, "limit", 50))
		offers, err := reader.GetAccountOffers(r.Context(), accountID, limit)
		if err != nil {
			log.Printf("ERROR: account offers: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account offers")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("offers", offers, len(offers)))
	}
}

func AccountContractsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		accountID := vars["id"]
		limit := int(parseIntParam(r, "limit", 50))
		contracts, err := reader.GetAccountContracts(r.Context(), accountID, limit)
		if err != nil {
			log.Printf("ERROR: account contracts: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account contracts")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("contracts", contracts, len(contracts)))
	}
}

func AccountActivityHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		accountID := vars["id"]
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		activity, err := reader.GetAccountActivity(r.Context(), accountID, limit, startLedger, endLedger)
		if err != nil {
			log.Printf("ERROR: account activity: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account activity")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("activity", activity, len(activity)))
	}
}

// ── Asset Handlers ──────────────────────────────────────────

func AssetListHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		cursor := r.URL.Query().Get("cursor")
		assets, err := reader.GetAssetList(r.Context(), limit, cursor)
		if err != nil {
			log.Printf("ERROR: asset list: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve assets")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("assets", assets, len(assets)))
	}
}

func TokenHoldersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		assetCode, assetIssuer := parseAssetParam(vars["asset"])
		if assetCode == "" {
			respondError(w, http.StatusBadRequest, "asset parameter required (format: CODE:ISSUER)")
			return
		}
		limit := int(parseIntParam(r, "limit", 50))
		holders, err := reader.GetTokenHolders(r.Context(), assetCode, assetIssuer, limit)
		if err != nil {
			log.Printf("ERROR: token holders: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve token holders")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("holders", holders, len(holders)))
	}
}

func TokenStatsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		assetCode, assetIssuer := parseAssetParam(vars["asset"])
		if assetCode == "" {
			respondError(w, http.StatusBadRequest, "asset parameter required (format: CODE:ISSUER)")
			return
		}
		stats, err := reader.GetTokenStats(r.Context(), assetCode, assetIssuer)
		if err != nil {
			log.Printf("ERROR: token stats: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve token stats")
			return
		}
		respondJSON(w, http.StatusOK, stats)
	}
}

// ── Operations Handlers ──────────────────────────────────────

func SorobanOperationsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		contractID := r.URL.Query().Get("contract_id")
		functionName := r.URL.Query().Get("function_name")
		ops, err := reader.GetSorobanOperations(r.Context(), limit, startLedger, endLedger, contractID, functionName)
		if err != nil {
			log.Printf("ERROR: soroban operations: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve soroban operations")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("operations", ops, len(ops)))
	}
}

func SorobanOpsByFunctionHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		contractID := r.URL.Query().Get("contract_id")
		functionName := r.URL.Query().Get("function_name")
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		ops, err := reader.GetSorobanOpsByFunction(r.Context(), contractID, functionName, limit, startLedger, endLedger)
		if err != nil {
			log.Printf("ERROR: soroban ops by function: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve soroban operations")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("operations", ops, len(ops)))
	}
}

func PaymentsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		account := r.URL.Query().Get("account")
		payments, err := reader.GetPayments(r.Context(), limit, startLedger, endLedger, account)
		if err != nil {
			log.Printf("ERROR: payments: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve payments")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("payments", payments, len(payments)))
	}
}

// ── Transfer Handlers ──────────────────────────────────────

func TokenTransfersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		account := r.URL.Query().Get("account")
		assetCode := r.URL.Query().Get("asset_code")
		transfers, err := reader.GetTokenTransfers(r.Context(), limit, startLedger, endLedger, account, assetCode)
		if err != nil {
			log.Printf("ERROR: token transfers: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve transfers")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("transfers", transfers, len(transfers)))
	}
}

func TokenTransferStatsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		assetCode := r.URL.Query().Get("asset_code")
		assetIssuer := r.URL.Query().Get("asset_issuer")
		stats, err := reader.GetTokenTransferStats(r.Context(), assetCode, assetIssuer)
		if err != nil {
			log.Printf("ERROR: transfer stats: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve transfer stats")
			return
		}
		respondJSON(w, http.StatusOK, stats)
	}
}

// ── Explorer Handlers ──────────────────────────────────────

func ExplorerAccountHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accountID := r.URL.Query().Get("account_id")
		if accountID == "" {
			respondError(w, http.StatusBadRequest, "account_id required")
			return
		}
		result, err := reader.GetExplorerAccount(r.Context(), accountID)
		if err != nil {
			log.Printf("ERROR: explorer account: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve account overview")
			return
		}
		if result == nil {
			respondError(w, http.StatusNotFound, "account not found")
			return
		}
		respondJSON(w, http.StatusOK, result)
	}
}

func ExplorerTransactionHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		txHash := r.URL.Query().Get("hash")
		if txHash == "" {
			respondError(w, http.StatusBadRequest, "hash required")
			return
		}
		result, err := reader.GetExplorerTransaction(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: explorer transaction: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve transaction")
			return
		}
		if result == nil {
			respondError(w, http.StatusNotFound, "transaction not found")
			return
		}
		respondJSON(w, http.StatusOK, result)
	}
}

func ExplorerAssetHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		assetCode := r.URL.Query().Get("asset_code")
		assetIssuer := r.URL.Query().Get("asset_issuer")
		if assetCode == "" {
			respondError(w, http.StatusBadRequest, "asset_code required")
			return
		}
		result, err := reader.GetExplorerAsset(r.Context(), assetCode, assetIssuer)
		if err != nil {
			log.Printf("ERROR: explorer asset: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve asset overview")
			return
		}
		respondJSON(w, http.StatusOK, result)
	}
}

// ── Offers Handlers (Silver) ──────────────────────────────────

func SilverOffersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sellerID := r.URL.Query().Get("seller")
		limit := int(parseIntParam(r, "limit", 50))
		offers, err := reader.GetSilverOffers(r.Context(), sellerID, limit)
		if err != nil {
			log.Printf("ERROR: silver offers: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve offers")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("offers", offers, len(offers)))
	}
}

func SilverOffersByPairHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sellCode := r.URL.Query().Get("selling_asset_code")
		sellIssuer := r.URL.Query().Get("selling_asset_issuer")
		buyCode := r.URL.Query().Get("buying_asset_code")
		buyIssuer := r.URL.Query().Get("buying_asset_issuer")
		limit := int(parseIntParam(r, "limit", 50))
		offers, err := reader.GetSilverOffersByPair(r.Context(), sellCode, sellIssuer, buyCode, buyIssuer, limit)
		if err != nil {
			log.Printf("ERROR: offers by pair: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve offers by pair")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("offers", offers, len(offers)))
	}
}

func SilverOfferByIDHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		idStr := vars["id"]
		offerID, _ := strconv.ParseInt(idStr, 10, 64)
		if offerID == 0 {
			respondError(w, http.StatusBadRequest, "valid offer id required")
			return
		}
		offer, err := reader.GetSilverOfferByID(r.Context(), offerID)
		if err != nil {
			log.Printf("ERROR: offer by ID: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve offer")
			return
		}
		if offer == nil {
			respondError(w, http.StatusNotFound, "offer not found")
			return
		}
		respondJSON(w, http.StatusOK, offer)
	}
}

// ── Claimable Balances Handlers ──────────────────────────────────

func ClaimableBalancesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sponsor := r.URL.Query().Get("sponsor")
		limit := int(parseIntParam(r, "limit", 50))
		balances, err := reader.GetClaimableBalances(r.Context(), sponsor, limit)
		if err != nil {
			log.Printf("ERROR: claimable balances: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve claimable balances")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("claimable_balances", balances, len(balances)))
	}
}

func ClaimableBalancesByAssetHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		assetCode := r.URL.Query().Get("asset_code")
		assetIssuer := r.URL.Query().Get("asset_issuer")
		limit := int(parseIntParam(r, "limit", 50))
		balances, err := reader.GetClaimableBalancesByAsset(r.Context(), assetCode, assetIssuer, limit)
		if err != nil {
			log.Printf("ERROR: claimable balances by asset: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve claimable balances")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("claimable_balances", balances, len(balances)))
	}
}

func ClaimableBalanceByIDHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		balanceID := vars["id"]
		balance, err := reader.GetClaimableBalanceByID(r.Context(), balanceID)
		if err != nil {
			log.Printf("ERROR: claimable balance by ID: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve claimable balance")
			return
		}
		if balance == nil {
			respondError(w, http.StatusNotFound, "claimable balance not found")
			return
		}
		respondJSON(w, http.StatusOK, balance)
	}
}

// ── Trades Handlers (Silver) ──────────────────────────────────

func SilverTradesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		account := r.URL.Query().Get("account")
		trades, err := reader.GetSilverTrades(r.Context(), limit, startLedger, endLedger, account)
		if err != nil {
			log.Printf("ERROR: silver trades: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve trades")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("trades", trades, len(trades)))
	}
}

func SilverTradesByPairHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sellCode := r.URL.Query().Get("selling_asset_code")
		sellIssuer := r.URL.Query().Get("selling_asset_issuer")
		buyCode := r.URL.Query().Get("buying_asset_code")
		buyIssuer := r.URL.Query().Get("buying_asset_issuer")
		limit := int(parseIntParam(r, "limit", 50))
		trades, err := reader.GetSilverTradesByPair(r.Context(), sellCode, sellIssuer, buyCode, buyIssuer, limit)
		if err != nil {
			log.Printf("ERROR: trades by pair: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve trades by pair")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("trades", trades, len(trades)))
	}
}

func SilverTradeStatsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sellCode := r.URL.Query().Get("selling_asset_code")
		sellIssuer := r.URL.Query().Get("selling_asset_issuer")
		buyCode := r.URL.Query().Get("buying_asset_code")
		buyIssuer := r.URL.Query().Get("buying_asset_issuer")
		stats, err := reader.GetSilverTradeStats(r.Context(), sellCode, sellIssuer, buyCode, buyIssuer)
		if err != nil {
			log.Printf("ERROR: trade stats: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve trade stats")
			return
		}
		respondJSON(w, http.StatusOK, stats)
	}
}

// ── Effects Handlers (Silver) ──────────────────────────────────

func SilverEffectsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 50))
		startLedger := parseIntParam(r, "start_ledger", 0)
		endLedger := parseIntParam(r, "end_ledger", 0)
		account := r.URL.Query().Get("account")
		effectType := r.URL.Query().Get("type")
		effects, err := reader.GetSilverEffects(r.Context(), limit, startLedger, endLedger, account, effectType)
		if err != nil {
			log.Printf("ERROR: silver effects: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve effects")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("effects", effects, len(effects)))
	}
}

func EffectTypesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		types, err := reader.GetEffectTypes(r.Context())
		if err != nil {
			log.Printf("ERROR: effect types: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve effect types")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("effect_types", types, len(types)))
	}
}

func EffectsByTransactionHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["tx_hash"]
		effects, err := reader.GetEffectsByTransaction(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: effects by transaction: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve effects")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"transaction_hash": txHash, "effects": effects, "count": len(effects)})
	}
}

// ── Soroban Handlers ──────────────────────────────────────────

func ContractCodeHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		contractID := r.URL.Query().Get("contract_id")
		if contractID == "" {
			respondError(w, http.StatusBadRequest, "contract_id required")
			return
		}
		code, err := reader.GetContractCode(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: contract code: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract code")
			return
		}
		if code == nil {
			respondError(w, http.StatusNotFound, "contract not found")
			return
		}
		respondJSON(w, http.StatusOK, code)
	}
}

func ContractDataHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		contractID := r.URL.Query().Get("contract_id")
		limit := int(parseIntParam(r, "limit", 50))
		data, err := reader.GetContractData(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: contract data: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract data")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("entries", data, len(data)))
	}
}

func ContractDataEntryHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		contractID := r.URL.Query().Get("contract_id")
		keyHash := r.URL.Query().Get("key_hash")
		if contractID == "" || keyHash == "" {
			respondError(w, http.StatusBadRequest, "contract_id and key_hash required")
			return
		}
		entry, err := reader.GetContractDataEntry(r.Context(), contractID, keyHash)
		if err != nil {
			log.Printf("ERROR: contract data entry: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract data entry")
			return
		}
		if entry == nil {
			respondError(w, http.StatusNotFound, "entry not found")
			return
		}
		respondJSON(w, http.StatusOK, entry)
	}
}

func ContractStorageHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["id"]
		limit := int(parseIntParam(r, "limit", 50))
		data, err := reader.GetContractStorage(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: contract storage: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract storage")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("entries", data, len(data)))
	}
}

// ── Contract Analysis Handlers ──────────────────────────────────

func ContractsInvolvedHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["hash"]
		contracts, err := reader.GetContractsInvolved(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: contracts involved: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contracts involved")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"contracts": contracts, "count": len(contracts)})
	}
}

func CallGraphHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["hash"]
		graph, err := reader.GetCallGraph(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: call graph: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve call graph")
			return
		}
		respondJSON(w, http.StatusOK, map[string]interface{}{"call_graph": graph, "count": len(graph)})
	}
}

func TransactionHierarchyHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["hash"]
		hierarchy, err := reader.GetTransactionHierarchy(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: transaction hierarchy: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve transaction hierarchy")
			return
		}
		respondJSON(w, http.StatusOK, hierarchy)
	}
}

func ContractsSummaryHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["hash"]
		summary, err := reader.GetContractsSummary(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: contracts summary: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contracts summary")
			return
		}
		respondJSON(w, http.StatusOK, summary)
	}
}

func TopContractsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := int(parseIntParam(r, "limit", 20))
		contracts, err := reader.GetTopContracts(r.Context(), limit)
		if err != nil {
			log.Printf("ERROR: top contracts: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve top contracts")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("contracts", contracts, len(contracts)))
	}
}

func ContractRecentCallsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["id"]
		limit := int(parseIntParam(r, "limit", 50))
		calls, err := reader.GetContractRecentCalls(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: contract recent calls: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve recent calls")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("calls", calls, len(calls)))
	}
}

func ContractCallersHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["id"]
		limit := int(parseIntParam(r, "limit", 50))
		callers, err := reader.GetContractCallers(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: contract callers: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve callers")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("callers", callers, len(callers)))
	}
}

func ContractCalleesHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["id"]
		limit := int(parseIntParam(r, "limit", 50))
		callees, err := reader.GetContractCallees(r.Context(), contractID, limit)
		if err != nil {
			log.Printf("ERROR: contract callees: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve callees")
			return
		}
		respondJSON(w, http.StatusOK, listResponse("callees", callees, len(callees)))
	}
}

func ContractCallSummaryHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["id"]
		summary, err := reader.GetContractCallSummary(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: contract call summary: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve call summary")
			return
		}
		if summary == nil {
			respondError(w, http.StatusNotFound, "contract not found")
			return
		}
		respondJSON(w, http.StatusOK, summary)
	}
}

func ContractAnalyticsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["id"]
		analytics, err := reader.GetContractAnalytics(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: contract analytics: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract analytics")
			return
		}
		respondJSON(w, http.StatusOK, analytics)
	}
}

func ContractMetadataHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["id"]
		metadata, err := reader.GetContractMetadata(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: contract metadata: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract metadata")
			return
		}
		if metadata == nil {
			respondError(w, http.StatusNotFound, "contract not found")
			return
		}
		respondJSON(w, http.StatusOK, metadata)
	}
}

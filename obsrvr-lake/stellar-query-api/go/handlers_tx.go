package main

import (
	"errors"
	"net/http"

	"github.com/gorilla/mux"
)

// TxDiffHandlers contains HTTP handlers for transaction diff queries
type TxDiffHandlers struct {
	reader *UnifiedDuckDBReader
}

// NewTxDiffHandlers creates new transaction diff API handlers
func NewTxDiffHandlers(reader *UnifiedDuckDBReader) *TxDiffHandlers {
	return &TxDiffHandlers{reader: reader}
}

// HandleTransactionDiffs returns balance and state changes for a transaction
// @Summary Get transaction balance/state diffs
// @Description Decodes tx_meta XDR to extract balance changes and state changes for a transaction
// @Tags Transactions
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} TxDiffs "Transaction diffs"
// @Failure 400 {object} map[string]interface{} "Missing transaction hash"
// @Failure 404 {object} map[string]interface{} "Transaction not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/{hash}/diffs [get]
func (h *TxDiffHandlers) HandleTransactionDiffs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	diffs, err := h.reader.GetTransactionDiffs(r.Context(), txHash)
	if err != nil {
		if errors.Is(err, ErrTxNotFound) {
			respondError(w, err.Error(), http.StatusNotFound)
			return
		}
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, diffs)
}

// SmartWalletHandlers contains HTTP handlers for SEP-50 smart wallet detection
type SmartWalletHandlers struct {
	reader *UnifiedDuckDBReader
}

// NewSmartWalletHandlers creates new smart wallet API handlers
func NewSmartWalletHandlers(reader *UnifiedDuckDBReader) *SmartWalletHandlers {
	return &SmartWalletHandlers{reader: reader}
}

// HandleSmartWalletInfo detects if a contract is a SEP-50 smart wallet
// @Summary Detect SEP-50 smart wallet
// @Description Heuristically detects if a contract implements SEP-50 smart wallet by checking for __check_auth events and signer storage
// @Tags Smart Wallets
// @Produce json
// @Param contract_id path string true "Contract ID (C...)"
// @Success 200 {object} SmartWalletInfo "Smart wallet info"
// @Failure 400 {object} map[string]interface{} "Missing contract_id"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/smart-wallet/{contract_id} [get]
func (h *SmartWalletHandlers) HandleSmartWalletInfo(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["contract_id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	info, err := h.reader.GetSmartWalletInfo(r.Context(), contractID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, info)
}

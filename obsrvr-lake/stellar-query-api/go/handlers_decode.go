package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"
)

// DecodeHandlers contains HTTP handlers for transaction decoding and human-readable summaries
type DecodeHandlers struct {
	hotReader     *SilverHotReader
	coldReader    *SilverColdReader
	bronzeCold    *ColdReader
	silverReader  *UnifiedSilverReader
	hotPathReader *TxHotPathReader
}

// NewDecodeHandlers creates new transaction decode API handlers
func NewDecodeHandlers(hotReader *SilverHotReader, coldReader *SilverColdReader, bronzeCold *ColdReader, silverReader *UnifiedSilverReader, hotPathReader *TxHotPathReader) *DecodeHandlers {
	return &DecodeHandlers{hotReader: hotReader, coldReader: coldReader, bronzeCold: bronzeCold, silverReader: silverReader, hotPathReader: hotPathReader}
}

// HandleDecodedTransaction returns a human-readable decoded transaction
// @Summary Get decoded transaction with human-readable summary
// @Description Returns a decoded transaction with human-readable summary, decoded operations (with contract/function details), and associated CAP-67 events. Summary type is auto-detected: transfer, mint, burn, swap, contract_call, or classic.
// @Tags Transactions
// @Accept json
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} DecodedTransaction "Decoded transaction with summary, operations, and events"
// @Failure 400 {object} map[string]interface{} "Missing transaction hash"
// @Failure 404 {object} map[string]interface{} "Transaction not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/{hash}/decoded [get]
func (h *DecodeHandlers) HandleDecodedTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	decoded, err := h.getTransactionForDecode(r.Context(), txHash)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if decoded.OpCount == 0 && len(decoded.Events) == 0 {
		respondError(w, "transaction not found", http.StatusNotFound)
		return
	}

	respondJSON(w, decoded)
}

// HandleContractInterface returns the detected interface for a contract
// @Summary Get contract interface detection
// @Description Returns the detected interface type (SEP-41 or unknown) for a contract based on observed function calls. SEP-41 detection requires at least 3 of 5 standard function signatures.
// @Tags Contracts
// @Accept json
// @Produce json
// @Param id path string true "Contract ID (C...)"
// @Success 200 {object} map[string]interface{} "Detected interface with observed functions"
// @Failure 400 {object} map[string]interface{} "Missing contract_id"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/contracts/{id}/interface [get]
func (h *DecodeHandlers) HandleContractInterface(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	// Get observed function names
	var functions []string
	var err error
	if h.hotReader != nil {
		functions, err = h.hotReader.GetContractFunctions(r.Context(), contractID)
	}
	if err != nil || len(functions) == 0 {
		if h.coldReader == nil {
			respondError(w, "contract interface detection requires cold reader", http.StatusInternalServerError)
			return
		}
		functions, err = h.coldReader.GetContractFunctions(r.Context(), contractID)
	}
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Detect contract type
	contractType := DetectContractType(functions)

	if contractType == ContractTypeSEP41 {
		iface := GetSEP41Interface(contractID)
		respondJSON(w, map[string]interface{}{
			"contract_id":        contractID,
			"detected_type":      string(contractType),
			"interface":          iface.Functions,
			"observed_functions": functions,
		})
		return
	}

	// Unknown contract type — return observed functions
	respondJSON(w, map[string]interface{}{
		"contract_id":        contractID,
		"detected_type":      string(ContractTypeUnknown),
		"observed_functions": functions,
	})
}

// HandleDecodeScVal decodes an ScVal from XDR or JSON
// @Summary Decode Soroban ScVal
// @Description Decodes a Soroban ScVal from XDR (base64) or JSON into a human-readable format. Supports types: bool, u32, i32, u64, i64, u128, i128, address, symbol, string, bytes, vec, map.
// @Tags Soroban
// @Accept json
// @Produce json
// @Param body body object true "ScVal to decode" SchemaExample({"xdr": "base64-encoded-xdr", "type_hint": "i128"})
// @Success 200 {object} DecodedScVal "Decoded ScVal with type, value, and display"
// @Failure 400 {object} map[string]interface{} "Invalid request body or missing xdr/json field"
// @Router /api/v1/silver/decode/scval [post]
func (h *DecodeHandlers) HandleDecodeScVal(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		respondError(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req struct {
		XDR      string          `json:"xdr"`
		JSON     json.RawMessage `json:"json"`
		TypeHint string          `json:"type_hint"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		respondError(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	var decoded DecodedScVal
	if req.XDR != "" {
		typeHint := req.TypeHint
		if typeHint == "" {
			typeHint = "unknown"
		}
		decoded = DecodeScValBase64(req.XDR, typeHint)
	} else if req.JSON != nil {
		decoded = DecodeScValJSON(req.JSON)
	} else {
		respondError(w, "either 'xdr' or 'json' field required", http.StatusBadRequest)
		return
	}

	respondJSON(w, decoded)
}

// HandleFullTransaction returns a composite view: decoded transaction + contracts involved + call graph
// @Summary Get full transaction analysis
// @Description Returns a composite view combining the decoded transaction (summary, operations, events), contracts involved, and the contract call graph in a single request.
// @Tags Transactions
// @Accept json
// @Produce json
// @Param hash path string true "Transaction hash"
// @Success 200 {object} map[string]interface{} "Full transaction analysis"
// @Failure 400 {object} map[string]interface{} "Missing transaction hash"
// @Failure 404 {object} map[string]interface{} "Transaction not found"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/{hash}/full [get]
func (h *DecodeHandlers) HandleFullTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// 1. Get decoded transaction (summary + ops + events)
	decoded, err := h.getTransactionForDecode(ctx, txHash)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if decoded.OpCount == 0 && len(decoded.Events) == 0 {
		respondError(w, "transaction not found", http.StatusNotFound)
		return
	}

	// 2. Get contracts involved (with 5s timeout to avoid blocking)
	var contractsInvolved []string
	if h.silverReader != nil {
		subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		contractsInvolved, _ = h.silverReader.GetContractsInvolved(subCtx, txHash)
		cancel()
	}

	// 3. Get call graph (with 5s timeout to avoid blocking)
	var callGraph []ContractCall
	if h.silverReader != nil {
		subCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		callGraph, _ = h.silverReader.GetTransactionCallGraph(subCtx, txHash)
		cancel()
	}

	// 4. Compose response
	txMap := map[string]interface{}{
		"tx_hash":         decoded.TxHash,
		"ledger_sequence": decoded.LedgerSeq,
		"closed_at":       decoded.ClosedAt,
		"successful":      decoded.Successful,
		"fee":             decoded.Fee,
		"operation_count": decoded.OpCount,
	}
	if decoded.SourceAccount != nil {
		txMap["source_account"] = *decoded.SourceAccount
	}
	if decoded.AccountSequence != nil {
		txMap["account_sequence"] = *decoded.AccountSequence
	}
	if decoded.MaxFee != nil {
		txMap["max_fee"] = *decoded.MaxFee
	}

	response := map[string]interface{}{
		"transaction":        txMap,
		"summary":            decoded.Summary,
		"operations":         decoded.Operations,
		"events":             decoded.Events,
		"contracts_involved": contractsInvolved,
		"call_graph":         callGraph,
	}

	// Add soroban_resources sub-object if available
	if decoded.SorobanResourcesInstructions != nil || decoded.SorobanResourcesReadBytes != nil || decoded.SorobanResourcesWriteBytes != nil {
		sorobanResources := map[string]interface{}{}
		if decoded.SorobanResourcesInstructions != nil {
			sorobanResources["instructions"] = *decoded.SorobanResourcesInstructions
		}
		if decoded.SorobanResourcesReadBytes != nil {
			sorobanResources["read_bytes"] = *decoded.SorobanResourcesReadBytes
		}
		if decoded.SorobanResourcesWriteBytes != nil {
			sorobanResources["write_bytes"] = *decoded.SorobanResourcesWriteBytes
		}
		response["soroban_resources"] = sorobanResources
	}

	respondJSON(w, response)
}

// HandleBatchDecodedTransactions returns decoded transactions for multiple hashes or a ledger.
// Supports GET with ?hashes=a,b,c or ?ledger=123, and POST with {"hashes":["a","b","c"]}.
// Each transaction is fully decoded with summary, operations, events, and Soroban resources —
// identical to /tx/{hash}/decoded but batched.
// @Summary Batch decoded transactions
// @Description Returns fully decoded transactions (summary, operations, events) for up to 25 hashes or all transactions in a ledger. Same response shape as /tx/{hash}/decoded per item.
// @Tags Transactions
// @Accept json
// @Produce json
// @Param hashes query string false "Comma-separated transaction hashes (max 25)"
// @Param ledger query int false "Ledger sequence — returns all transactions in that ledger"
// @Param limit query int false "Max transactions when using ledger (default: 25, max: 100)"
// @Success 200 {object} map[string]interface{} "Batch decoded transactions"
// @Failure 400 {object} map[string]interface{} "Invalid parameters"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/tx/batch/decoded [get]
// @Router /api/v1/silver/tx/batch/decoded [post]
func (h *DecodeHandlers) HandleBatchDecodedTransactions(w http.ResponseWriter, r *http.Request) {
	if h.coldReader == nil {
		respondError(w, "batch decoded requires cold reader", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	var hashes []string

	// Determine hashes from GET params or POST body
	if r.Method == "POST" {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			respondError(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var req struct {
			Hashes []string `json:"hashes"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			respondError(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		hashes = req.Hashes
	}

	hashesParam := r.URL.Query().Get("hashes")
	ledgerParam := r.URL.Query().Get("ledger")

	if len(hashes) == 0 && hashesParam != "" {
		for _, h := range strings.Split(hashesParam, ",") {
			if trimmed := strings.TrimSpace(h); trimmed != "" {
				hashes = append(hashes, trimmed)
			}
		}
	}

	// If ledger param, resolve hashes from that ledger
	fromLedger := false
	if len(hashes) == 0 && ledgerParam != "" {
		seq, err := strconv.ParseInt(ledgerParam, 10, 64)
		if err != nil {
			respondError(w, "invalid ledger sequence", http.StatusBadRequest)
			return
		}

		limit := 25
		limitStr := r.URL.Query().Get("limit")
		if limitStr != "" {
			if parsed, err := strconv.Atoi(limitStr); err == nil {
				if parsed < 1 {
					limit = 1
				} else if parsed > 100 {
					limit = 100
				} else {
					limit = parsed
				}
			}
		}

		resolved, err := h.resolveHashesFromLedger(ctx, seq, limit)
		if err != nil {
			respondError(w, "failed to resolve ledger transactions: "+err.Error(), http.StatusInternalServerError)
			return
		}
		hashes = resolved
		fromLedger = true
	}

	if len(hashes) == 0 {
		respondError(w, "provide 'hashes' (query or body) or 'ledger' parameter", http.StatusBadRequest)
		return
	}
	// Hashes mode capped at 25; ledger mode respects its own limit (up to 100)
	if !fromLedger && len(hashes) > 25 {
		respondError(w, "maximum 25 transactions per batch", http.StatusBadRequest)
		return
	}

	// Decode transactions in parallel. Each GetTransactionForDecode call fans
	// out to 4 separate DB queries, so doing them serially scales as ~4N round
	// trips and easily exceeds the gateway HTTP timeout for large batches.
	// Concurrency limit of 8 keeps DB pool pressure bounded while collapsing
	// the wall-clock time to roughly ceil(N/8) sequential rounds.
	type decodeResult struct {
		decoded *DecodedTransaction
		err     error
	}
	resultsByIndex := make([]decodeResult, len(hashes))

	// Each goroutine writes to a unique index in resultsByIndex, so no
	// synchronization is needed beyond errgroup's Wait.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, txHash := range hashes {
		i, txHash := i, txHash
		g.Go(func() error {
			decoded, err := h.getTransactionForDecode(gctx, txHash)
			resultsByIndex[i] = decodeResult{decoded: decoded, err: err}
			// Never propagate the error to errgroup — partial failures are
			// reported per-tx in the response, just like the old serial loop.
			return nil
		})
	}
	_ = g.Wait()

	// Assemble results in the original request order
	var results []interface{}
	var errors []map[string]string
	for i, txHash := range hashes {
		r := resultsByIndex[i]
		if r.err != nil {
			errors = append(errors, map[string]string{"tx_hash": txHash, "error": r.err.Error()})
			continue
		}
		if r.decoded.OpCount == 0 && len(r.decoded.Events) == 0 {
			errors = append(errors, map[string]string{"tx_hash": txHash, "error": "transaction not found"})
			continue
		}
		results = append(results, r.decoded)
	}

	resp := map[string]interface{}{
		"transactions": results,
		"count":        len(results),
	}
	if len(errors) > 0 {
		resp["errors"] = errors
	}

	respondJSON(w, resp)
}

// resolveHashesFromLedger finds distinct transaction hashes in a ledger
func (h *DecodeHandlers) resolveHashesFromLedger(ctx context.Context, ledgerSeq int64, limit int) ([]string, error) {
	if h.hotPathReader != nil {
		if hashes, err := h.hotPathReader.ResolveHashesFromLedger(ctx, ledgerSeq, limit); err == nil && len(hashes) > 0 {
			return hashes, nil
		}
	}

	if h.coldReader != nil {
		query := fmt.Sprintf(`
			SELECT DISTINCT transaction_hash
			FROM %s.%s.enriched_history_operations
			WHERE ledger_sequence = ?
			ORDER BY transaction_hash
			LIMIT ?
		`, h.coldReader.catalogName, h.coldReader.schemaName)
		rows, err := h.coldReader.db.QueryContext(ctx, query, ledgerSeq, limit)
		if err == nil {
			var hashes []string
			for rows.Next() {
				var hash string
				if err := rows.Scan(&hash); err != nil {
					rows.Close()
					break
				}
				hashes = append(hashes, hash)
			}
			rows.Close()
			if len(hashes) > 0 {
				return hashes, nil
			}
		} else {
			log.Printf("resolveHashesFromLedger silver cold: %v", err)
		}
	}

	// Bronze fallback intentionally disabled.
	//
	// Prior versions had a bronze fallback here that queried
	// transactions_row_v2 with `ORDER BY transaction_index`, but that
	// column only exists on operations_row_v2 (via migration 001). The
	// query errored silently every call and the fallback never worked.
	//
	// Fixing the ORDER BY (use transaction_id, a packed TOID that encodes
	// the TxOrder in its mid bits) makes the fallback discover hashes —
	// but then GetTransactionForDecode queries silver tables for every
	// hash, and when the silver-realtime-transformer is behind the
	// ingester (common during catch-up) those queries are empty but still
	// slow because silver_hot is IO-starved by the transformer's own
	// catch-up batches. The per-request latency explodes from a fast 500
	// ("no transactions found") to a 30+ second hang, which times out the
	// gateway's HTTP client and blocks whichever UI fragment is waiting.
	//
	// Until GetTransactionForDecode gains a bronze-only code path that
	// produces a usable DecodedTransaction without hitting silver, we
	// return the not-found error fast. That's strictly worse data (the tx
	// exists in bronze) but strictly better latency (fast 500 > 30s hang),
	// and the caller can still fall back to its own bronze display path.

	return nil, fmt.Errorf("no transactions found in ledger %d", ledgerSeq)
}

func (h *DecodeHandlers) getTransactionForDecode(ctx context.Context, txHash string) (*DecodedTransaction, error) {
	if h.hotPathReader != nil {
		decoded, err := h.hotPathReader.GetTransactionForDecode(ctx, txHash)
		if err == nil {
			return decoded, nil
		}
		if !strings.Contains(err.Error(), ErrTxNotFound.Error()) {
			log.Printf("decode hot-path fallback tx=%s err=%v", txHash, err)
		}
	}
	if h.coldReader == nil {
		return nil, fmt.Errorf("transaction decode requires cold reader")
	}
	return h.coldReader.GetTransactionForDecode(ctx, txHash, h.bronzeCold)
}

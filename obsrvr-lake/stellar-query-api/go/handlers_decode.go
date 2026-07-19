package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"
)

const contractObservedFunctionsTimeout = 500 * time.Millisecond

// DecodeHandlers contains HTTP handlers for transaction decoding and human-readable summaries
type DecodeHandlers struct {
	hotReader         *SilverHotReader
	coldReader        *SilverColdReader
	bronzeCold        *ColdReader
	silverReader      *UnifiedSilverReader
	hotPathReader     *TxHotPathReader
	indexReader       *IndexReader
	contractArtifacts ContractArtifactResolver
}

// NewDecodeHandlers creates new transaction decode API handlers
func NewDecodeHandlers(hotReader *SilverHotReader, coldReader *SilverColdReader, bronzeCold *ColdReader, silverReader *UnifiedSilverReader, hotPathReader *TxHotPathReader, indexReader *IndexReader, artifactResolvers ...ContractArtifactResolver) *DecodeHandlers {
	var artifacts ContractArtifactResolver
	if len(artifactResolvers) > 0 {
		artifacts = artifactResolvers[0]
	}
	return &DecodeHandlers{hotReader: hotReader, coldReader: coldReader, bronzeCold: bronzeCold, silverReader: silverReader, hotPathReader: hotPathReader, indexReader: indexReader, contractArtifacts: artifacts}
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
		badRequest(w, "transaction hash required")
		return
	}

	decoded, err := h.getTransactionForDisplay(r.Context(), txHash)
	if err != nil {
		internalError(w, err.Error())
		return
	}

	if decoded.OpCount == 0 && len(decoded.Events) == 0 {
		notFound(w, "transaction not found")
		return
	}

	respondJSON(w, decoded)
}

// HandleContractInterface returns the authoritative declared interface for a contract.
// @Summary Get authoritative contract interface
// @Description Resolves the contract's current executable, verifies its WASM hash, reports the verified byte size, and decodes the complete contractspecv0 interface. Observed calls are returned separately and are never treated as the declared interface. Use format=rust for a Rust-like text representation.
// @Tags Contracts
// @Accept json
// @Produce json
// @Param id path string true "Contract ID (C...)"
// @Param format query string false "Response format: json (default) or rust"
// @Success 200 {object} ContractInterfaceResponse "Authoritative declared interface"
// @Failure 400 {object} map[string]interface{} "Invalid contract ID or format"
// @Failure 404 {object} map[string]interface{} "Contract or active code not found"
// @Failure 503 {object} map[string]interface{} "Authoritative resolver unavailable"
// @Router /api/v1/silver/contracts/{id}/interface [get]
func (h *DecodeHandlers) HandleContractInterface(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		badRequest(w, "contract_id required")
		return
	}
	format := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("format")))
	if format != "" && format != "json" && format != "rust" {
		badRequest(w, "format must be json or rust")
		return
	}
	if h.contractArtifacts == nil {
		serviceUnavailable(w, "authoritative contract interface requires rpc_fallback configuration")
		return
	}
	response, err := h.contractArtifacts.Resolve(r.Context(), contractID)
	if err != nil {
		handleContractArtifactError(w, err)
		return
	}
	response.ObservedFunctions = h.observedContractFunctions(r.Context(), contractID)
	if format == "rust" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Contract-ID", response.ContractID)
		if response.Executable.WasmHash != "" {
			w.Header().Set("X-Wasm-SHA256", response.Executable.WasmHash)
		}
		_, _ = w.Write([]byte(RenderContractSpecRust(response.Interface)))
		return
	}
	respondJSON(w, response)
}

// HandleContractWASM downloads the hash-validated WASM for a contract's current executable.
// @Summary Download active contract WASM
// @Description Resolves the current executable by contract ID and streams the exact WASM after validating its SHA-256 ledger hash. Built-in Stellar Asset Contracts do not have downloadable WASM.
// @Tags Contracts
// @Produce application/wasm
// @Param id path string true "Contract ID (C...)"
// @Success 200 {file} binary "Contract WASM"
// @Failure 400 {object} map[string]interface{} "Invalid contract ID"
// @Failure 404 {object} map[string]interface{} "Contract/code not found or built-in SAC"
// @Failure 503 {object} map[string]interface{} "Authoritative resolver unavailable"
// @Router /api/v1/silver/contracts/{id}/wasm [get]
func (h *DecodeHandlers) HandleContractWASM(w http.ResponseWriter, r *http.Request) {
	contractID := mux.Vars(r)["id"]
	if contractID == "" {
		badRequest(w, "contract_id required")
		return
	}
	if h.contractArtifacts == nil {
		serviceUnavailable(w, "contract WASM download requires rpc_fallback configuration")
		return
	}
	artifact, err := h.contractArtifacts.ResolveWASM(r.Context(), contractID)
	if err != nil {
		handleContractArtifactError(w, err)
		return
	}
	etag := `"` + artifact.WasmHash + `"`
	if strings.TrimSpace(r.Header.Get("If-None-Match")) == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/wasm")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-%s.wasm"`, contractID, artifact.WasmHash))
	w.Header().Set("Content-Length", strconv.Itoa(len(artifact.WASM)))
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "public, max-age=60, must-revalidate")
	w.Header().Set("X-Contract-ID", contractID)
	w.Header().Set("X-Wasm-SHA256", artifact.WasmHash)
	if artifact.Executable.ResolvedAtLedger > 0 {
		w.Header().Set("X-Resolved-At-Ledger", strconv.FormatInt(artifact.Executable.ResolvedAtLedger, 10))
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(artifact.WASM)
}

func (h *DecodeHandlers) observedContractFunctions(ctx context.Context, contractID string) []string {
	lookupCtx, cancel := context.WithTimeout(ctx, contractObservedFunctionsTimeout)
	defer cancel()
	if h.hotReader != nil {
		if functions, err := h.hotReader.GetContractFunctions(lookupCtx, contractID); err == nil && len(functions) > 0 {
			return functions
		}
	}
	if lookupCtx.Err() != nil {
		return []string{}
	}
	if h.coldReader != nil {
		if functions, err := h.coldReader.GetContractFunctions(lookupCtx, contractID); err == nil {
			return functions
		}
	}
	return []string{}
}

func handleContractArtifactError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalidContractID):
		badRequest(w, err.Error())
	case errors.Is(err, ErrContractNotFound), errors.Is(err, ErrContractCodeAbsent):
		notFound(w, err.Error())
	case errors.Is(err, ErrContractIsSAC):
		notFound(w, "built-in Stellar Asset Contracts do not have downloadable WASM")
	default:
		serviceUnavailable(w, err.Error())
	}
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
	var req struct {
		XDR      string          `json:"xdr"`
		JSON     json.RawMessage `json:"json"`
		TypeHint string          `json:"type_hint"`
	}
	if err := readJSON(w, r, &req); err != nil {
		badRequest(w, "invalid JSON body: "+err.Error())
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
		badRequest(w, "either 'xdr' or 'json' field required")
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
		badRequest(w, "transaction hash required")
		return
	}

	ctx := r.Context()

	// 1. Get decoded transaction (summary + ops + events)
	decoded, err := h.getTransactionForDisplay(ctx, txHash)
	if err != nil {
		internalError(w, err.Error())
		return
	}

	if decoded.OpCount == 0 && len(decoded.Events) == 0 {
		notFound(w, "transaction not found")
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
		internalError(w, "batch decoded requires cold reader")
		return
	}

	ctx := r.Context()
	var hashes []string

	// Determine hashes from GET params or POST body
	if r.Method == http.MethodPost {
		var req struct {
			Hashes []string `json:"hashes"`
		}
		if err := readJSON(w, r, &req); err != nil {
			badRequest(w, "invalid JSON body: "+err.Error())
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
	var ledgerSeq int64
	if len(hashes) == 0 && ledgerParam != "" {
		seq, err := strconv.ParseInt(ledgerParam, 10, 64)
		if err != nil {
			badRequest(w, "invalid ledger sequence")
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
			internalError(w, "failed to resolve ledger transactions: "+err.Error())
			return
		}
		hashes = resolved
		fromLedger = true
		ledgerSeq = seq
	}

	if len(hashes) == 0 {
		badRequest(w, "provide 'hashes' (query or body) or 'ledger' parameter")
		return
	}
	// Hashes mode capped at 25; ledger mode respects its own limit (up to 100)
	if !fromLedger && len(hashes) > 25 {
		badRequest(w, "maximum 25 transactions per batch")
		return
	}

	// Decode transactions in parallel, but keep cold-storage pressure bounded.
	// Historical ledger mode can otherwise launch many DuckDB/S3 scans at once.
	type decodeResult struct {
		decoded *DecodedTransaction
		err     error
	}
	resultsByIndex := make([]decodeResult, len(hashes))

	// Each goroutine writes to a unique index in resultsByIndex, so no
	// synchronization is needed beyond errgroup's Wait.
	g, gctx := errgroup.WithContext(ctx)
	if fromLedger {
		g.SetLimit(2)
	} else {
		g.SetLimit(4)
	}
	for i, txHash := range hashes {
		i, txHash := i, txHash
		g.Go(func() error {
			decoded, err := h.getTransactionForDecodeWithLedgerHint(gctx, txHash, ledgerSeq)
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

func (h *DecodeHandlers) getTransactionForDisplay(ctx context.Context, txHash string) (*DecodedTransaction, error) {
	return h.getTransactionForDisplayWithLedgerHint(ctx, txHash, 0)
}

func (h *DecodeHandlers) getTransactionForDisplayWithLedgerHint(ctx context.Context, txHash string, ledgerHint int64) (*DecodedTransaction, error) {
	requestStart := time.Now()
	if h.hotPathReader != nil {
		hotStart := time.Now()
		hotCtx, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
		defer cancel()
		decoded, err := h.hotPathReader.GetTransactionForDecode(hotCtx, txHash)
		if err == nil {
			log.Printf("tx_display path=hot tx=%s duration_ms=%d total_ms=%d", txHash, time.Since(hotStart).Milliseconds(), time.Since(requestStart).Milliseconds())
			return decoded, nil
		}
		if !strings.Contains(err.Error(), ErrTxNotFound.Error()) {
			log.Printf("tx_display path=hot_fallback tx=%s duration_ms=%d err=%v", txHash, time.Since(hotStart).Milliseconds(), err)
		}
	}
	if h.coldReader == nil {
		return nil, fmt.Errorf("transaction decode requires cold reader")
	}
	ledgerSeq := ledgerHint
	if ledgerSeq == 0 && h.indexReader != nil {
		indexCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		loc, err := h.indexReader.LookupTransactionHash(indexCtx, txHash)
		cancel()
		if err == nil && loc != nil {
			ledgerSeq = loc.LedgerSequence
		} else if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			log.Printf("tx_display path=index_lookup_error tx=%s duration_ms=%d err=%v", txHash, time.Since(requestStart).Milliseconds(), err)
		}
	}
	coldStart := time.Now()
	coldCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	decoded, err := h.coldReader.GetTransactionForSemanticFastWithLedger(coldCtx, txHash, ledgerSeq, h.bronzeCold)
	if err == nil {
		log.Printf("tx_display path=cold_fast tx=%s ledger_hint=%d duration_ms=%d total_ms=%d", txHash, ledgerSeq, time.Since(coldStart).Milliseconds(), time.Since(requestStart).Milliseconds())
		return decoded, nil
	}
	if !strings.Contains(err.Error(), ErrTxNotFound.Error()) {
		log.Printf("tx_display path=cold_fast_error tx=%s ledger_hint=%d duration_ms=%d err=%v", txHash, ledgerSeq, time.Since(coldStart).Milliseconds(), err)
	}
	if coldCtx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || ledgerSeq > 0 {
		return nil, err
	}
	legacyStart := time.Now()
	decoded, err = h.coldReader.GetTransactionForDecode(ctx, txHash, h.bronzeCold)
	if err != nil {
		log.Printf("tx_display path=legacy_error tx=%s duration_ms=%d total_ms=%d err=%v", txHash, time.Since(legacyStart).Milliseconds(), time.Since(requestStart).Milliseconds(), err)
		return nil, err
	}
	log.Printf("tx_display path=legacy tx=%s duration_ms=%d total_ms=%d", txHash, time.Since(legacyStart).Milliseconds(), time.Since(requestStart).Milliseconds())
	return decoded, nil
}

func (h *DecodeHandlers) getTransactionForDecode(ctx context.Context, txHash string) (*DecodedTransaction, error) {
	return h.getTransactionForDisplay(ctx, txHash)
}

func (h *DecodeHandlers) getTransactionForDecodeWithLedgerHint(ctx context.Context, txHash string, ledgerHint int64) (*DecodedTransaction, error) {
	return h.getTransactionForDisplayWithLedgerHint(ctx, txHash, ledgerHint)
}

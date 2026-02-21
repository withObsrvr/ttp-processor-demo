package main

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/gorilla/mux"
)

// DecodeHandlers contains HTTP handlers for transaction decoding and human-readable summaries
type DecodeHandlers struct {
	reader *UnifiedDuckDBReader
}

// NewDecodeHandlers creates new transaction decode API handlers
func NewDecodeHandlers(reader *UnifiedDuckDBReader) *DecodeHandlers {
	return &DecodeHandlers{reader: reader}
}

// HandleDecodedTransaction returns a human-readable decoded transaction
// GET /api/v1/silver/tx/{hash}/decoded
func (h *DecodeHandlers) HandleDecodedTransaction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "transaction hash required", http.StatusBadRequest)
		return
	}

	decoded, err := h.reader.GetTransactionForDecode(r.Context(), txHash)
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
// GET /api/v1/silver/contracts/{id}/interface
func (h *DecodeHandlers) HandleContractInterface(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	// Get observed function names
	functions, err := h.reader.GetContractFunctions(r.Context(), contractID)
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
// POST /api/v1/silver/decode/scval
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

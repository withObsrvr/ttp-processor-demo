package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// DecodedTransactionHandler returns a human-readable decoded transaction.
func DecodedTransactionHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["hash"]
		if txHash == "" {
			respondError(w, http.StatusBadRequest, "transaction hash required")
			return
		}

		decoded, err := reader.GetTransactionForDecode(r.Context(), txHash)
		if err != nil {
			log.Printf("ERROR: decoded transaction: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to decode transaction")
			return
		}
		if decoded == nil {
			respondError(w, http.StatusNotFound, "transaction not found")
			return
		}
		respondJSON(w, http.StatusOK, decoded)
	}
}

// FullTransactionHandler returns a composite view: decoded tx + contracts + call graph.
func FullTransactionHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		txHash := vars["hash"]
		if txHash == "" {
			respondError(w, http.StatusBadRequest, "transaction hash required")
			return
		}

		ctx := r.Context()

		decoded, err := reader.GetTransactionForDecode(ctx, txHash)
		if err != nil {
			log.Printf("ERROR: full transaction: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to decode transaction")
			return
		}
		if decoded == nil {
			respondError(w, http.StatusNotFound, "transaction not found")
			return
		}

		contracts, _ := reader.GetContractsInvolved(ctx, txHash)
		callGraph, _ := reader.GetCallGraph(ctx, txHash)

		response := map[string]interface{}{
			"transaction":        decoded,
			"contracts_involved": contracts,
			"call_graph":         callGraph,
		}
		respondJSON(w, http.StatusOK, response)
	}
}

// BatchDecodedTransactionsHandler returns decoded transactions for multiple hashes or a ledger.
func BatchDecodedTransactionsHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var hashes []string

		if r.Method == "POST" {
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				respondError(w, http.StatusBadRequest, "failed to read request body")
				return
			}
			defer r.Body.Close()

			var req struct {
				Hashes []string `json:"hashes"`
			}
			if err := json.Unmarshal(body, &req); err == nil && len(req.Hashes) > 0 {
				hashes = req.Hashes
			}
		}

		if len(hashes) == 0 {
			hashParam := r.URL.Query().Get("hashes")
			if hashParam != "" {
				for _, h := range strings.Split(hashParam, ",") {
					h = strings.TrimSpace(h)
					if h != "" {
						hashes = append(hashes, h)
					}
				}
			}
		}

		// If ledger specified, get hashes from that ledger
		if len(hashes) == 0 {
			ledgerSeq := parseIntParam(r, "ledger", 0)
			if ledgerSeq > 0 {
				limit := int(parseIntParam(r, "limit", 25))
				if limit > 100 {
					limit = 100
				}
				txHashes, err := reader.GetTransactionHashesForLedger(ctx, ledgerSeq, limit)
				if err == nil {
					hashes = txHashes
				}
			}
		}

		if len(hashes) == 0 {
			respondError(w, http.StatusBadRequest, "hashes or ledger parameter required")
			return
		}
		if len(hashes) > 25 {
			hashes = hashes[:25]
		}

		var results []interface{}
		for _, h := range hashes {
			decoded, err := reader.GetTransactionForDecode(ctx, h)
			if err == nil && decoded != nil {
				results = append(results, decoded)
			}
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"transactions": results,
			"count":        len(results),
		})
	}
}

// ContractInterfaceHandler returns the detected interface for a contract.
func ContractInterfaceHandler(reader *DuckLakeReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		contractID := vars["id"]
		if contractID == "" {
			respondError(w, http.StatusBadRequest, "contract_id required")
			return
		}

		functions, err := reader.GetContractFunctions(r.Context(), contractID)
		if err != nil {
			log.Printf("ERROR: contract interface: %v", err)
			respondError(w, http.StatusInternalServerError, "failed to retrieve contract interface")
			return
		}

		contractType := DetectContractType(functions)

		if contractType == ContractTypeSEP41 {
			iface := GetSEP41Interface(contractID)
			respondJSON(w, http.StatusOK, map[string]interface{}{
				"contract_id":        contractID,
				"detected_type":      string(contractType),
				"interface":          iface.Functions,
				"observed_functions": functions,
			})
			return
		}

		respondJSON(w, http.StatusOK, map[string]interface{}{
			"contract_id":        contractID,
			"detected_type":      string(ContractTypeUnknown),
			"observed_functions": functions,
		})
	}
}

// DecodeScValHandler decodes an ScVal from XDR or JSON.
func DecodeScValHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			respondError(w, http.StatusBadRequest, "failed to read request body")
			return
		}
		defer r.Body.Close()

		var req struct {
			XDR      string          `json:"xdr"`
			JSON     json.RawMessage `json:"json"`
			TypeHint string          `json:"type_hint"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			respondError(w, http.StatusBadRequest, "invalid JSON body")
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
			respondError(w, http.StatusBadRequest, "either 'xdr' or 'json' field required")
			return
		}

		respondJSON(w, http.StatusOK, decoded)
	}
}

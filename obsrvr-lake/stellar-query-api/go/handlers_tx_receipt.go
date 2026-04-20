package main

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

// TxReceiptHandlers serves GET /api/v1/silver/tx/{hash}/receipt.
//
// Single-lookup alternative to Prism's existing per-page pattern of hitting
// /full + /semantic + /effects + /diffs + /events in parallel (observed
// P95 ~5s bounded by /diffs). This endpoint does one indexed PK read against
// serving.sv_tx_receipts and returns the pre-materialized wide JSON row.
//
// Response shape is a thin envelope around the projected columns — most of
// the weight is in the JSONB fields (full_json, semantic_json, effects_json,
// diffs_json, events_json). Prism consumes the ones it needs per page section.
//
// If the row is missing (row not yet projected / tx too recent / projector
// offline), returns 404 so the frontend can fall back to the classic
// per-section endpoints.
type TxReceiptHandlers struct {
	// Uses the SilverHotReader's PG handle — serving tables live in silver_hot
	// alongside public.* per the existing architecture.
	hotReader *SilverHotReader
}

func NewTxReceiptHandlers(hotReader *SilverHotReader) *TxReceiptHandlers {
	return &TxReceiptHandlers{hotReader: hotReader}
}

type TxReceiptResponse struct {
	TxHash             string          `json:"tx_hash"`
	LedgerSequence     int64           `json:"ledger_sequence"`
	CreatedAt          string          `json:"created_at"`
	SourceAccount      *string         `json:"source_account,omitempty"`
	Successful         bool            `json:"successful"`
	OperationCount     *int            `json:"operation_count,omitempty"`
	TxType             *string         `json:"tx_type,omitempty"`
	PrimaryContractID  *string         `json:"primary_contract_id,omitempty"`
	InvolvedContracts  []string        `json:"involved_contracts,omitempty"`
	InvolvedAccounts   []string        `json:"involved_accounts,omitempty"`

	// Raw JSONB sections — callers pick whichever they need.
	Full               json.RawMessage `json:"full,omitempty"`
	Semantic           json.RawMessage `json:"semantic,omitempty"`
	Effects            json.RawMessage `json:"effects,omitempty"`
	Diffs              json.RawMessage `json:"diffs,omitempty"`
	Events             json.RawMessage `json:"events,omitempty"`

	MaterializedAt     string          `json:"materialized_at"`
	SourceVersion      string          `json:"source_version"`
}

// HandleTxReceipt serves GET /api/v1/silver/tx/{hash}/receipt.
func (h *TxReceiptHandlers) HandleTxReceipt(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.hotReader == nil || h.hotReader.db == nil {
		respondError(w, "silver hot reader unavailable", http.StatusServiceUnavailable)
		return
	}
	vars := mux.Vars(r)
	txHash := vars["hash"]
	if txHash == "" {
		respondError(w, "tx hash required", http.StatusBadRequest)
		return
	}

	const q = `
		SELECT tx_hash, ledger_sequence, created_at, source_account, successful,
		       operation_count, tx_type, primary_contract_id,
		       involved_contracts, involved_accounts,
		       full_json, semantic_json, effects_json, diffs_json, events_json,
		       materialized_at, source_version
		  FROM serving.sv_tx_receipts
		 WHERE tx_hash = $1
	`
	var (
		resp              TxReceiptResponse
		createdAt         sql.NullString
		sourceAccount     sql.NullString
		opCount           sql.NullInt64
		txType            sql.NullString
		primaryContract   sql.NullString
		involvedContracts []string
		involvedAccounts  []string
		fullJSON          []byte
		semanticJSON      []byte
		effectsJSON       []byte
		diffsJSON         []byte
		eventsJSON        []byte
		materializedAt    sql.NullString
	)

	err := h.hotReader.db.QueryRowContext(r.Context(), q, txHash).Scan(
		&resp.TxHash, &resp.LedgerSequence, &createdAt, &sourceAccount, &resp.Successful,
		&opCount, &txType, &primaryContract,
		pq.Array(&involvedContracts), pq.Array(&involvedAccounts),
		&fullJSON, &semanticJSON, &effectsJSON, &diffsJSON, &eventsJSON,
		&materializedAt, &resp.SourceVersion,
	)
	if err == sql.ErrNoRows {
		respondError(w, "receipt not yet materialized for tx "+txHash, http.StatusNotFound)
		return
	}
	if err != nil {
		respondError(w, "tx receipt query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if createdAt.Valid {
		resp.CreatedAt = createdAt.String
	}
	if sourceAccount.Valid {
		s := sourceAccount.String
		resp.SourceAccount = &s
	}
	if opCount.Valid {
		v := int(opCount.Int64)
		resp.OperationCount = &v
	}
	if txType.Valid {
		s := txType.String
		resp.TxType = &s
	}
	if primaryContract.Valid {
		s := primaryContract.String
		resp.PrimaryContractID = &s
	}
	resp.InvolvedContracts = involvedContracts
	resp.InvolvedAccounts = involvedAccounts
	if len(fullJSON) > 0 {
		resp.Full = fullJSON
	}
	if len(semanticJSON) > 0 {
		resp.Semantic = semanticJSON
	}
	if len(effectsJSON) > 0 {
		resp.Effects = effectsJSON
	}
	if len(diffsJSON) > 0 {
		resp.Diffs = diffsJSON
	}
	if len(eventsJSON) > 0 {
		resp.Events = eventsJSON
	}
	if materializedAt.Valid {
		resp.MaterializedAt = materializedAt.String
	}

	respondJSON(w, resp)
}


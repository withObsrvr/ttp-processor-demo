package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/lib/pq"
)

// ContractTransactionsHandlers serves transaction lookups by contract_id
// backed by silver_hot.contract_invocations_raw.
type ContractTransactionsHandlers struct {
	hot *SilverHotReader
}

// NewContractTransactionsHandlers constructs the handler group. The hot reader
// must be non-nil — this endpoint is hot-only because contract_invocations_raw
// is not flushed to cold in today's pipeline.
func NewContractTransactionsHandlers(hot *SilverHotReader) *ContractTransactionsHandlers {
	return &ContractTransactionsHandlers{hot: hot}
}

// ContractTransactionEntry is a single tx row returned by the endpoint.
type ContractTransactionEntry struct {
	ContractID      string  `json:"contract_id"`
	TransactionHash string  `json:"transaction_hash"`
	LedgerSequence  int64   `json:"ledger_sequence"`
	OperationIndex  int     `json:"operation_index"`
	FunctionName    *string `json:"function_name,omitempty"`
	SourceAccount   *string `json:"source_account,omitempty"`
	Successful      bool    `json:"successful"`
	ClosedAt        *string `json:"closed_at,omitempty"`
}

// HandleContractTransactions serves GET /api/v1/silver/contracts/{contract_id}/transactions
//
// Query params:
//
//	function_name   exact match on function_name
//	function_any    comma-separated list of function names (ANY of)
//	successful      "true" | "false" to filter by tx success
//	after           RFC3339 lower bound on closed_at (inclusive)
//	before          RFC3339 upper bound on closed_at (exclusive)
//	from_ledger     lower bound on ledger_sequence (inclusive)
//	to_ledger       upper bound on ledger_sequence (exclusive)
//	limit           1..500, default 100
//	order           "asc" | "desc" on ledger_sequence (default desc)
func (h *ContractTransactionsHandlers) HandleContractTransactions(w http.ResponseWriter, r *http.Request) {
	if h.hot == nil {
		respondError(w, "silver_hot reader not available", http.StatusServiceUnavailable)
		return
	}

	vars := mux.Vars(r)
	contractID := strings.TrimSpace(vars["contract_id"])
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	limit := parseIntParam(r, "limit", 100, 1, 500)
	q := r.URL.Query()

	order := "DESC"
	if strings.EqualFold(q.Get("order"), "asc") {
		order = "ASC"
	}

	sqlQ := `
		SELECT contract_id, transaction_hash, ledger_sequence, operation_index,
		       function_name, source_account, successful,
		       -- Cast TIMESTAMP → ISO-8601 text so Scan into sql.NullString
		       -- below works; otherwise lib/pq/pgx returns time.Time which
		       -- can't be scanned into NullString.
		       to_char(closed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS closed_at
		FROM contract_invocations_raw
		WHERE contract_id = $1
	`
	args := []any{contractID}
	idx := 2

	if fn := q.Get("function_name"); fn != "" {
		sqlQ += " AND function_name = $" + strconv.Itoa(idx)
		args = append(args, fn)
		idx++
	}
	if fa := q.Get("function_any"); fa != "" {
		if fns := splitAndTrim(fa); len(fns) > 0 {
			sqlQ += " AND function_name = ANY($" + strconv.Itoa(idx) + ")"
			args = append(args, pq.Array(fns))
			idx++
		}
	}
	if s := q.Get("successful"); s != "" {
		switch strings.ToLower(s) {
		case "true":
			sqlQ += " AND successful = TRUE"
		case "false":
			sqlQ += " AND successful = FALSE"
		default:
			respondError(w, "successful must be 'true' or 'false'", http.StatusBadRequest)
			return
		}
	}
	if v := q.Get("after"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			respondError(w, "invalid 'after' timestamp: must be RFC3339", http.StatusBadRequest)
			return
		}
		sqlQ += " AND closed_at >= $" + strconv.Itoa(idx)
		args = append(args, t)
		idx++
	}
	if v := q.Get("before"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			respondError(w, "invalid 'before' timestamp: must be RFC3339", http.StatusBadRequest)
			return
		}
		sqlQ += " AND closed_at < $" + strconv.Itoa(idx)
		args = append(args, t)
		idx++
	}
	if v := q.Get("from_ledger"); v != "" {
		n, err := parsePositiveInt64(v)
		if err != nil {
			respondError(w, "invalid from_ledger: "+err.Error(), http.StatusBadRequest)
			return
		}
		sqlQ += " AND ledger_sequence >= $" + strconv.Itoa(idx)
		args = append(args, n)
		idx++
	}
	if v := q.Get("to_ledger"); v != "" {
		n, err := parsePositiveInt64(v)
		if err != nil {
			respondError(w, "invalid to_ledger: "+err.Error(), http.StatusBadRequest)
			return
		}
		sqlQ += " AND ledger_sequence < $" + strconv.Itoa(idx)
		args = append(args, n)
		idx++
	}

	sqlQ += " ORDER BY ledger_sequence " + order + ", operation_index " + order
	sqlQ += " LIMIT $" + strconv.Itoa(idx)
	args = append(args, limit+1) // +1 to detect has_more

	rows, err := h.hot.db.QueryContext(r.Context(), sqlQ, args...)
	if err != nil {
		respondError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	out := make([]ContractTransactionEntry, 0, limit)
	for rows.Next() {
		var e ContractTransactionEntry
		var fn, src, closed sql.NullString
		if err := rows.Scan(
			&e.ContractID, &e.TransactionHash, &e.LedgerSequence, &e.OperationIndex,
			&fn, &src, &e.Successful, &closed,
		); err != nil {
			respondError(w, "scan failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if fn.Valid {
			e.FunctionName = &fn.String
		}
		if src.Valid {
			e.SourceAccount = &src.String
		}
		if closed.Valid {
			e.ClosedAt = &closed.String
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		respondError(w, "rows error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	hasMore := len(out) > limit
	if hasMore {
		out = out[:limit]
	}

	respondJSON(w, map[string]any{
		"contract_id":  contractID,
		"transactions": out,
		"count":        len(out),
		"has_more":     hasMore,
	})
}

// parsePositiveInt64 parses a string as a non-negative int64.
func parsePositiveInt64(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("must be >= 0")
	}
	return n, nil
}

package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/stellar/go-stellar-sdk/strkey"
)

type RelationshipEdge struct {
	LedgerSequence  int64   `json:"ledger_sequence"`
	ClosedAt        string  `json:"closed_at"`
	TransactionHash string  `json:"transaction_hash"`
	InteractionType string  `json:"interaction_type"`
	AddressA        string  `json:"address_a"`
	AddressB        string  `json:"address_b"`
	Direction       string  `json:"direction"`
	Asset           *string `json:"asset,omitempty"`
	ContractID      *string `json:"contract_id,omitempty"`
	FunctionName    *string `json:"function_name,omitempty"`
	Amount          *string `json:"amount,omitempty"`
	SourceTable     string  `json:"source_table"`
	Confidence      string  `json:"confidence"`
}

type RelationshipCoverage struct {
	Version     string   `json:"version"`
	Includes    []string `json:"includes"`
	Limitations []string `json:"limitations"`
}

type RelationshipResponse struct {
	AddressA string               `json:"address_a"`
	AddressB string               `json:"address_b"`
	Edges    []RelationshipEdge   `json:"edges"`
	Count    int                  `json:"count"`
	Cursor   string               `json:"cursor,omitempty"`
	HasMore  bool                 `json:"has_more"`
	Coverage RelationshipCoverage `json:"coverage"`
}

type RelationshipFilters struct {
	AddressA    string
	AddressB    string
	Limit       int
	Cursor      *RelationshipCursor
	StartLedger int64
	EndLedger   int64
	Order       string
}

type RelationshipCursor struct {
	LedgerSequence  int64
	ClosedAt        time.Time
	TransactionHash string
	EdgeID          string
	Order           string
}

func (c RelationshipCursor) Encode() string {
	payload := struct {
		LedgerSequence  int64  `json:"l"`
		ClosedAt        string `json:"t"`
		TransactionHash string `json:"h"`
		EdgeID          string `json:"e"`
		Order           string `json:"o"`
	}{
		LedgerSequence:  c.LedgerSequence,
		ClosedAt:        c.ClosedAt.Format(time.RFC3339Nano),
		TransactionHash: c.TransactionHash,
		EdgeID:          c.EdgeID,
		Order:           c.Order,
	}
	raw, _ := json.Marshal(payload)
	return base64.URLEncoding.EncodeToString(raw)
}

func DecodeRelationshipCursor(cursor string) (*RelationshipCursor, error) {
	if cursor == "" {
		return nil, nil
	}
	decoded, err := base64.URLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	var payload struct {
		LedgerSequence  int64  `json:"l"`
		ClosedAt        string `json:"t"`
		TransactionHash string `json:"h"`
		EdgeID          string `json:"e"`
		Order           string `json:"o"`
	}
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, fmt.Errorf("invalid cursor format: %w", err)
	}
	ts, err := time.Parse(time.RFC3339Nano, payload.ClosedAt)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp in cursor: %w", err)
	}
	order := payload.Order
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		return nil, fmt.Errorf("invalid order in cursor")
	}
	return &RelationshipCursor{LedgerSequence: payload.LedgerSequence, ClosedAt: ts, TransactionHash: payload.TransactionHash, EdgeID: payload.EdgeID, Order: order}, nil
}

func relationshipCoverage() RelationshipCoverage {
	return RelationshipCoverage{
		Version: "v1",
		Includes: []string{
			"direct value transfers from token_transfers_raw and semantic_flows_value",
			"account-to-contract calls from contract_invocations_raw",
			"contract-to-contract calls from contract_invocation_calls",
			"conservative co-event edges from typed semantic_activities columns",
		},
		Limitations: []string{
			"does not scan arbitrary Soroban SCVal arguments or event topic payloads",
			"does not infer smart-wallet signer/controller relationships",
			"does not infer indirect router, pool, aggregator, or multi-hop relationships",
		},
	}
}

func validRelationshipAddress(address string) bool {
	if address == "" {
		return false
	}
	if _, err := strkey.Decode(strkey.VersionByteAccountID, address); err == nil {
		return true
	}
	if _, err := strkey.Decode(strkey.VersionByteContract, address); err == nil {
		return true
	}
	return false
}

func nullStringPtr(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	return &v.String
}

// HandleRelationship returns interactions between two addresses.
// @Summary Get address relationship history
// @Description Returns a paginated v1 interaction history between two Stellar addresses. v1 includes direct value transfers, account-to-contract calls, contract-to-contract calls where indexed, and conservative typed-column co-event edges. The response includes coverage limitations; v1 does not scan arbitrary SCVal arguments, raw XDR/JSON payloads, or undecoded event topics.
// @Tags Relationships
// @Accept json
// @Produce json
// @Param address_a path string true "First Stellar account (G...) or contract (C...) address"
// @Param address_b path string true "Second Stellar account (G...) or contract (C...) address"
// @Param limit query int false "Page size (default 100, max 500)"
// @Param cursor query string false "Opaque pagination cursor from previous response"
// @Param order query string false "Sort order: desc or asc (default desc)"
// @Param start_ledger query int false "Inclusive starting ledger sequence"
// @Param end_ledger query int false "Inclusive ending ledger sequence"
// @Success 200 {object} RelationshipResponse "Relationship edges with coverage limitations"
// @Failure 400 {object} map[string]interface{} "Invalid address, cursor, order, or ledger bound"
// @Failure 503 {object} map[string]interface{} "Unified reader unavailable"
// @Failure 500 {object} map[string]interface{} "Internal server error"
// @Router /api/v1/silver/relationships/{address_a}/{address_b} [get]
func (h *SilverHandlers) HandleRelationship(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	addressA := vars["address_a"]
	addressB := vars["address_b"]
	if !validRelationshipAddress(addressA) || !validRelationshipAddress(addressB) {
		respondError(w, "address_a and address_b must be valid Stellar account (G...) or contract (C...) addresses", http.StatusBadRequest)
		return
	}
	if h.unifiedReader == nil {
		respondError(w, "relationship endpoint requires unified reader", http.StatusServiceUnavailable)
		return
	}
	limit := parseLimit(r, 100, 500)
	order := strings.ToLower(r.URL.Query().Get("order"))
	if order == "" {
		order = "desc"
	}
	if order != "asc" && order != "desc" {
		respondError(w, "order must be asc or desc", http.StatusBadRequest)
		return
	}
	cursor, err := DecodeRelationshipCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if cursor != nil && cursor.Order != "" {
		order = cursor.Order
	}
	filters := RelationshipFilters{AddressA: addressA, AddressB: addressB, Limit: limit, Cursor: cursor, Order: order}
	if v := r.URL.Query().Get("start_ledger"); v != "" {
		filters.StartLedger, err = strconv.ParseInt(v, 10, 64)
		if err != nil || filters.StartLedger < 0 {
			respondError(w, "invalid start_ledger", http.StatusBadRequest)
			return
		}
	}
	if v := r.URL.Query().Get("end_ledger"); v != "" {
		filters.EndLedger, err = strconv.ParseInt(v, 10, 64)
		if err != nil || filters.EndLedger < 0 {
			respondError(w, "invalid end_ledger", http.StatusBadRequest)
			return
		}
	}
	edges, nextCursor, hasMore, err := h.unifiedReader.GetRelationshipEdges(r.Context(), filters)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	respondJSON(w, RelationshipResponse{
		AddressA: addressA,
		AddressB: addressB,
		Edges:    edges,
		Count:    len(edges),
		Cursor:   nextCursor,
		HasMore:  hasMore,
		Coverage: relationshipCoverage(),
	})
}

func (r *UnifiedDuckDBReader) GetRelationshipEdges(ctx context.Context, filters RelationshipFilters) ([]RelationshipEdge, string, bool, error) {
	requestLimit := filters.Limit + 1
	orderDir := "DESC"
	cursorOp := "<"
	if filters.Order == "asc" {
		orderDir = "ASC"
		cursorOp = ">"
	}
	whereParts := []string{"1=1"}
	args := []interface{}{filters.AddressA, filters.AddressB}
	argNum := 3
	if filters.StartLedger > 0 {
		whereParts = append(whereParts, fmt.Sprintf("ledger_sequence >= $%d", argNum))
		args = append(args, filters.StartLedger)
		argNum++
	}
	if filters.EndLedger > 0 {
		whereParts = append(whereParts, fmt.Sprintf("ledger_sequence <= $%d", argNum))
		args = append(args, filters.EndLedger)
		argNum++
	}
	if filters.Cursor != nil {
		whereParts = append(whereParts, fmt.Sprintf(`(
			ledger_sequence %s $%d
			OR (ledger_sequence = $%d AND closed_at %s $%d)
			OR (ledger_sequence = $%d AND closed_at = $%d AND transaction_hash %s $%d)
			OR (ledger_sequence = $%d AND closed_at = $%d AND transaction_hash = $%d AND edge_id %s $%d)
		)`, cursorOp, argNum, argNum, cursorOp, argNum+1, argNum, argNum+1, cursorOp, argNum+2, argNum, argNum+1, argNum+2, cursorOp, argNum+3))
		args = append(args, filters.Cursor.LedgerSequence, filters.Cursor.ClosedAt, filters.Cursor.TransactionHash, filters.Cursor.EdgeID)
		argNum += 4
	}
	whereClause := strings.Join(whereParts, " AND ")
	args = append(args, requestLimit)
	query := buildRelationshipQuery(r.hotSchema, r.coldSchema, whereClause, orderDir, argNum)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", false, fmt.Errorf("GetRelationshipEdges: %w", err)
	}
	defer rows.Close()
	var edges []RelationshipEdge
	var edgeIDs []string
	for rows.Next() {
		var e RelationshipEdge
		var closedAt time.Time
		var asset, contractID, functionName, amount sql.NullString
		var edgeID string
		if err := rows.Scan(&e.LedgerSequence, &closedAt, &e.TransactionHash, &e.InteractionType, &e.AddressA, &e.AddressB, &e.Direction, &asset, &contractID, &functionName, &amount, &e.SourceTable, &e.Confidence, &edgeID); err != nil {
			return nil, "", false, err
		}
		e.ClosedAt = closedAt.UTC().Format(time.RFC3339Nano)
		e.Asset = nullStringPtr(asset)
		e.ContractID = nullStringPtr(contractID)
		e.FunctionName = nullStringPtr(functionName)
		e.Amount = nullStringPtr(amount)
		edges = append(edges, e)
		edgeIDs = append(edgeIDs, edgeID)
	}
	if err := rows.Err(); err != nil {
		return nil, "", false, err
	}
	hasMore := len(edges) > filters.Limit
	if hasMore {
		edges = edges[:filters.Limit]
	}
	nextCursor := ""
	if hasMore && len(edges) > 0 {
		last := edges[len(edges)-1]
		ts, err := time.Parse(time.RFC3339Nano, last.ClosedAt)
		if err != nil {
			return nil, "", false, fmt.Errorf("internal cursor timestamp parse failed: %w", err)
		}
		nextCursor = RelationshipCursor{LedgerSequence: last.LedgerSequence, ClosedAt: ts, TransactionHash: last.TransactionHash, EdgeID: edgeIDs[len(edges)-1], Order: filters.Order}.Encode()
	}
	return edges, nextCursor, hasMore, nil
}

func buildRelationshipQuery(hotSchema, coldSchema, whereClause, orderDir string, limitArg int) string {
	one := func(schema string, includeCallGraph bool, sourceRank int) string {
		parts := []string{fmt.Sprintf(`
		SELECT ledger_sequence, timestamp AS closed_at, transaction_hash,
		       'transfer' AS interaction_type, $1 AS address_a, $2 AS address_b,
		       CASE WHEN from_account = $1 AND to_account = $2 THEN 'a_to_b' ELSE 'b_to_a' END AS direction,
		       COALESCE(asset_code, token_contract_id) AS asset, token_contract_id AS contract_id, NULL AS function_name,
		       CAST(amount AS VARCHAR) AS amount, 'token_transfers_raw' AS source_table, 'high' AS confidence,
		       'token_transfers_raw|' || transaction_hash || '|' || CAST(ledger_sequence AS VARCHAR) || '|' || source_type || '|' ||
		           COALESCE(from_account, '') || '|' || COALESCE(to_account, '') || '|' || COALESCE(token_contract_id, '') || '|' ||
		           COALESCE(CAST(event_index AS VARCHAR), '') || '|' || COALESCE(CAST(amount AS VARCHAR), '') AS edge_id,
		       %d AS source_rank
		FROM %s.token_transfers_raw
		WHERE transaction_successful = true AND ((from_account = $1 AND to_account = $2) OR (from_account = $2 AND to_account = $1))`, sourceRank, schema),
			fmt.Sprintf(`
		SELECT ledger_sequence, timestamp, transaction_hash,
		       'transfer', $1, $2,
		       CASE WHEN from_account = $1 AND to_account = $2 THEN 'a_to_b' ELSE 'b_to_a' END,
		       COALESCE(asset_code, contract_id), contract_id, NULL, CAST(amount AS VARCHAR), 'semantic_flows_value', 'high',
		       'semantic_flows_value|' || id AS edge_id,
		       %d AS source_rank
		FROM %s.semantic_flows_value sfv
		WHERE successful = true AND ((from_account = $1 AND to_account = $2) OR (from_account = $2 AND to_account = $1))
		  AND NOT EXISTS (
		      SELECT 1 FROM %s.token_transfers_raw ttr
		      WHERE ttr.transaction_hash = sfv.transaction_hash
		        AND ttr.ledger_sequence = sfv.ledger_sequence
		        AND COALESCE(ttr.from_account, '') = COALESCE(sfv.from_account, '')
		        AND COALESCE(ttr.to_account, '') = COALESCE(sfv.to_account, '')
		  )`, sourceRank, schema, schema),
			fmt.Sprintf(`
		SELECT ledger_sequence, closed_at, transaction_hash,
		       'contract_call', $1, $2,
		       CASE WHEN source_account = $1 AND contract_id = $2 THEN 'a_to_b' ELSE 'b_to_a' END,
		       NULL, contract_id, function_name, NULL, 'contract_invocations_raw', 'high',
		       'contract_invocations_raw|' || transaction_hash || '|' || CAST(ledger_sequence AS VARCHAR) || '|' ||
		           CAST(transaction_index AS VARCHAR) || '|' || CAST(operation_index AS VARCHAR) AS edge_id,
		       %d AS source_rank
		FROM %s.contract_invocations_raw
		WHERE successful = true AND ((source_account = $1 AND contract_id = $2) OR (source_account = $2 AND contract_id = $1))`, sourceRank, schema),
			fmt.Sprintf(`
		SELECT ledger_sequence, timestamp, transaction_hash,
		       'co_event', $1, $2, 'same_activity', COALESCE(asset_code, asset_issuer), contract_id, soroban_function_name,
		       CAST(amount AS VARCHAR), 'semantic_activities', 'medium',
		       'semantic_activities|' || id AS edge_id,
		       %d AS source_rank
		FROM %s.semantic_activities
		WHERE successful = true AND (
			(source_account IN ($1, $2) AND destination_account IN ($1, $2) AND source_account <> destination_account)
			OR (source_account IN ($1, $2) AND contract_id IN ($1, $2))
		)`, sourceRank, schema)}
		if includeCallGraph {
			parts = append(parts, fmt.Sprintf(`
		SELECT ledger_sequence, closed_at, transaction_hash,
		       'contract_call', $1, $2,
		       CASE WHEN from_contract = $1 AND to_contract = $2 THEN 'a_to_b' ELSE 'b_to_a' END,
		       NULL, to_contract, function_name, NULL, 'contract_invocation_calls', 'high',
		       'contract_invocation_calls|' || CAST(call_id AS VARCHAR) AS edge_id,
		       %d AS source_rank
		FROM %s.contract_invocation_calls
		WHERE successful = true AND ((from_contract = $1 AND to_contract = $2) OR (from_contract = $2 AND to_contract = $1))`, sourceRank, schema))
		}
		return strings.Join(parts, "\n\t\tUNION ALL\n")
	}

	combined := []string{}
	if strings.TrimSpace(hotSchema) != "" {
		combined = append(combined, one(hotSchema, true, 1))
	}
	if strings.TrimSpace(coldSchema) != "" {
		combined = append(combined, one(coldSchema, false, 2))
	}
	if len(combined) == 0 {
		combined = append(combined, `
		SELECT 0::BIGINT AS ledger_sequence, NOW() AS closed_at, '' AS transaction_hash, '' AS interaction_type,
		       '' AS address_a, '' AS address_b, '' AS direction, NULL AS asset, NULL AS contract_id, NULL AS function_name,
		       NULL AS amount, '' AS source_table, '' AS confidence, '' AS edge_id, 0 AS source_rank
		WHERE false`)
	}

	return fmt.Sprintf(`
		WITH combined AS (
			%s
		), filtered AS (
			SELECT * FROM combined WHERE %s
		), deduped AS (
			SELECT * EXCLUDE (rn)
			FROM (
				SELECT *, ROW_NUMBER() OVER (PARTITION BY edge_id ORDER BY source_rank ASC) AS rn
				FROM filtered
			) ranked
			WHERE rn = 1
		)
		SELECT ledger_sequence, closed_at, transaction_hash, interaction_type, address_a, address_b, direction,
		       asset, contract_id, function_name, amount, source_table, confidence, edge_id
		FROM deduped
		ORDER BY ledger_sequence %s, closed_at %s, transaction_hash %s, edge_id %s
		LIMIT $%d`, strings.Join(combined, "\n\t\t\tUNION ALL\n"), whereClause, orderDir, orderDir, orderDir, orderDir, limitArg)
}

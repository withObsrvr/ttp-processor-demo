package main

import (
	"database/sql"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

func TestRelationshipCursorRoundTrip(t *testing.T) {
	want := RelationshipCursor{
		LedgerSequence:  3207008,
		ClosedAt:        time.Date(2026, 6, 21, 12, 34, 56, 789, time.UTC),
		TransactionHash: "0eb7ae2ec92cfd6350db651d576d4a0951c97bc684c2a53d6c4d3c34fab87789",
		EdgeID:          "token_transfers_raw|example",
		Order:           "desc",
	}
	got, err := DecodeRelationshipCursor(want.Encode())
	if err != nil {
		t.Fatalf("DecodeRelationshipCursor returned error: %v", err)
	}
	if got.LedgerSequence != want.LedgerSequence || !got.ClosedAt.Equal(want.ClosedAt) || got.TransactionHash != want.TransactionHash || got.EdgeID != want.EdgeID || got.Order != want.Order {
		t.Fatalf("cursor round trip mismatch: got %+v want %+v", got, want)
	}
}

func TestBuildRelationshipQueryKeepsColdToExistingTables(t *testing.T) {
	query := buildRelationshipQuery("hot_db.public", "cold_db.silver", "1=1", "DESC", 3)
	if !strings.Contains(query, "hot_db.public.contract_invocation_calls") {
		t.Fatalf("expected hot contract_invocation_calls in query")
	}
	if strings.Contains(query, "cold_db.silver.contract_invocation_calls") {
		t.Fatalf("cold schema does not currently define contract_invocation_calls; query must not reference it")
	}
	for _, table := range []string{"token_transfers_raw", "semantic_flows_value", "contract_invocations_raw", "semantic_activities"} {
		if !strings.Contains(query, "cold_db.silver."+table) {
			t.Fatalf("expected cold table %s in query", table)
		}
	}
	if !strings.Contains(query, "PARTITION BY edge_id") {
		t.Fatalf("expected hot/cold dedupe by edge_id")
	}
	if !strings.Contains(query, "transaction_hash DESC, edge_id DESC") {
		t.Fatalf("expected stable edge_id tie-breaker in ORDER BY")
	}
}

func TestBuildRelationshipQueryAllowsEmptyColdSchema(t *testing.T) {
	query := buildRelationshipQuery("hot_db.public", "", "1=1", "DESC", 3)
	if strings.Contains(query, "FROM .") {
		t.Fatalf("query should not reference empty schema: %s", query)
	}
	if !strings.Contains(query, "hot_db.public.token_transfers_raw") {
		t.Fatalf("expected hot tables in query")
	}
}

// --- cursor decode error paths (pure unit; cursor is the untrusted-input surface) ---

func TestDecodeRelationshipCursorErrors(t *testing.T) {
	if c, err := DecodeRelationshipCursor(""); err != nil || c != nil {
		t.Fatalf("empty cursor: got (%+v, %v), want (nil, nil)", c, err)
	}
	if _, err := DecodeRelationshipCursor("@@@not base64@@@"); err == nil {
		t.Fatal("expected error for invalid base64 cursor")
	}
	if _, err := DecodeRelationshipCursor(base64.URLEncoding.EncodeToString([]byte("not json"))); err == nil {
		t.Fatal("expected error for non-JSON cursor")
	}
	badTS := base64.URLEncoding.EncodeToString([]byte(`{"l":1,"t":"not-a-time","h":"x","e":"y","o":"desc"}`))
	if _, err := DecodeRelationshipCursor(badTS); err == nil {
		t.Fatal("expected error for bad timestamp in cursor")
	}
	badOrder := base64.URLEncoding.EncodeToString([]byte(`{"l":1,"t":"2026-06-21T00:00:00Z","h":"x","e":"y","o":"sideways"}`))
	if _, err := DecodeRelationshipCursor(badOrder); err == nil {
		t.Fatal("expected error for invalid order in cursor")
	}
	emptyOrder := base64.URLEncoding.EncodeToString([]byte(`{"l":1,"t":"2026-06-21T00:00:00Z","h":"x","e":"y","o":""}`))
	c, err := DecodeRelationshipCursor(emptyOrder)
	if err != nil {
		t.Fatalf("empty order should default, got err: %v", err)
	}
	if c.Order != "desc" {
		t.Fatalf("empty order: got %q want desc", c.Order)
	}
}

// --- address validation (front-line input guard; documented G/C-only contract) ---

func TestValidRelationshipAddress(t *testing.T) {
	const validG = "GBTORQK3ZR3RPJF4WTTSH5KVDOAZ4BJI7PD2ECLSBDNHRG4ICNC4JJZV"
	const validC = "CCJQB4EEQLBL7RHIPYMYG26ZT2QRKEYNGVWWL2EPZCECFI6GZGNXMIEX"
	cases := []struct {
		in   string
		want bool
	}{
		{validG, true},
		{validC, true},
		{"", false},
		{"notanaddress", false},
		{validG[:len(validG)-1] + "A", false}, // corrupted checksum
	}
	for _, c := range cases {
		if got := validRelationshipAddress(c.in); got != c.want {
			t.Errorf("validRelationshipAddress(%q)=%v want %v", c.in, got, c.want)
		}
	}
}

// --- HandleRelationship request-validation contract (documented 400/503 codes) ---

func TestHandleRelationshipValidation(t *testing.T) {
	const validA = "GBTORQK3ZR3RPJF4WTTSH5KVDOAZ4BJI7PD2ECLSBDNHRG4ICNC4JJZV"
	const validB = "GB5FCYPSK4ET44OVBXLJHWFW5LNG3ZLPUFSJTJBCGIM43JIU4RGYRLCH"
	db := openRelationshipDuckDB(t)
	defer db.Close()
	withReader := &SilverHandlers{unifiedReader: &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}}

	call := func(h *SilverHandlers, a, b, rawQuery string) int {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/silver/relationships/"+a+"/"+b+"?"+rawQuery, nil)
		req = mux.SetURLVars(req, map[string]string{"address_a": a, "address_b": b})
		w := httptest.NewRecorder()
		h.HandleRelationship(w, req)
		return w.Code
	}

	cases := []struct {
		name        string
		h           *SilverHandlers
		a, b, query string
		want        int
	}{
		{"invalid address", withReader, "bad", "bad", "", http.StatusBadRequest},
		{"nil reader", &SilverHandlers{unifiedReader: nil}, validA, validB, "", http.StatusServiceUnavailable},
		{"invalid order", withReader, validA, validB, "order=sideways", http.StatusBadRequest},
		{"invalid cursor", withReader, validA, validB, "cursor=zzzz", http.StatusBadRequest},
		{"invalid start_ledger", withReader, validA, validB, "start_ledger=abc", http.StatusBadRequest},
		{"negative end_ledger", withReader, validA, validB, "end_ledger=-5", http.StatusBadRequest},
	}
	for _, c := range cases {
		if got := call(c.h, c.a, c.b, c.query); got != c.want {
			t.Errorf("%s: got status %d want %d", c.name, got, c.want)
		}
	}
}

// --- GetRelationshipEdges behavior against a real in-memory DuckDB (no mocks) ---

func TestGetRelationshipEdgesPagination(t *testing.T) {
	db := openRelationshipDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}
	insertTransfer(t, db, "memory.hot", 10, "2026-06-21 00:00:10", "h1", "A", "B", "100")
	insertTransfer(t, db, "memory.hot", 20, "2026-06-21 00:00:20", "h2", "A", "B", "200")
	insertTransfer(t, db, "memory.hot", 30, "2026-06-21 00:00:30", "h3", "A", "B", "300")

	filters := RelationshipFilters{AddressA: "A", AddressB: "B", Limit: 2, Order: "desc"}
	edges, cursor, hasMore, err := reader.GetRelationshipEdges(t.Context(), filters)
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(edges) != 2 || !hasMore || cursor == "" {
		t.Fatalf("page1: len=%d hasMore=%v cursor=%q want 2,true,nonempty", len(edges), hasMore, cursor)
	}
	if edges[0].LedgerSequence != 30 || edges[1].LedgerSequence != 20 {
		t.Fatalf("page1 order: got %d,%d want 30,20", edges[0].LedgerSequence, edges[1].LedgerSequence)
	}

	dec, err := DecodeRelationshipCursor(cursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	filters.Cursor = dec
	edges2, cursor2, hasMore2, err := reader.GetRelationshipEdges(t.Context(), filters)
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(edges2) != 1 || hasMore2 || cursor2 != "" {
		t.Fatalf("page2: len=%d hasMore=%v cursor=%q want 1,false,empty", len(edges2), hasMore2, cursor2)
	}
	if edges2[0].LedgerSequence != 10 {
		t.Fatalf("page2 ledger: got %d want 10", edges2[0].LedgerSequence)
	}

	seen := map[int64]bool{}
	for _, e := range append(append([]RelationshipEdge{}, edges...), edges2...) {
		if seen[e.LedgerSequence] {
			t.Fatalf("duplicate edge across pages: ledger %d", e.LedgerSequence)
		}
		seen[e.LedgerSequence] = true
	}
}

func TestGetRelationshipEdgesExactLimitNoCursor(t *testing.T) {
	db := openRelationshipDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}
	insertTransfer(t, db, "memory.hot", 10, "2026-06-21 00:00:10", "h1", "A", "B", "100")
	insertTransfer(t, db, "memory.hot", 20, "2026-06-21 00:00:20", "h2", "A", "B", "200")

	edges, cursor, hasMore, err := reader.GetRelationshipEdges(t.Context(),
		RelationshipFilters{AddressA: "A", AddressB: "B", Limit: 2, Order: "desc"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(edges) != 2 || hasMore || cursor != "" {
		t.Fatalf("exactly-limit page: len=%d hasMore=%v cursor=%q want 2,false,empty", len(edges), hasMore, cursor)
	}
}

func TestGetRelationshipEdgesDedupHotOverCold(t *testing.T) {
	db := openRelationshipDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}
	// Same logical transfer present in both hot and cold (flush overlap) -> identical edge_id.
	insertTransfer(t, db, "memory.hot", 10, "2026-06-21 00:00:10", "h1", "A", "B", "100")
	insertTransfer(t, db, "memory.cold", 10, "2026-06-21 00:00:10", "h1", "A", "B", "100")

	edges, _, _, err := reader.GetRelationshipEdges(t.Context(),
		RelationshipFilters{AddressA: "A", AddressB: "B", Limit: 10, Order: "desc"})
	if err != nil {
		t.Fatalf("dedup query: %v", err)
	}
	if len(edges) != 1 {
		t.Fatalf("expected 1 deduped edge, got %d", len(edges))
	}
	if edges[0].SourceTable != "token_transfers_raw" {
		t.Fatalf("source_table: got %q want token_transfers_raw", edges[0].SourceTable)
	}
}

func TestGetRelationshipEdgesRefusesEmptySchemas(t *testing.T) {
	db := openRelationshipDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "", coldSchema: ""}
	if _, _, _, err := reader.GetRelationshipEdges(t.Context(),
		RelationshipFilters{AddressA: "A", AddressB: "B", Limit: 10, Order: "desc"}); err == nil {
		t.Fatal("expected error when both schemas empty, got nil")
	}
}

// --- real-DuckDB harness for relationship tests (mirrors contract_data_live_only_test.go) ---

func openRelationshipDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	for _, schema := range []string{"hot", "cold"} {
		createRelationshipTables(t, db, schema)
	}
	return db
}

func createRelationshipTables(t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}
	stmts := []string{
		`CREATE TABLE memory.` + schema + `.token_transfers_raw (
			ledger_sequence BIGINT, "timestamp" TIMESTAMP, transaction_hash VARCHAR,
			from_account VARCHAR, to_account VARCHAR, asset_code VARCHAR, token_contract_id VARCHAR,
			amount NUMERIC, source_type VARCHAR, event_index INTEGER, transaction_successful BOOLEAN)`,
		`CREATE TABLE memory.` + schema + `.semantic_flows_value (
			ledger_sequence BIGINT, "timestamp" TIMESTAMP, transaction_hash VARCHAR,
			from_account VARCHAR, to_account VARCHAR, asset_code VARCHAR, contract_id VARCHAR,
			amount NUMERIC, id VARCHAR, successful BOOLEAN)`,
		`CREATE TABLE memory.` + schema + `.contract_invocations_raw (
			ledger_sequence BIGINT, closed_at TIMESTAMP, transaction_hash VARCHAR,
			source_account VARCHAR, contract_id VARCHAR, function_name VARCHAR,
			transaction_index INTEGER, operation_index INTEGER, successful BOOLEAN)`,
		`CREATE TABLE memory.` + schema + `.semantic_activities (
			ledger_sequence BIGINT, "timestamp" TIMESTAMP, transaction_hash VARCHAR,
			source_account VARCHAR, destination_account VARCHAR, contract_id VARCHAR,
			asset_code VARCHAR, asset_issuer VARCHAR, soroban_function_name VARCHAR,
			amount NUMERIC, id VARCHAR, successful BOOLEAN)`,
		`CREATE TABLE memory.` + schema + `.contract_invocation_calls (
			ledger_sequence BIGINT, closed_at TIMESTAMP, transaction_hash VARCHAR,
			from_contract VARCHAR, to_contract VARCHAR, function_name VARCHAR,
			call_id BIGINT, successful BOOLEAN)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("create table in %s: %v", schema, err)
		}
	}
}

func insertTransfer(t *testing.T, db *sql.DB, schema string, ledger int64, ts, txHash, from, to, amount string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO `+schema+`.token_transfers_raw
		(ledger_sequence, "timestamp", transaction_hash, from_account, to_account, asset_code, token_contract_id, amount, source_type, event_index, transaction_successful)
		VALUES (?, ?, ?, ?, ?, 'XLM', NULL, ?, 'transfer', 0, true)`,
		ledger, ts, txHash, from, to, amount); err != nil {
		t.Fatalf("insert transfer: %v", err)
	}
}

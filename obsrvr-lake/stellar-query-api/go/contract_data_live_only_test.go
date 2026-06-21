package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/gorilla/mux"
)

func TestUnifiedGetContractDataLiveOnlyFiltersExpiredAcrossHotAndCold(t *testing.T) {
	db := openContractDataDuckDB(t)
	defer db.Close()
	reader := &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}

	data, _, _, err := reader.GetContractData(t.Context(), ContractDataFilters{ContractID: "C1", LiveOnly: true, Limit: 10})
	if err != nil {
		t.Fatalf("GetContractData live_only: %v", err)
	}
	got := contractDataKeys(data)
	want := []string{"cold-live", "hot-live"}
	if !equalStringSlices(got, want) {
		t.Fatalf("live_only keys: got %#v want %#v", got, want)
	}

	data, _, _, err = reader.GetContractData(t.Context(), ContractDataFilters{ContractID: "C1", LiveOnly: false, Limit: 10})
	if err != nil {
		t.Fatalf("GetContractData raw: %v", err)
	}
	got = contractDataKeys(data)
	want = []string{"cold-expired", "cold-live", "hot-expired", "hot-live"}
	if !equalStringSlices(got, want) {
		t.Fatalf("raw keys: got %#v want %#v", got, want)
	}
}

func TestHandleContractStorageDefaultsToLiveOnly(t *testing.T) {
	db := openContractDataDuckDB(t)
	defer db.Close()
	h := &SilverHandlers{unifiedReader: &UnifiedDuckDBReader{db: db, hotSchema: "memory.hot", coldSchema: "memory.cold"}}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/silver/contracts/C1/storage", nil)
	req = mux.SetURLVars(req, map[string]string{"id": "C1"})
	w := httptest.NewRecorder()
	h.HandleContractStorage(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Entries  []ContractStorageEntry `json:"entries"`
		LiveOnly bool                   `json:"live_only"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp.LiveOnly {
		t.Fatalf("expected live_only=true response")
	}
	got := make([]string, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		got = append(got, e.KeyHash)
	}
	want := []string{"cold-live", "hot-live"}
	if !equalStringSlices(got, want) {
		t.Fatalf("storage keys: got %#v want %#v", got, want)
	}
}

func openContractDataDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	for _, schema := range []string{"hot", "cold"} {
		if _, err := db.Exec(`CREATE SCHEMA ` + schema); err != nil {
			t.Fatalf("create schema %s: %v", schema, err)
		}
		if _, err := db.Exec(`CREATE TABLE memory.` + schema + `.contract_data_current (
			contract_id VARCHAR,
			key_hash VARCHAR,
			durability VARCHAR,
			data_value VARCHAR,
			asset_type VARCHAR,
			asset_code VARCHAR,
			asset_issuer VARCHAR,
			last_modified_ledger BIGINT,
			closed_at VARCHAR
		)`); err != nil {
			t.Fatalf("create contract_data_current %s: %v", schema, err)
		}
		if _, err := db.Exec(`CREATE TABLE memory.` + schema + `.ttl_current (
			key_hash VARCHAR,
			live_until_ledger_seq BIGINT,
			ttl_remaining INTEGER,
			expired BOOLEAN
		)`); err != nil {
			t.Fatalf("create ttl_current %s: %v", schema, err)
		}
	}
	insertContractDataCurrent(t, db, "memory.hot", "C1", "hot-live", false)
	insertContractDataCurrent(t, db, "memory.hot", "C1", "hot-expired", true)
	insertContractDataCurrent(t, db, "memory.cold", "C1", "cold-live", false)
	insertContractDataCurrent(t, db, "memory.cold", "C1", "cold-expired", true)
	return db
}

func insertContractDataCurrent(t *testing.T, db *sql.DB, schema, contractID, keyHash string, expired bool) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO `+schema+`.contract_data_current VALUES (?, ?, 'persistent', 'xdr', NULL, NULL, NULL, 100, '2026-01-01T00:00:00Z')`, contractID, keyHash); err != nil {
		t.Fatalf("insert contract_data_current: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO `+schema+`.ttl_current VALUES (?, 200, 100, ?)`, keyHash, expired); err != nil {
		t.Fatalf("insert ttl_current: %v", err)
	}
}

func contractDataKeys(data []ContractData) []string {
	keys := make([]string, 0, len(data))
	for _, d := range data {
		keys = append(keys, d.KeyHash)
	}
	return keys
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

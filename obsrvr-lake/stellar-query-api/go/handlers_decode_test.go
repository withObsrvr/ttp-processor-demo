package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gorilla/mux"
)

type fakeContractArtifactResolver struct {
	response *ContractInterfaceResponse
	wasm     *ContractWASMArtifact
	err      error
}

func (f *fakeContractArtifactResolver) Resolve(context.Context, string) (*ContractInterfaceResponse, error) {
	return f.response, f.err
}

func (f *fakeContractArtifactResolver) ResolveWASM(context.Context, string) (*ContractWASMArtifact, error) {
	return f.wasm, f.err
}

func TestHandleDecodeScValRejectsUnknownFields(t *testing.T) {
	h := &DecodeHandlers{}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/silver/decode/scval", strings.NewReader(`{"xdr":"AAAAAQ==","unexpected":true}`))
	w := httptest.NewRecorder()

	h.HandleDecodeScVal(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", w.Code)
	}
}

func TestHandleBatchDecodedTransactionsRejectsUnknownFields(t *testing.T) {
	h := &DecodeHandlers{coldReader: &SilverColdReader{}}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/silver/tx/batch/decoded", strings.NewReader(`{"hashes":["abc"],"unexpected":true}`))
	w := httptest.NewRecorder()

	h.HandleBatchDecodedTransactions(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", w.Code)
	}
}

func TestHandleContractInterfaceReturnsAuthoritativeJSON(t *testing.T) {
	resolver := &fakeContractArtifactResolver{response: &ContractInterfaceResponse{
		ContractID: "CEXAMPLE", Network: "testnet", DetectedType: ContractTypeUnknown,
		Executable: ContractExecutableReference{Type: "wasm", WasmHash: strings.Repeat("a", 64), WasmSizeBytes: 123456},
		Interface:  ContractSpec{Functions: []ContractSpecFunction{{Name: "hello", Inputs: []ContractSpecField{}, Outputs: []string{"String"}}}, Structs: []ContractSpecStruct{}, Unions: []ContractSpecUnion{}, Enums: []ContractSpecEnum{}, Errors: []ContractSpecEnum{}, Events: []ContractSpecEvent{}},
		Metadata:   []ContractMetadataEntry{}, ObservedFunctions: []string{},
	}}
	h := &DecodeHandlers{contractArtifacts: resolver}
	req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/api/v1/silver/contracts/CEXAMPLE/interface", nil), map[string]string{"id": "CEXAMPLE"})
	w := httptest.NewRecorder()

	h.HandleContractInterface(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d body=%s", w.Code, w.Body.String())
	}
	var response ContractInterfaceResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Interface.Functions) != 1 || response.Interface.Functions[0].Name != "hello" {
		t.Fatalf("unexpected response: %#v", response)
	}
	if response.Executable.WasmSizeBytes != 123456 {
		t.Fatalf("wasm size: got %d want 123456", response.Executable.WasmSizeBytes)
	}
}

func TestHandleContractInterfaceSupportsRustRendering(t *testing.T) {
	resolver := &fakeContractArtifactResolver{response: &ContractInterfaceResponse{
		ContractID: "CEXAMPLE", Executable: ContractExecutableReference{WasmHash: strings.Repeat("b", 64)},
		Interface: ContractSpec{Functions: []ContractSpecFunction{{Name: "hello", Inputs: []ContractSpecField{}, Outputs: []string{"String"}}}},
	}}
	h := &DecodeHandlers{contractArtifacts: resolver}
	req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/api/v1/silver/contracts/CEXAMPLE/interface?format=rust", nil), map[string]string{"id": "CEXAMPLE"})
	w := httptest.NewRecorder()

	h.HandleContractInterface(w, req)

	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "fn hello() -> String") {
		t.Fatalf("unexpected rust response: status=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Fatalf("content type: %q", got)
	}
}

func TestHandleContractWASMReturnsDownloadAndHonorsETag(t *testing.T) {
	hash := strings.Repeat("c", 64)
	resolver := &fakeContractArtifactResolver{wasm: &ContractWASMArtifact{
		ContractID: "CEXAMPLE", WasmHash: hash, WASM: []byte("\x00asm"),
		Executable: ContractExecutableReference{ResolvedAtLedger: 123},
	}}
	h := &DecodeHandlers{contractArtifacts: resolver}
	req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/api/v1/silver/contracts/CEXAMPLE/wasm", nil), map[string]string{"id": "CEXAMPLE"})
	w := httptest.NewRecorder()
	h.HandleContractWASM(w, req)

	if w.Code != http.StatusOK || w.Body.String() != "\x00asm" {
		t.Fatalf("unexpected wasm response: status=%d body=%q", w.Code, w.Body.String())
	}
	if got := w.Header().Get("ETag"); got != `"`+hash+`"` {
		t.Fatalf("etag: %q", got)
	}
	if got := w.Header().Get("Content-Type"); got != "application/wasm" {
		t.Fatalf("content type: %q", got)
	}

	req = mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/api/v1/silver/contracts/CEXAMPLE/wasm", nil), map[string]string{"id": "CEXAMPLE"})
	req.Header.Set("If-None-Match", `"`+hash+`"`)
	w = httptest.NewRecorder()
	h.HandleContractWASM(w, req)
	if w.Code != http.StatusNotModified || w.Body.Len() != 0 {
		t.Fatalf("unexpected conditional response: status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestContractArtifactHandlersMapClientAndAvailabilityErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid contract", err: ErrInvalidContractID, want: http.StatusBadRequest},
		{name: "missing contract", err: ErrContractNotFound, want: http.StatusNotFound},
		{name: "archived code", err: ErrContractCodeAbsent, want: http.StatusNotFound},
		{name: "rpc unavailable", err: context.DeadlineExceeded, want: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := &DecodeHandlers{contractArtifacts: &fakeContractArtifactResolver{err: test.err}}
			req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/api/v1/silver/contracts/CEXAMPLE/interface", nil), map[string]string{"id": "CEXAMPLE"})
			w := httptest.NewRecorder()
			h.HandleContractInterface(w, req)
			if w.Code != test.want {
				t.Fatalf("status: got %d want %d body=%s", w.Code, test.want, w.Body.String())
			}
		})
	}
}

func TestObservedContractFunctionsDoesNotBlockAuthoritativeInterfaceOnColdScan(t *testing.T) {
	hotDB, hotMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer hotDB.Close()
	coldDB, coldMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer coldDB.Close()

	hotMock.ExpectQuery(`SELECT DISTINCT function_name`).
		WithArgs("CEXAMPLE").
		WillReturnRows(sqlmock.NewRows([]string{"function_name"}))
	coldMock.ExpectQuery(`SELECT DISTINCT function_name`).
		WithArgs("CEXAMPLE").
		WillDelayFor(2 * time.Second).
		WillReturnRows(sqlmock.NewRows([]string{"function_name"}).AddRow("historical_call"))

	h := &DecodeHandlers{
		hotReader:  &SilverHotReader{db: hotDB},
		coldReader: &SilverColdReader{db: coldDB, catalogName: "memory", schemaName: "silver"},
	}
	started := time.Now()
	functions := h.observedContractFunctions(t.Context(), "CEXAMPLE")
	elapsed := time.Since(started)

	if len(functions) != 0 {
		t.Fatalf("expected optional timed-out enrichment to be omitted, got %v", functions)
	}
	if elapsed > time.Second {
		t.Fatalf("optional observed-function lookup blocked for %s", elapsed)
	}
	if err := hotMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if err := coldMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCORSMiddlewareAddsHeaders(t *testing.T) {
	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, map[string]string{"ok": "true"})
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected Access-Control-Allow-Origin '*', got %q", got)
	}
	if got := w.Header().Get("Vary"); !strings.Contains(got, "Origin") {
		t.Fatalf("expected Vary header to mention Origin, got %q", got)
	}
}

func TestRequestIDMiddlewareSetsHeader(t *testing.T) {
	handler := requestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, map[string]string{"request_id": requestIDFromContext(r.Context())})
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	requestID := w.Header().Get("X-Request-Id")
	if requestID == "" {
		t.Fatal("expected X-Request-Id header to be set")
	}

	var decoded map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if decoded["request_id"] != requestID {
		t.Fatalf("expected request ID %q in body, got %q", requestID, decoded["request_id"])
	}
}

func TestRecoverPanicMiddlewareReturnsJSONError(t *testing.T) {
	handler := recoverPanicMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", w.Code)
	}

	var decoded struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if decoded.Error.Message == "" {
		t.Fatal("expected JSON error response")
	}
	if decoded.Error.Code != "internal_error" {
		t.Fatalf("expected internal_error code, got %q", decoded.Error.Code)
	}
}

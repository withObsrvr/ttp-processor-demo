package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDecodeDefiCursor(t *testing.T) {
	cursor, err := DecodeDefiCursor((DefiCursor{ID: "market-123"}).Encode())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cursor == nil || cursor.ID != "market-123" {
		t.Fatalf("unexpected cursor: %#v", cursor)
	}
}

func TestDecodeDefiCursorInvalid(t *testing.T) {
	if _, err := DecodeDefiCursor("not-base64"); err == nil {
		t.Fatal("expected error for invalid cursor")
	}
}

func TestHandleDefiExposureRequiresAddress(t *testing.T) {
	h := &SemanticHandlers{}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/semantic/defi/exposure", nil)
	w := httptest.NewRecorder()

	h.HandleDefiExposure(w, req)

	if w.Code != http.StatusServiceUnavailable && w.Code != http.StatusBadRequest {
		t.Fatalf("expected 503 or 400, got %d", w.Code)
	}
}

func TestHandleDefiPositionsRequiresAddress(t *testing.T) {
	h := &SemanticHandlers{unified: &UnifiedSilverReader{hot: &SilverHotReader{}}}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/semantic/defi/positions", nil)
	w := httptest.NewRecorder()

	h.HandleDefiPositions(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleDefiMarketsRejectsInvalidBool(t *testing.T) {
	h := &SemanticHandlers{unified: &UnifiedSilverReader{hot: &SilverHotReader{}}}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/semantic/defi/markets?active_only=maybe", nil)
	w := httptest.NewRecorder()

	h.HandleDefiMarkets(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

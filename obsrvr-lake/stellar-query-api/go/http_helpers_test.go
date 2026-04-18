package main

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadJSONRejectsUnknownFields(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"ok","extra":true}`))
	w := httptest.NewRecorder()

	var dst struct {
		Name string `json:"name"`
	}

	err := readJSON(w, req, &dst)
	if err == nil {
		t.Fatal("expected error for unknown JSON field")
	}
}

func TestReadJSONRejectsMultipleValues(t *testing.T) {
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"ok"} {"name":"two"}`))
	w := httptest.NewRecorder()

	var dst struct {
		Name string `json:"name"`
	}

	err := readJSON(w, req, &dst)
	if err == nil {
		t.Fatal("expected error for multiple JSON values")
	}
}

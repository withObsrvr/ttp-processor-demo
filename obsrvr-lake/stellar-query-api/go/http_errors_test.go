package main

import (
	"net/http"
	"testing"
)

func TestErrorCodeForStatus(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{http.StatusBadRequest, "invalid_request"},
		{http.StatusUnauthorized, "unauthorized"},
		{http.StatusForbidden, "forbidden"},
		{http.StatusNotFound, "not_found"},
		{http.StatusMethodNotAllowed, "method_not_allowed"},
		{http.StatusConflict, "conflict"},
		{http.StatusTooManyRequests, "rate_limited"},
		{http.StatusServiceUnavailable, "service_unavailable"},
		{http.StatusGatewayTimeout, "timeout"},
		{http.StatusInternalServerError, "internal_error"},
	}

	for _, tt := range tests {
		if got := errorCodeForStatus(tt.status); got != tt.want {
			t.Fatalf("status %d: got %q want %q", tt.status, got, tt.want)
		}
	}
}

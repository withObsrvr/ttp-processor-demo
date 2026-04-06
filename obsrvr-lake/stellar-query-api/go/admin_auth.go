package main

import (
	"net/http"
	"os"
)

// adminToken is loaded from ADMIN_TOKEN env var at startup
var adminToken string

func init() {
	adminToken = os.Getenv("ADMIN_TOKEN")
}

// requireAdmin wraps a handler to require X-Admin-Token header
func requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if adminToken == "" {
			respondError(w, "admin endpoints are disabled (ADMIN_TOKEN not configured)", http.StatusForbidden)
			return
		}
		token := r.Header.Get("X-Admin-Token")
		if token == "" || token != adminToken {
			respondError(w, "unauthorized: valid X-Admin-Token header required", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

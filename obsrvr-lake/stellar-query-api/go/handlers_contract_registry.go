package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

// ContractRegistryEntry represents a contract in the registry
type ContractRegistryEntry struct {
	ContractID  string  `json:"contract_id"`
	DisplayName string  `json:"display_name"`
	Category    *string `json:"category,omitempty"`
	Project     *string `json:"project,omitempty"`
	IconURL     *string `json:"icon_url,omitempty"`
	Website     *string `json:"website,omitempty"`
	Verified    bool    `json:"verified"`
	Source      string  `json:"source"`
	CreatedAt   string  `json:"created_at"`
	UpdatedAt   string  `json:"updated_at"`
}

// ContractRegistryHandlers contains HTTP handlers for the contract registry
type ContractRegistryHandlers struct {
	db     *sql.DB // direct PG connection to silver_hot
	reader *UnifiedDuckDBReader
}

// NewContractRegistryHandlers creates new contract registry API handlers
func NewContractRegistryHandlers(db *sql.DB, reader *UnifiedDuckDBReader) *ContractRegistryHandlers {
	return &ContractRegistryHandlers{db: db, reader: reader}
}

// HandleGetContract returns a single contract registry entry
// @Summary Get contract registry entry
// @Tags Contract Registry
// @Param id path string true "Contract ID (C... address)"
// @Success 200 {object} ContractRegistryEntry
// @Failure 404 {object} map[string]interface{}
// @Router /api/v1/explorer/contracts/{id} [get]
func (h *ContractRegistryHandlers) HandleGetContract(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	var entry ContractRegistryEntry
	var category, project, iconURL, website sql.NullString
	var createdAt, updatedAt time.Time

	err := h.db.QueryRowContext(r.Context(), `
		SELECT contract_id, display_name, category, project, icon_url, website,
		       verified, source, created_at, updated_at
		FROM contract_registry WHERE contract_id = $1
	`, contractID).Scan(
		&entry.ContractID, &entry.DisplayName, &category, &project,
		&iconURL, &website, &entry.Verified, &entry.Source,
		&createdAt, &updatedAt,
	)
	if err == sql.ErrNoRows {
		respondError(w, "contract not found in registry", http.StatusNotFound)
		return
	}
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if category.Valid {
		entry.Category = &category.String
	}
	if project.Valid {
		entry.Project = &project.String
	}
	if iconURL.Valid {
		entry.IconURL = &iconURL.String
	}
	if website.Valid {
		entry.Website = &website.String
	}
	entry.CreatedAt = createdAt.Format(time.RFC3339)
	entry.UpdatedAt = updatedAt.Format(time.RFC3339)

	respondJSON(w, entry)
}

// HandleListContracts returns contract registry entries with optional filters
// @Summary List contract registry entries
// @Tags Contract Registry
// @Param category query string false "Filter by category (token, dex, oracle, etc.)"
// @Param project query string false "Filter by project (soroswap, redstone, etc.)"
// @Param source query string false "Filter by source (manual, token_registry, etc.)"
// @Param limit query int false "Max results (default: 50, max: 500)"
// @Success 200 {object} map[string]interface{}
// @Router /api/v1/explorer/contracts [get]
func (h *ContractRegistryHandlers) HandleListContracts(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 50, 500)

	query := `SELECT contract_id, display_name, category, project, icon_url, website,
	                 verified, source, created_at, updated_at
	          FROM contract_registry WHERE 1=1`
	var args []any
	argIdx := 1

	if v := r.URL.Query().Get("category"); v != "" {
		query += fmt.Sprintf(" AND category = $%d", argIdx)
		args = append(args, v)
		argIdx++
	}
	if v := r.URL.Query().Get("project"); v != "" {
		query += fmt.Sprintf(" AND project = $%d", argIdx)
		args = append(args, v)
		argIdx++
	}
	if v := r.URL.Query().Get("source"); v != "" {
		query += fmt.Sprintf(" AND source = $%d", argIdx)
		args = append(args, v)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY display_name ASC LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := h.db.QueryContext(r.Context(), query, args...)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	entries := []ContractRegistryEntry{}
	for rows.Next() {
		var entry ContractRegistryEntry
		var category, project, iconURL, website sql.NullString
		var createdAt, updatedAt time.Time
		if err := rows.Scan(&entry.ContractID, &entry.DisplayName, &category, &project,
			&iconURL, &website, &entry.Verified, &entry.Source,
			&createdAt, &updatedAt); err != nil {
			continue
		}
		if category.Valid {
			entry.Category = &category.String
		}
		if project.Valid {
			entry.Project = &project.String
		}
		if iconURL.Valid {
			entry.IconURL = &iconURL.String
		}
		if website.Valid {
			entry.Website = &website.String
		}
		entry.CreatedAt = createdAt.Format(time.RFC3339)
		entry.UpdatedAt = updatedAt.Format(time.RFC3339)
		entries = append(entries, entry)
	}

	respondJSON(w, map[string]interface{}{
		"contracts": entries,
		"count":     len(entries),
	})
}

// HandleSearchContracts searches the registry by name
// @Summary Search contract registry
// @Tags Contract Registry
// @Param q query string true "Search query (matches display_name)"
// @Param limit query int false "Max results (default: 20, max: 100)"
// @Success 200 {object} map[string]interface{}
// @Router /api/v1/explorer/contracts/search [get]
func (h *ContractRegistryHandlers) HandleSearchContracts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		respondError(w, "q parameter required", http.StatusBadRequest)
		return
	}

	limit := parseLimit(r, 20, 100)

	rows, err := h.db.QueryContext(r.Context(), `
		SELECT contract_id, display_name, category, project, verified, source
		FROM contract_registry
		WHERE display_name ILIKE '%' || $1 || '%'
		   OR project ILIKE '%' || $1 || '%'
		ORDER BY verified DESC, display_name ASC
		LIMIT $2
	`, q, limit)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type searchResult struct {
		ContractID  string  `json:"contract_id"`
		DisplayName string  `json:"display_name"`
		Category    *string `json:"category,omitempty"`
		Project     *string `json:"project,omitempty"`
		Verified    bool    `json:"verified"`
		Source      string  `json:"source"`
	}

	results := []searchResult{}
	for rows.Next() {
		var r searchResult
		var category, project sql.NullString
		if err := rows.Scan(&r.ContractID, &r.DisplayName, &category, &project,
			&r.Verified, &r.Source); err != nil {
			continue
		}
		if category.Valid {
			r.Category = &category.String
		}
		if project.Valid {
			r.Project = &project.String
		}
		results = append(results, r)
	}

	respondJSON(w, map[string]interface{}{
		"query":   q,
		"results": results,
		"count":   len(results),
	})
}

// HandleUpsertContract adds or updates a contract registry entry
// @Summary Add/update contract registry entry
// @Tags Contract Registry
// @Accept json
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /api/v1/explorer/contracts [post]
func (h *ContractRegistryHandlers) HandleUpsertContract(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ContractID  string  `json:"contract_id"`
		DisplayName string  `json:"display_name"`
		Category    *string `json:"category,omitempty"`
		Project     *string `json:"project,omitempty"`
		IconURL     *string `json:"icon_url,omitempty"`
		Website     *string `json:"website,omitempty"`
		Verified    *bool   `json:"verified,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.ContractID == "" || req.DisplayName == "" {
		respondError(w, "contract_id and display_name are required", http.StatusBadRequest)
		return
	}

	// Validate contract_id is a valid Stellar contract address
	if _, err := normalizeContractID(req.ContractID); err != nil {
		respondError(w, "invalid contract_id: must be a valid C... contract address or 64-char hex hash", http.StatusBadRequest)
		return
	}

	verified := false
	if req.Verified != nil {
		verified = *req.Verified
	}

	_, err := h.db.ExecContext(r.Context(), `
		INSERT INTO contract_registry (contract_id, display_name, category, project, icon_url, website, verified, source)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'manual')
		ON CONFLICT (contract_id) DO UPDATE SET
			display_name = EXCLUDED.display_name,
			category = COALESCE(EXCLUDED.category, contract_registry.category),
			project = COALESCE(EXCLUDED.project, contract_registry.project),
			icon_url = COALESCE(EXCLUDED.icon_url, contract_registry.icon_url),
			website = COALESCE(EXCLUDED.website, contract_registry.website),
			verified = EXCLUDED.verified,
			source = 'manual',
			updated_at = NOW()
	`, req.ContractID, req.DisplayName, req.Category, req.Project,
		req.IconURL, req.Website, verified)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	respondJSON(w, map[string]interface{}{
		"status":      "ok",
		"contract_id": req.ContractID,
	})
}

// HandleDeleteContract removes a contract from the registry
// @Summary Delete contract registry entry
// @Tags Contract Registry
// @Param id path string true "Contract ID"
// @Success 200 {object} map[string]interface{}
// @Router /api/v1/explorer/contracts/{id} [delete]
func (h *ContractRegistryHandlers) HandleDeleteContract(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	contractID := vars["id"]
	if contractID == "" {
		respondError(w, "contract_id required", http.StatusBadRequest)
		return
	}

	result, err := h.db.ExecContext(r.Context(), `DELETE FROM contract_registry WHERE contract_id = $1`, contractID)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		respondError(w, "contract not found", http.StatusNotFound)
		return
	}

	respondJSON(w, map[string]interface{}{
		"status":      "deleted",
		"contract_id": contractID,
	})
}

// HandleSeedRegistry re-runs auto-seeding from token_registry
// @Summary Re-seed contract registry from token_registry
// @Tags Contract Registry
// @Success 200 {object} map[string]interface{}
// @Router /api/v1/explorer/contracts/seed [post]
func (h *ContractRegistryHandlers) HandleSeedRegistry(w http.ResponseWriter, r *http.Request) {
	result, err := h.db.ExecContext(r.Context(), `
		INSERT INTO contract_registry (contract_id, display_name, category, project, source)
		SELECT contract_id,
		       COALESCE(NULLIF(token_symbol, ''), NULLIF(token_name, ''), contract_id),
		       'token',
		       NULL,
		       'token_registry'
		FROM token_registry
		ON CONFLICT (contract_id) DO NOTHING
	`)
	if err != nil {
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows, _ := result.RowsAffected()
	respondJSON(w, map[string]interface{}{
		"status":     "seeded",
		"new_entries": rows,
	})
}

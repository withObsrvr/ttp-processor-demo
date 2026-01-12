package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

// ComplianceHandlers contains HTTP handlers for Compliance Archive API
type ComplianceHandlers struct {
	reader    *UnifiedSilverReader
	store     *ArchiveStore
	processor *ArchiveProcessor
}

// NewComplianceHandlers creates new Compliance Archive API handlers
func NewComplianceHandlers(reader *UnifiedSilverReader) *ComplianceHandlers {
	store := NewArchiveStore()
	processor := NewArchiveProcessor(store, reader)
	return &ComplianceHandlers{
		reader:    reader,
		store:     store,
		processor: processor,
	}
}

// ============================================
// COMPLIANCE ARCHIVE API - Handlers
// ============================================

// HandleTransactionArchive returns transaction lineage for an asset within a date range
// GET /api/v1/gold/compliance/transactions
func (h *ComplianceHandlers) HandleTransactionArchive(w http.ResponseWriter, r *http.Request) {
	assetCode := r.URL.Query().Get("asset_code")
	if assetCode == "" {
		respondError(w, "asset_code required", http.StatusBadRequest)
		return
	}

	assetIssuer := r.URL.Query().Get("asset_issuer")
	// asset_issuer is required for non-XLM assets
	if assetCode != "XLM" && assetCode != "native" && assetIssuer == "" {
		respondError(w, "asset_issuer required for non-native assets", http.StatusBadRequest)
		return
	}

	startDate, endDate, err := parseDateRange(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	includeFailed := parseIncludeFailed(r)
	limit := parseLimit(r, 1000, 10000)

	format := r.URL.Query().Get("format")

	log.Printf("Compliance API: GetAssetTransactions asset=%s:%s start=%s end=%s limit=%d format=%s",
		assetCode, assetIssuer, startDate.Format(time.RFC3339), endDate.Format(time.RFC3339), limit, format)

	result, err := h.reader.GetAssetTransactions(r.Context(), assetCode, assetIssuer, startDate, endDate, includeFailed, limit)
	if err != nil {
		log.Printf("Compliance API error: %v", err)
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch format {
	case "csv":
		csvData, err := TransactionsToCSV(result.Transactions)
		if err != nil {
			log.Printf("Compliance API CSV error: %v", err)
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=transactions.csv")
		w.Write(csvData)
		return
	case "parquet":
		parquetData, err := TransactionsToParquet(result.Transactions)
		if err != nil {
			log.Printf("Compliance API Parquet error: %v", err)
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apache.parquet")
		w.Header().Set("Content-Disposition", "attachment; filename=transactions.parquet")
		w.Write(parquetData)
		return
	default:
		respondJSON(w, result)
	}
}

// HandleBalanceArchive returns all holders of an asset at a specific timestamp
// GET /api/v1/gold/compliance/balances
func (h *ComplianceHandlers) HandleBalanceArchive(w http.ResponseWriter, r *http.Request) {
	assetCode := r.URL.Query().Get("asset_code")
	if assetCode == "" {
		respondError(w, "asset_code required", http.StatusBadRequest)
		return
	}

	assetIssuer := r.URL.Query().Get("asset_issuer")
	// asset_issuer is required for non-XLM assets
	if assetCode != "XLM" && assetCode != "native" && assetIssuer == "" {
		respondError(w, "asset_issuer required for non-native assets", http.StatusBadRequest)
		return
	}

	timestamp, err := parseSnapshotTimestamp(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	minBalance := r.URL.Query().Get("min_balance")
	limit := parseLimit(r, 1000, 10000)
	format := r.URL.Query().Get("format")

	log.Printf("Compliance API: GetComplianceBalances asset=%s:%s timestamp=%s limit=%d format=%s",
		assetCode, assetIssuer, timestamp.Format(time.RFC3339), limit, format)

	result, err := h.reader.GetComplianceBalances(r.Context(), assetCode, assetIssuer, timestamp, minBalance, limit)
	if err != nil {
		log.Printf("Compliance API error: %v", err)
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch format {
	case "csv":
		csvData, err := BalancesToCSV(result.Holders)
		if err != nil {
			log.Printf("Compliance API CSV error: %v", err)
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=balances.csv")
		w.Write(csvData)
		return
	case "parquet":
		parquetData, err := BalancesToParquet(result.Holders)
		if err != nil {
			log.Printf("Compliance API Parquet error: %v", err)
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apache.parquet")
		w.Header().Set("Content-Disposition", "attachment; filename=balances.parquet")
		w.Write(parquetData)
		return
	default:
		respondJSON(w, result)
	}
}

// HandleSupplyTimeline returns daily supply totals for an asset over a date range
// GET /api/v1/gold/compliance/supply
func (h *ComplianceHandlers) HandleSupplyTimeline(w http.ResponseWriter, r *http.Request) {
	assetCode := r.URL.Query().Get("asset_code")
	if assetCode == "" {
		respondError(w, "asset_code required", http.StatusBadRequest)
		return
	}

	assetIssuer := r.URL.Query().Get("asset_issuer")
	// asset_issuer is required for non-XLM assets
	if assetCode != "XLM" && assetCode != "native" && assetIssuer == "" {
		respondError(w, "asset_issuer required for non-native assets", http.StatusBadRequest)
		return
	}

	startDate, endDate, err := parseDateRange(r)
	if err != nil {
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	interval := parseInterval(r)
	format := r.URL.Query().Get("format")

	log.Printf("Compliance API: GetSupplyTimeline asset=%s:%s start=%s end=%s interval=%s format=%s",
		assetCode, assetIssuer, startDate.Format(time.RFC3339), endDate.Format(time.RFC3339), interval, format)

	result, err := h.reader.GetSupplyTimeline(r.Context(), assetCode, assetIssuer, startDate, endDate, interval)
	if err != nil {
		log.Printf("Compliance API error: %v", err)
		respondError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	switch format {
	case "csv":
		csvData, err := SupplyTimelineToCSV(result.Timeline)
		if err != nil {
			log.Printf("Compliance API CSV error: %v", err)
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=supply_timeline.csv")
		w.Write(csvData)
		return
	case "parquet":
		parquetData, err := SupplyTimelineToParquet(result.Timeline)
		if err != nil {
			log.Printf("Compliance API Parquet error: %v", err)
			respondError(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apache.parquet")
		w.Header().Set("Content-Disposition", "attachment; filename=supply_timeline.parquet")
		w.Write(parquetData)
		return
	default:
		respondJSON(w, result)
	}
}

// ============================================
// WEEK 2 - Full Archive API Handlers
// ============================================

// HandleFullArchive creates a new async archive job
// POST /api/v1/gold/compliance/archive
func (h *ComplianceHandlers) HandleFullArchive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		respondError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FullArchiveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.AssetCode == "" {
		respondError(w, "asset_code required", http.StatusBadRequest)
		return
	}
	if req.AssetCode != "XLM" && req.AssetCode != "native" && req.AssetIssuer == "" {
		respondError(w, "asset_issuer required for non-native assets", http.StatusBadRequest)
		return
	}
	if req.StartDate == "" || req.EndDate == "" {
		respondError(w, "start_date and end_date required", http.StatusBadRequest)
		return
	}

	// Validate dates
	_, err := time.Parse("2006-01-02", req.StartDate)
	if err != nil {
		respondError(w, "invalid start_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}
	_, err = time.Parse("2006-01-02", req.EndDate)
	if err != nil {
		respondError(w, "invalid end_date format, use YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	// Create the job
	job, err := h.store.CreateJob(req)
	if err != nil {
		log.Printf("Failed to create archive job: %v", err)
		respondError(w, "failed to create archive job", http.StatusInternalServerError)
		return
	}

	// Start processing in background
	go h.processor.ProcessJob(context.Background(), job.ID)

	// Return initial response
	response := FullArchiveResponse{
		ArchiveID:   job.ID,
		Status:      string(job.Status),
		CallbackURL: "/api/v1/gold/compliance/archive/" + job.ID,
		CreatedAt:   job.CreatedAt.Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(response)
}

// HandleArchiveStatus returns the status of an archive job
// GET /api/v1/gold/compliance/archive/{id}
func (h *ComplianceHandlers) HandleArchiveStatus(w http.ResponseWriter, r *http.Request) {
	// Extract archive ID from path
	path := r.URL.Path
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		respondError(w, "archive_id required", http.StatusBadRequest)
		return
	}
	archiveID := parts[len(parts)-1]

	if archiveID == "" || archiveID == "archive" {
		respondError(w, "archive_id required", http.StatusBadRequest)
		return
	}

	job, ok := h.store.GetJob(archiveID)
	if !ok {
		respondError(w, "archive not found", http.StatusNotFound)
		return
	}

	response := FullArchiveStatusResponse{
		ArchiveID:          job.ID,
		Status:             string(job.Status),
		Asset:              job.Asset,
		Period:             job.Period,
		Artifacts:          job.Artifacts,
		Error:              job.Error,
		MethodologyVersion: MethodologyArchiveV1,
		CreatedAt:          job.CreatedAt.Format(time.RFC3339),
	}

	if job.CompletedAt != nil {
		response.CompletedAt = job.CompletedAt.Format(time.RFC3339)
	}

	respondJSON(w, response)
}

// HandleLineage returns the audit trail of completed archives
// GET /api/v1/gold/compliance/lineage
func (h *ComplianceHandlers) HandleLineage(w http.ResponseWriter, r *http.Request) {
	assetCode := r.URL.Query().Get("asset_code")
	assetIssuer := r.URL.Query().Get("asset_issuer")
	limit := parseLimit(r, 50, 100)

	log.Printf("Compliance API: GetLineage asset=%s:%s limit=%d", assetCode, assetIssuer, limit)

	archives := h.store.GetLineage(assetCode, assetIssuer, limit)

	response := LineageResponse{
		Archives:    archives,
		Count:       len(archives),
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	respondJSON(w, response)
}

// HandleArchiveDownload serves archive artifacts (methodology.md, manifest.json)
// GET /api/v1/gold/compliance/archive/{id}/download/{artifact}
func (h *ComplianceHandlers) HandleArchiveDownload(w http.ResponseWriter, r *http.Request) {
	// Extract archive ID and artifact name from path
	path := r.URL.Path
	parts := strings.Split(path, "/")

	// Find the archive ID and artifact name
	var archiveID, artifactName string
	for i, part := range parts {
		if part == "archive" && i+1 < len(parts) {
			archiveID = parts[i+1]
		}
		if part == "download" && i+1 < len(parts) {
			artifactName = parts[i+1]
		}
	}

	if archiveID == "" || artifactName == "" {
		respondError(w, "archive_id and artifact name required", http.StatusBadRequest)
		return
	}

	job, ok := h.store.GetJob(archiveID)
	if !ok {
		respondError(w, "archive not found", http.StatusNotFound)
		return
	}

	// Find the requested artifact
	var foundArtifact *ArchiveArtifact
	for _, a := range job.Artifacts {
		if a.Name == artifactName {
			foundArtifact = &a
			break
		}
	}

	if foundArtifact == nil {
		respondError(w, "artifact not found", http.StatusNotFound)
		return
	}

	log.Printf("Compliance API: Downloading artifact %s from archive %s", artifactName, archiveID)

	// Generate the content based on artifact type
	switch foundArtifact.Type {
	case "documentation":
		// Generate methodology document
		methodologyDoc := MethodologyDoc{
			ArchiveID:          job.ID,
			AssetCode:          job.Asset.Code,
			AssetType:          job.Asset.Type,
			PeriodStart:        job.Period.Start,
			PeriodEnd:          job.Period.End,
			GeneratedAt:        foundArtifact.GeneratedAt,
			MethodologyVersion: MethodologyArchiveV1,
		}
		if job.Asset.Issuer != nil {
			methodologyDoc.AssetIssuer = *job.Asset.Issuer
		}
		for _, a := range job.Artifacts {
			if a.Type != "documentation" && a.Type != "manifest" {
				methodologyDoc.Artifacts = append(methodologyDoc.Artifacts, ArtifactInfo{
					Name:     a.Name,
					Type:     a.Type,
					Checksum: a.Checksum,
					RowCount: a.RowCount,
				})
			}
		}

		content := GenerateMethodologyDocument(methodologyDoc)
		w.Header().Set("Content-Type", "text/markdown")
		w.Header().Set("Content-Disposition", "attachment; filename=methodology.md")
		w.Write([]byte(content))

	case "manifest":
		// Generate manifest
		manifest, err := GenerateManifest(job)
		if err != nil {
			respondError(w, "failed to generate manifest", http.StatusInternalServerError)
			return
		}
		manifestJSON, _ := GenerateManifestJSON(manifest)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment; filename=manifest.json")
		w.Write(manifestJSON)

	default:
		respondError(w, "artifact type not downloadable directly, use the GET endpoints with format parameter", http.StatusBadRequest)
	}
}

package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// ============================================
// COMPLIANCE ARCHIVE STORE - In-Memory Job Tracking
// ============================================

// ArchiveJob represents an in-progress or completed archive job
type ArchiveJob struct {
	ID              string
	Request         FullArchiveRequest
	Status          ArchiveStatus
	Asset           AssetInfo
	Period          PeriodInfo
	Artifacts       []ArchiveArtifact
	Checksums       map[string]string
	Error           string
	CreatedAt       time.Time
	CompletedAt     *time.Time
}

// ArchiveStore manages archive jobs in memory
type ArchiveStore struct {
	mu      sync.RWMutex
	jobs    map[string]*ArchiveJob
	lineage []ArchiveLineageEntry // ordered list for lineage queries
}

// NewArchiveStore creates a new archive store
func NewArchiveStore() *ArchiveStore {
	return &ArchiveStore{
		jobs:    make(map[string]*ArchiveJob),
		lineage: make([]ArchiveLineageEntry, 0),
	}
}

// CreateJob creates a new archive job and returns its ID
func (s *ArchiveStore) CreateJob(req FullArchiveRequest) (*ArchiveJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Build asset info
	asset := AssetInfo{
		Code: req.AssetCode,
	}
	if req.AssetCode == "XLM" || req.AssetCode == "native" {
		asset.Type = "native"
	} else {
		asset.Type = "credit_alphanum4"
		if len(req.AssetCode) > 4 {
			asset.Type = "credit_alphanum12"
		}
		if req.AssetIssuer != "" {
			asset.Issuer = &req.AssetIssuer
		}
	}

	// Parse dates for period
	startDate, _ := time.Parse("2006-01-02", req.StartDate)
	endDate, _ := time.Parse("2006-01-02", req.EndDate)

	period := PeriodInfo{
		Start: startDate.Format(time.RFC3339),
		End:   endDate.Add(23*time.Hour + 59*time.Minute + 59*time.Second).Format(time.RFC3339),
	}

	// Generate archive ID
	archiveID := GenerateArchiveID("archive", req.AssetCode, time.Now())

	job := &ArchiveJob{
		ID:        archiveID,
		Request:   req,
		Status:    ArchiveStatusPending,
		Asset:     asset,
		Period:    period,
		Artifacts: make([]ArchiveArtifact, 0),
		Checksums: make(map[string]string),
		CreatedAt: time.Now().UTC(),
	}

	s.jobs[archiveID] = job
	return job, nil
}

// GetJob retrieves a job by ID
func (s *ArchiveStore) GetJob(id string) (*ArchiveJob, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return job, ok
}

// UpdateJobStatus updates the status of a job
func (s *ArchiveStore) UpdateJobStatus(id string, status ArchiveStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[id]; ok {
		job.Status = status
		if status == ArchiveStatusComplete || status == ArchiveStatusFailed {
			now := time.Now().UTC()
			job.CompletedAt = &now
		}
	}
}

// AddArtifact adds an artifact to a job
func (s *ArchiveStore) AddArtifact(id string, artifact ArchiveArtifact) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[id]; ok {
		job.Artifacts = append(job.Artifacts, artifact)
		job.Checksums[artifact.Type] = artifact.Checksum
	}
}

// SetJobError sets an error on a job
func (s *ArchiveStore) SetJobError(id string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[id]; ok {
		job.Error = err.Error()
		job.Status = ArchiveStatusFailed
		now := time.Now().UTC()
		job.CompletedAt = &now
	}
}

// CompleteJob marks a job as complete and adds to lineage
func (s *ArchiveStore) CompleteJob(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job, ok := s.jobs[id]; ok {
		job.Status = ArchiveStatusComplete
		now := time.Now().UTC()
		job.CompletedAt = &now

		// Add to lineage
		issuer := ""
		if job.Asset.Issuer != nil {
			issuer = *job.Asset.Issuer
		}
		entry := ArchiveLineageEntry{
			ArchiveID:          job.ID,
			AssetCode:          job.Asset.Code,
			AssetIssuer:        issuer,
			PeriodStart:        job.Period.Start,
			PeriodEnd:          job.Period.End,
			ArtifactsCount:     len(job.Artifacts),
			MethodologyVersion: MethodologyArchiveV1,
			Status:             string(job.Status),
			Checksums:          job.Checksums,
			GeneratedAt:        job.CompletedAt.Format(time.RFC3339),
		}
		s.lineage = append([]ArchiveLineageEntry{entry}, s.lineage...) // prepend for recent-first
	}
}

// GetLineage returns archive lineage entries with optional filtering
func (s *ArchiveStore) GetLineage(assetCode, assetIssuer string, limit int) []ArchiveLineageEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []ArchiveLineageEntry
	for _, entry := range s.lineage {
		// Apply filters
		if assetCode != "" && entry.AssetCode != assetCode {
			continue
		}
		if assetIssuer != "" && entry.AssetIssuer != assetIssuer {
			continue
		}
		results = append(results, entry)
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	return results
}

// ============================================
// ARCHIVE JOB PROCESSOR
// ============================================

// ArchiveProcessor handles async archive generation
type ArchiveProcessor struct {
	store  *ArchiveStore
	reader *UnifiedSilverReader
}

// NewArchiveProcessor creates a new archive processor
func NewArchiveProcessor(store *ArchiveStore, reader *UnifiedSilverReader) *ArchiveProcessor {
	return &ArchiveProcessor{
		store:  store,
		reader: reader,
	}
}

// ProcessJob processes an archive job in the background
func (p *ArchiveProcessor) ProcessJob(ctx context.Context, jobID string) {
	job, ok := p.store.GetJob(jobID)
	if !ok {
		log.Printf("Archive job %s not found", jobID)
		return
	}

	log.Printf("Starting archive job %s for asset %s", jobID, job.Asset.Code)
	p.store.UpdateJobStatus(jobID, ArchiveStatusProcessing)

	req := job.Request

	// Parse dates
	startDate, err := time.Parse("2006-01-02", req.StartDate)
	if err != nil {
		p.store.SetJobError(jobID, fmt.Errorf("invalid start_date: %w", err))
		return
	}
	endDate, err := time.Parse("2006-01-02", req.EndDate)
	if err != nil {
		p.store.SetJobError(jobID, fmt.Errorf("invalid end_date: %w", err))
		return
	}
	endDate = endDate.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

	// Determine what to include
	includeTransactions := contains(req.Include, "transactions")
	includeBalances := contains(req.Include, "balances")
	includeSupply := contains(req.Include, "supply")

	// Default to all if none specified
	if !includeTransactions && !includeBalances && !includeSupply {
		includeTransactions = true
		includeBalances = true
		includeSupply = true
	}

	// Generate transactions artifact
	if includeTransactions {
		log.Printf("Archive %s: generating transactions artifact", jobID)
		txResp, err := p.reader.GetAssetTransactions(ctx, req.AssetCode, req.AssetIssuer, startDate, endDate, false, 10000)
		if err != nil {
			p.store.SetJobError(jobID, fmt.Errorf("failed to get transactions: %w", err))
			return
		}
		artifact := ArchiveArtifact{
			Name:        "transactions.json",
			Type:        "transactions",
			RowCount:    len(txResp.Transactions),
			Checksum:    txResp.Checksum,
			GeneratedAt: txResp.GeneratedAt,
		}
		p.store.AddArtifact(jobID, artifact)
	}

	// Generate balance snapshot artifacts
	if includeBalances {
		snapshots := req.BalanceSnapshots
		if len(snapshots) == 0 {
			// Default to end date
			snapshots = []string{req.EndDate}
		}

		for _, snapDate := range snapshots {
			log.Printf("Archive %s: generating balance snapshot for %s", jobID, snapDate)
			snapTime, err := time.Parse("2006-01-02", snapDate)
			if err != nil {
				continue
			}
			snapTime = snapTime.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

			balResp, err := p.reader.GetComplianceBalances(ctx, req.AssetCode, req.AssetIssuer, snapTime, "", 10000)
			if err != nil {
				log.Printf("Archive %s: failed to get balances for %s: %v", jobID, snapDate, err)
				continue
			}

			artifact := ArchiveArtifact{
				Name:        fmt.Sprintf("balances_%s.json", snapDate),
				Type:        "balance_snapshot",
				SnapshotAt:  balResp.SnapshotAt,
				RowCount:    len(balResp.Holders),
				Checksum:    balResp.Checksum,
				GeneratedAt: balResp.GeneratedAt,
			}
			p.store.AddArtifact(jobID, artifact)
		}
	}

	// Generate supply timeline artifact
	if includeSupply {
		log.Printf("Archive %s: generating supply timeline artifact", jobID)
		supplyResp, err := p.reader.GetSupplyTimeline(ctx, req.AssetCode, req.AssetIssuer, startDate, endDate, "1d")
		if err != nil {
			p.store.SetJobError(jobID, fmt.Errorf("failed to get supply timeline: %w", err))
			return
		}
		artifact := ArchiveArtifact{
			Name:        "supply_timeline.json",
			Type:        "supply_timeline",
			RowCount:    len(supplyResp.Timeline),
			Checksum:    supplyResp.Checksum,
			GeneratedAt: supplyResp.GeneratedAt,
		}
		p.store.AddArtifact(jobID, artifact)
	}

	// Generate methodology document
	log.Printf("Archive %s: generating methodology document", jobID)
	job, _ = p.store.GetJob(jobID) // Refresh to get all artifacts
	methodologyDoc := MethodologyDoc{
		ArchiveID:          job.ID,
		AssetCode:          job.Asset.Code,
		AssetType:          job.Asset.Type,
		PeriodStart:        job.Period.Start,
		PeriodEnd:          job.Period.End,
		GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
		MethodologyVersion: MethodologyArchiveV1,
	}
	if job.Asset.Issuer != nil {
		methodologyDoc.AssetIssuer = *job.Asset.Issuer
	}

	// Add artifact info for methodology
	for _, a := range job.Artifacts {
		methodologyDoc.Artifacts = append(methodologyDoc.Artifacts, ArtifactInfo{
			Name:     a.Name,
			Type:     a.Type,
			Checksum: a.Checksum,
			RowCount: a.RowCount,
		})
	}

	methodologyContent := GenerateMethodologyDocument(methodologyDoc)
	methodologyChecksum, _ := GenerateChecksum(methodologyContent)

	methodologyArtifact := ArchiveArtifact{
		Name:        "methodology.md",
		Type:        "documentation",
		Checksum:    methodologyChecksum,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}
	p.store.AddArtifact(jobID, methodologyArtifact)

	// Generate manifest
	log.Printf("Archive %s: generating manifest", jobID)
	job, _ = p.store.GetJob(jobID) // Refresh again to include methodology
	now := time.Now().UTC()
	job.CompletedAt = &now // Set completion time for manifest

	manifest, err := GenerateManifest(job)
	if err != nil {
		log.Printf("Archive %s: failed to generate manifest: %v", jobID, err)
	} else {
		manifestJSON, _ := GenerateManifestJSON(manifest)
		manifestChecksum, _ := GenerateChecksum(string(manifestJSON))

		manifestArtifact := ArchiveArtifact{
			Name:        "manifest.json",
			Type:        "manifest",
			Checksum:    manifestChecksum,
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		}
		p.store.AddArtifact(jobID, manifestArtifact)
	}

	// Complete the job
	p.store.CompleteJob(jobID)
	log.Printf("Archive job %s completed successfully", jobID)
}

// Helper function
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

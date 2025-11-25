// Package main provides the audit layer integration for Cycle 2.
// This file encapsulates checkpoint, manifest, and PAS functionality.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/checkpoint"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/manifest"
	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/pas"
)

// AuditLayer manages checkpoint, manifest, and PAS subsystems.
type AuditLayer struct {
	// Checkpoint for resume capability
	checkpointer *checkpoint.Checkpointer

	// Manifest builder for batch metadata
	manifestBuilder *manifest.Builder

	// PAS emitter for hash-chained audit events
	pasEmitter *pas.Emitter

	// Producer identity for PAS events
	pasProducer pas.Producer

	// Configuration
	config AuditConfig
}

// AuditConfig holds configuration for all audit subsystems.
type AuditConfig struct {
	Checkpoint checkpoint.Config
	Manifest   manifest.Config
	PAS        pas.Config

	// Producer identity
	ProducerID      string // Unique instance identifier
	ProducerVersion string // Processor version
	Network         string // Stellar network name (mainnet, testnet)
	EraID           string // Processing era (e.g., "p23_plus")

	// Output directory for manifests (usually DuckLake data_path)
	ManifestOutputDir string
}

// NewAuditLayer creates a new audit layer with the given configuration.
func NewAuditLayer(cfg AuditConfig) (*AuditLayer, error) {
	al := &AuditLayer{
		config: cfg,
	}

	// Initialize checkpoint system
	if cfg.Checkpoint.Enabled {
		cfg.Checkpoint.ApplyDefaults()
		cp, err := checkpoint.NewCheckpointer(cfg.Checkpoint)
		if err != nil {
			return nil, err
		}
		al.checkpointer = cp

		// Load existing checkpoint if present
		if _, err := cp.Load(); err != nil {
			return nil, err
		}
		log.Printf("[audit] Checkpoint system initialized (dir: %s)", cfg.Checkpoint.Dir)
	}

	// Initialize manifest builder
	if cfg.Manifest.Enabled {
		outputDir := cfg.ManifestOutputDir
		if cfg.Manifest.Dir != "" {
			outputDir = cfg.Manifest.Dir
		}
		al.manifestBuilder = manifest.NewBuilder(cfg.ProducerVersion, outputDir)
		log.Printf("[audit] Manifest builder initialized (dir: %s)", outputDir)
	}

	// Initialize PAS emitter
	if cfg.PAS.Enabled {
		cfg.PAS.ApplyDefaults()
		emitter, err := pas.NewEmitter(cfg.PAS)
		if err != nil {
			return nil, err
		}
		al.pasEmitter = emitter
		log.Printf("[audit] PAS emitter initialized (backup_dir: %s)", cfg.PAS.BackupDir)
	}

	// Set up producer identity
	al.pasProducer = pas.Producer{
		ID:      cfg.ProducerID,
		Version: cfg.ProducerVersion,
		Network: cfg.Network,
		EraID:   cfg.EraID,
	}

	return al, nil
}

// GetResumePoint returns the ledger to resume from based on checkpoint state.
// Returns startLedger if no checkpoint exists.
func (al *AuditLayer) GetResumePoint(configStartLedger uint32) uint32 {
	if al.checkpointer == nil {
		return configStartLedger
	}
	return al.checkpointer.GetResumePoint(configStartLedger)
}

// ValidateCheckpoint validates checkpoint compatibility with current config.
func (al *AuditLayer) ValidateCheckpoint(network, sourceMode string) error {
	if al.checkpointer == nil {
		return nil
	}
	return al.checkpointer.Validate(network, sourceMode)
}

// BatchStats holds statistics for a processed batch.
type BatchStats struct {
	LedgerStart uint32
	LedgerEnd   uint32
	LedgerCount int
	Tables      map[string]manifest.TableStats
	StartTime   time.Time
}

// PASEventInfo holds information about the emitted PAS event for lineage linkage.
// Cycle 4: Added for era-aware meta tables.
type PASEventInfo struct {
	EventID   string // filename-based identifier
	EventHash string // SHA256 hash of the event
}

// OnFlushComplete is called after a successful flush to update audit state.
// It generates manifest, emits PAS event, and saves checkpoint.
// Cycle 4: Returns PASEventInfo for lineage linkage.
func (al *AuditLayer) OnFlushComplete(
	stats BatchStats,
	sourceMode, network string,
	configStartLedger, configEndLedger uint32,
) (*PASEventInfo, error) {
	processingDuration := time.Since(stats.StartTime)

	// 1. Generate and save manifest
	var manifestHash string
	var totalRows int64
	var tableSummaries []pas.TableSummary

	if al.manifestBuilder != nil {
		m := al.manifestBuilder.Build(
			stats.LedgerStart,
			stats.LedgerEnd,
			stats.LedgerCount,
			stats.Tables,
		)
		if err := al.manifestBuilder.Save(m); err != nil {
			log.Printf("[audit] Warning: failed to save manifest: %v", err)
		} else {
			manifestHash = m.ManifestChecksum
			totalRows = m.TotalRows

			// Convert table manifests to PAS summaries
			for _, t := range m.Tables {
				tableSummaries = append(tableSummaries, pas.TableSummary{
					Name:     t.Name,
					RowCount: t.RowCount,
					Checksum: t.Checksum,
				})
			}
		}
	}

	// 2. Emit PAS event
	var pasInfo *PASEventInfo
	if al.pasEmitter != nil {
		err := al.pasEmitter.EmitBatch(
			al.pasProducer,
			stats.LedgerStart,
			stats.LedgerEnd,
			stats.LedgerCount,
			tableSummaries,
			manifestHash,
			totalRows,
			processingDuration.Milliseconds(),
		)
		if err != nil {
			if al.config.PAS.Strict {
				return nil, err
			}
			log.Printf("[audit] Warning: failed to emit PAS event: %v", err)
		} else {
			// Cycle 4: Capture PAS event info for lineage linkage
			pasInfo = &PASEventInfo{
				EventID:   al.generatePASEventID(stats.LedgerStart, stats.LedgerEnd),
				EventHash: al.pasEmitter.GetPreviousHash(), // This is now the current event's hash
			}
		}
	}

	// 3. Save checkpoint
	if al.checkpointer != nil {
		err := al.checkpointer.Update(
			stats.LedgerEnd,
			sourceMode,
			network,
			al.config.ProducerVersion,
			configStartLedger,
			configEndLedger,
		)
		if err != nil {
			log.Printf("[audit] Warning: failed to save checkpoint: %v", err)
		}
	}

	return pasInfo, nil
}

// generatePASEventID creates a unique identifier for a PAS event based on ledger range.
// Cycle 4: Helper for lineage linkage.
func (al *AuditLayer) generatePASEventID(ledgerStart, ledgerEnd uint32) string {
	return fmt.Sprintf("%s/%s/%d-%d", al.config.ProducerID, al.config.EraID, ledgerStart, ledgerEnd)
}

// Close cleans up audit layer resources.
func (al *AuditLayer) Close() error {
	if al.pasEmitter != nil {
		return al.pasEmitter.Close()
	}
	return nil
}

// GetCheckpointState returns the current checkpoint state for inspection.
func (al *AuditLayer) GetCheckpointState() *checkpoint.Checkpoint {
	if al.checkpointer == nil {
		return nil
	}
	return al.checkpointer.Current()
}

// GetPASStats returns PAS chain statistics.
func (al *AuditLayer) GetPASStats() *pas.Stats {
	if al.pasEmitter == nil {
		return nil
	}
	stats := al.pasEmitter.GetStats()
	return &stats
}

// VerifyPASChain verifies the integrity of the PAS event chain.
func (al *AuditLayer) VerifyPASChain() error {
	if al.pasEmitter == nil {
		return nil
	}
	return al.pasEmitter.VerifyChainIntegrity()
}

// IsCheckpointEnabled returns true if checkpoint system is enabled.
func (al *AuditLayer) IsCheckpointEnabled() bool {
	return al.checkpointer != nil
}

// IsManifestEnabled returns true if manifest generation is enabled.
func (al *AuditLayer) IsManifestEnabled() bool {
	return al.manifestBuilder != nil
}

// IsPASEnabled returns true if PAS emission is enabled.
func (al *AuditLayer) IsPASEnabled() bool {
	return al.pasEmitter != nil
}

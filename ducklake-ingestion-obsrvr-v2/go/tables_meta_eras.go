package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

// Era represents an era record from _meta_eras.
type Era struct {
	EraID        string
	Network      string
	VersionLabel string
	LedgerStart  uint32
	LedgerEnd    *uint32 // NULL if active
	ProtocolMin  *int
	ProtocolMax  *int
	Status       string // 'active', 'frozen', 'deprecated'
	SchemaEpoch  *string
	PASChainHead *string
	CreatedAt    time.Time
	FrozenAt     *time.Time
}

// EraStatus constants
const (
	EraStatusActive     = "active"
	EraStatusFrozen     = "frozen"
	EraStatusDeprecated = "deprecated"
)

// createMetaErasTable creates the _meta_eras table if it doesn't exist.
func (ing *Ingester) createMetaErasTable(ctx context.Context) error {
	schema := ing.config.DuckLake.SchemaName
	catalogName := ing.config.DuckLake.CatalogName

	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s._meta_eras (
			era_id          VARCHAR NOT NULL,
			network         VARCHAR NOT NULL,
			version_label   VARCHAR NOT NULL,
			ledger_start    BIGINT NOT NULL,
			ledger_end      BIGINT,
			protocol_min    INTEGER,
			protocol_max    INTEGER,
			status          VARCHAR NOT NULL DEFAULT 'active',
			schema_epoch    VARCHAR,
			pas_chain_head  VARCHAR,
			created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			frozen_at       TIMESTAMP,
			PRIMARY KEY (era_id, network, version_label)
		)
	`, catalogName, schema)

	_, err := ing.db.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to create _meta_eras table: %w", err)
	}

	log.Printf("Metadata table ready: %s.%s._meta_eras", catalogName, schema)
	return nil
}

// registerEra registers the current era in _meta_eras if it doesn't exist.
// Returns an error if trying to write to a frozen or deprecated era.
func (ing *Ingester) registerEra(ctx context.Context) error {
	if ing.eraConfig == nil {
		log.Printf("[era] No era config provided, skipping era registration")
		return nil
	}

	schema := ing.config.DuckLake.SchemaName
	catalogName := ing.config.DuckLake.CatalogName
	eraCfg := ing.eraConfig

	// Check if era exists
	var status string
	var existingLedgerStart uint32
	err := ing.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT status, ledger_start
		FROM %s.%s._meta_eras
		WHERE era_id = $1 AND network = $2 AND version_label = $3
	`, catalogName, schema), eraCfg.EraID, eraCfg.Network, eraCfg.VersionLabel).Scan(&status, &existingLedgerStart)

	if err == sql.ErrNoRows {
		// New era - register it
		_, err = ing.db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s.%s._meta_eras
			(era_id, network, version_label, ledger_start, status, created_at)
			VALUES ($1, $2, $3, $4, 'active', CURRENT_TIMESTAMP)
		`, catalogName, schema),
			eraCfg.EraID, eraCfg.Network, eraCfg.VersionLabel, ing.config.Source.StartLedger)

		if err != nil {
			return fmt.Errorf("failed to register era: %w", err)
		}

		log.Printf("[era] Registered new era: %s/%s/%s starting at ledger %d",
			eraCfg.Network, eraCfg.EraID, eraCfg.VersionLabel, ing.config.Source.StartLedger)
		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to check era: %w", err)
	}

	// Era exists - check status
	switch status {
	case EraStatusFrozen:
		return fmt.Errorf("cannot write to frozen era %s/%s/%s", eraCfg.Network, eraCfg.EraID, eraCfg.VersionLabel)
	case EraStatusDeprecated:
		return fmt.Errorf("cannot write to deprecated era %s/%s/%s", eraCfg.Network, eraCfg.EraID, eraCfg.VersionLabel)
	case EraStatusActive:
		log.Printf("[era] Using existing era: %s/%s/%s (status: %s, ledger_start: %d)",
			eraCfg.Network, eraCfg.EraID, eraCfg.VersionLabel, status, existingLedgerStart)
		return nil
	default:
		return fmt.Errorf("unknown era status: %s", status)
	}
}

// updateEraPASChainHead updates the PAS chain head for the current era.
func (ing *Ingester) updateEraPASChainHead(ctx context.Context, pasHash string) error {
	if ing.eraConfig == nil {
		return nil
	}

	schema := ing.config.DuckLake.SchemaName
	catalogName := ing.config.DuckLake.CatalogName
	eraCfg := ing.eraConfig

	_, err := ing.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s.%s._meta_eras
		SET pas_chain_head = $1
		WHERE era_id = $2 AND network = $3 AND version_label = $4
	`, catalogName, schema), pasHash, eraCfg.EraID, eraCfg.Network, eraCfg.VersionLabel)

	if err != nil {
		return fmt.Errorf("failed to update PAS chain head: %w", err)
	}

	return nil
}

// freezeEra marks an era as frozen with its final ledger_end.
// This is typically called manually or by an operator script, not during normal operation.
func (ing *Ingester) freezeEra(ctx context.Context, eraID, network, versionLabel string, ledgerEnd uint32) error {
	schema := ing.config.DuckLake.SchemaName
	catalogName := ing.config.DuckLake.CatalogName

	result, err := ing.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s.%s._meta_eras
		SET status = 'frozen',
		    ledger_end = $1,
		    frozen_at = CURRENT_TIMESTAMP
		WHERE era_id = $2 AND network = $3 AND version_label = $4 AND status = 'active'
	`, catalogName, schema), ledgerEnd, eraID, network, versionLabel)

	if err != nil {
		return fmt.Errorf("failed to freeze era: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("era not found or already frozen: %s/%s/%s", network, eraID, versionLabel)
	}

	log.Printf("[era] Frozen era: %s/%s/%s at ledger %d", network, eraID, versionLabel, ledgerEnd)
	return nil
}

// getEra retrieves an era record by ID.
func (ing *Ingester) getEra(ctx context.Context, eraID, network, versionLabel string) (*Era, error) {
	schema := ing.config.DuckLake.SchemaName
	catalogName := ing.config.DuckLake.CatalogName

	var era Era
	var ledgerEnd, protocolMin, protocolMax sql.NullInt64
	var schemaEpoch, pasChainHead sql.NullString
	var frozenAt sql.NullTime

	err := ing.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT era_id, network, version_label, ledger_start, ledger_end,
		       protocol_min, protocol_max, status, schema_epoch, pas_chain_head,
		       created_at, frozen_at
		FROM %s.%s._meta_eras
		WHERE era_id = $1 AND network = $2 AND version_label = $3
	`, catalogName, schema), eraID, network, versionLabel).Scan(
		&era.EraID, &era.Network, &era.VersionLabel, &era.LedgerStart, &ledgerEnd,
		&protocolMin, &protocolMax, &era.Status, &schemaEpoch, &pasChainHead,
		&era.CreatedAt, &frozenAt)

	if err != nil {
		return nil, err
	}

	// Handle nullable fields
	if ledgerEnd.Valid {
		v := uint32(ledgerEnd.Int64)
		era.LedgerEnd = &v
	}
	if protocolMin.Valid {
		v := int(protocolMin.Int64)
		era.ProtocolMin = &v
	}
	if protocolMax.Valid {
		v := int(protocolMax.Int64)
		era.ProtocolMax = &v
	}
	if schemaEpoch.Valid {
		era.SchemaEpoch = &schemaEpoch.String
	}
	if pasChainHead.Valid {
		era.PASChainHead = &pasChainHead.String
	}
	if frozenAt.Valid {
		era.FrozenAt = &frozenAt.Time
	}

	return &era, nil
}

// getActiveEra retrieves the active era for a network.
func (ing *Ingester) getActiveEra(ctx context.Context, network string) (*Era, error) {
	schema := ing.config.DuckLake.SchemaName
	catalogName := ing.config.DuckLake.CatalogName

	var era Era
	var ledgerEnd, protocolMin, protocolMax sql.NullInt64
	var schemaEpoch, pasChainHead sql.NullString
	var frozenAt sql.NullTime

	err := ing.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT era_id, network, version_label, ledger_start, ledger_end,
		       protocol_min, protocol_max, status, schema_epoch, pas_chain_head,
		       created_at, frozen_at
		FROM %s.%s._meta_eras
		WHERE network = $1 AND status = 'active'
		ORDER BY ledger_start DESC
		LIMIT 1
	`, catalogName, schema), network).Scan(
		&era.EraID, &era.Network, &era.VersionLabel, &era.LedgerStart, &ledgerEnd,
		&protocolMin, &protocolMax, &era.Status, &schemaEpoch, &pasChainHead,
		&era.CreatedAt, &frozenAt)

	if err != nil {
		return nil, err
	}

	// Handle nullable fields (same as getEra)
	if ledgerEnd.Valid {
		v := uint32(ledgerEnd.Int64)
		era.LedgerEnd = &v
	}
	if protocolMin.Valid {
		v := int(protocolMin.Int64)
		era.ProtocolMin = &v
	}
	if protocolMax.Valid {
		v := int(protocolMax.Int64)
		era.ProtocolMax = &v
	}
	if schemaEpoch.Valid {
		era.SchemaEpoch = &schemaEpoch.String
	}
	if pasChainHead.Valid {
		era.PASChainHead = &pasChainHead.String
	}
	if frozenAt.Valid {
		era.FrozenAt = &frozenAt.Time
	}

	return &era, nil
}

// listEras retrieves all eras for a network.
func (ing *Ingester) listEras(ctx context.Context, network string) ([]*Era, error) {
	schema := ing.config.DuckLake.SchemaName
	catalogName := ing.config.DuckLake.CatalogName

	rows, err := ing.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT era_id, network, version_label, ledger_start, ledger_end,
		       protocol_min, protocol_max, status, schema_epoch, pas_chain_head,
		       created_at, frozen_at
		FROM %s.%s._meta_eras
		WHERE network = $1
		ORDER BY ledger_start
	`, catalogName, schema), network)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var eras []*Era
	for rows.Next() {
		var era Era
		var ledgerEnd, protocolMin, protocolMax sql.NullInt64
		var schemaEpoch, pasChainHead sql.NullString
		var frozenAt sql.NullTime

		err := rows.Scan(
			&era.EraID, &era.Network, &era.VersionLabel, &era.LedgerStart, &ledgerEnd,
			&protocolMin, &protocolMax, &era.Status, &schemaEpoch, &pasChainHead,
			&era.CreatedAt, &frozenAt)
		if err != nil {
			return nil, err
		}

		// Handle nullable fields
		if ledgerEnd.Valid {
			v := uint32(ledgerEnd.Int64)
			era.LedgerEnd = &v
		}
		if protocolMin.Valid {
			v := int(protocolMin.Int64)
			era.ProtocolMin = &v
		}
		if protocolMax.Valid {
			v := int(protocolMax.Int64)
			era.ProtocolMax = &v
		}
		if schemaEpoch.Valid {
			era.SchemaEpoch = &schemaEpoch.String
		}
		if pasChainHead.Valid {
			era.PASChainHead = &pasChainHead.String
		}
		if frozenAt.Valid {
			era.FrozenAt = &frozenAt.Time
		}

		eras = append(eras, &era)
	}

	return eras, nil
}

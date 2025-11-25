// Package resolver provides Bronze layer data resolution and routing.
// Cycle 5: Bronze Resolver Library - Era Resolution
package resolver

import (
	"context"
	"database/sql"
	"fmt"
)

// pickEra routes an intent to the appropriate era.
func (r *Resolver) pickEra(ctx context.Context, intent Intent) (*Era, error) {
	switch intent.Mode {
	case IntentLatest:
		return r.getActiveEra(ctx, intent.Network)

	case IntentAsOfLedger:
		if intent.Ledger == nil {
			return nil, fmt.Errorf("ledger is required for as_of_ledger intent")
		}
		return r.getEraForLedger(ctx, intent.Network, *intent.Ledger)

	case IntentAsOfProtocol:
		if intent.Protocol == nil {
			return nil, fmt.Errorf("protocol is required for as_of_protocol intent")
		}
		return r.getEraForProtocol(ctx, intent.Network, *intent.Protocol)

	case IntentExplicit:
		if intent.EraID == "" {
			return nil, fmt.Errorf("era_id is required for explicit intent")
		}
		return r.getEra(ctx, intent.Network, intent.EraID, intent.VersionLabel)

	default:
		return nil, fmt.Errorf("unknown intent mode: %s", intent.Mode)
	}
}

// getActiveEra returns the active era for a network.
func (r *Resolver) getActiveEra(ctx context.Context, network string) (*Era, error) {
	query := fmt.Sprintf(`
		SELECT era_id, network, version_label, ledger_start, ledger_end,
		       protocol_min, protocol_max, status, schema_epoch, pas_chain_head,
		       created_at, frozen_at
		FROM %s.%s._meta_eras
		WHERE network = ? AND status = 'active'
		ORDER BY ledger_start DESC
		LIMIT 1
	`, r.catalogName, r.schemaName)

	era, err := r.scanEra(r.db.QueryRowContext(ctx, query, network))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no active era found for network %s", network)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get active era: %w", err)
	}

	return era, nil
}

// getEraForLedger returns the era that covers a specific ledger.
func (r *Resolver) getEraForLedger(ctx context.Context, network string, ledger uint32) (*Era, error) {
	query := fmt.Sprintf(`
		SELECT era_id, network, version_label, ledger_start, ledger_end,
		       protocol_min, protocol_max, status, schema_epoch, pas_chain_head,
		       created_at, frozen_at
		FROM %s.%s._meta_eras
		WHERE network = ?
		  AND ledger_start <= ?
		  AND (ledger_end IS NULL OR ledger_end >= ?)
		ORDER BY ledger_start DESC
		LIMIT 1
	`, r.catalogName, r.schemaName)

	era, err := r.scanEra(r.db.QueryRowContext(ctx, query, network, ledger, ledger))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no era found covering ledger %d on network %s", ledger, network)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get era for ledger: %w", err)
	}

	return era, nil
}

// getEraForProtocol returns the era that covers a specific protocol version.
func (r *Resolver) getEraForProtocol(ctx context.Context, network string, protocol int) (*Era, error) {
	query := fmt.Sprintf(`
		SELECT era_id, network, version_label, ledger_start, ledger_end,
		       protocol_min, protocol_max, status, schema_epoch, pas_chain_head,
		       created_at, frozen_at
		FROM %s.%s._meta_eras
		WHERE network = ?
		  AND protocol_min <= ?
		  AND (protocol_max IS NULL OR protocol_max >= ?)
		ORDER BY ledger_start DESC
		LIMIT 1
	`, r.catalogName, r.schemaName)

	era, err := r.scanEra(r.db.QueryRowContext(ctx, query, network, protocol, protocol))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no era found covering protocol %d on network %s", protocol, network)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get era for protocol: %w", err)
	}

	return era, nil
}

// getEra returns a specific era by ID and version.
func (r *Resolver) getEra(ctx context.Context, network, eraID, versionLabel string) (*Era, error) {
	query := fmt.Sprintf(`
		SELECT era_id, network, version_label, ledger_start, ledger_end,
		       protocol_min, protocol_max, status, schema_epoch, pas_chain_head,
		       created_at, frozen_at
		FROM %s.%s._meta_eras
		WHERE network = ? AND era_id = ? AND version_label = ?
		LIMIT 1
	`, r.catalogName, r.schemaName)

	era, err := r.scanEra(r.db.QueryRowContext(ctx, query, network, eraID, versionLabel))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("era %s/%s not found for network %s", eraID, versionLabel, network)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get era: %w", err)
	}

	return era, nil
}

// listEras returns all eras for a network, ordered by ledger_start.
func (r *Resolver) listEras(ctx context.Context, network string) ([]Era, error) {
	query := fmt.Sprintf(`
		SELECT era_id, network, version_label, ledger_start, ledger_end,
		       protocol_min, protocol_max, status, schema_epoch, pas_chain_head,
		       created_at, frozen_at
		FROM %s.%s._meta_eras
		WHERE network = ?
		ORDER BY ledger_start
	`, r.catalogName, r.schemaName)

	rows, err := r.db.QueryContext(ctx, query, network)
	if err != nil {
		return nil, fmt.Errorf("failed to list eras: %w", err)
	}
	defer rows.Close()

	var eras []Era
	for rows.Next() {
		era, err := r.scanEraFromRows(rows)
		if err != nil {
			return nil, err
		}
		eras = append(eras, *era)
	}

	return eras, nil
}

// scanEra scans an era from a single row query.
func (r *Resolver) scanEra(row *sql.Row) (*Era, error) {
	var era Era
	var ledgerEnd sql.NullInt64
	var protocolMin, protocolMax sql.NullInt32
	var schemaEpoch, pasChainHead sql.NullString
	var frozenAt sql.NullTime

	err := row.Scan(
		&era.EraID, &era.Network, &era.VersionLabel,
		&era.LedgerStart, &ledgerEnd,
		&protocolMin, &protocolMax,
		&era.Status, &schemaEpoch, &pasChainHead,
		&era.CreatedAt, &frozenAt,
	)
	if err != nil {
		return nil, err
	}

	// Handle nullable fields
	if ledgerEnd.Valid {
		val := uint32(ledgerEnd.Int64)
		era.LedgerEnd = &val
	}
	if protocolMin.Valid {
		val := int(protocolMin.Int32)
		era.ProtocolMin = &val
	}
	if protocolMax.Valid {
		val := int(protocolMax.Int32)
		era.ProtocolMax = &val
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

// scanEraFromRows scans an era from a result set row.
func (r *Resolver) scanEraFromRows(rows *sql.Rows) (*Era, error) {
	var era Era
	var ledgerEnd sql.NullInt64
	var protocolMin, protocolMax sql.NullInt32
	var schemaEpoch, pasChainHead sql.NullString
	var frozenAt sql.NullTime

	err := rows.Scan(
		&era.EraID, &era.Network, &era.VersionLabel,
		&era.LedgerStart, &ledgerEnd,
		&protocolMin, &protocolMax,
		&era.Status, &schemaEpoch, &pasChainHead,
		&era.CreatedAt, &frozenAt,
	)
	if err != nil {
		return nil, err
	}

	// Handle nullable fields
	if ledgerEnd.Valid {
		val := uint32(ledgerEnd.Int64)
		era.LedgerEnd = &val
	}
	if protocolMin.Valid {
		val := int(protocolMin.Int32)
		era.ProtocolMin = &val
	}
	if protocolMax.Valid {
		val := int(protocolMax.Int32)
		era.ProtocolMax = &val
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

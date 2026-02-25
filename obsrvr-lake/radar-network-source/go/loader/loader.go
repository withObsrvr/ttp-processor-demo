package loader

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/gen/network_topology_service"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Loader queries Stellarbeat's PostgreSQL for scan data and measurements.
type Loader struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Loader {
	return &Loader{db: db}
}

// ScanRow represents a row from the network_scan table.
type ScanRow struct {
	ID                  uint32
	Time                time.Time
	LatestLedger        uint64
	LatestLedgerCloseTime time.Time
}

// LoadCompletedScansSince returns all completed scans with id > afterID, ordered ascending.
func (l *Loader) LoadCompletedScansSince(ctx context.Context, afterID uint32) ([]ScanRow, error) {
	rows, err := l.db.Query(ctx, `
		SELECT id, time, "latestLedger", "latestLedgerCloseTime"
		FROM network_scan
		WHERE completed = true AND id > $1
		ORDER BY id ASC
	`, afterID)
	if err != nil {
		return nil, fmt.Errorf("query completed scans: %w", err)
	}
	defer rows.Close()

	var scans []ScanRow
	for rows.Next() {
		var s ScanRow
		if err := rows.Scan(&s.ID, &s.Time, &s.LatestLedger, &s.LatestLedgerCloseTime); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		scans = append(scans, s)
	}
	return scans, rows.Err()
}

// LoadSnapshot loads a complete NetworkSnapshot for a given scan.
func (l *Loader) LoadSnapshot(ctx context.Context, scanID uint32) (*pb.NetworkSnapshot, error) {
	// Load scan metadata
	var scanTime, ledgerCloseTime time.Time
	var latestLedger uint64
	err := l.db.QueryRow(ctx, `
		SELECT time, "latestLedger", "latestLedgerCloseTime"
		FROM network_scan
		WHERE id = $1 AND completed = true
	`, scanID).Scan(&scanTime, &latestLedger, &ledgerCloseTime)
	if err != nil {
		return nil, fmt.Errorf("load scan %d: %w", scanID, err)
	}

	snapshot := &pb.NetworkSnapshot{
		ScanId:               scanID,
		ScanTime:             timestamppb.New(scanTime),
		LatestLedger:         latestLedger,
		LatestLedgerCloseTime: timestamppb.New(ledgerCloseTime),
	}

	// Load network measurement
	nm, err := l.loadNetworkMeasurement(ctx, scanTime)
	if err != nil {
		return nil, fmt.Errorf("load network measurement: %w", err)
	}
	snapshot.NetworkMeasurement = nm

	// Load node measurements
	nodes, err := l.loadNodeMeasurements(ctx, scanTime)
	if err != nil {
		return nil, fmt.Errorf("load node measurements: %w", err)
	}
	snapshot.NodeMeasurements = nodes

	// Load org measurements
	orgs, err := l.loadOrgMeasurements(ctx, scanTime)
	if err != nil {
		return nil, fmt.Errorf("load org measurements: %w", err)
	}
	snapshot.OrgMeasurements = orgs

	return snapshot, nil
}

func (l *Loader) loadNetworkMeasurement(ctx context.Context, scanTime time.Time) (*pb.NetworkMeasurement, error) {
	nm := &pb.NetworkMeasurement{}
	err := l.db.QueryRow(ctx, `
		SELECT
			"nrOfActiveWatchers",
			"nrOfConnectableNodes",
			"nrOfActiveValidators",
			"nrOfActiveFullValidators",
			"nrOfActiveOrganizations",
			"transitiveQuorumSetSize",
			"hasTransitiveQuorumSet",
			"topTierSize",
			"topTierOrgsSize",
			"hasSymmetricTopTier",
			"hasQuorumIntersection",
			"minBlockingSetSize",
			"minBlockingSetFilteredSize",
			"minBlockingSetOrgsSize",
			"minBlockingSetOrgsFilteredSize",
			"minBlockingSetCountrySize",
			"minBlockingSetCountryFilteredSize",
			"minBlockingSetISPSize",
			"minBlockingSetISPFilteredSize",
			"minSplittingSetSize",
			"minSplittingSetOrgsSize",
			"minSplittingSetCountrySize",
			"minSplittingSetISPSize"
		FROM network_measurement
		WHERE time = $1
	`, scanTime).Scan(
		&nm.NrOfActiveWatchers,
		&nm.NrOfConnectableNodes,
		&nm.NrOfActiveValidators,
		&nm.NrOfActiveFullValidators,
		&nm.NrOfActiveOrganizations,
		&nm.TransitiveQuorumSetSize,
		&nm.HasTransitiveQuorumSet,
		&nm.TopTierSize,
		&nm.TopTierOrgsSize,
		&nm.HasSymmetricTopTier,
		&nm.HasQuorumIntersection,
		&nm.MinBlockingSetSize,
		&nm.MinBlockingSetFilteredSize,
		&nm.MinBlockingSetOrgsSize,
		&nm.MinBlockingSetOrgsFilteredSize,
		&nm.MinBlockingSetCountrySize,
		&nm.MinBlockingSetCountryFilteredSize,
		&nm.MinBlockingSetIspSize,
		&nm.MinBlockingSetIspFilteredSize,
		&nm.MinSplittingSetSize,
		&nm.MinSplittingSetOrgsSize,
		&nm.MinSplittingSetCountrySize,
		&nm.MinSplittingSetIspSize,
	)
	if err != nil {
		return nil, err
	}
	return nm, nil
}

func (l *Loader) loadNodeMeasurements(ctx context.Context, scanTime time.Time) ([]*pb.NodeMeasurement, error) {
	rows, err := l.db.Query(ctx, `
		SELECT
			n."publicKey",
			nm."isActive",
			nm."isValidating",
			nm."isFullValidator",
			nm."isOverLoaded",
			nm."isActiveInScp",
			nm."connectivityError",
			nm."stellarCoreVersionBehind",
			nm."historyArchiveHasError",
			nm.lag,
			nm.index,
			COALESCE(nm."trustCentralityScore", 0),
			COALESCE(nm."pageRankScore", 0),
			COALESCE(nm."trustRank", 0)
		FROM node_measurement_v2 nm
		JOIN node n ON n.id = nm."nodeId"
		WHERE nm.time = $1
	`, scanTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []*pb.NodeMeasurement
	for rows.Next() {
		n := &pb.NodeMeasurement{}
		var lagMs *int32
		if err := rows.Scan(
			&n.PublicKey,
			&n.IsActive,
			&n.IsValidating,
			&n.IsFullValidator,
			&n.IsOverloaded,
			&n.IsActiveInScp,
			&n.ConnectivityError,
			&n.StellarCoreVersionBehind,
			&n.HistoryArchiveHasError,
			&lagMs,
			&n.Index,
			&n.TrustCentralityScore,
			&n.PageRankScore,
			&n.TrustRank,
		); err != nil {
			return nil, fmt.Errorf("scan node measurement: %w", err)
		}
		if lagMs != nil {
			n.LagMs = lagMs
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

func (l *Loader) loadOrgMeasurements(ctx context.Context, scanTime time.Time) ([]*pb.OrganizationMeasurement, error) {
	rows, err := l.db.Query(ctx, `
		SELECT
			o."organizationId",
			om."isSubQuorumAvailable",
			om.index,
			COALESCE(om."tomlState", '')
		FROM organization_measurement om
		JOIN organization o ON o.id = om."organizationId"
		WHERE om.time = $1
	`, scanTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orgs []*pb.OrganizationMeasurement
	for rows.Next() {
		o := &pb.OrganizationMeasurement{}
		if err := rows.Scan(
			&o.OrganizationId,
			&o.IsSubQuorumAvailable,
			&o.Index,
			&o.TomlState,
		); err != nil {
			return nil, fmt.Errorf("scan org measurement: %w", err)
		}
		orgs = append(orgs, o)
	}
	return orgs, rows.Err()
}

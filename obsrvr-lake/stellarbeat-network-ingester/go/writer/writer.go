package writer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pb "github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/gen/network_topology_service"
)

// Writer handles writing network topology snapshots to PostgreSQL.
type Writer struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Writer {
	return &Writer{db: db}
}

// WriteSnapshot writes a complete network snapshot as a single transaction.
// Uses ON CONFLICT DO NOTHING for idempotency.
func (w *Writer) WriteSnapshot(ctx context.Context, snapshot *pb.NetworkSnapshot) error {
	startTime := time.Now()

	tx, err := w.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert network-level snapshot with inlined network measurement
	nm := snapshot.NetworkMeasurement
	if nm == nil {
		nm = &pb.NetworkMeasurement{}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO network_topology_snapshots (
			scan_id, scan_time, latest_ledger, latest_ledger_close_time,
			nr_of_active_watchers, nr_of_connectable_nodes,
			nr_of_active_validators, nr_of_active_full_validators,
			nr_of_active_organizations,
			transitive_quorum_set_size, has_transitive_quorum_set,
			has_quorum_intersection,
			top_tier_size, top_tier_orgs_size, has_symmetric_top_tier,
			min_blocking_set_size, min_blocking_set_filtered_size,
			min_blocking_set_orgs_size, min_blocking_set_orgs_filtered_size,
			min_blocking_set_country_size, min_blocking_set_country_filtered_size,
			min_blocking_set_isp_size, min_blocking_set_isp_filtered_size,
			min_splitting_set_size, min_splitting_set_orgs_size,
			min_splitting_set_country_size, min_splitting_set_isp_size
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9,
			$10, $11, $12,
			$13, $14, $15,
			$16, $17, $18, $19, $20, $21, $22, $23,
			$24, $25, $26, $27
		) ON CONFLICT (scan_id) DO NOTHING
	`,
		snapshot.ScanId,
		snapshot.ScanTime.AsTime(),
		snapshot.LatestLedger,
		snapshot.LatestLedgerCloseTime.AsTime(),
		nm.NrOfActiveWatchers,
		nm.NrOfConnectableNodes,
		nm.NrOfActiveValidators,
		nm.NrOfActiveFullValidators,
		nm.NrOfActiveOrganizations,
		nm.TransitiveQuorumSetSize,
		nm.HasTransitiveQuorumSet,
		nm.HasQuorumIntersection,
		nm.TopTierSize,
		nm.TopTierOrgsSize,
		nm.HasSymmetricTopTier,
		nm.MinBlockingSetSize,
		nm.MinBlockingSetFilteredSize,
		nm.MinBlockingSetOrgsSize,
		nm.MinBlockingSetOrgsFilteredSize,
		nm.MinBlockingSetCountrySize,
		nm.MinBlockingSetCountryFilteredSize,
		nm.MinBlockingSetIspSize,
		nm.MinBlockingSetIspFilteredSize,
		nm.MinSplittingSetSize,
		nm.MinSplittingSetOrgsSize,
		nm.MinSplittingSetCountrySize,
		nm.MinSplittingSetIspSize,
	)
	if err != nil {
		return fmt.Errorf("insert snapshot: %w", err)
	}

	// Batch insert node measurements
	if len(snapshot.NodeMeasurements) > 0 {
		if err := w.insertNodeMeasurements(ctx, tx, snapshot.ScanId, snapshot.NodeMeasurements); err != nil {
			return fmt.Errorf("insert node measurements: %w", err)
		}
	}

	// Batch insert org measurements
	if len(snapshot.OrgMeasurements) > 0 {
		if err := w.insertOrgMeasurements(ctx, tx, snapshot.ScanId, snapshot.OrgMeasurements); err != nil {
			return fmt.Errorf("insert org measurements: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	log.Printf("Wrote scan_id=%d: %d nodes, %d orgs in %v",
		snapshot.ScanId, len(snapshot.NodeMeasurements), len(snapshot.OrgMeasurements),
		time.Since(startTime))

	return nil
}

func (w *Writer) insertNodeMeasurements(ctx context.Context, tx pgx.Tx, scanID uint32, nodes []*pb.NodeMeasurement) error {
	_, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"node_topology_measurements"},
		[]string{
			"scan_id", "public_key",
			"is_active", "is_validating", "is_full_validator",
			"is_overloaded", "is_active_in_scp",
			"connectivity_error", "stellar_core_version_behind", "history_archive_has_error",
			"lag_ms", "index",
			"trust_centrality_score", "page_rank_score", "trust_rank",
		},
		&nodeMeasurementSource{scanID: scanID, nodes: nodes},
	)
	return err
}

func (w *Writer) insertOrgMeasurements(ctx context.Context, tx pgx.Tx, scanID uint32, orgs []*pb.OrganizationMeasurement) error {
	_, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"org_topology_measurements"},
		[]string{"scan_id", "organization_id", "is_sub_quorum_available", "index", "toml_state"},
		&orgMeasurementSource{scanID: scanID, orgs: orgs},
	)
	return err
}

// nodeMeasurementSource implements pgx.CopyFromSource for bulk node measurement inserts.
type nodeMeasurementSource struct {
	scanID uint32
	nodes  []*pb.NodeMeasurement
	idx    int
}

func (s *nodeMeasurementSource) Next() bool {
	s.idx++
	return s.idx <= len(s.nodes)
}

func (s *nodeMeasurementSource) Values() ([]interface{}, error) {
	n := s.nodes[s.idx-1]
	var lagMs *int16
	if n.LagMs != nil {
		v := int16(*n.LagMs)
		lagMs = &v
	}
	return []interface{}{
		s.scanID, n.PublicKey,
		n.IsActive, n.IsValidating, n.IsFullValidator,
		n.IsOverloaded, n.IsActiveInScp,
		n.ConnectivityError, n.StellarCoreVersionBehind, n.HistoryArchiveHasError,
		lagMs, int16(n.Index),
		n.TrustCentralityScore, n.PageRankScore, int32(n.TrustRank),
	}, nil
}

func (s *nodeMeasurementSource) Err() error { return nil }

// orgMeasurementSource implements pgx.CopyFromSource for bulk org measurement inserts.
type orgMeasurementSource struct {
	scanID uint32
	orgs   []*pb.OrganizationMeasurement
	idx    int
}

func (s *orgMeasurementSource) Next() bool {
	s.idx++
	return s.idx <= len(s.orgs)
}

func (s *orgMeasurementSource) Values() ([]interface{}, error) {
	o := s.orgs[s.idx-1]
	return []interface{}{
		s.scanID, o.OrganizationId, o.IsSubQuorumAvailable, int16(o.Index), o.TomlState,
	}, nil
}

func (s *orgMeasurementSource) Err() error { return nil }

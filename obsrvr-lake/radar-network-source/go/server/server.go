package server

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/checkpoint"
	pb "github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/gen/network_topology_service"
	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/health"
	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/listener"
	"github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go/loader"
)

// NetworkTopologyServer implements the gRPC NetworkTopologyService.
type NetworkTopologyServer struct {
	pb.UnimplementedNetworkTopologyServiceServer
	db           *pgxpool.Pool
	checkpoint   *checkpoint.Checkpoint
	healthServer *health.Server
	loader       *loader.Loader
}

func New(db *pgxpool.Pool, cp *checkpoint.Checkpoint, hs *health.Server) *NetworkTopologyServer {
	return &NetworkTopologyServer{
		db:           db,
		checkpoint:   cp,
		healthServer: hs,
		loader:       loader.New(db),
	}
}

// StreamNetworkSnapshots streams completed network scan snapshots to the client.
// It first catches up from the requested start_scan_id, then enters a CDC loop
// driven by LISTEN/NOTIFY.
func (s *NetworkTopologyServer) StreamNetworkSnapshots(
	req *pb.StreamSnapshotsRequest,
	stream pb.NetworkTopologyService_StreamNetworkSnapshotsServer,
) error {
	ctx := stream.Context()

	// Determine start point
	startScanID := req.StartScanId
	if startScanID == 0 {
		if cpID := s.checkpoint.GetLastScanID(); cpID > 0 {
			startScanID = cpID
			log.Printf("Resuming from checkpoint scan_id=%d", startScanID)
		}
	}

	// Phase 1: Catch-up — stream all completed scans since startScanID
	scans, err := s.loader.LoadCompletedScansSince(ctx, startScanID)
	if err != nil {
		return err
	}
	log.Printf("Catch-up: %d scans since scan_id=%d", len(scans), startScanID)

	for _, scan := range scans {
		snapshot, err := s.loader.LoadSnapshot(ctx, scan.ID)
		if err != nil {
			s.healthServer.RecordError(err)
			log.Printf("Error loading snapshot for scan %d: %v", scan.ID, err)
			continue
		}
		if err := stream.Send(snapshot); err != nil {
			return err
		}
		s.checkpoint.Update(scan.ID)
		if err := s.checkpoint.Save(); err != nil {
			log.Printf("Warning: checkpoint save failed: %v", err)
		}
		log.Printf("Streamed catch-up scan_id=%d ledger=%d nodes=%d orgs=%d",
			scan.ID, snapshot.LatestLedger,
			len(snapshot.NodeMeasurements), len(snapshot.OrgMeasurements))
	}

	// Phase 2: CDC loop via LISTEN/NOTIFY
	lis := listener.New(s.db, s.checkpoint.GetLastScanID())

	// Run listener in background
	listenerCtx, listenerCancel := context.WithCancel(ctx)
	defer listenerCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- lis.Run(listenerCtx)
	}()

	for {
		select {
		case <-ctx.Done():
			listenerCancel()
			return ctx.Err()

		case err := <-errCh:
			return err

		case scanID, ok := <-lis.ScanIDs():
			if !ok {
				return nil
			}

			snapshot, err := s.loader.LoadSnapshot(ctx, scanID)
			if err != nil {
				s.healthServer.RecordError(err)
				log.Printf("Error loading snapshot for scan %d: %v", scanID, err)
				continue
			}

			if err := stream.Send(snapshot); err != nil {
				return err
			}

			s.checkpoint.Update(scanID)
			if err := s.checkpoint.Save(); err != nil {
				log.Printf("Warning: checkpoint save failed: %v", err)
			}

			log.Printf("Streamed CDC scan_id=%d ledger=%d nodes=%d orgs=%d",
				scanID, snapshot.LatestLedger,
				len(snapshot.NodeMeasurements), len(snapshot.OrgMeasurements))
		}
	}
}

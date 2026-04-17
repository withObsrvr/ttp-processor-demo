package main

import (
	"context"
	"log"
)

type ProjectorRunResult struct {
	TargetLedger int64
}

type CatchupProjector interface {
	ProjectorRunner
	SourceHighWatermark(context.Context) (int64, error)
}

const maxCatchupRunsPerProjector = 10000

func RunProjectors(ctx context.Context, healthServer *HealthServer, projectors []ProjectorRunner, targetLedger int64) error {
	for _, p := range projectors {
		if cp, ok := p.(CatchupProjector); ok {
			runs, target, err := runCatchupProjector(ctx, healthServer, cp, targetLedger)
			if err != nil {
				log.Printf("projector run failed projector=%s target=%d runs=%d err=%v", p.Name(), target, runs, err)
			}
			continue
		}
		if err := healthServer.RunProjector(ctx, p); err != nil {
			log.Printf("projector run failed projector=%s err=%v", p.Name(), err)
		}
	}
	return nil
}

func runCatchupProjector(ctx context.Context, healthServer *HealthServer, p CatchupProjector, targetLedger int64) (int, int64, error) {
	target := targetLedger
	if target <= 0 {
		wm, err := p.SourceHighWatermark(ctx)
		if err != nil {
			return 0, 0, err
		}
		target = wm
	}
	if target <= 0 {
		return 0, 0, nil
	}

	lastCheckpoint := healthServer.GetProjectorCheckpoint(p.Name())
	for run := 1; run <= maxCatchupRunsPerProjector; run++ {
		if err := healthServer.RunProjector(ctx, p); err != nil {
			return run, target, err
		}

		checkpoint := healthServer.GetProjectorCheckpoint(p.Name())
		if checkpoint >= target {
			return run, target, nil
		}
		if checkpoint <= lastCheckpoint {
			log.Printf("projector=%s stalled before target current=%d target=%d", p.Name(), checkpoint, target)
			return run, target, nil
		}
		lastCheckpoint = checkpoint
	}

	log.Printf("projector=%s hit catch-up safety cap current=%d target=%d max_runs=%d", p.Name(), lastCheckpoint, target, maxCatchupRunsPerProjector)
	return maxCatchupRunsPerProjector, target, nil
}

func highestCheckpoint(healthServer *HealthServer, projectors []ProjectorRunner) int64 {
	var max int64
	for _, p := range projectors {
		if cp := healthServer.GetProjectorCheckpoint(p.Name()); cp > max {
			max = cp
		}
	}
	return max
}

func anyProjectorBehind(healthServer *HealthServer, projectors []ProjectorRunner, target int64) bool {
	for _, p := range projectors {
		if healthServer.GetProjectorCheckpoint(p.Name()) < target {
			return true
		}
	}
	return false
}

package main

import (
	"context"
	"log"
)

type ProjectorRunResult struct {
	TargetLedger int64
}

func RunProjectors(ctx context.Context, healthServer *HealthServer, projectors []ProjectorRunner) error {
	for _, p := range projectors {
		if err := healthServer.RunProjector(ctx, p); err != nil {
			log.Printf("projector run failed projector=%s err=%v", p.Name(), err)
		}
	}
	return nil
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

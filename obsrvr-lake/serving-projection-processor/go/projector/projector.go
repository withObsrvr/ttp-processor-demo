package projector

import "context"

type RunStats struct {
	RowsApplied int64
	RowsDeleted int64
	Checkpoint  int64
}

type Projector interface {
	Name() string
	RunOnce(ctx context.Context) (RunStats, error)
}

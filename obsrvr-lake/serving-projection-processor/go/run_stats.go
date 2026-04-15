package main

import "time"

type RunStats struct {
	RowsApplied int64
	RowsDeleted int64
	Checkpoint  int64
}

type ProjectorRuntimeStatus struct {
	Name              string     `json:"name"`
	LastStartedAt     *time.Time `json:"last_started_at,omitempty"`
	LastCompletedAt   *time.Time `json:"last_completed_at,omitempty"`
	LastSuccessAt     *time.Time `json:"last_success_at,omitempty"`
	LastErrorAt       *time.Time `json:"last_error_at,omitempty"`
	LastError         string     `json:"last_error,omitempty"`
	LastDurationMs    int64      `json:"last_duration_ms"`
	LastRowsApplied   int64      `json:"last_rows_applied"`
	LastRowsDeleted   int64      `json:"last_rows_deleted"`
	LastCheckpoint    int64      `json:"last_checkpoint"`
	TotalRuns         int64      `json:"total_runs"`
	TotalSuccesses    int64      `json:"total_successes"`
	TotalFailures     int64      `json:"total_failures"`
	TotalRowsApplied  int64      `json:"total_rows_applied"`
	TotalRowsDeleted  int64      `json:"total_rows_deleted"`
	ConsecutiveErrors int64      `json:"consecutive_errors"`
}

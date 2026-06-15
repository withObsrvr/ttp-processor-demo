package main

import (
	"errors"
	"testing"

	flowctlpb "github.com/withobsrvr/flowctl/proto"
)

func TestClassifyFlowctlFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want flowctlpb.FailureClass
	}{
		{
			name: "catalog timeout is retryable infrastructure",
			err:  errors.New("catalog postgres connection timed out"),
			want: flowctlpb.FailureClass_FAILURE_CLASS_RETRYABLE_INFRASTRUCTURE,
		},
		{
			name: "parquet schema mismatch is non retryable schema",
			err:  errors.New("parquet column schema mismatch"),
			want: flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_SCHEMA,
		},
		{
			name: "utf8 encoding failure is non retryable data",
			err:  errors.New("invalid utf8 encoding in memo"),
			want: flowctlpb.FailureClass_FAILURE_CLASS_NON_RETRYABLE_DATA,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyFlowctlFailure(tt.err); got != tt.want {
				t.Fatalf("classifyFlowctlFailure() = %s, want %s", got, tt.want)
			}
		})
	}
}

package main

import (
	"context"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
)

// RPCSource reads ledgers from a Stellar RPC endpoint.
// Best for recent ledgers and small ranges.
type RPCSource struct {
	backend  ledgerbackend.LedgerBackend
	prepared bool
}

func NewRPCSource(rpcURL string) (*RPCSource, error) {
	options := ledgerbackend.RPCLedgerBackendOptions{
		RPCServerURL: rpcURL,
	}
	backend := ledgerbackend.NewRPCLedgerBackend(options)
	return &RPCSource{backend: backend}, nil
}

// PrepareRange must be called before GetLedger to initialize the RPC backend.
func (s *RPCSource) PrepareRange(ctx context.Context, start, end uint32) error {
	if s.prepared {
		return nil
	}
	r := ledgerbackend.BoundedRange(start, end)
	if err := s.backend.PrepareRange(ctx, r); err != nil {
		return fmt.Errorf("prepare range [%d, %d]: %w", start, end, err)
	}
	s.prepared = true
	return nil
}

func (s *RPCSource) GetLedger(ctx context.Context, sequence uint32) ([]byte, error) {
	lcm, err := s.backend.GetLedger(ctx, sequence)
	if err != nil {
		return nil, fmt.Errorf("rpc get ledger %d: %w", sequence, err)
	}
	raw, err := lcm.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal ledger %d: %w", sequence, err)
	}
	return raw, nil
}

func (s *RPCSource) Close() error {
	return s.backend.Close()
}

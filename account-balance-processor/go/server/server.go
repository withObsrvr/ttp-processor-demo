package server

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	// Import our generated protobuf packages
	accountbalance "github.com/withobsrvr/account-balance-processor/gen/account_balance_service"
	rawledger "github.com/withobsrvr/account-balance-processor/gen/raw_ledger_service"

	// Import Stellar SDK packages
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/trustline"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	// Default USDC issuer on mainnet (Circle)
	defaultUSDCIssuer = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
)

// ProcessorMetrics tracks metrics for the account balance processor
type ProcessorMetrics struct {
	mu                  sync.RWMutex
	SuccessCount        int64
	ErrorCount          int64
	TotalProcessed      int64
	TotalBalancesEmitted int64
	LastError           error
	LastErrorTime       time.Time
	StartTime           time.Time
	LastProcessedLedger uint32
	ProcessingLatency   time.Duration
}

// NewProcessorMetrics creates a new metrics instance
func NewProcessorMetrics() *ProcessorMetrics {
	return &ProcessorMetrics{
		StartTime: time.Now(),
	}
}

// RecordSuccess updates metrics for successful ledger processing
func (m *ProcessorMetrics) RecordSuccess(ledgerSequence uint32, balanceCount int, processingTime time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.SuccessCount++
	m.TotalProcessed++
	m.TotalBalancesEmitted += int64(balanceCount)
	m.LastProcessedLedger = ledgerSequence
	m.ProcessingLatency = processingTime
}

// RecordError updates metrics when an error occurs
func (m *ProcessorMetrics) RecordError(err error, ledgerSequence uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ErrorCount++
	m.LastError = err
	m.LastErrorTime = time.Now()
	m.LastProcessedLedger = ledgerSequence
}

// AccountBalanceServer implements the AccountBalanceServiceServer interface
type AccountBalanceServer struct {
	accountbalance.UnimplementedAccountBalanceServiceServer

	networkPassphrase string
	rawLedgerClient   rawledger.RawLedgerServiceClient
	rawLedgerConn     *grpc.ClientConn
	logger            *zap.Logger
	metrics           *ProcessorMetrics
}

// NewAccountBalanceServer creates a new instance of the account balance processor server
func NewAccountBalanceServer(passphrase string, sourceServiceAddr string) (*AccountBalanceServer, error) {
	// Initialize zap logger with production configuration
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize zap logger")
	}

	logger.Info("connecting to raw ledger source",
		zap.String("source_address", sourceServiceAddr))

	// Connect to the raw ledger source server
	conn, err := grpc.Dial(sourceServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, errors.Wrapf(err, "did not connect to raw ledger source service at %s", sourceServiceAddr)
	}
	logger.Info("successfully connected to raw ledger source service")

	client := rawledger.NewRawLedgerServiceClient(conn)

	return &AccountBalanceServer{
		networkPassphrase: passphrase,
		rawLedgerClient:   client,
		rawLedgerConn:     conn,
		logger:            logger,
		metrics:           NewProcessorMetrics(),
	}, nil
}

// Close cleans up resources
func (s *AccountBalanceServer) Close() error {
	s.logger.Info("closing connection to raw ledger source service")
	if s.rawLedgerConn != nil {
		return s.rawLedgerConn.Close()
	}
	return nil
}

// StreamAccountBalances implements the gRPC StreamAccountBalances method
func (s *AccountBalanceServer) StreamAccountBalances(req *accountbalance.StreamAccountBalancesRequest, stream accountbalance.AccountBalanceService_StreamAccountBalancesServer) error {
	ctx := stream.Context()
	logger := s.logger.With(
		zap.Uint32("start_ledger", req.StartLedger),
		zap.Uint32("end_ledger", req.EndLedger),
		zap.String("filter_asset_code", req.FilterAssetCode),
		zap.String("filter_asset_issuer", req.FilterAssetIssuer),
	)
	logger.Info("received StreamAccountBalances request")

	// Create a request for the raw ledger source service
	sourceReq := &rawledger.StreamLedgersRequest{
		StartLedger: req.StartLedger,
	}

	// Create a new context for the outgoing call to the source service
	sourceCtx, cancelSourceStream := context.WithCancel(ctx)
	defer cancelSourceStream()

	logger.Info("requesting raw ledger stream from source service")
	// Call the source service to get the stream of raw ledgers
	rawLedgerStream, err := s.rawLedgerClient.StreamRawLedgers(sourceCtx, sourceReq)
	if err != nil {
		logger.Error("failed to connect to raw ledger source",
			zap.Error(err))
		s.metrics.RecordError(err, 0)
		return status.Errorf(codes.Internal, "failed to connect to raw ledger source: %v", err)
	}
	logger.Info("successfully initiated raw ledger stream")

	// Loop receiving raw ledgers from the source stream
	for {
		// Check if the consumer's context is cancelled
		select {
		case <-ctx.Done():
			logger.Info("consumer context cancelled, stopping account balance stream",
				zap.Error(ctx.Err()))
			cancelSourceStream()
			return status.FromContextError(ctx.Err()).Err()
		default:
			// Continue if consumer is still connected
		}

		// Receive the next raw ledger message from the source service stream
		rawLedgerMsg, err := rawLedgerStream.Recv()
		if err == io.EOF {
			logger.Error("raw ledger source stream ended unexpectedly")
			s.metrics.RecordError(err, 0)
			return status.Error(codes.Unavailable, "raw ledger source stream ended unexpectedly")
		}
		if err != nil {
			// Check if the error is due to the cancellation we initiated
			if status.Code(err) == codes.Canceled && ctx.Err() != nil {
				logger.Info("source stream cancelled due to consumer disconnection")
				return status.FromContextError(ctx.Err()).Err()
			}
			logger.Error("error receiving from raw ledger source stream",
				zap.Error(err))
			s.metrics.RecordError(err, 0)
			return status.Errorf(codes.Internal, "error receiving data from raw ledger source: %v", err)
		}

		ledgerLogger := logger.With(zap.Uint32("ledger_sequence", rawLedgerMsg.Sequence))

		// Check if we need to stop based on the requested endLedger
		if req.EndLedger > 0 && rawLedgerMsg.Sequence > req.EndLedger {
			ledgerLogger.Info("reached end ledger requested by consumer")
			cancelSourceStream()
			return nil
		}

		ledgerLogger.Debug("processing raw ledger from source")

		// Process this ledger and stream balance changes
		balanceCount, err := s.processLedger(rawLedgerMsg, req, stream, ledgerLogger)
		if err != nil {
			ledgerLogger.Error("failed to process ledger",
				zap.Error(err))
			s.metrics.RecordError(err, rawLedgerMsg.Sequence)
			cancelSourceStream()
			return status.Errorf(codes.Internal, "failed to process ledger %d: %v", rawLedgerMsg.Sequence, err)
		}

		ledgerLogger.Info("finished processing ledger",
			zap.Int("balances_sent", balanceCount))
	}
}

// processLedger processes a single ledger and extracts account balances
func (s *AccountBalanceServer) processLedger(
	rawLedgerMsg *rawledger.RawLedger,
	req *accountbalance.StreamAccountBalancesRequest,
	stream accountbalance.AccountBalanceService_StreamAccountBalancesServer,
	logger *zap.Logger,
) (int, error) {
	processingStart := time.Now()

	// Unmarshal the raw XDR bytes into a LedgerCloseMeta object
	var lcm xdr.LedgerCloseMeta
	_, err := xdr.Unmarshal(bytes.NewReader(rawLedgerMsg.LedgerCloseMetaXdr), &lcm)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to unmarshal ledger %d XDR", rawLedgerMsg.Sequence)
	}

	// Create a transaction reader from the ledger
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(s.networkPassphrase, lcm)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to create transaction reader for ledger %d", rawLedgerMsg.Sequence)
	}
	defer txReader.Close()

	balancesSent := 0

	// Process all transactions in this ledger
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return balancesSent, errors.Wrapf(err, "failed to read transaction in ledger %d", rawLedgerMsg.Sequence)
		}

		// Get the ledger header for the trustline processor
		header := lcm.LedgerHeaderHistoryEntry()

		// Process all changes in this transaction
		changes, err := tx.GetChanges()
		if err != nil {
			logger.Warn("failed to get changes from transaction",
				zap.Error(err))
			continue
		}
		for _, change := range changes {
			// We only care about trustline entries
			if change.Type != xdr.LedgerEntryTypeTrustline {
				continue
			}

			// Use the Stellar SDK trustline processor to extract trustline data
			trustlineOutput, err := trustline.TransformTrustline(change, header)
			if err != nil {
				logger.Warn("failed to transform trustline",
					zap.Error(err))
				continue
			}

			// Filter by asset if requested
			if req.FilterAssetCode != "" && trustlineOutput.AssetCode != req.FilterAssetCode {
				continue
			}
			if req.FilterAssetIssuer != "" && trustlineOutput.AssetIssuer != req.FilterAssetIssuer {
				continue
			}

			// Convert to our AccountBalance message
			balance := &accountbalance.AccountBalance{
				AccountId:          trustlineOutput.AccountID,
				AssetCode:          trustlineOutput.AssetCode,
				AssetIssuer:        trustlineOutput.AssetIssuer,
				Balance:            int64(trustlineOutput.Balance),
				LastModifiedLedger: trustlineOutput.LastModifiedLedger,
			}

			// Send to consumer
			if err := stream.Send(balance); err != nil {
				logger.Error("failed to send balance to consumer",
					zap.Error(err))
				return balancesSent, errors.Wrap(err, "failed to send balance to consumer")
			}
			balancesSent++
		}
	}

	// Update metrics on successful processing
	processingTime := time.Since(processingStart)
	s.metrics.RecordSuccess(rawLedgerMsg.Sequence, balancesSent, processingTime)

	return balancesSent, nil
}

// GetMetrics returns a copy of the current server metrics
func (s *AccountBalanceServer) GetMetrics() *ProcessorMetrics {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()
	return s.metrics
}

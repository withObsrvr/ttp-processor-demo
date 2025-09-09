package processor

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/config"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
)

// ContractDataProcessor extracts contract data from Stellar ledgers
type ContractDataProcessor struct {
	config            *config.Config
	logger            *logging.ComponentLogger
	networkPassphrase string
	
	// Filtering
	contractFilter    map[string]bool
	assetCodeFilter   map[string]bool
	assetIssuerFilter map[string]bool
	
	// Metrics
	contractsProcessed uint64
	entriesSkipped     uint64
}

// ContractDataEntry represents a single contract data change
type ContractDataEntry struct {
	// From stellar/go ContractDataOutput
	ContractId                string
	ContractKeyType           string
	ContractDurability        string
	ContractDataAssetCode     string
	ContractDataAssetIssuer   string
	ContractDataAssetType     string
	ContractDataBalanceHolder string
	ContractDataBalance       string
	LastModifiedLedger        uint32
	LedgerEntryChange         uint32
	Deleted                   bool
	ClosedAt                  int64 // Unix timestamp in microseconds
	LedgerSequence            uint32
	LedgerKeyHash             string
	
	// Additional metadata
	Key                      xdr.ScVal
	Val                      xdr.ScVal
	KeyXdr                   string
	ValXdr                   string
	ContractInstanceType     string
	ContractInstanceWasmHash string
	ContractInstanceWasmRef  string
	AssetContractId          string
	ExpirationLedgerSeq      uint32
}

// NewContractDataProcessor creates a new contract data processor
func NewContractDataProcessor(cfg *config.Config, logger *logging.ComponentLogger) *ContractDataProcessor {
	p := &ContractDataProcessor{
		config:            cfg,
		logger:            logger,
		networkPassphrase: cfg.NetworkPassphrase,
	}
	
	// Build filter maps for O(1) lookup
	if len(cfg.FilterContractIDs) > 0 {
		p.contractFilter = make(map[string]bool, len(cfg.FilterContractIDs))
		for _, id := range cfg.FilterContractIDs {
			p.contractFilter[id] = true
		}
	}
	
	if len(cfg.FilterAssetCodes) > 0 {
		p.assetCodeFilter = make(map[string]bool, len(cfg.FilterAssetCodes))
		for _, code := range cfg.FilterAssetCodes {
			p.assetCodeFilter[code] = true
		}
	}
	
	if len(cfg.FilterAssetIssuers) > 0 {
		p.assetIssuerFilter = make(map[string]bool, len(cfg.FilterAssetIssuers))
		for _, issuer := range cfg.FilterAssetIssuers {
			p.assetIssuerFilter[issuer] = true
		}
	}
	
	logger.Info().
		Int("contract_filters", len(cfg.FilterContractIDs)).
		Int("asset_code_filters", len(cfg.FilterAssetCodes)).
		Int("asset_issuer_filters", len(cfg.FilterAssetIssuers)).
		Bool("include_deleted", cfg.IncludeDeleted).
		Msg("Created contract data processor with filters")
	
	return p
}

// ProcessLedger extracts contract data from a ledger
func (p *ContractDataProcessor) ProcessLedger(ctx context.Context, ledgerCloseMeta xdr.LedgerCloseMeta) ([]*ContractDataEntry, error) {
	var entries []*ContractDataEntry
	
	// Create transaction reader
	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(
		p.networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction reader: %w", err)
	}
	defer txReader.Close()
	
	ledgerHeader := ledgerCloseMeta.LedgerHeaderHistoryEntry()
	ledgerSeq := uint32(ledgerHeader.Header.LedgerSeq)
	closedAt := ledgerHeader.Header.ScpValue.CloseTime
	
	// Create stellar/go contract processor
	transformer := contract.NewTransformContractDataStruct(
		contract.AssetFromContractData,
		contract.ContractBalanceFromContractData,
	)
	
	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading transaction: %w", err)
		}
		
		// Get changes from transaction
		changes, err := tx.GetChanges()
		if err != nil {
			p.logger.Warn().
				Err(err).
				Msg("Failed to get transaction changes")
			continue
		}
		
		// Process each change
		for _, change := range changes {
			// Check if this is a contract data change
			if !p.isContractDataChange(change) {
				continue
			}
			
			// Transform using stellar/go processor
			contractData, err, shouldContinue := transformer.TransformContractData(
				change, p.networkPassphrase, ledgerHeader)
			if err != nil {
				p.logger.Debug().
					Err(err).
					Msg("Failed to transform contract data")
				continue
			}
			if !shouldContinue {
				continue
			}
			
			// Apply filters
			if !p.shouldInclude(contractData, change) {
				p.entriesSkipped++
				continue
			}
			
			// Convert to our entry format
			entry := p.convertToEntry(contractData, change, ledgerSeq, uint64(closedAt))
			entries = append(entries, entry)
			p.contractsProcessed++
		}
	}
	
	return entries, nil
}

// isContractDataChange checks if a change involves contract data
func (p *ContractDataProcessor) isContractDataChange(change ingest.Change) bool {
	if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
		return true
	}
	return false
}

// shouldInclude applies filters to determine if we should process this entry
func (p *ContractDataProcessor) shouldInclude(data contract.ContractDataOutput, change ingest.Change) bool {
	// Check if deleted and we're not including deleted entries
	if data.Deleted && !p.config.IncludeDeleted {
		return false
	}
	
	// Apply contract ID filter
	if p.contractFilter != nil && !p.contractFilter[data.ContractId] {
		return false
	}
	
	// Apply asset code filter
	if p.assetCodeFilter != nil && data.ContractDataAssetCode != "" {
		if !p.assetCodeFilter[data.ContractDataAssetCode] {
			return false
		}
	}
	
	// Apply asset issuer filter
	if p.assetIssuerFilter != nil && data.ContractDataAssetIssuer != "" {
		if !p.assetIssuerFilter[data.ContractDataAssetIssuer] {
			return false
		}
	}
	
	return true
}

// convertToEntry converts stellar/go output to our entry format
func (p *ContractDataProcessor) convertToEntry(
	data contract.ContractDataOutput,
	change ingest.Change,
	ledgerSeq uint32,
	closedAt uint64,
) *ContractDataEntry {
	entry := &ContractDataEntry{
		// Copy all fields from stellar/go output
		ContractId:                data.ContractId,
		ContractKeyType:           data.ContractKeyType,
		ContractDurability:        data.ContractDurability,
		ContractDataAssetCode:     data.ContractDataAssetCode,
		ContractDataAssetIssuer:   data.ContractDataAssetIssuer,
		ContractDataAssetType:     data.ContractDataAssetType,
		ContractDataBalanceHolder: data.ContractDataBalanceHolder,
		ContractDataBalance:       data.ContractDataBalance,
		LastModifiedLedger:        data.LastModifiedLedger,
		LedgerEntryChange:         data.LedgerEntryChange,
		Deleted:                   data.Deleted,
		LedgerSequence:            ledgerSeq,
		LedgerKeyHash:             data.LedgerKeyHash,
		ClosedAt:                  int64(closedAt) * 1000000, // Convert to microseconds
	}
	
	// Extract Key and Val from the raw ledger entry
	if change.Post != nil && change.Post.Data.Type == xdr.LedgerEntryTypeContractData {
		contractDataEntry := change.Post.Data.MustContractData()
		entry.Key = contractDataEntry.Key
		entry.Val = contractDataEntry.Val
		
		// Encode Key and Val to base64 XDR
		keyBytes, err := contractDataEntry.Key.MarshalBinary()
		if err == nil {
			entry.KeyXdr = base64.StdEncoding.EncodeToString(keyBytes)
		}
		
		valBytes, err := contractDataEntry.Val.MarshalBinary()
		if err == nil {
			entry.ValXdr = base64.StdEncoding.EncodeToString(valBytes)
		}
		
		// Extract contract instance data if this is a contract instance entry
		if contractDataEntry.Key.Type == xdr.ScValTypeScvLedgerKeyContractInstance {
			if instance, ok := contractDataEntry.Val.GetInstance(); ok {
				// Extract executable type
				switch instance.Executable.Type {
				case xdr.ContractExecutableTypeContractExecutableWasm:
					entry.ContractInstanceType = "wasm"
					if instance.Executable.WasmHash != nil {
						entry.ContractInstanceWasmHash = hex.EncodeToString(instance.Executable.WasmHash[:])
					}
				case xdr.ContractExecutableTypeContractExecutableStellarAsset:
					entry.ContractInstanceType = "stellar_asset"
				}
			}
		}
		
		// Extract expiration for temporary entries
		if contractDataEntry.Durability == xdr.ContractDataDurabilityTemporary {
			// TODO: This requires TTL information from a separate ledger entry
			// For now, we'll leave it as 0
			entry.ExpirationLedgerSeq = 0
		}
	} else if change.Pre != nil && change.Pre.Data.Type == xdr.LedgerEntryTypeContractData && change.Post == nil {
		// For deleted entries, extract from Pre
		contractDataEntry := change.Pre.Data.MustContractData()
		entry.Key = contractDataEntry.Key
		entry.Val = contractDataEntry.Val
		
		// Encode Key and Val to base64 XDR
		keyBytes, err := contractDataEntry.Key.MarshalBinary()
		if err == nil {
			entry.KeyXdr = base64.StdEncoding.EncodeToString(keyBytes)
		}
		
		valBytes, err := contractDataEntry.Val.MarshalBinary()
		if err == nil {
			entry.ValXdr = base64.StdEncoding.EncodeToString(valBytes)
		}
	}
	
	// Calculate asset contract ID if this is an asset contract
	if data.ContractDataAssetCode != "" && data.ContractDataAssetIssuer != "" {
		asset := xdr.MustNewCreditAsset(data.ContractDataAssetCode, data.ContractDataAssetIssuer)
		contractID, err := asset.ContractID(p.networkPassphrase)
		if err == nil {
			entry.AssetContractId = hex.EncodeToString(contractID[:])
		}
	}
	
	return entry
}

// GetMetrics returns processing metrics
func (p *ContractDataProcessor) GetMetrics() ProcessorMetrics {
	return ProcessorMetrics{
		ContractsProcessed: p.contractsProcessed,
		EntriesSkipped:     p.entriesSkipped,
	}
}

// ProcessorMetrics contains metrics about contract processing
type ProcessorMetrics struct {
	ContractsProcessed uint64
	EntriesSkipped     uint64
}
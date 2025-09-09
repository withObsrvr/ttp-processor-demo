package processor

import (
	"context"
	"testing"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/processors/contract"
	"github.com/stellar/go/xdr"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/config"
	"github.com/withObsrvr/ttp-processor-demo/contract-data-processor/logging"
)

func TestNewContractDataProcessor(t *testing.T) {
	tests := []struct {
		name    string
		config  *config.Config
		wantErr bool
	}{
		{
			name: "basic processor",
			config: &config.Config{
				NetworkPassphrase: "Test SDF Network ; September 2015",
			},
			wantErr: false,
		},
		{
			name: "processor with filters",
			config: &config.Config{
				NetworkPassphrase:  "Test SDF Network ; September 2015",
				FilterContractIDs:  []string{"CA3D5KRYM6CB7OWQ6TWYRR3Z4T7GNZLKERYNZGGA5SOAOPIFY6YQGAXE"},
				FilterAssetCodes:   []string{"USDC", "EURC"},
				FilterAssetIssuers: []string{"GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"},
				IncludeDeleted:     true,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logging.NewComponentLogger("test", "v1.0.0")
			processor := NewContractDataProcessor(tt.config, logger)
			
			if processor == nil {
				t.Error("Expected processor to be created")
			}
			
			// Verify filters were initialized
			if len(tt.config.FilterContractIDs) > 0 && processor.contractFilter == nil {
				t.Error("Expected contract filter to be initialized")
			}
			
			if len(tt.config.FilterAssetCodes) > 0 && processor.assetCodeFilter == nil {
				t.Error("Expected asset code filter to be initialized")
			}
			
			if len(tt.config.FilterAssetIssuers) > 0 && processor.assetIssuerFilter == nil {
				t.Error("Expected asset issuer filter to be initialized")
			}
		})
	}
}

func TestIsContractDataChange(t *testing.T) {
	processor := &ContractDataProcessor{}
	
	tests := []struct {
		name     string
		change   ingest.Change
		expected bool
	}{
		{
			name: "contract data in pre",
			change: ingest.Change{
				Pre: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeContractData,
					},
				},
			},
			expected: true,
		},
		{
			name: "contract data in post",
			change: ingest.Change{
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeContractData,
					},
				},
			},
			expected: true,
		},
		{
			name: "account change",
			change: ingest.Change{
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeAccount,
					},
				},
			},
			expected: false,
		},
		{
			name:     "empty change",
			change:   ingest.Change{},
			expected: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processor.isContractDataChange(tt.change)
			if result != tt.expected {
				t.Errorf("isContractDataChange() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFilterLogic(t *testing.T) {
	tests := []struct {
		name           string
		config         *config.Config
		contractID     string
		assetCode      string
		assetIssuer    string
		deleted        bool
		shouldInclude  bool
	}{
		{
			name: "no filters - include all",
			config: &config.Config{
				IncludeDeleted: true,
			},
			contractID:    "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHK3M",
			shouldInclude: true,
		},
		{
			name: "contract ID filter match",
			config: &config.Config{
				FilterContractIDs: []string{"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHK3M"},
			},
			contractID:    "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHK3M",
			shouldInclude: true,
		},
		{
			name: "contract ID filter no match",
			config: &config.Config{
				FilterContractIDs: []string{"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHK3M"},
			},
			contractID:    "CBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			shouldInclude: false,
		},
		{
			name: "asset code filter match",
			config: &config.Config{
				FilterAssetCodes: []string{"USDC"},
			},
			assetCode:     "USDC",
			shouldInclude: true,
		},
		{
			name: "deleted entry excluded",
			config: &config.Config{
				IncludeDeleted: false,
			},
			deleted:       true,
			shouldInclude: false,
		},
		{
			name: "deleted entry included",
			config: &config.Config{
				IncludeDeleted: true,
			},
			deleted:       true,
			shouldInclude: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logging.NewComponentLogger("test", "v1.0.0")
			processor := NewContractDataProcessor(tt.config, logger)
			
			// Create mock contract data output
			data := contract.ContractDataOutput{
				ContractId:              tt.contractID,
				ContractDataAssetCode:   tt.assetCode,
				ContractDataAssetIssuer: tt.assetIssuer,
				Deleted:                 tt.deleted,
			}
			
			result := processor.shouldInclude(data, ingest.Change{})
			if result != tt.shouldInclude {
				t.Errorf("shouldInclude() = %v, want %v", result, tt.shouldInclude)
			}
		})
	}
}

// MockLedgerCloseMeta creates a mock LedgerCloseMeta for testing
func createMockLedgerCloseMeta(sequence uint32) xdr.LedgerCloseMeta {
	return xdr.LedgerCloseMeta{
		V: 1,
		V1: &xdr.LedgerCloseMetaV1{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					LedgerSeq: xdr.Uint32(sequence),
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(1234567890),
					},
				},
			},
		},
	}
}

func TestProcessLedgerEmptyLedger(t *testing.T) {
	config := &config.Config{
		NetworkPassphrase: "Test SDF Network ; September 2015",
	}
	logger := logging.NewComponentLogger("test", "v1.0.0")
	processor := NewContractDataProcessor(config, logger)
	
	// Create empty ledger
	ledgerMeta := createMockLedgerCloseMeta(12345)
	
	entries, err := processor.ProcessLedger(context.Background(), ledgerMeta)
	if err != nil {
		t.Fatalf("ProcessLedger() error = %v", err)
	}
	
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(entries))
	}
}

func TestGetMetrics(t *testing.T) {
	processor := &ContractDataProcessor{
		contractsProcessed: 100,
		entriesSkipped:     25,
	}
	
	metrics := processor.GetMetrics()
	
	if metrics.ContractsProcessed != 100 {
		t.Errorf("Expected ContractsProcessed = 100, got %d", metrics.ContractsProcessed)
	}
	
	if metrics.EntriesSkipped != 25 {
		t.Errorf("Expected EntriesSkipped = 25, got %d", metrics.EntriesSkipped)
	}
}
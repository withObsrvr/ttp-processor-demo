package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/withObsrvr/flowctl-sdk/pkg/processor"
	flowctlv1 "github.com/withObsrvr/flow-proto/go/gen/flowctl/v1"
	stellarv1 "github.com/withObsrvr/flow-proto/go/gen/stellar/v1"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func main() {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		panic("failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	// Load configuration from environment
	config := processor.DefaultConfig()
	config.ID = getEnv("FLOWCTL_COMPONENT_ID", getEnv("COMPONENT_ID", "contract-events-processor"))
	config.Name = "Contract Events Processor"
	config.Description = "Extracts Soroban contract events from Stellar ledgers"
	config.Version = "2.0.0-sdk"
	config.Endpoint = getEnv("PORT", ":50054")
	config.HealthPort = parseInt(getEnv("HEALTH_PORT", "8082"))

	// Configure flowctl integration
	if strings.ToLower(getEnv("ENABLE_FLOWCTL", "false")) == "true" {
		config.FlowctlConfig.Enabled = true
		config.FlowctlConfig.Endpoint = getEnv("FLOWCTL_ENDPOINT", "localhost:8080")
		config.FlowctlConfig.HeartbeatInterval = parseDuration(getEnv("FLOWCTL_HEARTBEAT_INTERVAL", "10s"))
	}

	// Create processor
	proc, err := processor.New(config)
	if err != nil {
		log.Fatalf("Failed to create processor: %v", err)
	}

	// Get network passphrase
	networkPassphrase := getEnv("NETWORK_PASSPHRASE", "Test SDF Network ; September 2015")

	// Register processor handler
	err = proc.OnProcess(
		func(ctx context.Context, event *flowctlv1.Event) (*flowctlv1.Event, error) {
			// Only process stellar ledger events
			if event.Type != "stellar.ledger.v1" {
				return nil, nil // Skip non-ledger events
			}

			// Parse the RawLedger from payload
			var rawLedger stellarv1.RawLedger
			if err := proto.Unmarshal(event.Payload, &rawLedger); err != nil {
				logger.Error("Failed to unmarshal RawLedger", zap.Error(err))
				return nil, fmt.Errorf("failed to unmarshal ledger: %w", err)
			}

			// Decode XDR
			var ledgerCloseMeta xdr.LedgerCloseMeta
			if err := xdr.SafeUnmarshal(rawLedger.LedgerCloseMetaXdr, &ledgerCloseMeta); err != nil {
				logger.Error("Failed to unmarshal XDR", zap.Error(err), zap.Uint32("sequence", rawLedger.Sequence))
				return nil, fmt.Errorf("failed to unmarshal XDR: %w", err)
			}

			// Extract contract events
			contractEvents, err := extractContractEvents(ledgerCloseMeta, networkPassphrase, logger)
			if err != nil {
				logger.Error("Failed to extract contract events", zap.Error(err), zap.Uint32("sequence", rawLedger.Sequence))
				return nil, fmt.Errorf("failed to extract events: %w", err)
			}

			// If no events found, skip
			if len(contractEvents) == 0 {
				logger.Debug("No contract events in ledger", zap.Uint32("sequence", rawLedger.Sequence))
				return nil, nil
			}

			logger.Info("Extracted contract events",
				zap.Uint32("sequence", rawLedger.Sequence),
				zap.Int("count", len(contractEvents)))

			// Marshal contract events for output
			eventsData, err := proto.Marshal(&stellarv1.ContractEventBatch{
				Events: contractEvents,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to marshal events: %w", err)
			}

			// Create output event
			outputEvent := &flowctlv1.Event{
				Id:                fmt.Sprintf("contract-events-%d", rawLedger.Sequence),
				Type:              "stellar.contract.events.v1",
				Payload:           eventsData,
				Metadata:          make(map[string]string),
				SourceComponentId: config.ID,
				ContentType:       "application/protobuf",
				StellarCursor: &flowctlv1.StellarCursor{
					LedgerSequence: uint64(rawLedger.Sequence),
				},
			}

			// Copy original metadata
			for k, v := range event.Metadata {
				outputEvent.Metadata[k] = v
			}
			outputEvent.Metadata["ledger_sequence"] = fmt.Sprintf("%d", rawLedger.Sequence)
			outputEvent.Metadata["events_count"] = fmt.Sprintf("%d", len(contractEvents))
			outputEvent.Metadata["processor_version"] = config.Version

			return outputEvent, nil
		},
		[]string{"stellar.ledger.v1"},              // Input types
		[]string{"stellar.contract.events.v1"},     // Output types
	)
	if err != nil {
		log.Fatalf("Failed to register handler: %v", err)
	}

	// Start the processor
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := proc.Start(ctx); err != nil {
		log.Fatalf("Failed to start processor: %v", err)
	}

	logger.Info("Contract Events Processor is running",
		zap.String("endpoint", config.Endpoint),
		zap.Int("health_port", config.HealthPort),
		zap.Bool("flowctl_enabled", config.FlowctlConfig.Enabled))

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Stop the processor gracefully
	logger.Info("Shutting down processor...")
	if err := proc.Stop(); err != nil {
		log.Fatalf("Failed to stop processor: %v", err)
	}

	logger.Info("Processor stopped successfully")
}

// extractContractEvents extracts all contract events from a ledger
func extractContractEvents(ledgerCloseMeta xdr.LedgerCloseMeta, networkPassphrase string, logger *zap.Logger) ([]*stellarv1.ContractEvent, error) {
	sequence := ledgerCloseMeta.LedgerSequence()

	txReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, ledgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("error creating transaction reader: %w", err)
	}
	defer txReader.Close()

	var events []*stellarv1.ContractEvent

	// Process each transaction
	for {
		tx, err := txReader.Read()
		if err != nil {
			break // EOF or error
		}

		txSuccessful := tx.Result.Successful()
		txHash := tx.Result.TransactionHash.HexString()

		// Get transaction events
		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			continue // Not a Soroban transaction
		}

		// Process operation-level events
		for opIndex, opEvents := range txEvents.OperationEvents {
			for eventIdx, event := range opEvents {
				if event.Type != xdr.ContractEventTypeContract {
					continue
				}

				contractEvent := convertToContractEvent(event, sequence, txHash, txSuccessful, uint32(tx.Index), int32(eventIdx), &opIndex)
				if contractEvent != nil {
					events = append(events, contractEvent)
				}
			}
		}

		// Process transaction-level events
		for eventIdx, txEvent := range txEvents.TransactionEvents {
			if txEvent.Event.Type == xdr.ContractEventTypeContract {
				contractEvent := convertToContractEvent(txEvent.Event, sequence, txHash, txSuccessful, uint32(tx.Index), int32(eventIdx), nil)
				if contractEvent != nil {
					events = append(events, contractEvent)
				}
			}
		}
	}

	return events, nil
}

// convertToContractEvent converts XDR contract event to proto format
func convertToContractEvent(event xdr.ContractEvent, ledgerSeq uint32, txHash string, txSuccess bool, txIndex uint32, eventIdx int32, opIndex *int) *stellarv1.ContractEvent {
	// Extract contract ID - convert to C... address format
	var contractID string
	if event.ContractId != nil {
		// Use strkey to encode as C... address
		addr, err := strkey.Encode(strkey.VersionByteContract, (*event.ContractId)[:])
		if err == nil {
			contractID = addr
		} else {
			// Fallback to hex format
			contractID = fmt.Sprintf("C%x", (*event.ContractId)[:])
		}
	}

	// Extract event type from first topic if it's a Symbol
	var eventType string
	topics := event.Body.V0.Topics
	if len(topics) > 0 && topics[0].Type == xdr.ScValTypeScvSymbol && topics[0].Sym != nil {
		eventType = string(*topics[0].Sym)
	}

	// Marshal topics with both XDR base64 and decoded JSON
	var topicsProto []*stellarv1.ScValue
	for _, topic := range topics {
		xdrBytes, _ := topic.MarshalBinary()
		topicsProto = append(topicsProto, &stellarv1.ScValue{
			XdrBase64: base64.StdEncoding.EncodeToString(xdrBytes),
			Json:      scValToJSON(topic),
		})
	}

	// Marshal data with both XDR base64 and decoded JSON
	dataXDR, _ := event.Body.V0.Data.MarshalBinary()
	dataProto := &stellarv1.ScValue{
		XdrBase64: base64.StdEncoding.EncodeToString(dataXDR),
		Json:      scValToJSON(event.Body.V0.Data),
	}

	contractEvent := &stellarv1.ContractEvent{
		Meta: &stellarv1.EventMeta{
			LedgerSequence: ledgerSeq,
			TxHash:         txHash,
			TxSuccessful:   txSuccess,
			TxIndex:        txIndex,
		},
		ContractId:     contractID,
		EventType:      eventType,
		Topics:         topicsProto,
		Data:           dataProto,
		InSuccessfulTx: txSuccess,
		EventIndex:     eventIdx,
	}

	if opIndex != nil {
		idx := int32(*opIndex)
		contractEvent.OperationIndex = &idx
	}

	return contractEvent
}

// decodeScVal converts an XDR ScVal to a JSON-serializable value
func decodeScVal(scval xdr.ScVal) (interface{}, error) {
	switch scval.Type {
	case xdr.ScValTypeScvBool:
		if scval.B == nil {
			return false, nil
		}
		return bool(*scval.B), nil

	case xdr.ScValTypeScvVoid:
		return nil, nil

	case xdr.ScValTypeScvError:
		if scval.Error == nil {
			return map[string]interface{}{"error": "unknown"}, nil
		}
		result := map[string]interface{}{
			"error": scval.Error.Type.String(),
		}
		if scval.Error.Code != nil {
			result["code"] = uint32(*scval.Error.Code)
		}
		return result, nil

	case xdr.ScValTypeScvU32:
		if scval.U32 == nil {
			return uint32(0), nil
		}
		return uint32(*scval.U32), nil

	case xdr.ScValTypeScvI32:
		if scval.I32 == nil {
			return int32(0), nil
		}
		return int32(*scval.I32), nil

	case xdr.ScValTypeScvU64:
		if scval.U64 == nil {
			return uint64(0), nil
		}
		return uint64(*scval.U64), nil

	case xdr.ScValTypeScvI64:
		if scval.I64 == nil {
			return int64(0), nil
		}
		return int64(*scval.I64), nil

	case xdr.ScValTypeScvTimepoint:
		if scval.Timepoint == nil {
			return uint64(0), nil
		}
		return uint64(*scval.Timepoint), nil

	case xdr.ScValTypeScvDuration:
		if scval.Duration == nil {
			return uint64(0), nil
		}
		return uint64(*scval.Duration), nil

	case xdr.ScValTypeScvU128:
		if scval.U128 == nil {
			return "0", nil
		}
		// Combine hi and lo parts into a big.Int
		hi := big.NewInt(0).SetUint64(uint64(scval.U128.Hi))
		lo := big.NewInt(0).SetUint64(uint64(scval.U128.Lo))
		result := big.NewInt(0).Lsh(hi, 64)
		result = result.Or(result, lo)
		return result.String(), nil

	case xdr.ScValTypeScvI128:
		if scval.I128 == nil {
			return "0", nil
		}
		// For i128, hi is signed
		hi := big.NewInt(int64(scval.I128.Hi))
		lo := big.NewInt(0).SetUint64(uint64(scval.I128.Lo))
		result := big.NewInt(0).Lsh(hi, 64)
		result = result.Or(result, lo)
		return result.String(), nil

	case xdr.ScValTypeScvU256:
		if scval.U256 == nil {
			return "0", nil
		}
		// Combine 4 parts
		parts := []uint64{
			uint64(scval.U256.HiHi),
			uint64(scval.U256.HiLo),
			uint64(scval.U256.LoHi),
			uint64(scval.U256.LoLo),
		}
		result := big.NewInt(0)
		for i, part := range parts {
			p := big.NewInt(0).SetUint64(part)
			p = p.Lsh(p, uint(64*(3-i)))
			result = result.Or(result, p)
		}
		return result.String(), nil

	case xdr.ScValTypeScvI256:
		if scval.I256 == nil {
			return "0", nil
		}
		// Similar to U256 but hihi is signed
		hihi := big.NewInt(int64(scval.I256.HiHi))
		hilo := big.NewInt(0).SetUint64(uint64(scval.I256.HiLo))
		lohi := big.NewInt(0).SetUint64(uint64(scval.I256.LoHi))
		lolo := big.NewInt(0).SetUint64(uint64(scval.I256.LoLo))
		result := big.NewInt(0).Lsh(hihi, 192)
		result = result.Or(result, big.NewInt(0).Lsh(hilo, 128))
		result = result.Or(result, big.NewInt(0).Lsh(lohi, 64))
		result = result.Or(result, lolo)
		return result.String(), nil

	case xdr.ScValTypeScvBytes:
		if scval.Bytes == nil {
			return "", nil
		}
		// Return as hex string for readability
		return fmt.Sprintf("0x%x", []byte(*scval.Bytes)), nil

	case xdr.ScValTypeScvString:
		if scval.Str == nil {
			return "", nil
		}
		return string(*scval.Str), nil

	case xdr.ScValTypeScvSymbol:
		if scval.Sym == nil {
			return "", nil
		}
		return string(*scval.Sym), nil

	case xdr.ScValTypeScvVec:
		if scval.Vec == nil {
			return []interface{}{}, nil
		}
		// scval.Vec is *ScVec where ScVec is []ScVal
		// Dereference and cast to slice
		vecSlice := ([]xdr.ScVal)(**scval.Vec)
		result := make([]interface{}, 0, len(vecSlice))
		for _, item := range vecSlice {
			decoded, err := decodeScVal(item)
			if err != nil {
				return nil, err
			}
			result = append(result, decoded)
		}
		return result, nil

	case xdr.ScValTypeScvMap:
		if scval.Map == nil {
			return map[string]interface{}{}, nil
		}
		// scval.Map is *ScMap where ScMap is []ScMapEntry
		mapSlice := ([]xdr.ScMapEntry)(**scval.Map)
		result := make(map[string]interface{})
		for _, entry := range mapSlice {
			// Try to use key as string, otherwise use JSON representation
			keyDecoded, err := decodeScVal(entry.Key)
			if err != nil {
				return nil, err
			}
			keyStr, ok := keyDecoded.(string)
			if !ok {
				// Convert non-string key to JSON string
				keyBytes, _ := json.Marshal(keyDecoded)
				keyStr = string(keyBytes)
			}
			valDecoded, err := decodeScVal(entry.Val)
			if err != nil {
				return nil, err
			}
			result[keyStr] = valDecoded
		}
		return result, nil

	case xdr.ScValTypeScvAddress:
		if scval.Address == nil {
			return "", nil
		}
		switch scval.Address.Type {
		case xdr.ScAddressTypeScAddressTypeAccount:
			if scval.Address.AccountId == nil {
				return "", nil
			}
			// Encode as G... address
			addr, err := strkey.Encode(strkey.VersionByteAccountID, scval.Address.AccountId.Ed25519[:])
			if err != nil {
				return fmt.Sprintf("account:%x", scval.Address.AccountId.Ed25519[:]), nil
			}
			return addr, nil
		case xdr.ScAddressTypeScAddressTypeContract:
			if scval.Address.ContractId == nil {
				return "", nil
			}
			// Encode as C... address
			addr, err := strkey.Encode(strkey.VersionByteContract, (*scval.Address.ContractId)[:])
			if err != nil {
				return fmt.Sprintf("contract:%x", (*scval.Address.ContractId)[:]), nil
			}
			return addr, nil
		}
		return "", nil

	case xdr.ScValTypeScvLedgerKeyContractInstance:
		return map[string]interface{}{"type": "ledger_key_contract_instance"}, nil

	case xdr.ScValTypeScvLedgerKeyNonce:
		if scval.NonceKey == nil {
			return map[string]interface{}{"type": "ledger_key_nonce"}, nil
		}
		return map[string]interface{}{
			"type":  "ledger_key_nonce",
			"nonce": int64(scval.NonceKey.Nonce),
		}, nil

	case xdr.ScValTypeScvContractInstance:
		if scval.Instance == nil {
			return map[string]interface{}{"type": "contract_instance"}, nil
		}
		result := map[string]interface{}{
			"type":       "contract_instance",
			"executable": scval.Instance.Executable.Type.String(),
		}
		if scval.Instance.Storage != nil {
			storage := make(map[string]interface{})
			storageMap := ([]xdr.ScMapEntry)(*scval.Instance.Storage)
			for _, entry := range storageMap {
				keyDecoded, _ := decodeScVal(entry.Key)
				valDecoded, _ := decodeScVal(entry.Val)
				keyStr, ok := keyDecoded.(string)
				if !ok {
					keyBytes, _ := json.Marshal(keyDecoded)
					keyStr = string(keyBytes)
				}
				storage[keyStr] = valDecoded
			}
			result["storage"] = storage
		}
		return result, nil

	default:
		return map[string]interface{}{"type": "unknown", "raw_type": scval.Type.String()}, nil
	}
}

// scValToJSON converts an XDR ScVal to a JSON string
func scValToJSON(scval xdr.ScVal) string {
	decoded, err := decodeScVal(scval)
	if err != nil {
		return fmt.Sprintf(`{"error": %q}`, err.Error())
	}
	jsonBytes, err := json.Marshal(decoded)
	if err != nil {
		return fmt.Sprintf(`{"error": %q}`, err.Error())
	}
	return string(jsonBytes)
}

// Helper functions
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseDuration(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}

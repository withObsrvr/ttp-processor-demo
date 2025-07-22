package server

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"go.uber.org/zap"

	cipb "github.com/withobsrvr/contract-invocation-processor/gen/contract_invocation"
)

// ScValConverter provides utilities for converting between XDR ScVal and protobuf ScValue
type ScValConverter struct {
	logger *zap.Logger
}

// NewScValConverter creates a new ScVal converter
func NewScValConverter(logger *zap.Logger) *ScValConverter {
	return &ScValConverter{
		logger: logger,
	}
}

// ConvertScValToProto converts XDR ScVal to protobuf ScValue
func (c *ScValConverter) ConvertScValToProto(scVal xdr.ScVal) *cipb.ScValue {
	switch scVal.Type {
	case xdr.ScValTypeScvVoid:
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_VOID,
		}

	case xdr.ScValTypeScvBool:
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_BOOL,
			Value: &cipb.ScValue_BoolValue{
				BoolValue: bool(scVal.MustB()),
			},
		}

	case xdr.ScValTypeScvU32:
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_U32,
			Value: &cipb.ScValue_U32Value{
				U32Value: uint32(scVal.MustU32()),
			},
		}

	case xdr.ScValTypeScvI32:
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_I32,
			Value: &cipb.ScValue_I32Value{
				I32Value: int32(scVal.MustI32()),
			},
		}

	case xdr.ScValTypeScvU64:
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_U64,
			Value: &cipb.ScValue_U64Value{
				U64Value: uint64(scVal.MustU64()),
			},
		}

	case xdr.ScValTypeScvI64:
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_I64,
			Value: &cipb.ScValue_I64Value{
				I64Value: int64(scVal.MustI64()),
			},
		}

	case xdr.ScValTypeScvBytes:
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_BYTES,
			Value: &cipb.ScValue_BytesValue{
				BytesValue: []byte(scVal.MustBytes()),
			},
		}

	case xdr.ScValTypeScvString:
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_STRING,
			Value: &cipb.ScValue_StringValue{
				StringValue: string(scVal.MustStr()),
			},
		}

	case xdr.ScValTypeScvSymbol:
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_SYMBOL,
			Value: &cipb.ScValue_SymbolValue{
				SymbolValue: string(scVal.MustSym()),
			},
		}

	case xdr.ScValTypeScvVec:
		vec := scVal.MustVec()
		if vec == nil {
			return &cipb.ScValue{
				Type: cipb.ScValueType_SC_VALUE_TYPE_VEC,
				Value: &cipb.ScValue_VecValue{
					VecValue: &cipb.ScValueVector{Values: []*cipb.ScValue{}},
				},
			}
		}

		values := make([]*cipb.ScValue, len(*vec))
		for i, item := range *vec {
			values[i] = c.ConvertScValToProto(item)
		}

		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_VEC,
			Value: &cipb.ScValue_VecValue{
				VecValue: &cipb.ScValueVector{Values: values},
			},
		}

	case xdr.ScValTypeScvMap:
		mapVal := scVal.MustMap()
		if mapVal == nil {
			return &cipb.ScValue{
				Type: cipb.ScValueType_SC_VALUE_TYPE_MAP,
				Value: &cipb.ScValue_MapValue{
					MapValue: &cipb.ScValueMap{Entries: []*cipb.ScValueMapEntry{}},
				},
			}
		}

		entries := make([]*cipb.ScValueMapEntry, len(*mapVal))
		for i, entry := range *mapVal {
			entries[i] = &cipb.ScValueMapEntry{
				Key:   c.ConvertScValToProto(entry.Key),
				Value: c.ConvertScValToProto(entry.Val),
			}
		}

		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_MAP,
			Value: &cipb.ScValue_MapValue{
				MapValue: &cipb.ScValueMap{Entries: entries},
			},
		}

	case xdr.ScValTypeScvAddress:
		address := scVal.MustAddress()
		addressStr, err := c.convertAddressToString(address)
		if err != nil {
			c.logger.Error("failed to convert address", zap.Error(err))
			return &cipb.ScValue{
				Type: cipb.ScValueType_SC_VALUE_TYPE_STRING,
				Value: &cipb.ScValue_StringValue{
					StringValue: fmt.Sprintf("invalid_address: %v", err),
				},
			}
		}

		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_ADDRESS,
			Value: &cipb.ScValue_AddressValue{
				AddressValue: addressStr,
			},
		}

	case xdr.ScValTypeScvInstance:
		// Handle instance type (complex objects)
		instance := scVal.MustInstance()
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_INSTANCE,
			Value: &cipb.ScValue_InstanceValue{
				InstanceValue: &cipb.ScValueInstance{
					InstanceType: string(instance.InstanceType),
					Fields:       []*cipb.ScValue{}, // TODO: Extract fields if needed
				},
			},
		}

	default:
		c.logger.Warn("unknown ScVal type", zap.String("type", scVal.Type.String()))
		return &cipb.ScValue{
			Type: cipb.ScValueType_SC_VALUE_TYPE_STRING,
			Value: &cipb.ScValue_StringValue{
				StringValue: fmt.Sprintf("unknown_type: %s", scVal.Type.String()),
			},
		}
	}
}

// convertAddressToString converts an ScAddress to a human-readable string
func (c *ScValConverter) convertAddressToString(address xdr.ScAddress) (string, error) {
	switch address.Type {
	case xdr.ScAddressTypeScAddressTypeAccount:
		accountID := address.MustAccountId()
		return accountID.Address(), nil

	case xdr.ScAddressTypeScAddressTypeContract:
		contractID := address.MustContractId()
		return strkey.Encode(strkey.VersionByteContract, contractID[:])

	default:
		return "", fmt.Errorf("unknown address type: %s", address.Type.String())
	}
}

// ConvertScValToJSON converts XDR ScVal to a JSON-serializable interface
// This is useful for debugging and human-readable representations
func (c *ScValConverter) ConvertScValToJSON(scVal xdr.ScVal) (interface{}, error) {
	switch scVal.Type {
	case xdr.ScValTypeScvVoid:
		return nil, nil

	case xdr.ScValTypeScvBool:
		return bool(scVal.MustB()), nil

	case xdr.ScValTypeScvU32:
		return uint32(scVal.MustU32()), nil

	case xdr.ScValTypeScvI32:
		return int32(scVal.MustI32()), nil

	case xdr.ScValTypeScvU64:
		return strconv.FormatUint(uint64(scVal.MustU64()), 10), nil

	case xdr.ScValTypeScvI64:
		return strconv.FormatInt(int64(scVal.MustI64()), 10), nil

	case xdr.ScValTypeScvBytes:
		return base64.StdEncoding.EncodeToString([]byte(scVal.MustBytes())), nil

	case xdr.ScValTypeScvString:
		return string(scVal.MustStr()), nil

	case xdr.ScValTypeScvSymbol:
		return string(scVal.MustSym()), nil

	case xdr.ScValTypeScvVec:
		vec := scVal.MustVec()
		if vec == nil {
			return []interface{}{}, nil
		}

		result := make([]interface{}, len(*vec))
		for i, item := range *vec {
			converted, err := c.ConvertScValToJSON(item)
			if err != nil {
				return nil, fmt.Errorf("failed to convert vector item %d: %w", i, err)
			}
			result[i] = converted
		}
		return result, nil

	case xdr.ScValTypeScvMap:
		mapVal := scVal.MustMap()
		if mapVal == nil {
			return map[string]interface{}{}, nil
		}

		result := make(map[string]interface{})
		for i, entry := range *mapVal {
			keyJSON, err := c.ConvertScValToJSON(entry.Key)
			if err != nil {
				return nil, fmt.Errorf("failed to convert map key %d: %w", i, err)
			}

			valueJSON, err := c.ConvertScValToJSON(entry.Val)
			if err != nil {
				return nil, fmt.Errorf("failed to convert map value %d: %w", i, err)
			}

			// Convert key to string for JSON map
			keyStr := fmt.Sprintf("%v", keyJSON)
			result[keyStr] = valueJSON
		}
		return result, nil

	case xdr.ScValTypeScvAddress:
		address := scVal.MustAddress()
		return c.convertAddressToString(address)

	case xdr.ScValTypeScvInstance:
		instance := scVal.MustInstance()
		return map[string]interface{}{
			"type":         string(instance.InstanceType),
			"instance_id": "complex_instance", // Simplified for now
		}, nil

	default:
		return map[string]interface{}{
			"type":  scVal.Type.String(),
			"error": "unknown_scval_type",
		}, nil
	}
}

// ExtractFunctionName extracts function name from contract invocation arguments
func (c *ScValConverter) ExtractFunctionName(args []xdr.ScVal) string {
	if len(args) == 0 {
		return "unknown"
	}

	// First argument is often the function name in Soroban contracts
	firstArg := args[0]
	if firstArg.Type == xdr.ScValTypeScvSymbol {
		return string(firstArg.MustSym())
	}

	if firstArg.Type == xdr.ScValTypeScvString {
		return string(firstArg.MustStr())
	}

	return "unknown"
}

// ValidateScVal performs basic validation on ScVal data
func (c *ScValConverter) ValidateScVal(scVal xdr.ScVal) error {
	switch scVal.Type {
	case xdr.ScValTypeScvVec:
		vec := scVal.MustVec()
		if vec != nil {
			for i, item := range *vec {
				if err := c.ValidateScVal(item); err != nil {
					return fmt.Errorf("invalid vector item %d: %w", i, err)
				}
			}
		}

	case xdr.ScValTypeScvMap:
		mapVal := scVal.MustMap()
		if mapVal != nil {
			for i, entry := range *mapVal {
				if err := c.ValidateScVal(entry.Key); err != nil {
					return fmt.Errorf("invalid map key %d: %w", i, err)
				}
				if err := c.ValidateScVal(entry.Val); err != nil {
					return fmt.Errorf("invalid map value %d: %w", i, err)
				}
			}
		}

	case xdr.ScValTypeScvAddress:
		address := scVal.MustAddress()
		_, err := c.convertAddressToString(address)
		if err != nil {
			return fmt.Errorf("invalid address: %w", err)
		}
	}

	return nil
}

// CreateErrorScValue creates an ScValue representing an error
func (c *ScValConverter) CreateErrorScValue(errMsg string) *cipb.ScValue {
	return &cipb.ScValue{
		Type: cipb.ScValueType_SC_VALUE_TYPE_STRING,
		Value: &cipb.ScValue_StringValue{
			StringValue: fmt.Sprintf("error: %s", errMsg),
		},
	}
}
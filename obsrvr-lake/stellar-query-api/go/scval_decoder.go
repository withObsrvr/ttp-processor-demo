package main

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// DecodedScVal represents a decoded Soroban ScVal with type info and display string
type DecodedScVal struct {
	Type    string      `json:"type"`
	Value   interface{} `json:"value"`
	Display string      `json:"display"`
}

// DecodeScValJSON attempts to decode a JSON-encoded ScVal into a human-readable form.
// Handles the common ScVal types encountered in Soroban contract invocations.
func DecodeScValJSON(raw json.RawMessage) DecodedScVal {
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return DecodedScVal{Type: "raw", Value: string(raw), Display: string(raw)}
	}

	// Handle typed ScVal objects (e.g., {"type": "address", "value": "G..."})
	if t, ok := obj["type"].(string); ok {
		val := obj["value"]
		return decodeTypedScVal(t, val)
	}

	// Handle XDR base64 string
	if xdr, ok := obj["xdr"].(string); ok {
		return DecodedScVal{Type: "xdr", Value: xdr, Display: truncateStr(xdr, 32) + "..."}
	}

	return DecodedScVal{Type: "object", Value: obj, Display: truncateJSON(raw)}
}

// DecodeScValBase64 decodes a base64-encoded ScVal XDR into a basic representation.
// For full XDR decoding, the stellar/go/xdr package would be needed.
func DecodeScValBase64(b64 string, typeHint string) DecodedScVal {
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return DecodedScVal{Type: "error", Value: b64, Display: "invalid base64"}
	}

	// Basic type hint handling
	switch typeHint {
	case "symbol":
		// Symbols are often plain ASCII
		if isPrintableASCII(data) {
			return DecodedScVal{Type: "symbol", Value: string(data), Display: string(data)}
		}
	case "string":
		if isPrintableASCII(data) {
			return DecodedScVal{Type: "string", Value: string(data), Display: string(data)}
		}
	case "address":
		// Stellar addresses start with G (ed25519) or C (contract)
		s := string(data)
		if len(s) == 56 && (s[0] == 'G' || s[0] == 'C') {
			return DecodedScVal{Type: "address", Value: s, Display: s[:4] + "..." + s[52:]}
		}
	}

	// Default: return hex representation
	hex := fmt.Sprintf("%x", data)
	return DecodedScVal{Type: typeHint, Value: hex, Display: truncateStr(hex, 16) + "..."}
}

func decodeTypedScVal(scType string, value interface{}) DecodedScVal {
	switch scType {
	case "bool":
		b, _ := value.(bool)
		return DecodedScVal{Type: "bool", Value: b, Display: fmt.Sprintf("%v", b)}

	case "u32", "i32":
		n, _ := value.(float64)
		return DecodedScVal{Type: scType, Value: int64(n), Display: fmt.Sprintf("%d", int64(n))}

	case "u64", "i64":
		s, ok := value.(string)
		if !ok {
			n, _ := value.(float64)
			return DecodedScVal{Type: scType, Value: int64(n), Display: fmt.Sprintf("%d", int64(n))}
		}
		return DecodedScVal{Type: scType, Value: s, Display: s}

	case "u128", "i128":
		return DecodedScVal{Type: scType, Value: value, Display: fmt.Sprintf("%v", value)}

	case "address":
		s, _ := value.(string)
		display := s
		if len(s) > 10 {
			display = s[:4] + "..." + s[len(s)-4:]
		}
		return DecodedScVal{Type: "address", Value: s, Display: display}

	case "symbol", "sym":
		s, _ := value.(string)
		return DecodedScVal{Type: "symbol", Value: s, Display: s}

	case "string", "str":
		s, _ := value.(string)
		return DecodedScVal{Type: "string", Value: s, Display: truncateStr(s, 64)}

	case "bytes":
		s, _ := value.(string)
		return DecodedScVal{Type: "bytes", Value: s, Display: truncateStr(s, 32)}

	case "vec":
		arr, _ := value.([]interface{})
		return DecodedScVal{Type: "vec", Value: arr, Display: fmt.Sprintf("vec[%d]", len(arr))}

	case "map":
		m, _ := value.(map[string]interface{})
		return DecodedScVal{Type: "map", Value: m, Display: fmt.Sprintf("map{%d}", len(m))}

	default:
		return DecodedScVal{Type: scType, Value: value, Display: fmt.Sprintf("%v", value)}
	}
}

func isPrintableASCII(data []byte) bool {
	for _, b := range data {
		if b < 32 || b > 126 {
			return false
		}
	}
	return len(data) > 0
}

func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

func truncateJSON(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if len(s) > 64 {
		return s[:64] + "..."
	}
	return s
}

// DecodeScValXDR converts a parsed Soroban ScVal into a canonical JSON-friendly
// representation. Large integers are returned as decimal strings to avoid JSON
// number precision loss.
func DecodeScValXDR(val xdr.ScVal) DecodedScVal {
	decoded, err := scValToDecoded(val)
	if err != nil {
		return DecodedScVal{Type: "error", Value: val.Type.String(), Display: err.Error()}
	}
	return decoded
}

func scValToDecoded(val xdr.ScVal) (DecodedScVal, error) {
	t := strings.TrimPrefix(strings.ToLower(val.Type.String()), "scv_")
	switch val.Type {
	case xdr.ScValTypeScvBool:
		if val.B == nil {
			return DecodedScVal{}, fmt.Errorf("bool has nil value")
		}
		return DecodedScVal{Type: "bool", Value: *val.B, Display: strconv.FormatBool(*val.B)}, nil
	case xdr.ScValTypeScvVoid:
		return DecodedScVal{Type: "void", Value: nil, Display: "void"}, nil
	case xdr.ScValTypeScvError:
		if val.Error == nil {
			return DecodedScVal{}, fmt.Errorf("error has nil value")
		}
		v := map[string]interface{}{"type": val.Error.Type.String()}
		display := val.Error.Type.String()
		if val.Error.ContractCode != nil {
			v["contract_code"] = uint32(*val.Error.ContractCode)
			display = fmt.Sprintf("%s:%d", val.Error.Type.String(), *val.Error.ContractCode)
		} else if val.Error.Code != nil {
			v["code"] = val.Error.Code.String()
			display = fmt.Sprintf("%s:%s", val.Error.Type.String(), val.Error.Code.String())
		}
		return DecodedScVal{Type: "error", Value: v, Display: display}, nil
	case xdr.ScValTypeScvU32:
		if val.U32 == nil {
			return DecodedScVal{}, fmt.Errorf("u32 has nil value")
		}
		s := strconv.FormatUint(uint64(*val.U32), 10)
		return DecodedScVal{Type: "u32", Value: uint32(*val.U32), Display: s}, nil
	case xdr.ScValTypeScvI32:
		if val.I32 == nil {
			return DecodedScVal{}, fmt.Errorf("i32 has nil value")
		}
		s := strconv.FormatInt(int64(*val.I32), 10)
		return DecodedScVal{Type: "i32", Value: int32(*val.I32), Display: s}, nil
	case xdr.ScValTypeScvU64:
		if val.U64 == nil {
			return DecodedScVal{}, fmt.Errorf("u64 has nil value")
		}
		s := strconv.FormatUint(uint64(*val.U64), 10)
		return DecodedScVal{Type: "u64", Value: s, Display: s}, nil
	case xdr.ScValTypeScvI64:
		if val.I64 == nil {
			return DecodedScVal{}, fmt.Errorf("i64 has nil value")
		}
		s := strconv.FormatInt(int64(*val.I64), 10)
		return DecodedScVal{Type: "i64", Value: s, Display: s}, nil
	case xdr.ScValTypeScvTimepoint:
		if val.Timepoint == nil {
			return DecodedScVal{}, fmt.Errorf("timepoint has nil value")
		}
		s := strconv.FormatUint(uint64(*val.Timepoint), 10)
		return DecodedScVal{Type: "timepoint", Value: s, Display: s}, nil
	case xdr.ScValTypeScvDuration:
		if val.Duration == nil {
			return DecodedScVal{}, fmt.Errorf("duration has nil value")
		}
		s := strconv.FormatUint(uint64(*val.Duration), 10)
		return DecodedScVal{Type: "duration", Value: s, Display: s}, nil
	case xdr.ScValTypeScvU128:
		if val.U128 == nil {
			return DecodedScVal{}, fmt.Errorf("u128 has nil value")
		}
		s := uint128String(*val.U128)
		return DecodedScVal{Type: "u128", Value: s, Display: s}, nil
	case xdr.ScValTypeScvI128:
		if val.I128 == nil {
			return DecodedScVal{}, fmt.Errorf("i128 has nil value")
		}
		s := int128String(*val.I128)
		return DecodedScVal{Type: "i128", Value: s, Display: s}, nil
	case xdr.ScValTypeScvU256:
		if val.U256 == nil {
			return DecodedScVal{}, fmt.Errorf("u256 has nil value")
		}
		s := uint256Big(*val.U256).String()
		return DecodedScVal{Type: "u256", Value: s, Display: s}, nil
	case xdr.ScValTypeScvI256:
		if val.I256 == nil {
			return DecodedScVal{}, fmt.Errorf("i256 has nil value")
		}
		s := int256String(*val.I256)
		return DecodedScVal{Type: "i256", Value: s, Display: s}, nil
	case xdr.ScValTypeScvBytes:
		if val.Bytes == nil {
			return DecodedScVal{}, fmt.Errorf("bytes has nil value")
		}
		raw := []byte(*val.Bytes)
		v := map[string]interface{}{"base64": base64.StdEncoding.EncodeToString(raw), "hex": hex.EncodeToString(raw), "length": len(raw)}
		return DecodedScVal{Type: "bytes", Value: v, Display: fmt.Sprintf("bytes[%d]", len(raw))}, nil
	case xdr.ScValTypeScvString:
		if val.Str == nil {
			return DecodedScVal{}, fmt.Errorf("string has nil value")
		}
		s := string(*val.Str)
		return DecodedScVal{Type: "string", Value: s, Display: truncateStr(s, 64)}, nil
	case xdr.ScValTypeScvSymbol:
		if val.Sym == nil {
			return DecodedScVal{}, fmt.Errorf("symbol has nil value")
		}
		s := string(*val.Sym)
		return DecodedScVal{Type: "symbol", Value: s, Display: s}, nil
	case xdr.ScValTypeScvAddress:
		if val.Address == nil {
			return DecodedScVal{}, fmt.Errorf("address has nil value")
		}
		addr, err := scAddressString(*val.Address)
		if err != nil {
			return DecodedScVal{}, err
		}
		return DecodedScVal{Type: "address", Value: addr, Display: compactAddress(addr)}, nil
	case xdr.ScValTypeScvVec:
		if val.Vec == nil || *val.Vec == nil {
			return DecodedScVal{}, fmt.Errorf("vec has nil value")
		}
		vec := **val.Vec
		items := make([]interface{}, 0, len(vec))
		displays := make([]string, 0, len(vec))
		for _, item := range vec {
			decoded := DecodeScValXDR(item)
			items = append(items, decoded)
			displays = append(displays, decoded.Display)
		}
		return DecodedScVal{Type: "vec", Value: items, Display: "[" + strings.Join(displays, ", ") + "]"}, nil
	case xdr.ScValTypeScvMap:
		if val.Map == nil || *val.Map == nil {
			return DecodedScVal{}, fmt.Errorf("map has nil value")
		}
		scMap := **val.Map
		entries := make([]interface{}, 0, len(scMap))
		object := make(map[string]interface{}, len(scMap))
		for _, entry := range scMap {
			k := DecodeScValXDR(entry.Key)
			v := DecodeScValXDR(entry.Val)
			entries = append(entries, map[string]interface{}{"key": k, "value": v})
			object[k.Display] = v
		}
		return DecodedScVal{Type: "map", Value: map[string]interface{}{"entries": entries, "object": object}, Display: fmt.Sprintf("map{%d}", len(scMap))}, nil
	case xdr.ScValTypeScvContractInstance:
		if val.Instance == nil {
			return DecodedScVal{}, fmt.Errorf("contract_instance has nil value")
		}
		v := map[string]interface{}{"executable_type": val.Instance.Executable.Type.String(), "storage_entries": 0}
		if val.Instance.Storage != nil {
			v["storage_entries"] = len(*val.Instance.Storage)
		}
		return DecodedScVal{Type: "contract_instance", Value: v, Display: "contract_instance"}, nil
	case xdr.ScValTypeScvLedgerKeyContractInstance:
		return DecodedScVal{Type: "ledger_key_contract_instance", Value: "instance", Display: "instance"}, nil
	case xdr.ScValTypeScvLedgerKeyNonce:
		if val.NonceKey == nil {
			return DecodedScVal{}, fmt.Errorf("ledger_key_nonce has nil value")
		}
		s := strconv.FormatInt(int64(val.NonceKey.Nonce), 10)
		return DecodedScVal{Type: "ledger_key_nonce", Value: map[string]interface{}{"nonce": s}, Display: "nonce:" + s}, nil
	default:
		return DecodedScVal{}, fmt.Errorf("unsupported ScVal type: %s", t)
	}
}

func scAddressString(addr xdr.ScAddress) (string, error) {
	switch addr.Type {
	case xdr.ScAddressTypeScAddressTypeAccount:
		if addr.AccountId == nil {
			return "", fmt.Errorf("account address has nil account_id")
		}
		raw := addr.AccountId.Ed25519
		return strkey.Encode(strkey.VersionByteAccountID, raw[:])
	case xdr.ScAddressTypeScAddressTypeContract:
		if addr.ContractId == nil {
			return "", fmt.Errorf("contract address has nil contract_id")
		}
		raw := *addr.ContractId
		return strkey.Encode(strkey.VersionByteContract, raw[:])
	default:
		return "", fmt.Errorf("unsupported ScAddress type: %s", addr.Type.String())
	}
}

func compactAddress(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:4] + "..." + s[len(s)-4:]
}

func uint128String(v xdr.UInt128Parts) string {
	n := big.NewInt(0).SetUint64(uint64(v.Hi))
	n.Lsh(n, 64)
	n.Add(n, big.NewInt(0).SetUint64(uint64(v.Lo)))
	return n.String()
}

func int128String(v xdr.Int128Parts) string {
	n := big.NewInt(0).SetUint64(uint64(v.Hi))
	if uint64(v.Hi)&(uint64(1)<<63) != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 64))
	}
	n.Lsh(n, 64)
	n.Add(n, big.NewInt(0).SetUint64(uint64(v.Lo)))
	return n.String()
}

func uint256Big(v xdr.UInt256Parts) *big.Int {
	n := big.NewInt(0).SetUint64(uint64(v.HiHi))
	n.Lsh(n, 64).Add(n, big.NewInt(0).SetUint64(uint64(v.HiLo)))
	n.Lsh(n, 64).Add(n, big.NewInt(0).SetUint64(uint64(v.LoHi)))
	n.Lsh(n, 64).Add(n, big.NewInt(0).SetUint64(uint64(v.LoLo)))
	return n
}

func int256String(v xdr.Int256Parts) string {
	u := xdr.UInt256Parts{HiHi: xdr.Uint64(v.HiHi), HiLo: xdr.Uint64(v.HiLo), LoHi: xdr.Uint64(v.LoHi), LoLo: xdr.Uint64(v.LoLo)}
	n := uint256Big(u)
	if uint64(v.HiHi)&(uint64(1)<<63) != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	return n.String()
}

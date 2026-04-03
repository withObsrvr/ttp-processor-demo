package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// DecodedScVal represents a decoded Soroban ScVal with type info and display string.
type DecodedScVal struct {
	Type    string      `json:"type"`
	Value   interface{} `json:"value"`
	Display string      `json:"display"`
}

// DecodeScValJSON attempts to decode a JSON-encoded ScVal into a human-readable form.
func DecodeScValJSON(raw json.RawMessage) DecodedScVal {
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return DecodedScVal{Type: "raw", Value: string(raw), Display: string(raw)}
	}

	if t, ok := obj["type"].(string); ok {
		val := obj["value"]
		return decodeTypedScVal(t, val)
	}

	if xdr, ok := obj["xdr"].(string); ok {
		return DecodedScVal{Type: "xdr", Value: xdr, Display: truncateDisplay(xdr, 32) + "..."}
	}

	return DecodedScVal{Type: "object", Value: obj, Display: truncateJSON(raw)}
}

// DecodeScValBase64 decodes a base64-encoded ScVal XDR into a basic representation.
func DecodeScValBase64(b64 string, typeHint string) DecodedScVal {
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return DecodedScVal{Type: "error", Value: b64, Display: "invalid base64"}
	}

	switch typeHint {
	case "symbol":
		if isPrintableASCII(data) {
			return DecodedScVal{Type: "symbol", Value: string(data), Display: string(data)}
		}
	case "string":
		if isPrintableASCII(data) {
			return DecodedScVal{Type: "string", Value: string(data), Display: string(data)}
		}
	case "address":
		s := string(data)
		if len(s) == 56 && (s[0] == 'G' || s[0] == 'C') {
			return DecodedScVal{Type: "address", Value: s, Display: s[:4] + "..." + s[52:]}
		}
	}

	hex := fmt.Sprintf("%x", data)
	return DecodedScVal{Type: typeHint, Value: hex, Display: truncateDisplay(hex, 16) + "..."}
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
		return DecodedScVal{Type: "string", Value: s, Display: truncateDisplay(s, 64)}
	case "bytes":
		s, _ := value.(string)
		return DecodedScVal{Type: "bytes", Value: s, Display: truncateDisplay(s, 32)}
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

func truncateDisplay(s string, maxLen int) string {
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

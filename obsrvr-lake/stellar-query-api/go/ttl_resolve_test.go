package main

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stellar/go/xdr"
)

func TestDecodeScValBase64SupportsCommonQueryEncodings(t *testing.T) {
	sym := xdr.ScSymbol("Balance")
	val := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	raw, err := val.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal scval: %v", err)
	}

	std := base64.StdEncoding.EncodeToString(raw)
	url := base64.URLEncoding.EncodeToString(raw)

	for name, input := range map[string]string{
		"std":          std,
		"std_plus_space": strings.ReplaceAll(std, "+", " "),
		"url":          url,
	} {
		t.Run(name, func(t *testing.T) {
			decoded, err := decodeScValBase64(input)
			if err != nil {
				t.Fatalf("decodeScValBase64(%q): %v", input, err)
			}
			if string(decoded) != string(raw) {
				t.Fatalf("decoded bytes mismatch")
			}
		})
	}
}

func TestComputeContractDataKeyHashRejectsUnsupportedKeyType(t *testing.T) {
	_, err := computeContractDataKeyHash(
		"CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM",
		"persistent",
		"",
		"balance",
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "unsupported key_type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

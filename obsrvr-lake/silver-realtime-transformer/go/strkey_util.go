package main

import (
	"encoding/hex"
	"fmt"

	"github.com/stellar/go-stellar-sdk/strkey"
)

// hexToStrKey converts a 64-char hex contract ID to a 56-char Stellar C-encoded strkey.
// If the input is already a valid contract strkey, it is returned as-is after validation.
func hexToStrKey(hexID string) (string, error) {
	// Fast-path: already a contract strkey — validate checksum before accepting
	if len(hexID) == 56 && hexID[0] == 'C' {
		if _, err := strkey.Decode(strkey.VersionByteContract, hexID); err == nil {
			return hexID, nil
		}
		// Not a valid strkey; fall through to try hex decoding
	}

	if len(hexID) != 64 {
		return "", fmt.Errorf("contract ID must be 64 hex characters, got %d", len(hexID))
	}

	hashBytes, err := hex.DecodeString(hexID)
	if err != nil {
		return "", fmt.Errorf("failed to decode contract ID hex: %w", err)
	}

	return strkey.Encode(strkey.VersionByteContract, hashBytes)
}

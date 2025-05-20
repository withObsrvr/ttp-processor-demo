package xdr

import (
	"io"
)

// LedgerCloseMeta is the metadata for a closed ledger
type LedgerCloseMeta struct {
	// Mock implementation
}

// LedgerSequence returns the sequence number of the ledger
func (lcm LedgerCloseMeta) LedgerSequence() uint32 {
	return 0
}

// Unmarshal unmarshals XDR from a reader into a value
func Unmarshal(r io.Reader, v interface{}) (int, error) {
	return 0, nil
}
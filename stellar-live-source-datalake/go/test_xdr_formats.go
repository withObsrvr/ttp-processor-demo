package main

import (
	"bytes"
	"fmt"
	"os"

	"github.com/stellar/go/xdr"
)

func main() {
	// Read the decompressed XDR file
	data, err := os.ReadFile("/tmp/test_ledger.xdr")
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	fmt.Printf("File size: %d bytes\n", len(data))
	fmt.Printf("First 20 bytes: %x\n", data[:20])

	// Try different XDR types
	
	// 1. Try as LedgerHeader
	fmt.Printf("\n=== Trying LedgerHeader ===\n")
	var lh xdr.LedgerHeader
	reader := bytes.NewReader(data)
	if _, err := xdr.Unmarshal(reader, &lh); err != nil {
		fmt.Printf("Error unmarshaling LedgerHeader: %v\n", err)
	} else {
		fmt.Printf("Successfully unmarshaled LedgerHeader!\n")
		fmt.Printf("Ledger sequence: %d\n", lh.LedgerSeq)
		fmt.Printf("Previous ledger hash: %x\n", lh.PreviousLedgerHash)
	}

	// 2. Try as LedgerHeaderHistoryEntry
	fmt.Printf("\n=== Trying LedgerHeaderHistoryEntry ===\n")
	var lhhe xdr.LedgerHeaderHistoryEntry
	reader = bytes.NewReader(data)
	if _, err := xdr.Unmarshal(reader, &lhhe); err != nil {
		fmt.Printf("Error unmarshaling LedgerHeaderHistoryEntry: %v\n", err)
	} else {
		fmt.Printf("Successfully unmarshaled LedgerHeaderHistoryEntry!\n")
		fmt.Printf("Ledger sequence: %d\n", lhhe.Header.LedgerSeq)
	}

	// 3. Try as TransactionHistoryEntry
	fmt.Printf("\n=== Trying TransactionHistoryEntry ===\n")
	var the xdr.TransactionHistoryEntry
	reader = bytes.NewReader(data)
	if _, err := xdr.Unmarshal(reader, &the); err != nil {
		fmt.Printf("Error unmarshaling TransactionHistoryEntry: %v\n", err)
	} else {
		fmt.Printf("Successfully unmarshaled TransactionHistoryEntry!\n")
		fmt.Printf("Ledger sequence: %d\n", the.LedgerSeq)
	}

	// 4. Try reading the structure manually
	fmt.Printf("\n=== Manual structure analysis ===\n")
	reader = bytes.NewReader(data)
	
	// Read first few uint32 values
	for i := 0; i < 10 && reader.Len() >= 4; i++ {
		var val uint32
		if _, err := xdr.Unmarshal(reader, &val); err != nil {
			fmt.Printf("Error reading uint32 %d: %v\n", i, err)
			break
		}
		fmt.Printf("uint32[%d]: %d (0x%x)\n", i, val, val)
	}
}
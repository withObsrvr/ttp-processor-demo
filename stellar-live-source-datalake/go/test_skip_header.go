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

	// Try skipping the first 4 bytes (ledger sequence header)
	fmt.Printf("\n=== Skipping first 4 bytes ===\n")
	if len(data) > 4 {
		dataSkipped := data[4:]
		fmt.Printf("Remaining size: %d bytes\n", len(dataSkipped))
		fmt.Printf("First 20 bytes after skip: %x\n", dataSkipped[:20])
		
		var lcm xdr.LedgerCloseMeta
		reader := bytes.NewReader(dataSkipped)
		if _, err := xdr.Unmarshal(reader, &lcm); err != nil {
			fmt.Printf("Error unmarshaling LedgerCloseMeta after skip: %v\n", err)
		} else {
			fmt.Printf("Successfully unmarshaled LedgerCloseMeta after skipping header!\n")
			fmt.Printf("Version: %d\n", lcm.V)
			switch lcm.V {
			case 0:
				fmt.Printf("Ledger sequence: %d\n", lcm.V0.LedgerHeader.Header.LedgerSeq)
			case 1:
				fmt.Printf("Ledger sequence: %d\n", lcm.V1.LedgerHeader.Header.LedgerSeq)
			}
		}
	}

	// Try skipping the first 8 bytes (both repeated sequence numbers)
	fmt.Printf("\n=== Skipping first 8 bytes ===\n")
	if len(data) > 8 {
		dataSkipped := data[8:]
		fmt.Printf("Remaining size: %d bytes\n", len(dataSkipped))
		fmt.Printf("First 20 bytes after skip: %x\n", dataSkipped[:20])
		
		var lcm xdr.LedgerCloseMeta
		reader := bytes.NewReader(dataSkipped)
		if _, err := xdr.Unmarshal(reader, &lcm); err != nil {
			fmt.Printf("Error unmarshaling LedgerCloseMeta after skip: %v\n", err)
		} else {
			fmt.Printf("Successfully unmarshaled LedgerCloseMeta after skipping 8 bytes!\n")
			fmt.Printf("Version: %d\n", lcm.V)
			switch lcm.V {
			case 0:
				fmt.Printf("Ledger sequence: %d\n", lcm.V0.LedgerHeader.Header.LedgerSeq)
			case 1:
				fmt.Printf("Ledger sequence: %d\n", lcm.V1.LedgerHeader.Header.LedgerSeq)
			}
		}
	}
}
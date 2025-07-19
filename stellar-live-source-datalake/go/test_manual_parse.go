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
	
	// Skip the first 8 bytes (custom header)
	dataSkipped := data[8:]
	fmt.Printf("Data after skipping header: %x\n", dataSkipped[:40])
	
	// Manual parsing - read the version first
	reader := bytes.NewReader(dataSkipped)
	var version uint32
	if _, err := xdr.Unmarshal(reader, &version); err != nil {
		fmt.Printf("Error reading version: %v\n", err)
		return
	}
	fmt.Printf("Version: %d\n", version)
	
	if version == 1 {
		// Try to read as LedgerCloseMetaV1
		fmt.Printf("Attempting to read as LedgerCloseMetaV1...\n")
		
		reader = bytes.NewReader(dataSkipped)
		var lcm xdr.LedgerCloseMeta
		// Manually set the version
		lcm.V = 1
		
		// Try to unmarshal just the V1 part
		if _, err := xdr.Unmarshal(reader, &lcm.V1); err != nil {
			fmt.Printf("Error unmarshaling LedgerCloseMetaV1: %v\n", err)
			
			// Try without version prefix
			reader = bytes.NewReader(dataSkipped[4:]) // Skip the version field
			if _, err := xdr.Unmarshal(reader, &lcm.V1); err != nil {
				fmt.Printf("Error unmarshaling LedgerCloseMetaV1 without version: %v\n", err)
			} else {
				fmt.Printf("Successfully unmarshaled LedgerCloseMetaV1 without version!\n")
				fmt.Printf("Ledger sequence: %d\n", lcm.V1.LedgerHeader.Header.LedgerSeq)
			}
		} else {
			fmt.Printf("Successfully unmarshaled LedgerCloseMetaV1!\n")
			fmt.Printf("Ledger sequence: %d\n", lcm.V1.LedgerHeader.Header.LedgerSeq)
		}
	}
}
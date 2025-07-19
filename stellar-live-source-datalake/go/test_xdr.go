package main

import (
	"bytes"
	"fmt"
	"io"
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

	// Try to unmarshal as LedgerCloseMeta
	var lcm xdr.LedgerCloseMeta
	reader := bytes.NewReader(data)
	
	fmt.Printf("Attempting to unmarshal as LedgerCloseMeta...\n")
	
	if _, err := xdr.Unmarshal(reader, &lcm); err != nil {
		fmt.Printf("Error unmarshaling LedgerCloseMeta: %v\n", err)
		
		// Try to read just the first 4 bytes as uint32
		reader.Seek(0, io.SeekStart)
		var firstUint32 uint32
		if _, err := xdr.Unmarshal(reader, &firstUint32); err != nil {
			fmt.Printf("Error reading first uint32: %v\n", err)
		} else {
			fmt.Printf("First uint32: %d (%x)\n", firstUint32, firstUint32)
		}
		
		// Try to read second uint32
		var secondUint32 uint32
		if _, err := xdr.Unmarshal(reader, &secondUint32); err != nil {
			fmt.Printf("Error reading second uint32: %v\n", err)
		} else {
			fmt.Printf("Second uint32: %d (%x)\n", secondUint32, secondUint32)
		}
	} else {
		fmt.Printf("Successfully unmarshaled LedgerCloseMeta!\n")
		
		// Try to extract ledger sequence
		switch lcm.V {
		case 0:
			fmt.Printf("LedgerCloseMeta V0, sequence: %d\n", lcm.V0.LedgerHeader.Header.LedgerSeq)
		case 1:
			fmt.Printf("LedgerCloseMeta V1, sequence: %d\n", lcm.V1.LedgerHeader.Header.LedgerSeq)
		default:
			fmt.Printf("LedgerCloseMeta version: %d\n", lcm.V)
		}
	}
}
package main

import (
	"github.com/stellar/go-stellar-sdk/processors/token_transfer"
	"google.golang.org/protobuf/proto"
)

// TokenTransferEventBatch is a simple wrapper for batching Stellar SDK's TokenTransferEvents
// This is a local convenience type - consumers should import Stellar SDK directly and unmarshal
// the individual events from this batch.
type TokenTransferEventBatch struct {
	Events []*token_transfer.TokenTransferEvent
}

// Marshal implements a simple marshaler for the batch
// Format: [event1_len][event1_bytes][event2_len][event2_bytes]...
func (b *TokenTransferEventBatch) Marshal() ([]byte, error) {
	// Simple approach: marshal each event and concatenate with length prefix
	var result []byte

	for _, event := range b.Events {
		eventBytes, err := proto.Marshal(event)
		if err != nil {
			return nil, err
		}

		// Write length (4 bytes, big-endian)
		length := uint32(len(eventBytes))
		result = append(result, byte(length>>24), byte(length>>16), byte(length>>8), byte(length))

		// Write event bytes
		result = append(result, eventBytes...)
	}

	return result, nil
}

// Unmarshal reconstructs the batch from bytes
func (b *TokenTransferEventBatch) Unmarshal(data []byte) error {
	b.Events = nil
	offset := 0

	for offset < len(data) {
		if offset+4 > len(data) {
			break
		}

		// Read length (4 bytes, big-endian)
		length := uint32(data[offset])<<24 | uint32(data[offset+1])<<16 | uint32(data[offset+2])<<8 | uint32(data[offset+3])
		offset += 4

		if offset+int(length) > len(data) {
			break
		}

		// Read and unmarshal event
		eventBytes := data[offset : offset+int(length)]
		event := &token_transfer.TokenTransferEvent{}
		if err := proto.Unmarshal(eventBytes, event); err != nil {
			return err
		}

		b.Events = append(b.Events, event)
		offset += int(length)
	}

	return nil
}

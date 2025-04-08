// main.go handles both native and WebAssembly builds
//
// For WebAssembly build: GOOS=js GOARCH=wasm go build
// For native build: go build

//go:build js && wasm
// +build js,wasm

// The above build tags ensure this file is only included when building for WebAssembly

package main

import (
	"context"
	"fmt"
	"strconv"
	"syscall/js"
	"time"

	"github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/internal/client"
	assetpb "github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/cmd/consumer_wasm/gen/ingest/asset"
	ttpb "github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/cmd/consumer_wasm/gen/ingest/processors/token_transfer"
)

// For WASM, we'll export JS functions that can be called from JavaScript

func registerCallbacks() {
	// Create a function to get TTP events
	js.Global().Set("getTTPEvents", js.FuncOf(getTTPEvents))
	
	// Create a function to clean up (close connections, etc.)
	js.Global().Set("cleanupTTPClient", js.FuncOf(cleanupTTPClient))
	
	// Keep the program running
	select {}
}

// Global client to be used by all exported functions
var globalClient *client.EventServiceClient

// getTTPEvents is the WASM exported function to get TTP events
func getTTPEvents(this js.Value, args []js.Value) interface{} {
	if len(args) < 3 {
		return js.ValueOf("Error: Expected 3 arguments: serverAddress, startLedger, endLedger")
	}

	serverAddress := args[0].String()
	startLedgerStr := args[1].String()
	endLedgerStr := args[2].String()

	startLedger, err := strconv.ParseUint(startLedgerStr, 10, 32)
	if err != nil {
		return js.ValueOf(fmt.Sprintf("Error parsing startLedger: %v", err))
	}

	endLedger, err := strconv.ParseUint(endLedgerStr, 10, 32)
	if err != nil {
		return js.ValueOf(fmt.Sprintf("Error parsing endLedger: %v", err))
	}

	// Create a promise to return to JavaScript
	promiseConstructor := js.Global().Get("Promise")
	return promiseConstructor.New(js.FuncOf(func(this js.Value, args []js.Value) interface{} {
		resolve := args[0]
		reject := args[1]

		// Process events in a goroutine
		go func() {
			// Create a new client if we don't have one
			if globalClient == nil {
				c, err := client.NewEventServiceClient(serverAddress)
				if err != nil {
					errorMsg := fmt.Sprintf("Error creating client: %v", err)
					reject.Invoke(js.ValueOf(errorMsg))
					return
				}
				globalClient = c
			}

			// Create an array to store events
			jsEvents := js.Global().Get("Array").New()
			eventCount := 0

			// Get events
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			err := globalClient.GetTTPEvents(ctx, uint32(startLedger), uint32(endLedger), func(event *ttpb.TokenTransferEvent) {
				// Convert the event to a JavaScript object
				jsEvent := convertEventToJS(event)
				jsEvents.Call("push", jsEvent)
				eventCount++
			})

			if err != nil {
				errorMsg := fmt.Sprintf("Error getting events: %v", err)
				reject.Invoke(js.ValueOf(errorMsg))
				return
			}

			// Return the events array
			resultObj := js.Global().Get("Object").New()
			resultObj.Set("events", jsEvents)
			resultObj.Set("count", eventCount)
			resolve.Invoke(resultObj)
		}()

		return nil
	}))
}

// cleanupTTPClient closes the client connection
func cleanupTTPClient(this js.Value, args []js.Value) interface{} {
	if globalClient != nil {
		err := globalClient.Close()
		globalClient = nil
		if err != nil {
			return js.ValueOf(fmt.Sprintf("Error closing client: %v", err))
		}
	}
	return js.ValueOf("Client closed successfully")
}

// convertEventToJS converts a TokenTransferEvent to a JavaScript object
func convertEventToJS(event *ttpb.TokenTransferEvent) js.Value {
	jsEvent := js.Global().Get("Object").New()
	
	// Add meta
	if meta := event.GetMeta(); meta != nil {
		jsMeta := js.Global().Get("Object").New()
		jsMeta.Set("ledgerSequence", meta.GetLedgerSequence())
		jsMeta.Set("txHash", meta.GetTxHash())
		jsMeta.Set("contractAddress", meta.GetContractAddress())
		jsMeta.Set("transactionIndex", meta.GetTransactionIndex())
		
		if meta.OperationIndex != nil {
			jsMeta.Set("operationIndex", *meta.OperationIndex)
		}
		
		if closedAt := meta.GetClosedAt(); closedAt != nil {
			jsTimestamp := js.Global().Get("Object").New()
			jsTimestamp.Set("seconds", closedAt.GetSeconds())
			jsTimestamp.Set("nanos", closedAt.GetNanos())
			jsMeta.Set("closedAt", jsTimestamp)
		}
		
		jsEvent.Set("meta", jsMeta)
	}
	
	// Determine event type and add appropriate data
	eventType := "unknown"
	var jsEventData js.Value
	
	switch e := event.Event.(type) {
	case *ttpb.TokenTransferEvent_Transfer:
		eventType = "transfer"
		transfer := e.Transfer
		jsEventData = js.Global().Get("Object").New()
		jsEventData.Set("from", transfer.GetFrom())
		jsEventData.Set("to", transfer.GetTo())
		jsEventData.Set("amount", transfer.GetAmount())
		if asset := transfer.GetAsset(); asset != nil {
			jsEventData.Set("asset", convertAssetToJS(asset))
		}
		
	case *ttpb.TokenTransferEvent_Mint:
		eventType = "mint"
		mint := e.Mint
		jsEventData = js.Global().Get("Object").New()
		jsEventData.Set("to", mint.GetTo())
		jsEventData.Set("amount", mint.GetAmount())
		if asset := mint.GetAsset(); asset != nil {
			jsEventData.Set("asset", convertAssetToJS(asset))
		}
		
	case *ttpb.TokenTransferEvent_Burn:
		eventType = "burn"
		burn := e.Burn
		jsEventData = js.Global().Get("Object").New()
		jsEventData.Set("from", burn.GetFrom())
		jsEventData.Set("amount", burn.GetAmount())
		if asset := burn.GetAsset(); asset != nil {
			jsEventData.Set("asset", convertAssetToJS(asset))
		}
		
	case *ttpb.TokenTransferEvent_Clawback:
		eventType = "clawback"
		clawback := e.Clawback
		jsEventData = js.Global().Get("Object").New()
		jsEventData.Set("from", clawback.GetFrom())
		jsEventData.Set("amount", clawback.GetAmount())
		if asset := clawback.GetAsset(); asset != nil {
			jsEventData.Set("asset", convertAssetToJS(asset))
		}
		
	case *ttpb.TokenTransferEvent_Fee:
		eventType = "fee"
		fee := e.Fee
		jsEventData = js.Global().Get("Object").New()
		jsEventData.Set("from", fee.GetFrom())
		jsEventData.Set("amount", fee.GetAmount())
		if asset := fee.GetAsset(); asset != nil {
			jsEventData.Set("asset", convertAssetToJS(asset))
		}
	}
	
	jsEvent.Set("eventType", eventType)
	if !jsEventData.IsUndefined() {
		jsEvent.Set("data", jsEventData)
	}
	
	return jsEvent
}

// convertAssetToJS converts an Asset to a JavaScript object
func convertAssetToJS(asset *assetpb.Asset) js.Value {
	jsAsset := js.Global().Get("Object").New()
	
	switch at := asset.AssetType.(type) {
	case *assetpb.Asset_Native:
		jsAsset.Set("native", true)
	case *assetpb.Asset_IssuedAsset:
		jsAsset.Set("native", false)
		jsIssued := js.Global().Get("Object").New()
		jsIssued.Set("assetCode", at.IssuedAsset.GetAssetCode())
		jsIssued.Set("issuer", at.IssuedAsset.GetIssuer())
		jsAsset.Set("issuedAsset", jsIssued)
	}
	
	return jsAsset
}

func main() {
	// Register JS callbacks
	registerCallbacks()
} 
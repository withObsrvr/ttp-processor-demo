package server

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/schema"
)

func TestStellarArrowServer_Creation(t *testing.T) {
	config := &ServerConfig{
		Port:              8815,
		SourceEndpoint:    "localhost:8080",
		NetworkPassphrase: "Test SDF Network ; September 2015",
		BatchSize:         1000,
		MaxConnections:    100,
		FlowCtlEnabled:    false,
	}

	server, err := NewStellarArrowServer(config)
	if err != nil {
		t.Fatalf("Failed to create Arrow server: %v", err)
	}

	if server == nil {
		t.Fatal("Server is nil")
	}

	// Test allocator
	if server.allocator == nil {
		t.Error("Allocator is nil")
	}

	// Test schema manager
	if server.schemaManager == nil {
		t.Error("Schema manager is nil")
	}

	// Test config
	if server.config.Port != 8815 {
		t.Errorf("Expected port 8815, got %d", server.config.Port)
	}
}

func TestStellarArrowServer_HealthStatus(t *testing.T) {
	config := &ServerConfig{
		Port:              8815,
		SourceEndpoint:    "localhost:8080",
		NetworkPassphrase: "Test SDF Network ; September 2015",
		BatchSize:         1000,
		MaxConnections:    100,
		FlowCtlEnabled:    false,
	}

	server, err := NewStellarArrowServer(config)
	if err != nil {
		t.Fatalf("Failed to create Arrow server: %v", err)
	}

	health := server.GetHealthStatus()
	
	if health["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got %v", health["status"])
	}

	if health["server_type"] != "native_arrow_flight" {
		t.Errorf("Expected server_type 'native_arrow_flight', got %v", health["server_type"])
	}

	if health["protocol"] != "arrow-flight" {
		t.Errorf("Expected protocol 'arrow-flight', got %v", health["protocol"])
	}
}

func TestStellarArrowServer_SchemaRetrieval(t *testing.T) {
	config := &ServerConfig{
		Port:              8815,
		SourceEndpoint:    "localhost:8080",
		NetworkPassphrase: "Test SDF Network ; September 2015",
		BatchSize:         1000,
		MaxConnections:    100,
		FlowCtlEnabled:    false,
	}

	server, err := NewStellarArrowServer(config)
	if err != nil {
		t.Fatalf("Failed to create Arrow server: %v", err)
	}

	// Test stellar ledger schema
	stellarSchema := server.schemaManager.GetStellarLedgerSchema()
	if stellarSchema == nil {
		t.Error("Stellar ledger schema is nil")
	}

	if stellarSchema.NumFields() == 0 {
		t.Error("Stellar ledger schema has no fields")
	}

	// Test TTP event schema
	ttpSchema := server.schemaManager.GetTTPEventSchema()
	if ttpSchema == nil {
		t.Error("TTP event schema is nil")
	}

	if ttpSchema.NumFields() == 0 {
		t.Error("TTP event schema has no fields")
	}
}

func TestParseStreamParams(t *testing.T) {
	tests := []struct {
		ticket      string
		expectStart uint32
		expectEnd   uint32
		expectType  string
	}{
		{"stellar_ledger:100:200", 100, 200, "stellar_ledger"},
		{"ttp_events:500:1000", 500, 1000, "ttp_events"},
		{"stellar_ledger:42:", 42, 0, "stellar_ledger"},
		{"transactions", 0, 0, "transactions"},
		{"", 0, 0, "stellar_ledger"},
	}

	for _, tt := range tests {
		t.Run(tt.ticket, func(t *testing.T) {
			params, err := parseStreamParams([]byte(tt.ticket))
			if err != nil {
				t.Fatalf("Failed to parse ticket '%s': %v", tt.ticket, err)
			}

			if params.StartLedger != tt.expectStart {
				t.Errorf("Expected start ledger %d, got %d", tt.expectStart, params.StartLedger)
			}

			if params.EndLedger != tt.expectEnd {
				t.Errorf("Expected end ledger %d, got %d", tt.expectEnd, params.EndLedger)
			}

			if params.StreamType != tt.expectType {
				t.Errorf("Expected stream type '%s', got '%s'", tt.expectType, params.StreamType)
			}
		})
	}
}

func TestArrowMemoryManagement(t *testing.T) {
	allocator := memory.NewGoAllocator()
	schemaManager := schema.NewSchemaManager()

	// Test schema creation
	schema := schemaManager.GetStellarLedgerSchema()
	if schema == nil {
		t.Fatal("Schema is nil")
	}

	// Verify allocator exists
	if allocator == nil {
		t.Error("Allocator is nil")
	}

	// Phase 1: Basic memory management test
	// In future phases, we'll implement proper memory tracking
}

func BenchmarkSchemaCreation(b *testing.B) {
	schemaManager := schema.NewSchemaManager()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		schema := schemaManager.GetStellarLedgerSchema()
		if schema == nil {
			b.Fatal("Schema is nil")
		}
	}
}

func BenchmarkServerCreation(b *testing.B) {
	config := &ServerConfig{
		Port:              8815,
		SourceEndpoint:    "localhost:8080",
		NetworkPassphrase: "Test SDF Network ; September 2015",
		BatchSize:         1000,
		MaxConnections:    100,
		FlowCtlEnabled:    false,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server, err := NewStellarArrowServer(config)
		if err != nil {
			b.Fatalf("Failed to create server: %v", err)
		}
		if server == nil {
			b.Fatal("Server is nil")
		}
	}
}
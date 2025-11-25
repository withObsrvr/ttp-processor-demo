package resolver_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/withObsrvr/ttp-processor-demo/ducklake-ingestion-obsrvr-v2/go/resolver"
)

// ExampleResolver_ResolveDataset demonstrates basic resolver usage.
func ExampleResolver_ResolveDataset() {
	// 1. Connect to DuckDB with DuckLake catalog attached
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Load DuckLake extension and attach catalog
	_, _ = db.Exec("LOAD ducklake")
	_, _ = db.Exec(`
		ATTACH 'ducklake:postgres:postgresql:///ducklake_cycle4_test?host=/path/to/sockets'
		AS testnet
		(DATA_PATH '/path/to/data', METADATA_SCHEMA 'testnet')
	`)

	// 2. Create resolver
	r, err := resolver.New(resolver.Config{
		DB:           db,
		CatalogName:  "testnet",
		SchemaName:   "testnet",
		CacheEnabled: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// 3. Resolve dataset with "latest" intent
	result, err := r.ResolveDataset(ctx, "core.ledgers_row_v2", resolver.Intent{
		Mode:    resolver.IntentLatest,
		Network: "testnet",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Dataset: %s\n", result.Dataset)
	fmt.Printf("Era: %s/%s\n", result.EraID, result.VersionLabel)
	fmt.Printf("Coverage: %d ledgers (tail: %d)\n",
		len(result.Coverage.CommittedRanges), result.Coverage.TailLedger)
	fmt.Printf("PAS Verified: %v\n", result.PASVerified)
}

// ExampleResolver_ResolveDataset_asOfLedger demonstrates resolving to a specific ledger.
func ExampleResolver_ResolveDataset_asOfLedger() {
	db, _ := sql.Open("duckdb", "")
	defer db.Close()

	r, _ := resolver.New(resolver.Config{
		DB:          db,
		CatalogName: "testnet",
		SchemaName:  "testnet",
	})

	ctx := context.Background()
	ledger := uint32(1500025)

	result, err := r.ResolveDataset(ctx, "core.transactions_row_v2", resolver.Intent{
		Mode:    resolver.IntentAsOfLedger,
		Network: "testnet",
		Ledger:  &ledger,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Resolved to era: %s (covers ledger %d)\n", result.EraID, ledger)
}

// ExampleResolver_ResolveDataset_withRange demonstrates reading a specific range.
func ExampleResolver_ResolveDataset_withRange() {
	db, _ := sql.Open("duckdb", "")
	defer db.Close()

	r, _ := resolver.New(resolver.Config{
		DB:          db,
		CatalogName: "testnet",
		SchemaName:  "testnet",
	})

	ctx := context.Background()

	result, err := r.ResolveDataset(ctx, "core.ledgers_row_v2", resolver.Intent{
		Mode:    resolver.IntentLatest,
		Network: "testnet",
		Range: &resolver.LedgerRange{
			Start: 1500000,
			End:   1500050,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Manifest files: %d\n", len(result.Manifest.Files))
	fmt.Printf("Total rows: %d\n", result.Manifest.TotalRows)
	fmt.Printf("Snapshot ID: %d\n", result.Manifest.SnapshotID)
}

// ExampleResolver_GetEraMap demonstrates listing all eras.
func ExampleResolver_GetEraMap() {
	db, _ := sql.Open("duckdb", "")
	defer db.Close()

	r, _ := resolver.New(resolver.Config{
		DB:          db,
		CatalogName: "testnet",
		SchemaName:  "testnet",
	})

	ctx := context.Background()

	eras, err := r.GetEraMap(ctx, "testnet")
	if err != nil {
		log.Fatal(err)
	}

	for _, era := range eras {
		fmt.Printf("Era: %s (%s) - ledgers %d to %v\n",
			era.EraID, era.Status, era.LedgerStart, era.LedgerEnd)
	}
}

// ExampleResolver_ListDatasets demonstrates listing available datasets.
func ExampleResolver_ListDatasets() {
	db, _ := sql.Open("duckdb", "")
	defer db.Close()

	r, _ := resolver.New(resolver.Config{
		DB:          db,
		CatalogName: "testnet",
		SchemaName:  "testnet",
	})

	ctx := context.Background()

	datasets, err := r.ListDatasets(ctx, "testnet", nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Available datasets: %d\n", len(datasets))
	for _, ds := range datasets {
		fmt.Printf("  - %s (v%d.%d)\n", ds.Name, ds.MajorVersion, ds.MinorVersion)
	}
}

// ExampleResolver_GetCoverage demonstrates checking coverage.
func ExampleResolver_GetCoverage() {
	db, _ := sql.Open("duckdb", "")
	defer db.Close()

	r, _ := resolver.New(resolver.Config{
		DB:          db,
		CatalogName: "testnet",
		SchemaName:  "testnet",
	})

	ctx := context.Background()

	coverage, err := r.GetCoverage(ctx, "core.ledgers_row_v2", "p23_plus", "v1")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Committed ranges: %d\n", len(coverage.CommittedRanges))
	fmt.Printf("Tail ledger: %d\n", coverage.TailLedger)
	fmt.Printf("Gaps: %d\n", len(coverage.Gaps))
	fmt.Printf("Total rows: %d\n", coverage.TotalRows)
	fmt.Printf("Last verified: %d\n", coverage.LastVerified)
}

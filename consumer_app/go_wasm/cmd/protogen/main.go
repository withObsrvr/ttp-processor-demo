package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	// Set up paths
	workDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	protoDir := filepath.Join(workDir, "protos")
	genDir := filepath.Join(workDir, "cmd", "consumer_wasm", "gen")

	// Create output directory
	err = os.MkdirAll(genDir, 0755)
	if err != nil {
		log.Fatal(err)
	}

	// Change to protos directory
	err = os.Chdir(protoDir)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Current directory:", protoDir)
	fmt.Println("Output directory:", genDir)

	// List proto files
	fmt.Println("\nProto files:")
	err = filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".proto" {
			fmt.Println(path)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Run protoc command
	fmt.Println("\nGenerating protobuf code...")
	cmd := exec.Command("protoc",
		"--go_out=../cmd/consumer_wasm/gen",
		"--go_opt=paths=source_relative",
		"--go_opt=Mingest/asset/asset.proto=github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/cmd/consumer_wasm/gen/ingest/asset",
		"--go_opt=Mingest/processors/token_transfer/token_transfer_event.proto=github.com/stellar/ttp-processor-demo/consumer_app/go_wasm/cmd/consumer_wasm/gen/ingest/processors/token_transfer",
		"--go-grpc_out=../cmd/consumer_wasm/gen",
		"--go-grpc_opt=paths=source_relative",
		"--experimental_allow_proto3_optional",
		"event_service/event_service.proto",
		"ingest/processors/token_transfer/token_transfer_event.proto",
		"ingest/asset/asset.proto",
	)

	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("Error running protoc:", string(output))
		log.Fatal(err)
	}

	fmt.Println("Protobuf code generation complete!")

	// List generated files
	fmt.Println("\nGenerated files:")
	err = filepath.Walk(genDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, err := filepath.Rel(genDir, path)
			if err != nil {
				return err
			}
			fmt.Println(rel)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
} 
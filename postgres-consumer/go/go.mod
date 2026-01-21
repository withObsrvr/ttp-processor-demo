module github.com/withobsrvr/postgres-consumer

go 1.24.0

toolchain go1.24.9

require (
	github.com/lib/pq v1.10.9
	github.com/stellar/go-stellar-sdk v0.0.0-20251210134752-6c46f8811c13
	github.com/withObsrvr/flow-proto v0.0.0-00010101000000-000000000000
	github.com/withObsrvr/flowctl-sdk v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.36.8
)

require (
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/grpc v1.71.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// For local development, uncomment these replace directives:
// replace github.com/withObsrvr/flow-proto => /path/to/flow-proto
// replace github.com/withObsrvr/flowctl-sdk => /path/to/flowctl-sdk

module github.com/withObsrvr/ledger-jsonrpc-processor

go 1.22

require (
	github.com/stellar/go v0.0.0-00010101000000-000000000000
	github.com/withObsrvr/flowctl v0.0.1
	go.uber.org/zap v1.26.0
	google.golang.org/grpc v1.62.0
	google.golang.org/protobuf v1.33.0
)

require (
	github.com/golang/protobuf v1.5.3 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.23.0 // indirect
	golang.org/x/sys v0.18.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
)

// Mock dependencies
replace (
	github.com/stellar/go => ./mocks/stellar/go
	github.com/withObsrvr/flowctl => ./mocks/flowctl
)

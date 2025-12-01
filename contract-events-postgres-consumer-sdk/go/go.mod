module github.com/withObsrvr/contract-events-postgres-consumer-sdk

go 1.25

toolchain go1.25.4

require (
	github.com/lib/pq v1.10.9
	github.com/withObsrvr/flow-proto v0.0.0
	github.com/withObsrvr/flowctl-sdk v0.0.0
	go.uber.org/zap v1.27.0
	google.golang.org/protobuf v1.36.6
)

require (
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/grpc v1.71.0 // indirect
)

replace github.com/withObsrvr/flowctl-sdk => /home/tillman/Documents/flowctl-sdk

replace github.com/withObsrvr/flow-proto => /home/tillman/Documents/flow-proto

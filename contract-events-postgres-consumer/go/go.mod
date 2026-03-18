module github.com/withObsrvr/contract-events-postgres-consumer

go 1.25

require (
	github.com/lib/pq v1.10.9
	github.com/withObsrvr/contract-events-postgres-consumer/gen/contract_event_service v0.0.0-00010101000000-000000000000
	github.com/withobsrvr/flowctl v0.0.1
	google.golang.org/grpc v1.79.3
	google.golang.org/protobuf v1.36.10
)

require (
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
)

replace github.com/withObsrvr/contract-events-postgres-consumer/gen/contract_event_service => ../gen/contract_event_service

// Remove replace directive to use published version from GitHub
// replace github.com/withobsrvr/flowctl => ../../../../flowctl

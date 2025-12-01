module github.com/withObsrvr/contract-events-postgres-consumer

go 1.25

require (
	github.com/lib/pq v1.10.9
	github.com/withObsrvr/contract-events-postgres-consumer/gen/contract_event_service v0.0.0-00010101000000-000000000000
	github.com/withobsrvr/flowctl v0.0.1
	google.golang.org/grpc v1.72.0
	google.golang.org/protobuf v1.36.6
)

require (
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250218202821-56aae31c358a // indirect
)

replace github.com/withObsrvr/contract-events-postgres-consumer/gen/contract_event_service => ../gen/contract_event_service

replace github.com/withobsrvr/flowctl => /home/tillman/Documents/flowctl

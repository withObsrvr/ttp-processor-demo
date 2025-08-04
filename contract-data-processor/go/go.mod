module github.com/withObsrvr/ttp-processor-demo/contract-data-processor

go 1.21

require (
	github.com/apache/arrow/go/v17 v17.0.0
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.10.9
	github.com/prometheus/client_golang v1.19.1
	github.com/rs/zerolog v1.33.0
	github.com/stellar/go v0.0.0-20250718194041-56335b4c7e0c
	github.com/withObsrvr/flowctl v0.0.0-00010101000000-000000000000
	github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.2
)

// Local replacements for development
replace (
	github.com/withObsrvr/flowctl => ../../../flowctl
	github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake => ../stellar-live-source-datalake
)
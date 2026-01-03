module github.com/withObsrvr/ttp-processor-demo/stellar-postgres-ingester/go

go 1.24.0

require (
	github.com/jackc/pgx/v5 v5.5.1
	github.com/stellar/go-stellar-sdk v0.0.0-20251125210943-4134368d57d8
	github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.76.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/stellar/go-xdr v0.0.0-20231122183749-b53fb00bcac2 // indirect
	golang.org/x/crypto v0.40.0 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250804133106-a7a43d27e69b // indirect
	google.golang.org/protobuf v1.36.6 // indirect
)

replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake/go => ../../stellar-live-source-datalake/go

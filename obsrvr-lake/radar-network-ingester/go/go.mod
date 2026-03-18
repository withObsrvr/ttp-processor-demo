module github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-ingester/go

go 1.24.0

require (
	github.com/jackc/pgx/v5 v5.7.2
	github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.79.3
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	golang.org/x/crypto v0.46.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.39.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace github.com/withObsrvr/ttp-processor-demo/obsrvr-lake/radar-network-source/go => ../../radar-network-source/go

module github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source

go 1.23.4

toolchain go1.24.3

require (
	github.com/apache/arrow/go/v17 v17.0.0
	github.com/klauspost/compress v1.17.9
	github.com/pierrec/lz4/v4 v4.1.21
	github.com/prometheus/client_golang v1.19.0
	github.com/rs/zerolog v1.33.0
	github.com/stellar/go v0.0.0-20250716214416-01d16bf8185f
	github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake v0.0.0-00010101000000-000000000000
	github.com/withobsrvr/flowctl v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.72.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/flatbuffers v24.3.25+incompatible // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.5.0 // indirect
	github.com/prometheus/common v0.48.0 // indirect
	github.com/prometheus/procfs v0.12.0 // indirect
	github.com/stellar/go-xdr v0.0.0-20231122183749-b53fb00bcac2 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	golang.org/x/exp v0.0.0-20240506185415-9bf2ced13842 // indirect
	golang.org/x/mod v0.18.0 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/tools v0.22.0 // indirect
	golang.org/x/xerrors v0.0.0-20231012003039-104605ab7028 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250218202821-56aae31c358a // indirect
)

// Local package references
replace github.com/withobsrvr/flowctl => github.com/withObsrvr/flowctl v0.0.1

replace github.com/withObsrvr/ttp-processor-demo/stellar-live-source-datalake => ../../stellar-live-source-datalake/go

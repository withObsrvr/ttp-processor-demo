module github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source/consumer/parquet

go 1.21

require (
	github.com/apache/arrow/go/v17 v17.0.0
	github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source v0.0.0
	google.golang.org/grpc v1.64.0
)

replace github.com/withObsrvr/ttp-processor-demo/stellar-arrow-source => ../../..
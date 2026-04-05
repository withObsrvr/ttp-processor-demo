.PHONY: build run clean gen-proto

build:
	cd go && CGO_ENABLED=1 go build -o ../bin/obsrvr-lake-unified-silver-processor .

run: build
	./bin/obsrvr-lake-unified-silver-processor -config go/config.yaml

clean:
	rm -f bin/obsrvr-lake-unified-silver-processor

gen-proto:
	cd ../obsrvr-lake-unified-processor && \
	protoc --go_out=go --go_opt=paths=source_relative \
		--go-grpc_out=go --go-grpc_opt=paths=source_relative \
		protos/bronze_ledger_service/bronze_ledger_service.proto

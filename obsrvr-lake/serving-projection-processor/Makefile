BINARY=serving-projection-processor
GO_DIR=go

.PHONY: build run tidy

build:
	cd $(GO_DIR) && go build -o $(BINARY) .

run:
	cd $(GO_DIR) && go run . -config ../config.yaml

tidy:
	cd $(GO_DIR) && go mod tidy

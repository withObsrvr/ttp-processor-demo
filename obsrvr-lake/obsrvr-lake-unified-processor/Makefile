BINARY_NAME=obsrvr-lake-unified-processor
VERSION=$(shell git rev-parse --short HEAD 2>/dev/null || echo "dev")

.PHONY: build clean docker-build docker-push

build:
	cd go && GOWORK=off go build -ldflags "-X main.Version=$(VERSION)" -o ../bin/$(BINARY_NAME) .

clean:
	rm -rf bin/

docker-build:
	docker build -t withobsrvr/$(BINARY_NAME):latest .

docker-push: docker-build
	docker push withobsrvr/$(BINARY_NAME):latest

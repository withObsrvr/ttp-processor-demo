.PHONY: build run clean

build:
	cd go && go build -o bin/postgres-ducklake-flusher

run: build
	./go/bin/postgres-ducklake-flusher -config config.yaml

clean:
	rm -rf go/bin

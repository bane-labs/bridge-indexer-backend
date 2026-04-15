VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -w -s -X main.version=$(VERSION)
BINARY  := bridge-indexer

.PHONY: build swagger test

build:
	go build -trimpath -ldflags="$(LDFLAGS)" -o dist/$(BINARY) ./cmd/server

test:
	go test -race -count=1 ./...

swagger:
	swag init -g cmd/server/main.go -o docs/swagger --parseInternal --parseDependency

# distsched — build, codegen, and test targets.
# Requires: go >= 1.23, protoc, protoc-gen-go, protoc-gen-go-grpc.

GOPATH_BIN := $(shell go env GOPATH)/bin
export PATH := $(GOPATH_BIN):$(PATH)

PROTO_DIR := proto
PROTO_FILES := $(shell find $(PROTO_DIR) -name '*.proto')
MODULE := github.com/kshama7/distsched

.PHONY: all
all: proto build

## tools: install the protoc plugins pinned for this repo.
.PHONY: tools
tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.5
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

## proto: regenerate Go gRPC + message code from the .proto files.
.PHONY: proto
proto:
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_FILES)

## build: compile all binaries into ./bin.
.PHONY: build
build:
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/worker ./cmd/worker
	go build -o bin/schedulerctl ./cmd/schedulerctl

## test: run the unit test suite with the race detector.
.PHONY: test
test:
	go test -race ./...

## vet: static checks.
.PHONY: vet
vet:
	go vet ./...

## tidy: sync go.mod/go.sum.
.PHONY: tidy
tidy:
	go mod tidy

## clean: remove build artifacts.
.PHONY: clean
clean:
	rm -rf bin

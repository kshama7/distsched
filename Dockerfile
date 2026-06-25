# Multi-stage build producing a single image with all three binaries. The
# runtime is Alpine (not distroless) because workers shell out to commands like
# echo/sleep, which need a real userland.
FROM golang:1.25 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=docker
ARG COMMIT=none
RUN CGO_ENABLED=0 go build \
      -ldflags "-X github.com/kshama7/distsched/internal/buildinfo.Version=${VERSION} -X github.com/kshama7/distsched/internal/buildinfo.Commit=${COMMIT}" \
      -o /out/scheduler ./cmd/scheduler && \
    CGO_ENABLED=0 go build -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 go build -o /out/schedulerctl ./cmd/schedulerctl

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget && \
    adduser -D -u 10001 distsched && \
    mkdir -p /data && chown distsched:distsched /data
COPY --from=build /out/scheduler /out/worker /out/schedulerctl /usr/local/bin/
USER distsched
WORKDIR /data
# Command is supplied by the orchestrator (compose / k8s): scheduler | worker.
ENTRYPOINT []

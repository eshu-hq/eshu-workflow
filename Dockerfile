# SPDX-License-Identifier: MIT
# Copyright (c) 2025-2026 eshu-hq

# Workflow service image: static binaries plus CA roots, no shell. One
# image ships both binaries; deployments override the entrypoint to run
# the coordinator instead of the claim-plane server.
ARG GO_VERSION=1.26.10

FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/claim-plane \
    ./cmd/claim-plane
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/workflow-coordinator \
    ./cmd/workflow-coordinator

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/claim-plane /claim-plane
COPY --from=build /out/workflow-coordinator /workflow-coordinator
USER nonroot:nonroot
EXPOSE 8080 8081 8082
ENTRYPOINT ["/claim-plane"]

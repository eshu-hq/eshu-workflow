# SPDX-License-Identifier: MIT
# Copyright (c) 2025-2026 eshu-hq

# Workflow service image: static binaries plus CA roots, no shell. One
# image ships both binaries; deployments override the entrypoint to run
# the coordinator instead of the claim-plane server.
ARG GO_VERSION=1.26.10

# Base digests resolved 2026-10-28 via `crane digest`; re-pin with the same
# command when bumping either base. Digest pins keep ops-qa deploys on an
# immutable rootfs (the ci image lane enforces every FROM is pinned).
FROM golang:${GO_VERSION}-bookworm@sha256:386f0cbb8fb3402ff5a15048c96731e6ff37142d31182f3b72306359a1f5e9b7 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# EKS nodes are linux/amd64; pin GOARCH so builds on any host (e.g. arm64
# dev boxes) produce a runnable binary. An unpinned build shipped an arm64
# image that failed with "exec format error" on ops-qa (#8868).
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/claim-plane \
    ./cmd/claim-plane
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/workflow-coordinator \
    ./cmd/workflow-coordinator

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/claim-plane /claim-plane
COPY --from=build /out/workflow-coordinator /workflow-coordinator
USER nonroot:nonroot
EXPOSE 8080 8081 8082
ENTRYPOINT ["/claim-plane"]

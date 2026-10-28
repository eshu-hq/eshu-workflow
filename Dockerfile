# SPDX-License-Identifier: MIT
# Copyright (c) 2025-2026 eshu-hq

# Claim-plane server image: static binary plus CA roots, no shell.
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

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/claim-plane /claim-plane
USER nonroot:nonroot
EXPOSE 8080 8081
ENTRYPOINT ["/claim-plane"]

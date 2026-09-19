#!/usr/bin/sh

MITM_VERSION=$(git describe --tags)

CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${MITM_VERSION}" -o ./bin/mitm-deliver ./cmd/deliver/main.go

cp bin/mitm-deliver ../../scheduler/mitm_scheduler/bin/.
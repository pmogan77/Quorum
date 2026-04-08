#!/usr/bin/env bash
set -e

docker build -t quorum-kv-node:latest -f Dockerfile.kvnode .
docker build -t quorum-redis:latest -f Dockerfile.redis .
docker build -t otel-lab-node:latest -f Dockerfile.otelcol .
docker build -t jaeger-lab-node:latest -f Dockerfile.jaeger .
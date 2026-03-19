#!/usr/bin/env bash
set -e

docker build -t quorum-kv-node:latest -f Dockerfile.kvnode .
docker build -t quorum-redis:latest -f Dockerfile.redis .
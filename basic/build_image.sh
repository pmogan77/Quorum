#!/usr/bin/env bash
set -e

docker build -t quorum-kv-node:latest -f Dockerfile.kvnode .
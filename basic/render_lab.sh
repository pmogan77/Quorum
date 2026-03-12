#!/bin/bash
set -e

python3 tools/generate_lab.py \
  --config config/lab_config.json \
  --base-dir .


cd shared

python3 -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --grpc_python_out=. \
  kv.proto


cd ycsb/quorum-binding

mvn clean package

# copy the target jar to the lib directory
cp target/quorum-binding-0.17.0.jar ../lib/quorum-binding-0.17.0.jar
#!/usr/bin/env bash
set -euo pipefail

echo "=== LOADING DATA ==="
./bin/ycsb load basic \
  -db site.ycsb.db.quorum.QuorumDB \
  -P workloads/workload_w \
  -P quorum-binding/conf/quorum.properties \
  -p recordcount=10 \
  -p fieldcount=1 \
  > load.txt

echo "=== PHASE 1: READ WARMUP, FORCE KEYS INTO READ CONFIG ==="
./bin/ycsb run basic \
  -db site.ycsb.db.quorum.QuorumDB \
  -P workloads/workload_read_warmup \
  -P quorum-binding/conf/quorum.properties \
  > phase1_read_warmup.txt

echo "=== PHASE 2: MIXED READ/WRITE FROM READ CONFIG START ==="
./bin/ycsb run basic \
  -db site.ycsb.db.quorum.QuorumDB \
  -P workloads/workload_mixed \
  -P quorum-binding/conf/quorum.properties \
  > phase2_mixed.txt

echo "=== DONE ==="
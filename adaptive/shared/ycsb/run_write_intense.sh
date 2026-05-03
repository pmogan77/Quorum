#!/usr/bin/env bash
set -euo pipefail

DB="site.ycsb.db.quorum.QuorumDB"
CONF="quorum-binding/conf/quorum.properties"

echo "LOADING DATA"
./bin/ycsb load basic \
  -db "$DB" \
  -P workloads/workload_write_intense \
  -P "$CONF" \
  -p recordcount=10 \
  -p fieldcount=1

echo "WRITE INTENSIVE WORKLOAD"
./bin/ycsb run basic \
  -db "$DB" \
  -P workloads/workload_write_intense \
  -P "$CONF" \
  > write_intense.txt

echo "DONE"
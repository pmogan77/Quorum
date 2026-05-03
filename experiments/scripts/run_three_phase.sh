#!/usr/bin/env bash

echo "=== LOADING DATA ==="
./bin/ycsb load basic -db site.ycsb.db.quorum.QuorumDB -P workloads/workload_w -P quorum-binding/conf/quorum.properties -p recordcount=10 -p fieldcount=1 

echo "=== PHASE 1: WRITE HEAVY ==="
./bin/ycsb run basic -db site.ycsb.db.quorum.QuorumDB -P workloads/workload_w -P quorum-binding/conf/quorum.properties > phase1_write.txt

echo "=== PHASE 2: READ HEAVY ==="
./bin/ycsb run basic -db site.ycsb.db.quorum.QuorumDB -P workloads/workload_r -P quorum-binding/conf/quorum.properties > phase2_read.txt

echo "=== PHASE 3: BALANCED ==="
./bin/ycsb run basic -db site.ycsb.db.quorum.QuorumDB -P workloads/workload_b -P quorum-binding/conf/quorum.properties > phase3_balanced.txt

echo "=== DONE ==="

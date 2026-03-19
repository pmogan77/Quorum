# Quorum Key-Value Store Lab

## Requirements

-   Docker
-   Maven
-   Bash
-   Python 3
-   Docker

## Setup

From the basic folder, run the following commands:

1.  Build the Docker image

`bash ./build_image.sh`

2.  Generate the lab configuration

`bash ./render_lab.sh`

3.  Start the Kathara lab

`bash ./start_lab.sh`

------------------------------------------------------------------------

## Running the Client

Connect to the client container and navigate to the shared directory:

`cd ../shared`

### Manual Test

Insert a key-value pair:

`python client.py put x 3`

Read the value:

`python client.py get x`

------------------------------------------------------------------------

## Running the YCSB Benchmark

Navigate to the YCSB directory:

`cd ../shared/ycsb`

Load the initial dataset:

```
./bin/ycsb load basic -db site.ycsb.db.quorum.QuorumDB -P workloads/workloada -P quorum-binding/conf/quorum.properties -p recordcount=10 -p fieldcount=1
```

This will populate the distributed key-value store using the quorum
coordinator.

Run the workload:

```
./bin/ycsb run basic -db site.ycsb.db.quorum.QuorumDB -P workloads/workloada -P quorum-binding/conf/quorum.properties -p recordcount=10 -p fieldcount=1 -p operationcount=100
```

Reset metrics after running load:

```
cd ..
python signal_run_start.py
```
This will remove all metrics keys from the Redis server and reset the temporary in-memory metrics dictionaries. 

------------------------------------------------------------------------

## Running Redis

Connect to the Redis server from any node: 

`redis-cli -h 10.0.0.250`

Useful commands:

```
KEYS *

HGETALL <key>

LRANGE <queue> 0 -1

ZRANGE <set> 0 -1 WITHSCORES
```

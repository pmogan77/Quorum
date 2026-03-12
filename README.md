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

bash ./build_image.sh

2.  Generate the lab configuration

bash ./render_lab.sh

3.  Start the Kathara lab

bash ./start_lab.sh

------------------------------------------------------------------------

## Running the Client

Connect to the client container and navigate to the shared directory:

cd ../shared

### Manual Test

Insert a key-value pair:

python client.py put x 3

Read the value:

python client.py get x

------------------------------------------------------------------------

## Running the YCSB Benchmark

Navigate to the YCSB directory:

cd ../shared/ycsb

Load the initial dataset:

./bin/ycsb load basic -db site.ycsb.db.quorum.QuorumDB -P
workloads/workloada -P quorum-binding/conf/quorum.properties -p
recordcount=10 -p fieldcount=1

This will populate the distributed key-value store using the quorum
coordinator.
# Latency Benchmark

This is a simple benchmark designed to measure concurrent read and write
latencies of Cypher queries across a set of Aura databases within an Aura
environment, typically a development or firedrill environment.

It can be run locally within a shell, or within a docker container in the
Aura environment itself.

It is necessary to have the actual neo4j user password to run Cypher queries
against the database clusters, and for this reason we also need that
password combination when configuring a database for use in the benchmark.

> :warning: **This is not production code**: It is very pre-alpha, and was used as a training excercise!

## Usage

Build the benchmark service:

    make build

Start the benchmark service:

    export ENVIRONMENT=firedrillxyz
    make run

Or without make:

    export LISTEN_PORT=8099
    export ENVIRONMENT=firedrillxyz
    ./latency-benchmark-service

And then in another terminal run curl commands to configure the service.

    curl -s -u neo4j:<password> http://localhost:8099/

The above command will output help in text format describing all available
commands. All other commands will output in JSON for easier downstream
processing.

For example:

    curl -s -u neo4j:<password> http://localhost:8099/neo4j/add/123abc00

Will add the database with DBID `123abc00` to the benchmark service.

    curl -s -u neo4j:<password> http://localhost:8099/start

Will start the benchmark.

    curl -s -u neo4j:<password> http://localhost:8099/results

Will dump results.

## Convenient client script

There is a convenient script for running benchmarks based on a pre-defined table
of databases. Create a file called `latency-benchmark.txt` with a format described
in the sample file `latency-benchmark.txt.sample`.

Then run the command: `./latency-benchmark.sh`.
With no arguments it will output client help. With the argument `help` it will output client and server help.

Example usage:

    ./client-benchmark.sh add
    ./client-benchmark.sh show
    ./client-benchmark.sh start
    ./client-benchmark.sh stop
    ./client-benchmark.sh save_all


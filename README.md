# Latency Benchmark

This is a simple benchmark designed to measure concurrent read and write
latencies of Cypher queries accross a set of Aura databases within an Aura
environment, typically a development or firedrill environment.

It can be run locally within a shell, or within a docker container in the
Aura environment itself.

It is necessary to have the actual neo4j user password to run Cypher queries
against the database clusters, and for this reason we also need that
password combination when configuring a database for use in the benchmark.

## Usage

Build the benchmark service:

    make build

Start the benchmark service:

    export ENVIRONMENT=firedrillxyz
    make run

Or without make:

    export LISTEN_PORT=8099
    export ENVIRONMENT=firedrillxyz
    ./latency-benchmark

And then in another terminal run curl commands to configure the service.

    curl -s -u neo4j:<password> http://localhost:8099/

The above command will output help in text format describing all available
commands. All other commands will output in JSON for easier downstream
processing.

For example:

    curl


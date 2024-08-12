#!/bin/bash

# The purpose of this script is to be called after `docker compose up -d` has been run for a given database
# The idea is to block until the database is available to serve requests. Once the database can serve requests,
# the integration tests can be run.
# Therefore, the ports etc are tightly coupled with the compose.yml files under tests/core/engine_adapter/docker/

set -e

if [ -z "$1" ]; then
    echo "USAGE: $0 <engine>"
    exit 1
fi

ENGINE="$1"

function_exists() {
    declare -f -F $1 > /dev/null
    return $?
}

probe_port() {
    while ! nc -z localhost $1; do
        sleep 1
    done
}

clickhouse_ready() {
    probe_port 8123
}

postgres_ready() {
    probe_port 5432
}

mssql_ready() {
    probe_port 1433
}

mysql_ready() {
    probe_port 3306
}

spark_ready() {
    probe_port 15002
}

trino_ready() {
    # Trino has a built-in healthcheck script, just call that
    docker compose -f tests/core/engine_adapter/docker/compose.trino.yaml exec trino /bin/bash -c '/usr/lib/trino/bin/health-check'
}

echo "Waiting for $ENGINE to be ready..."

READINESS_FUNC="${ENGINE}_ready"

# If called with an unimplemented / unsupported engine, just exit
if ! function_exists $READINESS_FUNC ; then
    echo "WARN: $READINESS_FUNC not implemeted; exiting"
    exit 0
fi

EXIT_CODE=1

while [ $EXIT_CODE -ne 0 ]; do
    echo "Checking $ENGINE"
    $READINESS_FUNC && EXIT_CODE=$? || EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "$ENGINE not ready; sleeping"
        sleep 5
    fi
done

echo "$ENGINE is ready!"
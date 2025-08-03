#!/bin/bash

# The purpose of this script is to be called after `docker compose up -d` has been run for a given database
# The idea is to block until the database is available to serve requests. Once the database can serve requests,
# the integration tests can be run.
# Therefore, the ports etc are tightly coupled with the compose.yml files under tests/core/engine_adapter/docker/
#
# Note that if the docker daemon is not running `localhost`, you can set the DOCKER_HOSTNAME environment variable to the
# correct host Docker is running on

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
    HOSTNAME=${DOCKER_HOSTNAME:-localhost}
    echo "Probing '$HOSTNAME' on port $1"
    while ! nc -z $HOSTNAME $1; do
        sleep 1
    done
}

clickhouse_ready() {
    probe_port 8123
}

doris_ready() {
    probe_port 9030
    
    # Check that we have 3 alive backends
    echo "Checking for 3 alive Doris backends..."
    
    while true; do
        echo "Checking Doris backends..."
        # Use docker compose exec to run mysql command inside the fe-01 container
        # Use timeout to prevent hanging, and handle connection errors gracefully
        ALIVE_BACKENDS=$(timeout 10 docker compose -f tests/core/engine_adapter/integration/docker/compose.doris.yaml exec -T fe-01 mysql -uroot -e "show backends \G" 2>/dev/null | grep -c "Alive: true" || echo "0")
        echo "Found $ALIVE_BACKENDS alive backends"
        if [ "$ALIVE_BACKENDS" -ge 3 ]; then
            echo "Doris has 3 or more alive backends"
            break
        fi
        echo "Waiting for more backends to become alive..."
        sleep 5
    done
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
    docker compose -f tests/core/engine_adapter/integration/docker/compose.trino.yaml exec trino /bin/bash -c '/usr/lib/trino/bin/health-check'
}

risingwave_ready() {
    probe_port 4566
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
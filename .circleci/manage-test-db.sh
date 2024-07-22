#!/bin/bash

# The purpose of this script is to create and destroy temporary test databases on the cloud engines
# The idea is that a database is created, the integration tests are run on that database and then the database is dropped
# This allows builds for multiple PR's to run concurrently without the tests clobbering each other and also gives each set of tests a fresh environment

# Note: It is expected that the environment variables defined in 'tests/core/engine_adapter/config.yaml' for each cloud engine are set

set -e

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
    echo "USAGE: $0 <engine> <db_name> <up/down>"
    exit 1
fi

ENGINE="$1"
DB_NAME="$2"
DIRECTION="$3"

function_exists() {
    declare -f -F $1 > /dev/null
    return $?
}

# Snowflake
snowflake_init() {
    echo "Installing Snowflake CLI"
    pip install snowflake-cli-labs
}

snowflake_up() {
    snow sql -q "create database if not exists $1" --temporary-connection
}

snowflake_down() {
    snow sql -q "drop database if exists $1" --temporary-connection
}

# Databricks
databricks_init() {
    echo "Installing Databricks CLI"
    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh || true

    echo "Writing out Databricks CLI config file"
    echo -e "[DEFAULT]\nhost = $DATABRICKS_SERVER_HOSTNAME\ntoken = $DATABRICKS_ACCESS_TOKEN" > ~/.databrickscfg

    # this takes a path like 'sql/protocolv1/o/2934659247569/0723-005339-foobar' and extracts '0723-005339-foobar' from it
    CLUSTER_ID=${DATABRICKS_HTTP_PATH##*/}

    echo "Extracted cluster id: $CLUSTER_ID from '$DATABRICKS_HTTP_PATH'"

    # Note: the cluster doesnt need to be running to create / drop catalogs, but it does need to be running to run the integration tests
    echo "Ensuring cluster is running"
    databricks clusters start $CLUSTER_ID || true
}

databricks_up() {
    databricks catalogs create $1 || true
}

databricks_down() {
    databricks catalogs delete $1 --force || true
}

# Redshift
redshift_init() {
    psql --version
}

redshift_exec() {
    PGPASSWORD=$REDSHIFT_PASSWORD psql -h $REDSHIFT_HOST -p $REDSHIFT_PORT -U $REDSHIFT_USER -c "$1" dev
}

redshift_up() {
    redshift_exec "create database $1"
}

redshift_down() {
    # try to prevent a "database is being accessed by other users" error when running DROP DATABASE
    EXIT_CODE=1
    ATTEMPTS=0
    while [ $EXIT_CODE -ne 0 ] && [ $ATTEMPTS -lt 5 ]; do
        redshift_exec "select pg_terminate_backend(procpid) from pg_stat_activity where datname = '$1'"
        redshift_exec "drop database $1;" && EXIT_CODE=$? || EXIT_CODE=$?
        if [ $EXIT_CODE -ne 0 ]; then
            echo "Unable to drop database; retrying..."
            ATTEMPTS=$((ATTEMPTS + 1))
            sleep 5
        fi
    done
}

# BigQuery
bigquery_init() {
    # Write out the keyfile for the integration tests to pick up
    echo "Writing out keyfile to $BIGQUERY_KEYFILE"
    echo "$BIGQUERY_KEYFILE_CONTENTS" > $BIGQUERY_KEYFILE
}

bigquery_up() {
    echo "BigQuery doesnt support creating databases"
}

bigquery_down() {
    echo "BigQuery doesnt support dropping databases"
}

INIT_FUNC="${ENGINE}_init"
UP_FUNC="${ENGINE}_up"
DOWN_FUNC="${ENGINE}_down"

# If called with an unimplemented / unsupported engine, just exit
if ! function_exists $INIT_FUNC ; then
    echo "WARN: $INIT_FUNC not implemeted; exiting"
    exit 0
fi

echo "Initializing $ENGINE"
$INIT_FUNC

if [ "$DIRECTION" == "up" ]; then
    echo "Creating database $DB_NAME"
    $UP_FUNC $DB_NAME
elif [ "$DIRECTION" == "down" ]; then
    echo "Dropping database $DB_NAME"
    $DOWN_FUNC $DB_NAME
fi

echo "All done"

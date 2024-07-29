#!/bin/bash

# This script is intended to be run by an Ubuntu build agent on CircleCI
# The goal is to install OS-level dependencies that are required before trying to install Python dependencies

set -e

if [ -z "$1" ]; then
    echo "USAGE: $0 <engine>"
    exit 1
fi

ENGINE="$1"

COMMON_DEPENDENCIES="libpq-dev netcat-traditional"
ENGINE_DEPENDENCIES=""
PIP_DEPENDENCIES=""

if [ "$ENGINE" == "spark" ]; then
    ENGINE_DEPENDENCIES="default-jdk"
elif [ "$ENGINE" == "databricks" ]; then
    # databricks-connect version needs to match the target cluster version that the tests are being run against
    PIP_DEPENDENCIES="databricks-connect==15.3.*"
fi

ALL_DEPENDENCIES="$COMMON_DEPENDENCIES $ENGINE_DEPENDENCIES"

echo "Installing OS-level dependencies: $ALL_DEPENDENCIES"

sudo apt-get -y update && sudo apt-get -y install $ALL_DEPENDENCIES

if [ "$PIP_DEPENDENCIES" != "" ]; then
    echo "Installing extra pip dependencies"
    pip install "$PIP_DEPENDENCIES"
fi

echo "All done"
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

if [ "$ENGINE" == "spark" ]; then
    ENGINE_DEPENDENCIES="default-jdk"
fi

ALL_DEPENDENCIES="$COMMON_DEPENDENCIES $ENGINE_DEPENDENCIES"

echo "Installing OS-level dependencies: $ALL_DEPENDENCIES"

sudo apt-get -y update && sudo apt-get -y install $ALL_DEPENDENCIES

echo "All done"
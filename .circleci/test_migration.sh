#!/usr/bin/env bash
set -ex

GATEWAY_NAME="duckdb_persistent"
TMP_DIR=$(mktemp -d)
SUSHI_DIR="$TMP_DIR/sushi"


if [[ -z $(git tag --points-at HEAD) ]]; then
    # If the current commit is not tagged, we need to find the last tag
    LAST_TAG=$(git describe --tags --abbrev=0)
else
    # If the current commit is tagged, we need to find the previous tag
    LAST_TAG=$(git tag --sort=-creatordate | head -n 2 | tail -n 1)
fi

git checkout $LAST_TAG

# Install dependencies from the previous release.
make install-dev

cp -r ./examples/sushi $TMP_DIR

# Run initial plan
pushd $SUSHI_DIR
rm -rf ./data/*
sqlmesh --gateway $GATEWAY_NAME plan --no-prompts --auto-apply
popd

# Switch back to the starting state of the repository
git checkout -

# Install updated dependencies.
make install-dev

# Migrate and make sure the diff is empty
pushd $SUSHI_DIR
sqlmesh --gateway $GATEWAY_NAME migrate
sqlmesh --gateway $GATEWAY_NAME diff prod
popd

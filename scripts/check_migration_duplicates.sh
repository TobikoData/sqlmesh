#!/bin/bash
# Check for duplicate migration version numbers in SQLMesh

# Extract version numbers from migration files and check for duplicates
duplicates=$(ls sqlmesh/migrations/v[0-9][0-9][0-9][0-9]_*.py 2>/dev/null | \
    sed 's/.*\/v\([0-9]\{4\}\)_.*/\1/' | \
    sort | uniq -d)

if [ -n "$duplicates" ]; then
    echo "Error: Duplicate migration version(s) found:" >&2
    for version in $duplicates; do
        echo "  Version v$version appears in:" >&2
        ls sqlmesh/migrations/v${version}_*.py | sed 's/^/    - /' >&2
    done
    exit 1
fi

exit 0

# State

SQLMesh stores information about your project in a state database separate from your main warehouse.

The SQLMesh state database contains:

- Information about every [Model Version](./models/overview.md) in your project (query, loaded intervals, dependencies)
- A list of every [Virtual Data Environment](./environments.md) in the project
- Which model versions are [promoted](./plans.md#plan-application) into each [Virtual Data Environment](./environments.md)
- Information about any [auto restatements](./models/overview.md#auto_restatement_cron) present in your project
- Other metadata about your project such as current SQLMesh / SQLGlot version

The state database is how SQLMesh "remembers" what it's done before so it can compute a minimum set of operations to apply changes instead of rebuilding everything every time. It's also how SQLMesh tracks what historical data has already been backfilled for [incremental models](./models/model_kinds.md#incremental_by_time_range) so you dont need to add branching logic into the model query to handle this.

!!! info "State database performance"

    The workload against the state database is an OLTP workload that requires transaction support in order to work correctly.

    For the best experience, we recommend [Tobiko Cloud](../cloud/cloud_index.md) or databases designed for OLTP workloads such as [PostgreSQL](../integrations/engines/postgres.md).

    Using your warehouse OLAP database to store state is supported for proof-of-concept projects but is not suitable for production and **will** lead to poor performance and consistency.

    For more information on supported state databases, see the [configuration guide](../guides/configuration.md#state-connection).

## Dumping / Loading State

SQLMesh supports dumping the state database to a `.json` file. From there, you can inspect the file with any tool that can read text files. You can also pass the file around and load it back in to a SQLMesh project running elsewhere.

### Dumping state

SQLMesh can dump the state database to a file like so:

```bash
$ sqlmesh state dump -o state.json
Dumping state to 'state.json' from the following connection:

Gateway: dev
State Connection:
├── Type: duckdb
├── Catalog: state
└── Dialect: duckdb

Continue? [y/n]: y

         Dumping versions ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 4/4 • 0:00:00
        Dumping snapshots ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00
     Dumping environments ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
Dumping auto restatements ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 0.0%   • 0/0 • 0:00:00

State dumped successfully to 'state.json'
```

This will produce a file `state.json` in the current directory containing the SQLMesh state.

The state file is a simple `json` file that looks like:

```json
{
    /* UTC timestamp of when the file was produced */
    "timestamp": "2025-03-16 23:09:00+00:00",
    /* Library versions used to produce this state dump file */
    "versions": {
        "schema_version": 76 /* sqlmesh state database schema version */,
        "sqlglot_version": "26.10.1" /* version of SQLGlot used to produce the state file */,
        "sqlmesh_version": "0.165.1" /* version of SQLMesh used to produce the state file */,
        "state_dump_version": 1 /* state dump file format version */
    },
    /* array of objects containing every Snapshot (physical table) tracked by the SQLMesh project */
    "snapshots": [
        { "name": "..." }
    ],
    /* object for every Virtual Data Environment in the project. key = environment name, value = environment details */
    "environments": {
        "prod": {
            "..."
        }
    },
    /* array of objects describing any Auto Restatements present in the project */
    "auto_restatements": [
        { "name": "..." }
    ]
}
```

### Loading state

!!! warning "Back up your state database first!"

    Please ensure you have created an independent backup of your state database in case something goes wrong during the state load.

    SQLMesh tries to wrap the state load in a transaction but some database engines do not support transactions against DDL which means
    a load error has the potential to leave the state database in an inconsistent state.

SQLMesh can load a state file into the state database like so:

```bash
$ sqlmesh state load -i state.json
Loading state from 'state.json' into the following connection:

Gateway: migration
State Connection:
├── Type: duckdb
├── Catalog: state-migration
└── Dialect: duckdb

[WARNING] This destructive operation will delete all existing state against the 'migration' gateway
and replace it with what\'s in the 'state.json' file.
Are you sure? [y/n]: y

State File Information:
├── Creation Timestamp: 2025-03-26 03:26:00+00:00
├── File Version: 1
├── SQLMesh version: 0.165.1
├── SQLMesh migration version: 76
└── SQLGlot version: 26.11.1

         Loading versions ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3   • 0:00:00
        Loading snapshots ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 17/17 • 0:00:00
     Loading environments ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1   • 0:00:00
Loading auto restatements ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1   • 0:00:00

State loaded successfully from 'state.json'
```

Note that the state database structure needs to be present and up to date, so run `sqlmesh migrate` before running `sqlmesh state load` if you get a version mismatch error.

### Specific gateways

If your project has [multiple gateways](../guides/configuration.md#gateways) with different state connections per gateway, you can target the [state_connection](../guides/configuration.md#state-connection) of a specific gateway like so:

```bash
# state dump
$ sqlmesh --gateway <gateway> state dump -o state.json

# state load
$ sqlmesh --gateway <gateway> state load -i state.json
```

## Version Compatibility

When loading state, the state file must have been produced with the same major and minor version of SQLMesh that is being used to load it.

If you attempt to load state with an incompatible version, you will get the following error:

```bash
$ sqlmesh state load -i state.json
...SNIP...

State load failed!
Error: SQLMesh version mismatch. You are running '0.165.1' but the state file was created with '0.164.1'.
Please upgrade/downgrade your SQLMesh version to match the state file before performing the import.
```

### Upgrading a state file

You can upgrade a state file produced by an old SQLMesh version to be compatible with a newer SQLMesh version by:

- Loading it into a local database using the older SQLMesh version
- Installing the newer SQLMesh version
- Running `sqlmesh migrate` to upgrade the state within the local database
- Running `sqlmesh state dump` to dump it back out again. The new dump is now compatible with the newer version of SQLMesh.

Below is an example of how to upgrade a state file created with SQLMesh `0.164.1` to be compatible with SQLMesh `0.165.1`.

First, create and activate a virtual environment to isolate the SQLMesh versions from your main environment:

```bash
$ python -m venv migration-env

$ . ./migration-env/bin/activate

(migration-env)$
```

Install the SQLMesh version compatible with your state file. The correct version to use is printed in the error message, eg `the state file was created with '0.164.1'` means you need to install SQLMesh `0.164.1`:

```bash
(migration-env)$ pip install "sqlmesh==0.164.1"
```

Add a gateway to your `config.yaml` like so:

```yaml
gateways:
  migration:
    connection:
      type: duckdb
      database: ./state-migration.duckdb
```

The goal here is to define just enough config for SQLMesh to be able to use a local database to run the state dump/load commands. SQLMesh still needs to inherit things like the `model_defaults` from your project in order to migrate state correctly which is why we have not used an isolated directory.

!!! warning

    From here on, be sure to specify `--gateway migration` to all SQLMesh commands or you run the risk of accidentally clobbering any state on your main gateway

You can now load your state dump using the same version of SQLMesh it was created with:

```bash
(migration-env)$ sqlmesh --gateway migration migrate

(migration-env)$ sqlmesh --gateway migration state load -i state.json
...
State loaded successfully from 'state.json'
```

Now we have the state loaded, we can upgrade SQLMesh and dump the state from the new version.
The new version was printed in the original error message, eg `You are running '0.165.1'`

To upgrade SQLMesh, simply install the new version:

```bash
(migration-env)$ pip install --upgrade "sqlmesh==0.165.1"
```

Migrate the state to the new version:

```bash
(migration-env)$ sqlmesh --gateway migration migrate
```

And finally, create a new state file which is now compatible with the new SQLMesh version:

```
 (migration-env)$ sqlmesh --gateway migration state dump -o state-migrated.json
```

The `state-migrated.json` file is now compatible with the newer version of SQLMesh.
You can then transfer it to the place you originally needed it and load it in:

```bash
$ sqlmesh state load -i state-migrated.json
...
State loaded successfully from 'state-migrated.json'
```
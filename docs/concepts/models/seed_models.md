# Seed models

A `SEED` is a special kind of model in which data is sourced from a static dataset defined as a CSV file (rather than from a data source accessed via SQL or Python). The CSV files themselves are a part of your SQLMesh project.

Since seeds are also models in SQLMesh, they capitalize on all the same benefits that SQL or Python models provide:

* A physical table is created in the data warehouse, which reflects the contents of the seed's CSV file.
* Seed models can be referenced in downstream models in the same way as other models.
* Changes to CSV files are captured during [planning](../plans.md#plan-application) and versioned using the same [fingerprinting](../architecture/snapshots.md#fingerprinting) mechanism.
* [Environment](../environments.md) isolation also applies to seed models.

Seed models are a good fit for static datasets that change infrequently or not at all. Examples of such datasets include:

* Names of national holidays and their dates
* A static list of identifiers that should be excluded

## Creating a seed model

Similar to [SQL models](./sql_models.md), `SEED` models are defined in files with the `.sql` extension in the `models/` directory of the SQLMesh project.

Use the special kind `SEED` in the `MODEL` definition to indicate that the model is a seed model:

```sql linenums="1" hl_lines="3-5"
MODEL (
  name test_db.national_holidays,
  kind SEED (
    path 'national_holidays.csv'
  )
);
```
The `path` attribute contains the path to the seed's CSV file **relative** to the path of the model's `.sql` file.

The physical table with the seed CSV's content is created using column types inferred by Pandas. Alternatively, you can manually specify the dataset schema as part of the `MODEL` definition:
```sql linenums="1" hl_lines="6 7 8 9"
MODEL (
  name test_db.national_holidays,
  kind SEED (
    path 'national_holidays.csv'
  ),
  columns (
    name VARCHAR,
    date DATE
  )
);
```
**Note:** The dataset schema provided in the definition takes precedence over column names defined in the header of the CSV file. This means that the order in which columns are provided in the `MODEL` definition must match the order of columns in the CSV file.

## Example

In this example, we use the model definition from the previous section saved in the `models/national_holidays.sql` file of the SQLMesh project.

We also add the seed CSV file itself in the `models/` directory as a CSV file named `national_holidays.csv` with the following contents:

```csv linenums="1"
name,date
New Year,2023-01-01
Christmas,2023-12-25
```

When we run the `sqlmesh plan` command, the new seed model is automatically detected:
```bash hl_lines="6-7"
$ sqlmesh plan
======================================================================
Successfully Ran 0 tests against duckdb
----------------------------------------------------------------------
Summary of differences against `prod`:
└── Added Models:
    └── test_db.national_holidays
Models needing backfill (missing dates):
└── test_db.national_holidays: (2023-02-16, 2023-02-16)
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank for the beginning of history:
Apply - Backfill Tables [y/n]: y

All model batches have been executed successfully

test_db.national_holidays ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
```

Applying the plan created a new table `test_db.national_holidays`.

You can now run a custom query against the table with `sqlmesh fetchdf`:
```bash
$ sqlmesh fetchdf "SELECT * FROM test_db.national_holidays"

        name        date
0   New Year  2023-01-01
1  Christmas  2023-12-25
```

Changes to the seed CSV file get picked up when the `sqlmesh plan` command is run:
```bash
$ sqlmesh plan
======================================================================
Successfully Ran 0 tests against duckdb
----------------------------------------------------------------------
Summary of differences against `prod`:
└── Directly Modified:
    └── test_db.national_holidays
---

+++

@@ -1,3 +1,4 @@

 name,date
 New Year,2023-01-01
 Christmas,2023-12-25
+Independence Day,2023-07-04
Directly Modified: test_db.national_holidays
[1] [Breaking] Backfill test_db.national_holidays and indirectly modified children
[2] [Non-breaking] Backfill test_db.national_holidays but not indirectly modified children: 1
Models needing backfill (missing dates):
└── test_db.national_holidays: (2023-02-16, 2023-02-16)
Enter the backfill start date (eg. '1 year', '2020-01-01') or blank for the beginning of history:
Apply - Backfill Tables [y/n]: y

All model batches have been executed successfully

test_db.national_holidays ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00
```

## Pre- and post-statements

Seed models also support pre- and post-statements, which are evaluated before inserting the seed's content and after, respectively.

Below is an example that only involves pre-statements:

```sql linenums="1" hl_lines="8"
MODEL (
  name test_db.national_holidays,
  kind SEED (
    path 'national_holidays.csv'
  )
);

ALTER SESSION SET TIMEZONE = 'UTC';
```

To add post-statements, you should use the special `@INSERT_SEED()` macro to separate pre- and post-statements:

```sql linenums="1" hl_lines="11"
MODEL (
  name test_db.national_holidays,
  kind SEED (
    path 'national_holidays.csv'
  )
);

-- These are pre-statements
ALTER SESSION SET TIMEZONE = 'UTC';

@INSERT_SEED();

-- These are post-statements
ALTER SESSION SET TIMEZONE = 'PST';
```

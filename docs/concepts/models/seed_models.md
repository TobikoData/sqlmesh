# Seed models

Seed is a special kind of model, data for which is sourced from a static dataset defined as a CSV file rather than from a SQL / Python implementation defined by a user.

Since seeds are also models in SQLMesh, they enjoy all the same benefits the latter provides:

* A physical table gets created in the data warehouse which reflects the contents of the seed's CSV file.
* Seed models can be referenced in downstream models the same way as other models.
* Changes to CSV files are captured during [planning](../plans.md#plan-application) and versioned using the same [fingerprinting](../architecture/snapshots.md#fingerprinting) mechanism.
* [Environment](../environments.md) isolation also applies to seed models.

Seed models are a good fit for static datasets that don't change often or at all. Examples of such datasets include:

* Names of national holidays and their dates.
* A static list of identifiers that should be excluded.

## Creating a seed model

Similarly to [SQL models](sql_models.md), seed models are defined in files with the `.sql` extension as part of the `models/` folder of the SQLMesh project. To indicate that the model is a seed model, the special kind `SEED` should be used in the model definition:
```sql linenums="1"
MODEL (
  name test_db.national_holidays,
  kind SEED (
    path 'national_holidays.csv'
  )
);
```
The `path` attribute provided as part of the definition represents a path to the seed's CSV file **relative** to the path of the definition's `.sql` file.

The physical table with the seed's content gets created using column types inferred by Pandas. The dataset schema can be overridden as part of the model definition:
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
**Note:** the dataset schema provided in the definition takes precedence over column names defined in the header of a CSV file. This means that the order in which columns are provided in the model definition must match the order of columns in the CSV file.

## Example

For this example we use the model definition from the previous section and assume it's been saved to the `models/national_holidays.sql` file of the SQLMesh project.

Add the seed's CSV file with name `national_holidays.csv` to the `models/` folder with the following contents:
```csv linenums="1"
name,date
New Year,2023-01-01
Christmas,2023-12-25
```

When running the `sqlmesh plan` command the new model gets automatically detected:
```bash
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

After successful plan application you can now query the table which resulted from model evaluation:
```bash
$ sqlmesh fetchdf "SELECT * FROM test_db.national_holidays"

        name        date
0   New Year  2023-01-01
1  Christmas  2023-12-25
```

Changes to the CSV files get picked up during the subsequent `sqlmesh plan` command:
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

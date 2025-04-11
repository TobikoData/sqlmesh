# SQLMesh and ClickHouse for Rapid, Reliable Data Transformation

Accurate, timely data is mission-critical for today’s businesses. SQLMesh and ClickHouse make it easier to deliver than ever before.

ClickHouse rapidly processes vast amounts of data. With this speed, your team can rapidly detect and respond to changes impacting the business. But that rapid response comes at the cost of increased complexity.

Data systems are shared, so change requires coordination. Coordination is hard, and time pressure makes it even harder.

SQLMesh lets you manage these changes without fear. It ensures that people understand the full impact of changes they make, that they can safely develop code without affecting others, and that the right version of the data is deployed in production at all times.

This post adapts an existing ClickHouse example analysis to demonstrate how SQLMesh lets you maintain fast, efficient data pipelines in dynamic organizations.

--------- Partially drafted "why should I care" section (retained in case anything usable)
Why would someone want to convert an existing `CREATE TABLE` statement and `INSERT` query to a SQLMesh model?

The primary benefits are:

- Fewer opportunities for DDL errors
    - SQLMesh will automatically generate DDL statements (creating tables and inserting the data) for the model.
    - You configure the model’s materialization and loading behavior with metadata in the model file.
    - You only write the SQL query that determines what data should be inserted into the table.
- Simplified change management
    - SQLMesh automatically infers the dependencies among models and derives the implicit DAG. You can view the DAG in the free browser UI.
    - SQLMesh automatically determines column-level lineage, so you can trace the flow of data across individual models. You can view column-level lineage in the free browser UI.
    - When a model is modified, SQLMesh identifies the specific changes that were made and classifies them as breaking (invalidating existing data) or non-breaking before running them in ClickHouse. This prevents inadvertently breaking other people’s pipelines and drastically reduces unneeded recomputation.
- Isolation during development without redoing computations
    - SQLMesh’s Virtual Data Environments allow people to modify or add models in isolation, without affecting production or other people’s in-progress work.
    - Virtual Data Environments work by providing data via views pointing to versioned physical tables. If a physical table exists for a specific model version, it will be reused when safe to do so, preventing unneeded recomputation.
- Orchestration, scheduling, and easy incremental loading
  - SQLMesh automatically orchestrates model execution, inferring the dependencies among models and ensuring that computations run in the correct order.
  - Every SQLMesh model has a user-specified run cadence, such as daily or hourly. SQLMesh’s built-in scheduler tracks and automatically determines whether it is time for a model to run.
  - SQLMesh has extensive support for incremental loading by time range. Its built-in scheduler automatically tracks which time ranges have already been loaded and ensures that only the correct rows are ingested in each run.
- Safe, rapid deployment
    - Because users access data via views, deployment is simply a reference swap that updates the physical table the view points to.
    - Rollbacks are trivial because the view reference can be easily reverted to the previous physical table.

------------

## Background: data transformation

## Transforming ClickHouse Github data with SQLMesh

Like SQLMesh, ClickHouse is an open-source tool that is developed publicly on Github in collaboration with the user community.

ClickHouse’s documentation [includes an example](https://clickhouse.com/docs/en/getting-started/example-datasets/github) that analyzes Github data capturing development activities like creating or modifying ClickHouse code files.

In this post, we adapt part of that example to demonstrate how SQLMesh makes it easy to safely manage change and deploy with confidence.

## Installation

### ClickHouse
To get started, we need a ClickHouse server to execute the analysis. Fortunately, it is easy to download and run a standalone server.

First, we open a CLI terminal and create a new directory `sqlmesh-example` where both ClickHouse and our SQLMesh project files will live.

We then create a `clickhouse` sub-directory and navigate to it:

``` bash
❯ mkdir sqlmesh-example && cd sqlmesh-example
❯ mkdir clickhouse && cd clickhouse
```

We download ClickHouse by executing a shell script from the website:

``` bash
❯ curl https://clickhouse.com/ | sh

  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  2956    0  2956    0     0   9933      0 --:--:-- --:--:-- --:--:--  9919

Will download https://builds.clickhouse.com/master/macos-aarch64/clickhouse into clickhouse

  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100 72.2M  100 72.2M    0     0  22.8M      0  0:00:03  0:00:03 --:--:-- 22.8M

Successfully downloaded the ClickHouse binary, you can run it as:
    ./clickhouse
```

And we start the server:

``` bash
❯ ./clickhouse server

Processing configuration file 'config.xml'.
There is no file 'config.xml', will use embedded config.
2024.11.01 15:23:20.165336 [ 1076497 ] {} <Information> Application: Starting ClickHouse 24.11.1.679 (revision: 54492, git hash: 3ecb02139aca4d7fac4b7323fabae2fe6a6b68e0, build id: <unknown>), PID 44603
2024.11.01 15:23:20.165411 [ 1076497 ] {} <Information> Application: starting up
2024.11.01 15:23:20.165441 [ 1076497 ] {} <Information> Application: OS name: Darwin, version: 24.1.0, architecture: arm64
[...lots of output omitted...]
```

The server is now running with the default host, port, user, and password.

The server process blocks the command line instance we're using, so we need to open a new one to confirm that everything is working.

We query the server with the ClickHouse client, passing our query `SHOW DATABASES` in the `--query` option and the server connection information in other options:

``` bash
❯ cd sqlmesh-example/clickhouse

❯ ./clickhouse client --query "SHOW DATABASES" --host 127.0.0.1 --port 9000 --user default --password ''
INFORMATION_SCHEMA
default
information_schema
system
```

Success! ClickHouse returned a list of the four existing databases.

### SQLMesh

Now let’s install SQLMesh in a Python virtual environment.

We navigate out of the `clickhouse` directory, then create and activate the environment:

``` bash
❯ cd ..
❯ python3 -m venv .venv
❯ source .venv/bin/activate
```

Now we can install SQLMesh, including the add-ons needed to run on ClickHouse and to view SQLMesh assets in a browser UI:

``` bash
❯ pip install "sqlmesh[clickhouse,web]"

Collecting sqlmesh[clickhouse,web]
  Downloading sqlmesh-0.130.1-py3-none-any.whl.metadata (10 kB)
[...lots of output omitted...]
```

Let's confirm it was installed by getting the SQLMesh version:

``` bash
❯ sqlmesh --version
0.130.1
```

## Setup

### Load Github data

We must have data to analyze, so we first load the ClickHouse Github data.

We have collated table creation and insertion code from the [ClickHouse example page](https://clickhouse.com/docs/en/getting-started/example-datasets/github) into a file hosted in a SQLMesh Github repository.

Let’s download that code into the `load-github-data.sql` file:

``` bash
# TODO: update this URL to final location
❯ curl https://raw.githubusercontent.com/TobikoData/sqlmesh-public-assets/refs/heads/trey/ch-github-data/examples/clickhouse_load-git-data.sql > load-github-data.sql
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100    14  100    14    0     0    109      0 --:--:-- --:--:-- --:--:--   109
```

We can now execute that code file with the ClickHouse client. This step takes about 30 seconds over a fast internet connection.

``` bash
❯ ./clickhouse/clickhouse client --queries-file load-github-data.sql --host 127.0.0.1 --port 9000 --user default --password ''
```

Let’s confirm the data was loaded by querying a row from the `git.file_changes` table:

```bash
❯ ./clickhouse/clickhouse client --query "SELECT * FROM git.file_changes LIMIT 1" --host 127.0.0.1 --port 9000 --user default --password '' --vertical

Row 1:
──────
change_type:           Add
path:                  libs/libcommon/README
old_path:
file_extension:
lines_added:           1
lines_deleted:         0
hunks_added:           1
hunks_removed:         0
hunks_changed:         0
commit_hash:           d98b7d731555ae78ce7cdd7c7f4c8f8e302e71aa
author:                Michael Razuvaev
time:                  2008-12-01 11:10:46
commit_message:        Metrica:         Каталоги относящиеся к исходникам перенесены в src/
commit_files_added:    9
commit_files_deleted:  0
commit_files_renamed:  0
commit_files_modified: 0
commit_lines_added:    2043
commit_lines_deleted:  0
commit_hunks_added:    9
commit_hunks_removed:  0
commit_hunks_changed:  0
```

Success! Looks like the `README` file was added in December 2008.

### Configure SQLMesh connection

We now need to configure the connection between SQLMesh and our ClickHouse server.

First, we initialize an empty SQLMesh project template, specifying that `clickhouse` is our project’s default SQL dialect:

``` bash
❯ sqlmesh init clickhouse --template empty
```

Our directory now contains sub-directories to organize project files and a `config.yaml` file for configuring our project. Let's take a look:

``` bash
❯ ls
audits               config.yaml          macros               seeds
clickhouse           load-github-data.sql models               tests
```

We’re now ready to configure a “gateway” containing the ClickHouse connection in the `config.yaml` file.

When we open the file, we can see that the `local` gateway using local DuckDB already exists, along with some project default values:

``` yaml
gateways:
  local:
    connection:
      type: duckdb
      database: db.db

default_gateway: local

model_defaults:
  dialect: clickhouse
  start: 2024-10-31
```

We'll make a new `clickhouse` gateway configuring the connection to ClickHouse. We use a different port than in earlier `client` commands because SQLMesh connects over HTTP.

SQLMesh maintains state data for model versioning and scheduling purposes. ClickHouse cannot be used to store state data, so we also specify a `state_connection` that uses DuckDB:

``` yaml
gateways:
  clickhouse:
    # ClickHouse connection for data
    connection:
      type: clickhouse
      host: 127.0.0.1
      port: 8123
      username: default
      password: ''
    # DuckDB connection for state
    state_connection:
      type: duckdb
      database: db.db

default_gateway: clickhouse

model_defaults:
  dialect: clickhouse
  start: 2024-10-31
```

If we were using a ClickHouse cluster, we could specify its name in the `cluster` key in the connection configuration. SQLMesh would ensure all created objects were visible to the cluster by automatically adding the `ON CLUSTER` clause to DDL statements.

With everything installed and configured, we're ready to use SQLMesh and ClickHouse!

## Creating a SQLMesh project

We demonstrate how SQLMesh organizes data transformation code by adapting a query from the ClickHouse example page.

Before diving in, we will briefly summarize the query and results it returns.

### ClickHouse example query

The query we adapt is one section of a larger analysis of the ClickHouse Github data. That section examines the contributions of different authors to the ClickHouse repository through the lens of individual files.

Specifically, the query [identifies files that both have many lines and few authors](https://clickhouse.com/docs/en/getting-started/example-datasets/github#largest-files-with-lowest-number-of-authors) in the ClickHouse Github repository.

The query contains a sequence of transformation steps, combined into a single query via common tables expressions (CTEs):

``` sql
-- All unique files CTE
WITH unique_files AS (
  SELECT
    old_path AS path,
    max(time) AS last_time,
    2 AS change_type
  FROM git.file_changes
  GROUP BY old_path
  UNION ALL
  SELECT
    path,
    max(time) AS last_time,
    argMax(change_type, time) AS change_type
  FROM git.file_changes
  GROUP BY path
),
-- Current unique files CTE
current_files AS (
  SELECT
    path
  FROM unique_files
  GROUP BY path
  HAVING (argMax(change_type, last_time) != 2) AND (NOT match(path, '(^dbms/)|(^libs/)|(^tests/testflows/)|(^programs/server/store/)'))
  ORDER BY path ASC
)
-- Main query: files with highest (number of lines):(number of authors) ratio
SELECT
  path,
  sum(lines_added) - sum(lines_deleted) AS num_lines,
  uniqExact(author) AS num_authors,
  num_lines / num_authors AS lines_author_ratio
FROM git.file_changes
WHERE path IN (current_files)
GROUP BY path
ORDER BY lines_author_ratio DESC
LIMIT 10
```

The three sequential transformations in the query are:

1. Identify all unique files (first CTE)
2. Identify current unique files that haven't been renamed or deleted (second CTE)
3. Calculate number of lines, number of authors, and their ratio for each file (main query)

The query identifies the following ten files as having the highest number of lines per author:

``` bash
┌─path──────────────────────────────────────────────────────────────────┬─num_lines─┬─num_authors─┬─lines_author_ratio─┐
│ src/Common/ClassificationDictionaries/emotional_dictionary_rus.txt    │    148590 │           1 │             148590 │
│ src/Functions/ClassificationDictionaries/emotional_dictionary_rus.txt │     55533 │           1 │              55533 │
│ src/Functions/ClassificationDictionaries/charset_freq.txt             │     35722 │           1 │              35722 │
│ src/Common/ClassificationDictionaries/charset_freq.txt                │     35722 │           1 │              35722 │
│ tests/integration/test_storage_meilisearch/movies.json                │     19549 │           1 │              19549 │
│ tests/queries/0_stateless/02364_multiSearch_function_family.reference │     12874 │           1 │              12874 │
│ src/Functions/ClassificationDictionaries/programming_freq.txt         │      9434 │           1 │               9434 │
│ src/Common/ClassificationDictionaries/programming_freq.txt            │      9434 │           1 │               9434 │
│ tests/performance/explain_ast.xml                                     │      5911 │           1 │               5911 │
│ src/Analyzer/QueryAnalysisPass.cpp                                    │      5686 │           1 │               5686 │
└───────────────────────────────────────────────────────────────────────┴───────────┴─────────────┴────────────────────┘
```

Most of these are static dictionaries or data, so the example updates the query to only include code files with extensions `.h`, `.cpp`, or `.sql`. We will also make this change during our example below.

It identifies these ten code files as having the highest number of lines per author:

``` bash
┌─path──────────────────────────────────┬─num_lines─┬─num_authors─┬─lines_author_ratio─┐
│ src/Analyzer/QueryAnalysisPass.cpp    │      5686 │           1 │               5686 │
│ src/Analyzer/QueryTreeBuilder.cpp     │       880 │           1 │                880 │
│ src/Planner/Planner.cpp               │       873 │           1 │                873 │
│ src/Backups/RestorerFromBackup.cpp    │       869 │           1 │                869 │
│ utils/memcpy-bench/FastMemcpy.h       │       770 │           1 │                770 │
│ src/Planner/PlannerActionsVisitor.cpp │       765 │           1 │                765 │
│ src/Functions/sphinxstemen.cpp        │       728 │           1 │                728 │
│ src/Planner/PlannerJoinTree.cpp       │       708 │           1 │                708 │
│ src/Planner/PlannerJoins.cpp          │       695 │           1 │                695 │
│ src/Analyzer/QueryNode.h              │       607 │           1 │                607 │
└───────────────────────────────────────┴───────────┴─────────────┴────────────────────┘
```

### Unpacking the query

This query does its job well - why would we want to unpack it into separate steps?

Choosing a structure for transformation steps is a business decision. If the data returned by this query is all that anybody needs, it can remain as is.

However, we can imagine a scenario where the data in the CTE steps is useful to other business units. Unpacking the query allows us to materialize those intermediate steps so they can be used by others.

We now demonstrate how we could unpack this query into three sequential SQLMesh models.

### What is a SQLMesh model?

In SQLMesh, each materialized object in the data warehouse is specified in a “model.” At minimum, a model consists of a name and a single `SELECT` statement.

A model’s name serves as the database identifier for querying its data. For example, an existing model named `git.unique_files` could be queried as `SELECT * FROM git.unique_files`.

The ClickHouse object named `git.unique_files` that SQLMesh created is a view that `SELECT`s from an underlying physical table.

SQLMesh tracks unique model versions and their corresponding physical tables, updating the view to reference the correct table as the project changes over time.

### Creating models

Let’s convert the query's first unique files CTE into a SQLMesh model.

First, we create a new file in the projects `/models` directory. The file may have any name, but using part of the model name is convenient:

``` bash
❯ touch ./models/unique_files.sql
```

After opening the file in a text editor, we add the `MODEL` configuration block containing the model name, followed by a semi-colon. We then add the SQL query, extracted from the CTE:

``` sql
MODEL (
  name git.unique_files
);

SELECT
    old_path AS path,
    max(time) AS last_time,
    2 AS change_type
FROM git.file_changes
GROUP BY old_path
UNION ALL
SELECT
    path,
    max(time) AS last_time,
    argMax(change_type, time) AS change_type
FROM git.file_changes
GROUP BY path;
```

Next, we can create a model for the second transformation step in the current files CTE.

We create a new model file:

``` bash
❯ touch ./models/current_files.sql
```

After opening it in a text editor, we add the `MODEL` block and query, replacing the existing sub-query with a reference to the `git.unique_files` model we just created:

``` sql
MODEL (
  name git.current_files
);

SELECT
    path
FROM git.unique_files -- Model `git.unique_files`
GROUP BY path
HAVING (argMax(change_type, last_time) != 2) AND (NOT match(path, '(^dbms/)|(^libs/)|(^tests/testflows/)|(^programs/server/store/)'))
ORDER BY path ASC;
```

And, finally, we create a model for the main query:

``` bash
❯ touch ./models/file_length_authors.sql
```

We have converted the `current_files` CTE reference to a subquery and removed the main query's `LIMIT` clause:

``` sql
MODEL (
  name git.file_length_authors
);

SELECT
    path,
    sum(lines_added) - sum(lines_deleted) AS num_lines,
    uniqExact(author) AS num_authors,
    num_lines / num_authors AS lines_author_ratio
FROM git.file_changes
WHERE
    -- Converted to subquery of `git.current_files` model
    path IN (SELECT path from git.current_files)
GROUP BY path
ORDER BY lines_author_ratio DESC;
```

## Transforming data with SQLMesh

Now that we have created models, we're ready to use SQLMesh.

As background, SQLMesh is organized by *environments*. The `prod` environment always exists, and new environments can be cheaply created and deleted on demand.

SQLMesh tracks the state of every environment. If a project's files are modified, the `sqlmesh plan` command determines the steps needed to bring an environment's state into alignment with the updated project files. Applying the `plan` executes those steps.

This approach is loosely inspired by the [Terraform](https://www.hashicorp.com/products/terraform) infrastructure management tool.

### Making a plan

In our brand new project, the `prod` environment hasn't been created. Let's create it with a plan.

Before getting started, though, let's confirm that SQLMesh can connect to ClickHouse with the `sqlmesh info` command:

```bash
❯ sqlmesh info

Models: 3
Macros: 0
Data warehouse connection succeeded
State backend connection succeeded
```

Excellent! SQLMesh detected our three models and confirmed our connections to ClickHouse and DuckDB.

We can now run `sqlmesh plan`. Let's walk through its output in pieces.

In the first section, SQLMesh informs us that we are creating the `prod` environment and that it detects three added models:

```bash
❯ sqlmesh plan

`prod` environment will be initialized

Models:
└── Added:
    ├── git.current_files
    ├── git.file_length_authors
    └── git.unique_files
```

The second section reports that all three models have unprocessed time intervals that need to be backfilled. (In this context, we use "backfill" as a generic term for updating or adding to a table's data, including an initial load or full refresh.)

The output shows the missing time interval start and end as 2024-10-31 because it's the default model start date in `config.yaml` and because this document was written on 2024-11-01.

``` bash
Models needing backfill (missing dates):
├── git.current_files: 2024-10-31 - 2024-10-31
├── git.file_length_authors: 2024-10-31 - 2024-10-31
└── git.unique_files: 2024-10-31 - 2024-10-31

Apply - Backfill Tables [y/n]: y
```

The last line beginning with `Apply` is interactive - we enter `y` and press Enter to perform the backfill operation.

The next section of output tracks the backfill operation.

First, each model's physical table is created. Then each model is evaluated, populating the physical tables with data:

```bash
Creating physical tables ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00
All model versions have been created successfully

[1/1] git.unique_files evaluated in 0.01s
[1/1] git.current_files evaluated in 0.01s
[1/1] git.file_length_authors evaluated in 0.01s
Evaluating models ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00

All model batches have been executed successfully
```

The final section confirms that the `prod` environment model views have been updated to reference the new physical tables:

```bash
Virtually Updating 'prod' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully
```

Our models have now been backfilled with transformed data.

### Checking the data

Let's see what SQLMesh did during the plan.

First, what now exists in the `git` database? We can use the `sqlmesh fetchdf` command to query ClickHouse:

```bash
❯ sqlmesh fetchdf "SHOW TABLES FROM git"
                  name
0              commits
1        current_files
2         file_changes
3  file_length_authors
4         line_changes
5         unique_files
```

The first three rows are the tables we initially created and loaded data into. The last three rows are the model views SQLMesh created in the final step of the `plan`.

The versioned physical tables underneath the views are stored in the automatically created `sqlmesh__git` database:

```bash
❯ sqlmesh fetchdf "SHOW TABLES FROM sqlmesh__git"

                                  name
0         git__current_files__86199695
1  git__file_length_authors__415060819
2        git__unique_files__2942122347
```

Let's check the model output. If we query the final model, we see that the results match those in the ClickHouse example page:

```bash
❯ sqlmesh fetchdf "select * from git.file_length_authors limit 10"

                                                path  num_lines  num_authors  lines_author_ratio
0  src/Common/ClassificationDictionaries/emotiona...     148590            1            148590.0
1  src/Functions/ClassificationDictionaries/emoti...      55533            1             55533.0
2  src/Common/ClassificationDictionaries/charset_...      35722            1             35722.0
3  src/Functions/ClassificationDictionaries/chars...      35722            1             35722.0
4  tests/integration/test_storage_meilisearch/mov...      19549            1             19549.0
5  tests/queries/0_stateless/02364_multiSearch_fu...      12874            1             12874.0
6  src/Common/ClassificationDictionaries/programm...       9434            1              9434.0
7  src/Functions/ClassificationDictionaries/progr...       9434            1              9434.0
8                  tests/performance/explain_ast.xml       5911            1              5911.0
9                 src/Analyzer/QueryAnalysisPass.cpp       5686            1              5686.0
```

### Making changes

As the ClickHouse example notes, the query results are mostly text dictionaries and may not be representative of all files in the codebase. Following their lead, let's update our project to exclude those files.

We will exclude files based on their extension (e.g., `.txt`, `.sql`). The exclusion `WHERE` clause will go in the `git.current_files` model, but the necessary `file_extension` column isn't available to the model. Therefore, we must first add it to the upstream `git.unique_files` model.

#### Adding a column

Let's update the `git.unique_files` model to include the `file_extension` column:

```sql
MODEL (
  name git.unique_files
);

SELECT
    old_path AS path,
    max(time) AS last_time,
    2 AS change_type,
    max(file_extension) as file_extension -- New column
FROM git.file_changes
GROUP BY old_path
UNION ALL
SELECT
    path,
    max(time) AS last_time,
    argMax(change_type, time) AS change_type,
    max(file_extension) as file_extension -- New column
FROM git.file_changes
GROUP BY path;
```

Now we can make a new `plan` to see how SQLMesh interpreted the change.

Because we want to test our changes before deploying them to the `prod` environment, we make our plan in a new environment called `dev`:

```bash
sqlmesh plan dev
```

In the output, we will see models in the ClickHouse database `git__dev`. By default, every new environment is isolated to its own database.

In the first part of the output, SQLMesh detected that we directly modified the `git__dev.unique_files` model.

It also detected that the other two models were indirectly modified because they are downstream of the `git__dev.unique_files` model:

```bash
❯ sqlmesh plan dev
New environment `dev` will be created from `prod`

Differences from the `prod` environment:

Models:
├── Directly Modified:
│   └── git__dev.unique_files
└── Indirectly Modified:
    ├── git__dev.file_length_authors
    └── git__dev.current_files
```

We omit the text diff of the modified `git__dev.unique_files` model, jumping to the final section of the output.

This section shows that SQLMesh recognized the change as `Non-breaking` because adding a column doesn't impact existing downstream data.

Only the `git__dev.unique_files` model needs to backfill since the downstream models do not have breaking changes:

```bash
Directly Modified: git__dev.unique_files (Non-breaking)
└── Indirectly Modified Children:
    ├── git__dev.current_files (Indirect Non-breaking)
    └── git__dev.file_length_authors (Indirect Non-breaking)

Models needing backfill (missing dates):
└── git__dev.unique_files: 2024-10-31 - 2024-10-31

Enter the backfill start date (eg. '1 year', '2020-01-01') or blank to backfill from the beginning of history:
Enter the backfill end date (eg. '1 month ago', '2020-01-01') or blank to backfill up until '2024-11-01 00:00:00':
Apply - Backfill Tables [y/n]: y
```

We hit Enter at each of the backfill date prompts, enter `y` at the Apply prompt, and hit Enter once more to apply the plan.

#### Excluding text dictionaries

Now we can update the final `git.current_files` model by adding a `WHERE` clause to exclude text dictionaries:

```sql
MODEL (
  name git.current_files
);

SELECT
    path
FROM git.unique_files
WHERE file_extension IN ('h', 'cpp', 'sql') -- New WHERE clause
GROUP BY path
HAVING (argMax(change_type, last_time) != 2) AND (NOT match(path, '(^dbms/)|(^libs/)|(^tests/testflows/)|(^programs/server/store/)'))
ORDER BY path ASC;
```

Let's make another plan in our `dev` environment to see how SQLMesh interpreted the change.

In the first part of the output, SQLMesh detected that we directly modified the `git__dev.current_files` model.

It also detected that the downstream `git__dev.file_length_authors` model was indirectly modified because it consumes from the `git__dev.current_files` model:

```bash
❯ sqlmesh plan dev
Differences from the `prod` environment:

Models:
├── Directly Modified:
│   └── git__dev.current_files
└── Indirectly Modified:
    └── git__dev.file_length_authors
```

We omit the text diff of the modified `git__dev.current_files` model, jumping to the final section of the output.

This section shows that SQLMesh recognized the change as `Breaking` because modifying a `WHERE` clause may impact existing downstream data.

Both the `git.current_files` and `git.file_length_authors` models need to backfill since they had breaking changes:

```bash
Directly Modified: git__dev.current_files (Breaking)
└── Indirectly Modified Children:
    └── git__dev.file_length_authors (Indirect Breaking)

Models needing backfill (missing dates):
├── git__dev.current_files: 2024-10-31 - 2024-10-31
└── git__dev.file_length_authors: 2024-10-31 - 2024-10-31

Enter the backfill start date (eg. '1 year', '2020-01-01') or blank to backfill from the beginning of history:
Enter the backfill end date (eg. '1 month ago', '2020-01-01') or blank to backfill up until '2024-11-01 00:00:00':
Apply - Backfill Tables [y/n]: y
```

We hit Enter at each of the backfill date prompts, enter `y`, and hit Enter once more to apply the plan.

### Checking the updated models

Let's see how our changes affected the final query results.

If we run the same query as before, only against our `dev` environment, we see that the results now exclude text dictionaries:

```bash
❯ sqlmesh fetchdf "select * from git__dev.file_length_authors limit 10"

                                       path  num_lines  num_authors  lines_author_ratio
0        src/Analyzer/QueryAnalysisPass.cpp       5686            1              5686.0
1        src/Backups/RestorerFromBackup.cpp        869            1               869.0
2           utils/memcpy-bench/FastMemcpy.h        770            1               770.0
3     src/Planner/PlannerActionsVisitor.cpp        765            1               765.0
4              src/Planner/PlannerJoins.cpp        729            1               729.0
5            src/Functions/sphinxstemen.cpp        728            1               728.0
6  src/Parsers/Kusto/ParserKQLOperators.cpp        598            1               598.0
7         utils/memcpy-bench/glibc/dwarf2.h        592            1               592.0
8  src/Common/tests/gtest_interval_tree.cpp        586            1               586.0
9         src/Storages/NamedCollections.cpp        545            1               545.0
```

Note that all files in both this output and the ClickHouse example page's output have a single author (`num_authors` column).

Our results don't quite match the results in the ClickHouse example page. Let's examine some files that are in its output but not in ours:

```bash
❯ sqlmesh fetchdf "select * from git__dev.file_length_authors where path like '%QueryTreeBuilder.cpp' or path like '%Planner.cpp' or path like '%PlannerJoinTree.cpp' or path like '%QueryNode.h'"

                                path  num_lines  num_authors  lines_author_ratio
0  src/Analyzer/QueryTreeBuilder.cpp        881            2          440.500000
1    src/Planner/PlannerJoinTree.cpp        709            2          354.500000
2           src/Analyzer/QueryNode.h        620            2          310.000000
3            src/Planner/Planner.cpp        866            3          288.666667
```

Aha! All of them have more authors than they did when the ClickHouse example was written, decreasing their `lines_author_ratio` significantly.

### Deploying changes

We have made and validated our changes in the `dev` environment, so we're ready to create a `sqlmesh plan` to push them to `prod`.

Our project files contain both of the changes we made in the `dev` environment, so there are two directly modified models:

```bash
❯ sqlmesh plan
Differences from the `prod` environment:

Models:
├── Directly Modified:
│   ├── git.current_files
│   └── git.unique_files
└── Indirectly Modified:
    └── git.file_length_authors
```

Omitting the text diff and breaking change classification, we see that we are not prompted for a backfill operation:

```bash
Directly Modified: git.current_files (Breaking)
└── Indirectly Modified Children:
    └── git.file_length_authors (Indirect Breaking)

Directly Modified: git.unique_files (Non-breaking)
└── Indirectly Modified Children:
    └── git.file_length_authors (Indirect Breaking)

Apply - Virtual Update [y/n]: y
```

We don't need to perform a backfill because the computations were already executed in our `dev` environment.

SQLMesh knows the model versions we're pushing to `prod` are identical to the ones we ran in `dev`, so the computations can be reused.

All SQLMesh needs to do is update the `prod` views to reference the new physical tables created in `dev`. This update is almost instantaneous:

```bash
Virtually Updating 'prod' ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 0:00:00

The target environment has been updated successfully

Virtual Update executed successfully
```

Querying the `prod` view now returns identical results to the `dev` environment:

```bash
❯ sqlmesh fetchdf "select * from git.file_length_authors limit 10"

                                       path  num_lines  num_authors  lines_author_ratio
0        src/Analyzer/QueryAnalysisPass.cpp       5686            1              5686.0
1        src/Backups/RestorerFromBackup.cpp        869            1               869.0
2           utils/memcpy-bench/FastMemcpy.h        770            1               770.0
3     src/Planner/PlannerActionsVisitor.cpp        765            1               765.0
4              src/Planner/PlannerJoins.cpp        729            1               729.0
5            src/Functions/sphinxstemen.cpp        728            1               728.0
6  src/Parsers/Kusto/ParserKQLOperators.cpp        598            1               598.0
7         utils/memcpy-bench/glibc/dwarf2.h        592            1               592.0
8  src/Common/tests/gtest_interval_tree.cpp        586            1               586.0
9         src/Storages/NamedCollections.cpp        545            1               545.0
```

### Running on a cadence

Now that our models are in place, we need to run them on a cadence to ingest new data. That's what the `sqlmesh run` command is for.

When you execute `sqlmesh run`, the SQLMesh scheduler examines each model's `cron` setting to determine whether enough time has elapsed for the model to run again. It then runs those models.

By default, each model's `cron` is daily. You can modify it with the `MODEL` block `cron` parameters, using a textual description like `@daily` or any valid cron string. The minimum `cron` frequency is 5 minutes.

To run models on a cadence, use the Linux cron tool to execute `sqlmesh run` on a schedule. It should execute at least as frequently as your most rapid model `cron`. For example, if the most frequent model is `hourly`, cron should execute at least once an hour.

## Conclusion

ClickHouse is blazing fast, ensuring your data delivers insights in real time.

SQLMesh gives you confidence those insights are correct by providing isolated development environments, determining the full impact of changes you make before executing them, and letting you instantly deploy to production with confidence.

If you’re interested in data, SQL, or just want to chat we’d love to meet you! Please join us in our Slack community!

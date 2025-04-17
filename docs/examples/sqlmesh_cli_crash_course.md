# SQLMesh CLI Crash Course

This doc is designed to get you intimate with 90% of the SQLMesh commands you’ll use to build *and* maintain data pipelines. The goal is after 30 minutes, using SQLMesh becomes muscle memory. This is designed to live on your second monitor or in a side by side window, so you can swiftly copy/paste into your terminal. 

This is *not* an exhaustive list, but it is an earnest one.

## **Development Commands**

You’ll use these commands 80% of the time because this is how you apply code changes.

=== "SQLMesh"

    ```bash
    sqlmesh plan dev
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev
    ```

asdf
  
=== "SQLMesh"

    ```bash
    sqlmesh plan dev --start 2025-01-01 --end now
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev --start 2025-01-01 --end now
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh --debug plan dev
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh --debug plan dev
    ```

asdf
=== "SQLMesh"

    ```bash
    sqlmesh plan dev --auto-apply
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev --auto-apply # - good for iterating faster
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh plan dev --dry-run
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev --dry-run
    ```

asdf
=== "SQLMesh"

    ```bash
    sqlmesh plan dev --diff-rendered
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev --diff-rendered
    ```

asdf
=== "SQLMesh"

    ```bash
    sqlmesh plan dev --no-diff
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev --no-diff
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh plan dev --empty-backfill
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev --empty-backfill
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh plan dev --forward-only
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev --forward-only
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh create_external_models
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh create_external_models
    ```

asdf

## **Run Commands**

=== "SQLMesh"

    ```bash
    sqlmesh run
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh run
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh run dev
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh run dev
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh run --ignore-cron
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh run --ignore-cron
    ```

asdf

- allow_partials

## **Working with Incremental Forward-Only Models**

https://www.loom.com/share/209181d9532d44969313ac0ac23f501f

- codify this especially the commented out steps


## **Validating Changes**

You'll these commands ad hoc to verify your changes are behaving as expected.

=== "SQLMesh"

    ```bash
    sqlmesh render sqlmesh_example.incremental_model 
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh render sqlmesh_example.incremental_model 
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh table_diff ajwioejfioajowe
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh table_diff ajwioejfioajowe
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh fetchdf "select * from tcloud_demo__dev_debug.gsheets_example_v3"
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh fetchdf "select * from tcloud_demo__dev_debug.gsheets_example_v3"
    ```

asdf

=== "SQLMesh"

    ```bash
    sqlmesh create_test demo.stg_payments --query demo.seed_raw_payments "select * from demo.seed_raw_payments limit 5" 
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh create_test demo.stg_payments --query demo.seed_raw_payments "select * from demo.seed_raw_payments limit 5" 
    ```

asdf

## **Miscellaneous**

=== "SQLMesh"

    ```bash
    sqlmesh migrate
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh migrate
    ```

- migrate state schemas

=== "SQLMesh"

    ```bash
    sqlmesh janitor
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh janitor
    ```

- Manually clean up old development schemas outside the automated schedule
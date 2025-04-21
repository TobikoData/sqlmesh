# SQLMesh CLI Crash Course

This doc is designed to get you intimate with a **majority** of the SQLMesh workflows you’ll use to build *and* maintain transformation data pipelines. The goal is after 30 minutes, using SQLMesh becomes muscle memory. 

This is inspired by community observations, face to face conversations, live screenshares, and debugging sessions. This is *not* an exhaustive list but is rooted in lived experience.

You can follow along in this: [open source GitHub repo](https://github.com/sungchun12/sqlmesh-cli-crash-course)

If you're new to how SQLMesh uses virtual data environments, [watch this quick explainer](https://www.loom.com/share/216835d64b3a4d56b2e061fa4bd9ee76?sid=88b3289f-e19b-4ccc-8b88-3faf9d7c9ce3).

!!! important "Put this on your second monitor or in a side by side window to swiftly copy/paste into your terminal."

## **Development Workflow**

You’ll use these commands 80% of the time because this is how you apply code changes. The workflow is as follows:

1. Make changes to your models directly in SQL and python files (pre-made in examples below)
2. Plan the changes in your dev environment
3. Apply the changes to your dev environment
4. Audit the changes (test data quality)
5. Run data diff against prod
6. Apply the changes to prod

### Preview, Apply, and Audit Changes in `dev`

You can make changes quickly and confidently through a simple command:

- Plan the changes in your dev environment.
- Apply the changes to your dev environment by entering `y` at the prompt.
- Audit the changes (test data quality). This happens automatically when you apply the changes to dev.

Note: If you run this without making any changes, SQLMesh will prompt you to make changes or use the `--include-unmodified` flag like this `sqlmesh plan dev --include-unmodified`. We recommend you make changes first before running this command to avoid creating a lot of noise in your dev environment with extraneous views in the virtual layer.

=== "SQLMesh"

    ```bash
    sqlmesh plan dev
    ```

    ```bash
    sqlmesh plan <environment>
    ```

    If you want to move faster, you can add the `--auto-apply` flag to avoid the manual prompt and apply the plan. You should do this when you're familiar with the plan output, and don't need to see tiny changes in the diff output before applying the plan.

    ```bash
    sqlmesh plan <environment> --auto-apply
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev
    ```

    ```bash
    tcloud sqlmesh plan <environment>
    ```

    If you want to move faster, you can add the `--auto-apply` flag to avoid the manual prompt.

    ```bash
    tcloud sqlmesh plan <environment> --auto-apply
    ```

??? "Example Output"
    I made a breaking change to `incremental_model` and `full_model`. 

    - Showed me the models impacted by the changes.
    - Showed me the changes that will be made to the models.
    - Showed me the models that need to be backfilled.
    - Prompted me to apply the changes to `dev`.
    - Showed me the audit failures that raise as warnings.
    - Updated the physical layer to validate the SQL.
    - Executed the model batches by inserting the data into the physical layer.
    - Updated the virtual layer with view pointers to reflect the changes.

    ```bash
    Differences from the `dev` environment:

    Models:
    ├── Directly Modified:
    │   ├── sqlmesh_example__dev.incremental_model
    │   └── sqlmesh_example__dev.full_model
    └── Indirectly Modified:
        └── sqlmesh_example__dev.view_model

    ---                                                                                                                                                                                     
                                                                                                                                                                                            
    +++                                                                                                                                                                                     
                                                                                                                                                                                            
    @@ -9,7 +9,8 @@                                                                                                                                                                         
                                                                                                                                                                                            
     SELECT                                                                                                                                                                                 
       item_id,                                                                                                                                                                             
       COUNT(DISTINCT id) AS num_orders,                                                                                                                                                    
    -  6 AS new_column                                                                                                                                                                      
    +  new_column                                                                                                                                                                           
     FROM sqlmesh_example.incremental_model                                                                                                                                                 
     GROUP BY                                                                                                                                                                               
    -  item_id                                                                                                                                                                              
    +  item_id,                                                                                                                                                                             
    +  new_column                                                                                                                                                                           

    Directly Modified: sqlmesh_example__dev.full_model (Breaking)

    ---                                                                                                                                                                                     
                                                                                                                                                                                            
    +++                                                                                                                                                                                     
                                                                                                                                                                                            
    @@ -15,7 +15,7 @@                                                                                                                                                                       
                                                                                                                                                                                            
       id,                                                                                                                                                                                  
       item_id,                                                                                                                                                                             
       event_date,                                                                                                                                                                          
    -  5 AS new_column                                                                                                                                                                      
    +  7 AS new_column                                                                                                                                                                      
     FROM sqlmesh_example.seed_model                                                                                                                                                        
     WHERE                                                                                                                                                                                  
       event_date BETWEEN @start_date AND @end_date                                                                                                                                         

    Directly Modified: sqlmesh_example__dev.incremental_model (Breaking)
    └── Indirectly Modified Children:
        └── sqlmesh_example__dev.view_model (Indirect Breaking)
    Models needing backfill:
    ├── sqlmesh_example__dev.full_model: [full refresh]
    ├── sqlmesh_example__dev.incremental_model: [2020-01-01 - 2025-04-16]
    └── sqlmesh_example__dev.view_model: [recreate view]
    Apply - Backfill Tables [y/n]: y

    Updating physical layer ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 2/2 • 0:00:00

    ✔ Physical layer updated

    [1/1]  sqlmesh_example__dev.incremental_model               [insert 2020-01-01 - 2025-04-16]                 0.03s   
    Executing model batches ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 0.0% • pending • 0:00:00
    sqlmesh_example__dev.incremental_model .                                                 
    [WARNING] sqlmesh_example__dev.full_model: 'assert_positive_order_ids' audit error: 2 rows failed. Learn more in logs: 
    /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/logs/sqlmesh_2025_04_18_10_33_43.log
    [1/1]  sqlmesh_example__dev.full_model                      [full refresh, audits ❌1]                       0.01s   
    Executing model batches ━━━━━━━━━━━━━╺━━━━━━━━━━━━━━━━━━━━━━━━━━ 33.3% • 1/3 • 0:00:00
    sqlmesh_example__dev.full_model .                                                     
    [WARNING] sqlmesh_example__dev.view_model: 'assert_positive_order_ids' audit error: 2 rows failed. Learn more in logs: 
    /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/logs/sqlmesh_2025_04_18_10_33_43.log
    [1/1]  sqlmesh_example__dev.view_model                      [recreate view, audits ✔2 ❌1]                   0.01s   
    Executing model batches ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00                                                                                                 
                                                                                                                                                                                            
    ✔ Model batches executed

    Updating virtual layer  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00

    ✔ Virtual layer updated
    ```

### Run Data Diff Against Prod

Run data diff against prod. This is a good way to verify the changes are behaving as expected **after** applying them to `dev`.

=== "SQLMesh"

    ```bash
    sqlmesh table_diff prod:dev sqlmesh_example.full_model --show-sample
    ```

    ```bash
    sqlmesh table_diff <environment>:<environment> <model_name> --show-sample
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh table_diff prod:dev sqlmesh_example.full_model --show-sample
    ```

    ```bash
    tcloud sqlmesh table_diff <environment>:<environment> <model_name> --show-sample
    ```

??? "Example Output"
    I compare the `prod` and `dev` environments for `sqlmesh_example.full_model`.

    - Verified environments and models to diff along with the join on grain configured.
    - Showed me schema diffs between the environments.
    - Showed me row count diffs between the environments.
    - Showed me common rows stats between the environments.
    - Showed me sample data differences between the environments.
    - This is where your human judgement comes in to verify the changes are behaving as expected.

    ```sql linenums="1" hl_lines="6"
    -- models/full_model.sql
    MODEL (
      name sqlmesh_example.full_model,
      kind FULL,
      cron '@daily',
      grain item_id, -- grain is optional BUT necessary for table diffs to work correctly. It's your primary key that is unique and not null.
      audits (assert_positive_order_ids),
    );

    SELECT
      item_id,
      COUNT(DISTINCT id) AS num_orders,
      new_column
    FROM
        sqlmesh_example.incremental_model
    GROUP BY item_id, new_column
    ```

    ```bash
    Table Diff
    ├── Model:
    │   └── sqlmesh_example.full_model
    ├── Environment:
    │   ├── Source: prod
    │   └── Target: dev
    ├── Tables:
    │   ├── Source: db.sqlmesh_example.full_model
    │   └── Target: db.sqlmesh_example__dev.full_model
    └── Join On:
        └── item_id

    Schema Diff Between 'PROD' and 'DEV' environments for model 'sqlmesh_example.full_model':
    └── Schemas match


    Row Counts:
    └──  PARTIAL MATCH: 5 rows (100.0%)

    COMMON ROWS column comparison stats:
                pct_match
    num_orders      100.0
    new_column        0.0


    COMMON ROWS sample data differences:
    Column: new_column
    ┏━━━━━━━━━┳━━━━━━┳━━━━━┓
    ┃ item_id ┃ PROD ┃ DEV ┃
    ┡━━━━━━━━━╇━━━━━━╇━━━━━┩
    │ -11     │ 5    │ 7   │
    │ -3      │ 5    │ 7   │
    │ 1       │ 5    │ 7   │
    │ 3       │ 5    │ 7   │
    │ 9       │ 5    │ 7   │
    └─────────┴──────┴─────┘
    ```

### Apply Changes to Prod

!!! warning "Apply the changes to prod"
    This step is recommended [**only in CI/CD**](../integrations/github.md) as best practice. 
    For learning purposes and hot fixes, you can apply the changes to prod by entering `y` at the prompt.

=== "SQLMesh"

    ```bash
    sqlmesh plan 
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan
    ```

??? "Example Output"
    After I feel confident about the changes, I apply them to `prod`.

    - Showed me the models impacted by the changes.
    - Showed me the changes that will be made to the models.
    - Showed me the models that need to be backfilled. None in this case as it was already backfilled earlier in `dev`.
    - Prompted me to apply the changes to `prod`.
    - Showed me physical layer and execution steps are skipped as the changes were already applied to `dev`.
    - Updated the virtual layer with view pointers to reflect the changes.

    ```bash
    Differences from the `prod` environment:

    Models:
    ├── Directly Modified:
    │   ├── sqlmesh_example.full_model
    │   └── sqlmesh_example.incremental_model
    └── Indirectly Modified:
        └── sqlmesh_example.view_model

    ---                                                                                                                                                                                     
                                                                                                                                                                                            
    +++                                                                                                                                                                                     
                                                                                                                                                                                            
    @@ -9,7 +9,8 @@                                                                                                                                                                         
                                                                                                                                                                                            
    SELECT                                                                                                                                                                                 
      item_id,                                                                                                                                                                             
      COUNT(DISTINCT id) AS num_orders,                                                                                                                                                    
    -  5 AS new_column                                                                                                                                                                      
    +  new_column                                                                                                                                                                           
    FROM sqlmesh_example.incremental_model                                                                                                                                                 
    GROUP BY                                                                                                                                                                               
    -  item_id                                                                                                                                                                              
    +  item_id,                                                                                                                                                                             
    +  new_column                                                                                                                                                                           

    Directly Modified: sqlmesh_example.full_model (Breaking)

    ---                                                                                                                                                                                     
                                                                                                                                                                                            
    +++                                                                                                                                                                                     
                                                                                                                                                                                            
    @@ -15,7 +15,7 @@                                                                                                                                                                       
                                                                                                                                                                                            
      id,                                                                                                                                                                                  
      item_id,                                                                                                                                                                             
      event_date,                                                                                                                                                                          
    -  5 AS new_column                                                                                                                                                                      
    +  7 AS new_column                                                                                                                                                                      
    FROM sqlmesh_example.seed_model                                                                                                                                                        
    WHERE                                                                                                                                                                                  
      event_date BETWEEN @start_date AND @end_date                                                                                                                                         

    Directly Modified: sqlmesh_example.incremental_model (Breaking)
    └── Indirectly Modified Children:
        └── sqlmesh_example.view_model (Indirect Breaking)
    Apply - Virtual Update [y/n]: y

    SKIP: No physical layer updates to perform

    SKIP: No model batches to execute

    Updating virtual layer  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00

    ✔ Virtual layer updated
    ```

---

## **Enhanced Testing Workflow**

You'll use these commands to validate your changes are behaving as expected. Audits (data tests) are a great first step, and you'll want to grow from there to feel confident about your pipelines. The workflow is as follows:

1. Create and audit external models outside of SQLMesh's control (ex: data loaded in by Fivetran, Airbyte, etc.)
2. Automatically generate unit tests
3. Ad hoc query the data directly in the CLI
4. Lint your models to catch known syntax errors

---

### Create and Audit External Models

You can automatically parse fully qualified table/view names that are outside of SQLMesh's control (ex: `bigquery-public-data`.`ga4_obfuscated_sample_ecommerce`.`events_20210131`) and create full schemas with data types. These schemas will be used for column level lineage. You can add audits to test data quality. If they fail, SQLMesh prevents downstream models from wastefully running.

=== "SQLMesh"

    ```bash
    sqlmesh create_external_models
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh create_external_models
    ```

??? "Example Output"
    Note: this is an example from a separate Tobiko Cloud project, so you can't following along in the github repo above.

    - Generated external models from the `bigquery-public-data`.`ga4_obfuscated_sample_ecommerce`.`events_20210131` tabled parsed in the model's SQL.
    - I added an audit to the external model to ensure `event_date` is not null.
    - Viewed a plan preview of the changes that will be made for the external model.
  
    ```sql linenums="1" hl_lines="29"  title="models/external_model_example.sql"
    MODEL (
      name tcloud_demo.external_model
    );

    SELECT
      event_date,
      event_timestamp,
      event_name,
      event_params,
      event_previous_timestamp,
      event_value_in_usd,
      event_bundle_sequence_id,
      event_server_timestamp_offset,
      user_id,
      user_pseudo_id,
      privacy_info,
      user_properties,
      user_first_touch_timestamp,
      user_ltv,
      device,
      geo,
      app_info,
      traffic_source,
      stream_id,
      platform,
      event_dimensions,
      ecommerce
    /*   items */
    FROM bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131 -- I fully qualified the external table name and sqlmesh will automatically create the external model
    ```
  
    ```yaml linenums="1" hl_lines="2 3 4" title="external_models.yaml"
    - name: '`bigquery-public-data`.`ga4_obfuscated_sample_ecommerce`.`events_20210131`'
      audits: # I added this audit manually to the external model
        - name: not_null
          columns: "[event_date]"
      columns:
        event_date: STRING
        event_timestamp: INT64
        event_name: STRING
        event_params: ARRAY<STRUCT<key STRING, value STRUCT<string_value STRING, int_value
          INT64, float_value FLOAT64, double_value FLOAT64>>>
        event_previous_timestamp: INT64
        event_value_in_usd: FLOAT64
        event_bundle_sequence_id: INT64
        event_server_timestamp_offset: INT64
        user_id: STRING
        user_pseudo_id: STRING
        privacy_info: STRUCT<analytics_storage INT64, ads_storage INT64, uses_transient_token
          STRING>
        user_properties: ARRAY<STRUCT<key INT64, value STRUCT<string_value INT64, int_value
          INT64, float_value INT64, double_value INT64, set_timestamp_micros INT64>>>
        user_first_touch_timestamp: INT64
        user_ltv: STRUCT<revenue FLOAT64, currency STRING>
        device: STRUCT<category STRING, mobile_brand_name STRING, mobile_model_name STRING,
          mobile_marketing_name STRING, mobile_os_hardware_model INT64, operating_system
          STRING, operating_system_version STRING, vendor_id INT64, advertising_id INT64,
          language STRING, is_limited_ad_tracking STRING, time_zone_offset_seconds INT64,
          web_info STRUCT<browser STRING, browser_version STRING>>
        geo: STRUCT<continent STRING, sub_continent STRING, country STRING, region STRING,
          city STRING, metro STRING>
        app_info: STRUCT<id STRING, version STRING, install_store STRING, firebase_app_id
          STRING, install_source STRING>
        traffic_source: STRUCT<medium STRING, name STRING, source STRING>
        stream_id: INT64
        platform: STRING
        event_dimensions: STRUCT<hostname STRING>
        ecommerce: STRUCT<total_item_quantity INT64, purchase_revenue_in_usd FLOAT64,
          purchase_revenue FLOAT64, refund_value_in_usd FLOAT64, refund_value FLOAT64,
          shipping_value_in_usd FLOAT64, shipping_value FLOAT64, tax_value_in_usd FLOAT64,
          tax_value FLOAT64, unique_items INT64, transaction_id STRING>
        items: ARRAY<STRUCT<item_id STRING, item_name STRING, item_brand STRING, item_variant
          STRING, item_category STRING, item_category2 STRING, item_category3 STRING,
          item_category4 STRING, item_category5 STRING, price_in_usd FLOAT64, price FLOAT64,
          quantity INT64, item_revenue_in_usd FLOAT64, item_revenue FLOAT64, item_refund_in_usd
          FLOAT64, item_refund FLOAT64, coupon STRING, affiliation STRING, location_id
          STRING, item_list_id STRING, item_list_name STRING, item_list_index STRING,
          promotion_id STRING, promotion_name STRING, creative_name STRING, creative_slot
          STRING>>
      gateway: public-demo
    ```

    `tcloud sqlmesh plan dev_sung`

    ```bash
    Differences from the `dev_sung` environment:

    Models:
    └── Metadata Updated:
        └── "bigquery-public-data".ga4_obfuscated_sample_ecommerce__dev_sung.events_20210131

    ---                                                                                                                                                                                                                   
                                                                                                                                                                                                                          
    +++                                                                                                                                                                                                                   
                                                                                                                                                                                                                          
    @@ -29,5 +29,6 @@                                                                                                                                                                                                     
                                                                                                                                                                                                                          
        ecommerce STRUCT<total_item_quantity INT64, purchase_revenue_in_usd FLOAT64, purchase_revenue FLOAT64, refund_value_in_usd FLOAT64, refund_value FLOAT64, shipping_value_in_usd FLOAT64, shipping_value FLOAT64, 
    tax_value_in_usd FLOAT64, tax_value FLOAT64, unique_items INT64, transaction_id STRING>,                                                                                                                              
        items ARRAY<STRUCT<item_id STRING, item_name STRING, item_brand STRING, item_variant STRING, item_category STRING, item_category2 STRING, item_category3 STRING, item_category4 STRING, item_category5 STRING,   
    price_in_usd FLOAT64, price FLOAT64, quantity INT64, item_revenue_in_usd FLOAT64, item_revenue FLOAT64, item_refund_in_usd FLOAT64, item_refund FLOAT64, coupon STRING, affiliation STRING, location_id STRING,       
    item_list_id STRING, item_list_name STRING, item_list_index STRING, promotion_id STRING, promotion_name STRING, creative_name STRING, creative_slot STRING>>                                                          
      ),                                                                                                                                                                                                                 
    +  audits (not_null('columns' = [event_date])),                                                                                                                                                                       
      gateway `public-demo`                                                                                                                                                                                              
    )                                                                                                                                                                                                                    

    Metadata Updated: "bigquery-public-data".ga4_obfuscated_sample_ecommerce__dev_sung.events_20210131
    Models needing backfill:
    └── "bigquery-public-data".ga4_obfuscated_sample_ecommerce__dev_sung.events_20210131: [full refresh]
    Apply - Backfill Tables [y/n]:
    ```

### Automatically Generate Unit Tests

You can ensure business logic is working as expected with static sample data. Unit tests run *before* a plan is applied automatically. This is great for testing complex business logic (ex: `CASE WHEN` conditions) *before* you backfill data. No need to write them manually neither!

=== "SQLMesh"

    ```bash
    sqlmesh create_test sqlmesh_example.full_model \
      --query sqlmesh_example.incremental_model \
      "select * from sqlmesh_example.incremental_model limit 5"
    ```

    ```bash
    sqlmesh create_test <model_name> \
      --query <model_name upstream> \
      "select * from <model_name upstream> limit 5" 
    ```


=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh create_test demo.stg_payments \
      --query demo.seed_raw_payments \
      "select * from demo.seed_raw_payments limit 5" 
    ```

    ```bash
    tcloud sqlmesh create_test <model_name> \
      --query <model_name upstream> \
      "select * from <model_name upstream> limit 5" 
    ```

??? "Example Output"
    - Generated unit tests for the `sqlmesh_example.full_model` model live querying the data.
    - I ran the tests and they passed locally.
    - If you're using a cloud data warehouse, this will transpile your SQL syntax to its equivalent in duckdb.
    - This runs fast and free on your local machine.
  
    ```yaml linenums="1" title="tests/test_full_model.yaml"
    test_full_model:
      model: '"db"."sqlmesh_example"."full_model"'
      inputs:
        '"db"."sqlmesh_example"."incremental_model"':
        - id: -11
          item_id: -11
          event_date: 2020-01-01
          new_column: 7
        - id: 1
          item_id: 1
          event_date: 2020-01-01
          new_column: 7
        - id: 3
          item_id: 3
          event_date: 2020-01-03
          new_column: 7
        - id: 4
          item_id: 1
          event_date: 2020-01-04
          new_column: 7
        - id: 5
          item_id: 1
          event_date: 2020-01-05
          new_column: 7
      outputs:
        query:
        - item_id: 3
          num_orders: 1
          new_column: 7
        - item_id: 1
          num_orders: 3
          new_column: 7
        - item_id: -11
          num_orders: 1
          new_column: 7
    ```

    ```bash
    (demo) ➜  demo git:(main) ✗ sqlmesh test                                    
    .
    ----------------------------------------------------------------------
    Ran 1 test in 0.053s 

    OK
    ```

    ```bash
    # what if the test fails?
    (demo) ➜  demo git:(main) ✗ sqlmesh test                                    
    F
    ======================================================================
    FAIL: test_full_model (/Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/tests/test_full_model.yaml)
    None
    ----------------------------------------------------------------------
    AssertionError: Data mismatch (exp: expected, act: actual)

      new_column     
            exp  act
    0        0.0  7.0

    ----------------------------------------------------------------------
    Ran 1 test in 0.020s 

    FAILED (failures=1)
    ```

### Run Ad Hoc Queries

You can run live queries directly from the CLI. This is great to validate the look and feel of your changes without context switching to your query console.

Pro tip: run this after running `sqlmesh table_diff` to get a full picture of your changes.

=== "SQLMesh"

    ```bash
    sqlmesh fetchdf "select * from sqlmesh_example__dev.full_model limit 5"
    ```

    ```bash
    # construct arbitrary query
    sqlmesh fetchdf "select * from <schema__environment>.<model_name> limit 5" # double underscore is important. Not needed for prod.
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh fetchdf "select * from sqlmesh_example__dev.full_model limit 5"
    ```

    ```bash
    # construct arbitrary query
    tcloud sqlmesh fetchdf "select * from <schema__environment>.<model_name> limit 5" # double underscore is important. Not needed for prod.
    ```

??? "Example Output"
    ```bash
    item_id  num_orders  new_column
    0        9           1           7
    1      -11           1           7
    2        3           1           7
    3       -3           1           7
    4        1           4           7
    ```

### Linting

If enabled, this will run automatically during development. These can be overridden per model too.
This is great to catch issues before wasting runtime in your data warehouse. You can run it manually to check for any issues.

=== "SQLMesh"

    ```bash
    sqlmesh lint
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh lint
    ```

??? "Example Output"

    You add linting rules in your `config.yaml` file.

    ```yaml linenums="1" hl_lines="13-17" title="config.yaml"
    gateways:
      duckdb:
        connection:
          type: duckdb
          database: db.db

    default_gateway: duckdb

    model_defaults:
      dialect: duckdb
      start: 2025-03-26

    linter:
      enabled: true
      rules: ["ambiguousorinvalidcolumn", "invalidselectstarexpansion"] # raise errors for these rules
      warn_rules: ["noselectstar", "nomissingaudits"]
      # ignored_rules: ["noselectstar"]
    ```

    ```bash
    [WARNING] Linter warnings for /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/lint_warn.sql:
    - noselectstar: Query should not contain SELECT * on its outer most projections, even if it can be 
    expanded.
    - nomissingaudits: Model `audits` must be configured to test data quality.
    [WARNING] Linter warnings for 
    /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/incremental_by_partition.sql:
    - nomissingaudits: Model `audits` must be configured to test data quality.
    [WARNING] Linter warnings for /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/seed_model.sql:
    - nomissingaudits: Model `audits` must be configured to test data quality.
    [WARNING] Linter warnings for 
    /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/incremental_by_unique_key.sql:
    - nomissingaudits: Model `audits` must be configured to test data quality.
    [WARNING] Linter warnings for 
    /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/incremental_model.sql:
    - nomissingaudits: Model `audits` must be configured to test data quality.
    ```

## **Debugging Workflow**

You'll use these commands ad hoc to validate your changes are behaving as expected. This is great to get more details beyond the defaults above. The workflow is as follows:

1. Render the model to verify the SQL is looking as expected
2. Run in verbose mode to verify SQLMesh's behavior.
3. View the logs easily in your terminal.

### Render your SQL Changes

This is great to verify the SQL is looking as expected before applying the changes. This is especially important if you're migrating from another query engine (ex: postgres to databricks).

=== "SQLMesh"

    ```bash
    sqlmesh render sqlmesh_example.incremental_model 
    ```

    ```bash
    sqlmesh render sqlmesh_example.incremental_model --dialect databricks
    ```

    ```bash
    sqlmesh render <model_name> --dialect <dialect>
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh render sqlmesh_example.incremental_model 
    ```

    ```bash
    tcloud sqlmesh render sqlmesh_example.incremental_model --dialect databricks
    ```

    ```bash
    tcloud sqlmesh render <model_name> --dialect <dialect>
    ```

??? "Example Output"

    It outputs the full SQL code in the default or target dialect.

    ```sql hl_lines="10"
    -- rendered sql in default dialect
    SELECT                                                                                                
      "seed_model"."id" AS "id",                                                                          
      "seed_model"."item_id" AS "item_id",                                                                
      "seed_model"."event_date" AS "event_date",                                                          
      7 AS "new_column"                                                                                   
    FROM "db"."sqlmesh__sqlmesh_example"."sqlmesh_example__seed_model__3294646944" AS "seed_model" /*     
    db.sqlmesh_example.seed_model */                                                                      
    WHERE                                                                                                 
      "seed_model"."event_date" <= CAST('1970-01-01' AS DATE) -- placeholder dates for date macros                                             
      AND "seed_model"."event_date" >= CAST('1970-01-01' AS DATE) 
    ```

    ```sql
    -- rendered sql in databricks dialect
    SELECT                                                                                                
      `seed_model`.`id` AS `id`,                                                                          
      `seed_model`.`item_id` AS `item_id`,                                                                
      `seed_model`.`event_date` AS `event_date`,                                                          
      7 AS `new_column`                                                                                   
    FROM `db`.`sqlmesh__sqlmesh_example`.`sqlmesh_example__seed_model__3294646944` AS `seed_model` /*     
    db.sqlmesh_example.seed_model */                                                                      
    WHERE                                                                                                 
      `seed_model`.`event_date` <= CAST('1970-01-01' AS DATE)                                             
      AND `seed_model`.`event_date` >= CAST('1970-01-01' AS DATE)   
    ```

    ```sql linenums="1" title="models/incremental_model.sql"
    MODEL (
      name sqlmesh_example.incremental_model,
      kind INCREMENTAL_BY_TIME_RANGE (
        time_column event_date
      ),
      start '2020-01-01',
      cron '@daily',
      grain (id, event_date)
    );

    SELECT
      id,
      item_id,
      event_date,
      7 as new_column
    FROM
      sqlmesh_example.seed_model
    WHERE
      event_date BETWEEN @start_date AND @end_date
    ```

### Apply Plan Changes in Verbose Mode

You can see detailed operations in the physical and virtual layers. This is useful to see exactly what SQLMesh is doing every step. After, you can copy/paste the fully qualified table/view name into your query console to validate the data (if that's your preference).

=== "SQLMesh"

    ```bash
    sqlmesh plan dev -vv
    ```

    ```bash
    sqlmesh plan <environment> -vv
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev -vv
    ```

    ```bash
    tcloud sqlmesh plan <environment> -vv
    ```

??? "Example Output"

    ```bash hl_lines="47-49"
    [WARNING] Linter warnings for 
    /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/incremental_by_partition.sql:
    - nomissingaudits: Model `audits` must be configured to test data quality.
    [WARNING] Linter warnings for /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/seed_model.sql:
    - nomissingaudits: Model `audits` must be configured to test data quality.
    [WARNING] Linter warnings for 
    /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/incremental_by_unique_key.sql:
    - nomissingaudits: Model `audits` must be configured to test data quality.
    [WARNING] Linter warnings for 
    /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/incremental_model.sql:
    - nomissingaudits: Model `audits` must be configured to test data quality.

    Differences from the `dev` environment:

    Models:
    ├── Directly Modified:
    │   └── db.sqlmesh_example__dev.incremental_model
    └── Indirectly Modified:
        ├── db.sqlmesh_example__dev.full_model
        └── db.sqlmesh_example__dev.view_model

    ---                                                                                                   
                                                                                                          
    +++                                                                                                   
                                                                                                          
    @@ -15,7 +15,7 @@                                                                                     
                                                                                                          
      id,                                                                                                
      item_id,                                                                                           
      event_date,                                                                                        
    -  9 AS new_column                                                                                    
    +  7 AS new_column                                                                                    
    FROM sqlmesh_example.seed_model                                                                      
    WHERE                                                                                                
      event_date BETWEEN @start_date AND @end_date                                                       

    Directly Modified: db.sqlmesh_example__dev.incremental_model (Breaking)
    └── Indirectly Modified Children:
        ├── db.sqlmesh_example__dev.full_model (Breaking)
        └── db.sqlmesh_example__dev.view_model (Indirect Breaking)
    Apply - Virtual Update [y/n]: y

    SKIP: No physical layer updates to perform

    SKIP: No model batches to execute

    db.sqlmesh_example__dev.incremental_model  updated # you'll notice that it's updated vs. promoted because we changed the existing view definition
    db.sqlmesh_example__dev.full_model         updated
    db.sqlmesh_example__dev.view_model         updated
    Updating virtual layer  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00

    ✔ Virtual layer updated
    ```

### View Logs Easily

Each time you perform a SQLMesh command, it creates a log file in the `logs` directory. You can view them by manually navigating to the correct file name with latest timestamp or with this simple shell command. This is useful to see what exact queries were executed to apply your changes. Admittedly, this is outside of native functionality, but it's a quick and easy way to view logs.

```bash
# install this open source tool that enhances the default `cat` command
# https://github.com/sharkdp/bat
brew install bat
```

```bash
bat --theme='ansi' $(ls -t logs/ | head -n 1 | sed 's/^/logs\//')
```

- In simple terms this command works like this: "Show me the contents of the newest log file in the `logs/` directory, with nice formatting and syntax highlighting.”
- press `q` to quit out of big files in the terminal

??? "Example Output"

    This is the log file for the `sqlmesh plan dev` command. If you want to see the log file directly, you can click on the file path in the output directly to open it in your code editor.

    ```bash
    ──────┬──────────────────────────────────────────────────────────────────────────────────────────────
          │ File: logs/sqlmesh_2025_04_18_12_34_35.log
    ──────┼──────────────────────────────────────────────────────────────────────────────────────────────
      1   │ 2025-04-18 12:34:35,715 - MainThread - sqlmesh.core.config.connection - INFO - Creating new D
          │ uckDB adapter for data files: {'db.db'} (connection.py:319)
      2   │ 2025-04-18 12:34:35,951 - MainThread - sqlmesh.core.console - WARNING - Linter warnings for /
          │ Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/incremental_by_partition.sql:
      3   │  - nomissingaudits: Model `audits` must be configured to test data quality. (console.py:1848)
      4   │ 2025-04-18 12:34:35,953 - MainThread - sqlmesh.core.console - WARNING - Linter warnings for /
          │ Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/seed_model.sql:
      5   │  - nomissingaudits: Model `audits` must be configured to test data quality. (console.py:1848)
      6   │ 2025-04-18 12:34:35,953 - MainThread - sqlmesh.core.console - WARNING - Linter warnings for /
          │ Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/incremental_by_unique_key.sql:
      7   │  - nomissingaudits: Model `audits` must be configured to test data quality. (console.py:1848)
      8   │ 2025-04-18 12:34:35,953 - MainThread - sqlmesh.core.console - WARNING - Linter warnings for /
          │ Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/models/incremental_model.sql:
      9   │  - nomissingaudits: Model `audits` must be configured to test data quality. (console.py:1848)
      10  │ 2025-04-18 12:34:35,954 - MainThread - sqlmesh.core.config.connection - INFO - Using existing
          │  DuckDB adapter due to overlapping data file: db.db (connection.py:309)
      11  │ 2025-04-18 12:34:37,071 - MainThread - sqlmesh.core.snapshot.evaluator - INFO - Listing data 
          │ objects in schema db.sqlmesh__sqlmesh_example (evaluator.py:338)
      12  │ 2025-04-18 12:34:37,072 - MainThread - sqlmesh.core.engine_adapter.base - INFO - Executing SQ
          │ L: SELECT CURRENT_CATALOG() (base.py:2128)
      13  │ 2025-04-18 12:34:37,072 - MainThread - sqlmesh.core.engine_adapter.base - INFO - Executing SQ
          │ L: SELECT CURRENT_CATALOG() (base.py:2128)
    ```

## **Run on Production Schedule**

SQLMesh schedules your transformation on a per-model basis in proper DAG order. This makes it easy to configure how often each step in your pipeline runs to backfill data without running when upstream models are late or failed. Rerunning from point of failure is also a default! Example below:

`stg_transactions`(cron: `@hourly`) -> `fct_transcations`(cron: `@daily`). All times in UTC.

1. `stg_transactions` runs hourly
2. `fct_transcations` runs at 12am UTC if `stg_transactions` is fresh and updated since its most recent hour interval
3. If `stg_transactions` failed from 11pm-11:59:59pm, it will prevent `fct_transcations` from running and put it in a `pending` state
4. If `fct_transactions` is `pending` past its full interval (1 full day), it will be put in a `late` state
5. Once `stg_transactions` runs successfully either from a retry or a fix from a pull request, `fct_transactions` will rerun from the point of failure. This is true even if `fct_transactions` has been `late` for several days.

If you're using open source SQLMesh, you can run this command in your orchestrator (ex: Dagster, GitHub Actions, etc.) every 5 minutes or at your lowest model cron schedule (ex: every 1 hour). Don't worry! It will only run executions that need to be run.

If you're using Tobiko Cloud, this configures automatically without additional configuration.

### Run Models

This command is intended be run on a schedule. It will skip the physical and virtual layer updates and simply execute the model batches. 

=== "SQLMesh"

    ```bash
    sqlmesh run
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh run
    ```

??? "Example Output"

    This is what it looks like if models are ready to run.

    ```bash
    [1/1] sqlmesh_example.incremental_model               [insert 2025-04-17 - 2025-04-17]                
    0.01s   
    [1/1] sqlmesh_example.incremental_unique_model        [insert/update rows]                            
    0.01s   
    [1/1] sqlmesh_example_v3.incremental_partition_model  [insert partitions]                             
    0.01s   
    Executing model batches ━━━━━━━━━━━━━━━━╺━━━━━━━━━━━━━━━━━━━━━━━ 40.0% • 2/5 • 0:00:00
    sqlmesh_example_v3.incremental_partition_model .                                      
    [WARNING] sqlmesh_example.full_model: 'assert_positive_order_ids' audit error: 2 rows failed. Learn 
    more in logs: /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/logs/sqlmesh_2025_04_18_12_48_35.log
    [1/1] sqlmesh_example.full_model                      [full refresh, audits ❌1]                      
    0.01s   
    Executing model batches ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╺━━━━━━━ 80.0% • 4/5 • 0:00:00
    sqlmesh_example.view_model .                                                          
    [WARNING] sqlmesh_example.view_model: 'assert_positive_order_ids' audit error: 2 rows failed. Learn 
    more in logs: /Users/sung/Desktop/git_repos/sqlmesh-cli-revamp/logs/sqlmesh_2025_04_18_12_48_35.log
    [1/1] sqlmesh_example.view_model                      [recreate view, audits ✔2 ❌1]                  
    0.01s   
    Executing model batches ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 5/5 • 0:00:00               
                                                                                                          
    ✔ Model batches executed

    Run finished for environment 'prod'
    ```

    This is what it looks like if no models are ready to run.

    ```bash
    No models are ready to run. Please wait until a model `cron` interval has elapsed.

    Next run will be ready at 2025-04-18 05:00PM PDT (2025-04-19 12:00AM UTC).
    ```

### Run Models with Incomplete Intervals (Warning)

You can run models that execute backfills each time you invoke a run whether ad hoc or on a schedule.

!!! warning "Run Models with Incomplete Intervals"
    This only applies to incremental models that have `allow_partials` set to `true`. 
    This is generally not recommended for production environments as you risk shipping incomplete data which will be perceived as broken data.

=== "SQLMesh"

    ```bash
    sqlmesh run --ignore-cron
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh run --ignore-cron
    ```

??? "Example Output"

    ```sql linenums="1" hl_lines="15" title="models/incremental_model.sql"
    MODEL (
      name sqlmesh_example.incremental_model,
      kind INCREMENTAL_BY_TIME_RANGE (
        time_column event_date
      ),
      start '2020-01-01',
      cron '@daily',
      grain (id, event_date),
      audits( UNIQUE_VALUES(columns = (
          id,
      )), NOT_NULL(columns = (
          id,
          event_date
      ))),
      allow_partials true
    );

    SELECT
      id,
      item_id,
      event_date,
      16 as new_column
    FROM
      sqlmesh_example.seed_model
    WHERE
      event_date BETWEEN @start_date AND @end_date
    ```

    ```bash
    [1/1] sqlmesh_example.incremental_model  [insert 2025-04-19 - 2025-04-19, audits ✔2] 0.05s   
    Executing model batches ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 1/1 • 0:00:00         
                                                                                                    
    ✔ Model batches executed

    Run finished for environment 'prod'
    ```

## **Forward-Only Development Workflow**

This is an advanced workflow and specifically designed for large incremental models (ex: > 200 million rows) that take a long time to run even during development. It solves for:

- Transforming data with schema evolution in `struct` and nested `array` data types.
- Retaining history of a calculated column and applying a new calculation to new rows going forward.
- Retain history of a column with complex conditional `CASE WHEN` logic and apply new conditions to new rows going forward.

When you apply the plan to `prod` after the dev worfklow, it will NOT backfill historical data. It will only execute model batches **forward only** for new intervals (new rows).

If you want to see a full walkthough, [go here](incremental_time_full_walkthrough.md). 

=== "SQLMesh"

    ```bash
    sqlmesh plan dev --forward-only
    ```

    ```bash
    sqlmesh plan <environment> --forward-only
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh plan dev --forward-only
    ```

    ```bash
    tcloud sqlmesh plan <environment> --forward-only
    ```

??? "Example Output"

    - I applied a change to a new column
    - It impacts 2 downstream models
    - I enforced a forward-only plan to avoid backfilling historical data for the incremental model (ex: `preview` language in the CLI output)
    - I previewed the changes in a clone of the incremental impacted (clones will NOT be reused in production) along with the full and view models (these are NOT clones).

    ```bash
    Differences from the `dev` environment:

    Models:
    ├── Directly Modified:
    │   └── sqlmesh_example__dev.incremental_model
    └── Indirectly Modified:
        ├── sqlmesh_example__dev.view_model
        └── sqlmesh_example__dev.full_model

    ---                                                                                                   
                                                                                                          
    +++                                                                                                   
                                                                                                          
    @@ -16,7 +16,7 @@                                                                                     
                                                                                                          
      id,                                                                                                
      item_id,                                                                                           
      event_date,                                                                                        
    -  9 AS new_column                                                                                    
    +  10 AS new_column                                                                                   
    FROM sqlmesh_example.seed_model                                                                      
    WHERE                                                                                                
      event_date BETWEEN @start_date AND @end_date                                                       

    Directly Modified: sqlmesh_example__dev.incremental_model (Forward-only)
    └── Indirectly Modified Children:
        ├── sqlmesh_example__dev.full_model (Forward-only)
        └── sqlmesh_example__dev.view_model (Forward-only)
    Models needing backfill:
    ├── sqlmesh_example__dev.full_model: [full refresh] (preview)
    ├── sqlmesh_example__dev.incremental_model: [2025-04-17 - 2025-04-17] (preview)
    └── sqlmesh_example__dev.view_model: [recreate view] (preview)
    Apply - Preview Tables [y/n]: y

    Updating physical layer ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00

    ✔ Physical layer updated

    [1/1] sqlmesh_example__dev.incremental_model  [insert 2025-04-17 - 2025-04-17]                0.01s   
    [1/1] sqlmesh_example__dev.full_model         [full refresh, audits ✔1]                       0.01s   
    [1/1] sqlmesh_example__dev.view_model         [recreate view, audits ✔3]                      0.01s   
    Executing model batches ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00               
                                                                                                          
    ✔ Model batches executed

    Updating virtual layer  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00

    ✔ Virtual layer updated
    ```


    `sqlmesh plan`

    When this is applied to `prod`, it will only execute model batches for new intervals (new rows). This will NOT re-use `preview` models (backfilled data) in development.

    ```bash
    Differences from the `prod` environment:

    Models:
    ├── Directly Modified:
    │   └── sqlmesh_example.incremental_model
    └── Indirectly Modified:
        ├── sqlmesh_example.view_model
        └── sqlmesh_example.full_model

    ---                                                                                                   
                                                                                                          
    +++                                                                                                   
                                                                                                          
    @@ -9,13 +9,14 @@                                                                                     
                                                                                                          
        disable_restatement FALSE,                                                                       
        on_destructive_change 'ERROR'                                                                    
      ),                                                                                                 
    -  grains ((id, event_date))                                                                          
    +  grains ((id, event_date)),                                                                         
    +  allow_partials TRUE                                                                                
    )                                                                                                    
    SELECT                                                                                               
      id,                                                                                                
      item_id,                                                                                           
      event_date,                                                                                        
    -  7 AS new_column                                                                                    
    +  10 AS new_column                                                                                   
    FROM sqlmesh_example.seed_model                                                                      
    WHERE                                                                                                
      event_date BETWEEN @start_date AND @end_date                                                       

    Directly Modified: sqlmesh_example.incremental_model (Forward-only)
    └── Indirectly Modified Children:
        ├── sqlmesh_example.full_model (Forward-only)
        └── sqlmesh_example.view_model (Forward-only)
    Apply - Virtual Update [y/n]: y

    SKIP: No physical layer updates to perform

    SKIP: No model batches to execute

    Updating virtual layer  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100.0% • 3/3 • 0:00:00

    ✔ Virtual layer updated
    ```


## **Miscellaneous**

If you notice you have a lot of old development schemas/data, you can clean them up with the following command. This process runs automatically during the `sqlmesh run` command. This defaults to deleting old data past 7 days.

=== "SQLMesh"

    ```bash
    sqlmesh janitor
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh janitor
    ```
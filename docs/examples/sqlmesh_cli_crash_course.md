# SQLMesh CLI Crash Course

This doc is designed to get you intimate with a **majority** of the SQLMesh commands you’ll use to build *and* maintain data pipelines. The goal is after 30 minutes, using SQLMesh becomes muscle memory. This is designed to live on your second monitor or in a side by side window, so you can swiftly copy/paste into your terminal. 

This is designed based on community observations, face to face conversations, live screenshares, and debugging sessions. This is *not* an exhaustive list, but it is an earnest one.

You can follow along in this: [open source github repo](https://github.com/sungchun12/sqlmesh-cli-crash-course)

## **Development Workflow**

You’ll use these commands 80% of the time because this is how you apply code changes. The workflow is as follows:

1. Make changes to your models directly in SQL and python files (pre-made in examples below)
2. Plan the changes in your dev environment
3. Apply the changes to your dev environment
4. Audit the changes (test data quality)
5. Run data diff against prod
6. Apply the changes to prod

---

All these steps are bundled into a single command below:

- Plan the changes in your dev environment.
- Apply the changes to your dev environment by entering `y` at the prompt.
- Audit the changes (test data quality). This happens automatically when you apply the changes to dev.

=== "SQLMesh"

    ```bash
    sqlmesh plan dev
    ```

    ```bash
    sqlmesh plan <environment>
    ```

    If you want to move faster, you can add the `--auto-apply` flag to avoid the manual prompt.

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

    ```sql
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

!!! warning "Apply the changes to prod"
    This step is recommended **only in CI/CD** as best practice. # TODO: link to github cicd bot setup
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

You'll use these commands ad hoc to validate your changes are behaving as expected. Audits (data tests) are a great first step, and you'll want to evolve into to feel confident about the changes. The workflow is as follows:

1. Create external models outside of SQLMesh's control (ex: data loaded in by Fivetran, Airbyte, etc.)
2. Automatically generate unit tests
3. Ad hoc query the data directly in the CLI


=== "SQLMesh"

    ```bash
    sqlmesh create_external_models
    ```

=== "Tobiko Cloud"

    ```bash
    tcloud sqlmesh create_external_models
    ```

## **Debugging Workflow**

You'll use these commands ad hoc to validate your changes are behaving as expected. Audits (data tests) are a great first step, and you'll want to evolve into to feel confident about the changes. The workflow is as follows:

1. Render the model to verify the SQL is looking as expected
2. Run in debug mode to verify SQLMesh's behavior.


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

Fun for the whole family:

```bash
brew install bat
```

```bash
bat --theme='ansi' $(ls -t logs/ | head -n 1 | sed 's/^/logs\//')
```

- view logs
- a special treat for you ;)
- https://github.com/sharkdp/bat
- In simple terms: "Show me the contents of the newest log file in the logs directory, with nice formatting and syntax highlighting.”
- `q` to quit out of big files in the terminal

- Manually clean up old development schemas outside the automated schedule
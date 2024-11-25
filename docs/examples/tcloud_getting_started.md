# Tobiko Cloud: Getting Started

Tobiko Cloud is a data platform that extends SQLMesh to make it easy to manage data at scale. We're here to make it easy to get started and feel confident that everything is working as expected. After you've completed the steps below, you'll have achieved the following:

- Login to Tobiko Cloud via the browser
- Connect Tobiko Cloud to your local machine via the CLI
- Connect Tobiko Cloud to your data warehouse
- Verify Tobiko Cloud interacts with your data warehouse as expected


## Steps in Order:

[ ]  Shared external slack channel
    - Solutions Architect will set this up
  
- SQLMesh to Tobiko Cloud Migrations Only
    - [ ]  Tobiko Engineering to validate current state. SA on account to send them [this](https://github.com/TobikoData/sqlmesh-enterprise/tree/main/scripts/onboarding_state_migration) example script to run to get a dump of the current state for us to test. (Need to create more scripts to handle other databases)
    - Tag Ryan Eakman as needed
    - [ ]  After testing, schedule a migration date and call to move their state over. Remember there will be some downtime. Complete state migration at this step.
- [ ]  Setup 1 hour meeting to fully onboard (be prepared for random errors to debug real time)
- [ ]  Login to Tobiko Cloud via the browser
    - [ ]  Ask Tobiko Cloud engineering team to create a new account (single tenant by default)
    - [ ]  Get Tobiko Cloud engineering team to share a 1pass link that expires in 7 days
    - [ ]  Make sure customer saves the password and let them know the link will expire
    - Password only, leave the username blank
- [ ]  Connect Tobiko Cloud to local computer (ex: via terminal and CLI)
- Create this file to then download the tcloud CLI
- open this up in VS Code (or your IDE)

```bash
# requirements.txt
# source: https://pypi.org/project/tcloud/
tcloud==1.2.0
```

```bash
# run this in your terminal
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# verify the python version is 3.10 is above
```

- [ ]  Connect Tobiko Cloud to Data Warehouse (ex: bigquery, snowflake, databricks, etc.)
    
    ```yaml
    # tcloud.yaml
    projects:
        public-demo: # TODO: update this for the project name in the URL
            url: https://cloud.tobikodata.com/sqlmesh/tobiko/public-demo/ # TODO: update for your unique URL
            gateway: tobiko_cloud
            extras: bigquery,web,github  # TODO: update bigquery for your data warehouse
    default_project: public-demo # TODO: update
    
    # export TCLOUD_TOKEN=<your token> in your shell and as a secret in github actions
    # you can alias the tcloud cli in your shell for UX familiarity: alias sqlmesh='tcloud sqlmesh'
    ```
    
    ```yaml
    # config.yaml
    
    gateways:
        tobiko_cloud: # this will use the config in tcloud.yaml for state_connection
            connection: #TODO: update for your data warehouse configs: https://sqlmesh.readthedocs.io/en/stable/integrations/overview/
                type: bigquery
                method: service-account-json
                concurrent_tasks: 5
                register_comments: true
                keyfile_json: {{ env_var('GOOGLE_SQLMESH_CREDENTIALS') }}
                project: sqlmesh-public-demo
    
    default_gateway: tobiko_cloud
    
    model_defaults:
        dialect: bigquery #TODO: update for your data warehouse
    
    # make Tobiko Cloud only allow deploying to dev environments, use env var to override in CI/CD
    # allow_prod_deploy: {{ env_var('ALLOW_PROD_DEPLOY', 'false') }}
    
    # enables synchronized deployments to prod when a PR is merged
    cicd_bot:
        type: github
        merge_method: squash
        enable_deploy_command: true
        auto_categorize_changes:
          external: full
          python: full
          sql: full
          seed: full
    
    plan:
      enable_preview: true
    
    ```
    
    - SQL Server Example
        
        ```yaml
        gateways:
          mssql_gateway:
            connection:
              type: mssql # pip install "sqlmesh[mssql]" if you haven't already
              host: {{ env_var("MSSQL_HOST") }} # in your windows terminal, run "set MSSQL_HOST=your_mssql_host"
              port: {{ env_var("MSSQL_PORT") }} # in your windows terminal, run "set MSSQL_PORT=your_mssql_port"
              user: {{ env_var("MSSQL_USERNAME") }} # in your windows terminal, run "set MSSQL_USERNAME=your_mssql_username"
              password: {{ env_var("MSSQL_PASSWORD") }} # in your windows terminal, run "set MSSQL_PASSWORD=your_mssql_password"
              database: {{ env_var("MSSQL_DATABASE") }} # in your windows terminal, run "set MSSQL_DATABASE=your_mssql_database"
              conn_properties:
        
        default_gateway: mssql_gateway
        
        model_defaults:
          dialect: tsql # updated the dialect to tsql for mssql syntax rendering
          start: 2024-08-19 # I recommend updating this to an earlier date representing the historical data you want to backfill
        ```
        
- [ ]  Make sure SQLMesh interacts with all the above as expected

```bash
# run these commands 
alias sqlmesh='tcloud sqlmesh' # familiar CLI UX
sqlmesh init
sqlmesh plan

# you should see success messages and are now fully onboarded!
```

- [ ]  Share helpful links
- [Walkthrough Example](https://sqlmesh.readthedocs.io/en/latest/examples/incremental_time_full_walkthrough/)
- [Quickstart](https://sqlmesh.readthedocs.io/en/stable/quick_start/)
- [Project Guide and getting setup](https://sqlmesh.readthedocs.io/en/stable/guides/projects/)
- [Models Guide](https://sqlmesh.readthedocs.io/en/stable/guides/models/)
- [GitHub Actions CI/CD bot](https://sqlmesh.readthedocs.io/en/stable/integrations/github/)
- [Testing Models](https://sqlmesh.readthedocs.io/en/stable/concepts/tests/)
- [SQLMesh Macros](https://sqlmesh.readthedocs.io/en/stable/concepts/macros/sqlmesh_macros/)
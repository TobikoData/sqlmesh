# BigQuery

## Local/Built-in Scheduler

**Engine Adapter Type**: `bigquery`

### Installation
```
pip install "sqlmesh[bigquery]"
```

### Connection options

| Option                          | Description                                                                                                    |  Type  | Required |
|---------------------------------|----------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`                          | Engine type name - must be `bigquery`                                                                          | string |    Y     |
| `method`                        | Connection methods - see [allowed values below](#connection-methods). Default: `oauth`.                        | string |    N     |
| `project`                       | The name of the GCP project                                                                                    | string |    N     |
| `location`                      | The location of for the datasets (can be regional or multi-regional)                                           | string |    N     |
| `keyfile`                       | Path to the keyfile to be used with service-account method                                                     | string |    N     |
| `keyfile_json`                  | Keyfile information provided inline (not recommended)                                                          |  dict  |    N     |
| `token`                         | OAuth 2.0 access token                                                                                         | string |    N     |
| `refresh_token`                 | OAuth 2.0 refresh token                                                                                        | string |    N     |
| `client_id`                     | OAuth 2.0 client ID                                                                                            | string |    N     |
| `client_secret`                 | OAuth 2.0 client secret                                                                                        | string |    N     |
| `token_uri`                     | OAuth 2.0 authorization server's toke endpoint URI                                                             | string |    N     |
| `scopes`                        | The scopes used to obtain authorization                                                                        |  list  |    N     |
| `job_creation_timeout_seconds`  | The maximum amount of time, in seconds, to wait for the underlying job to be created.                          |  int   |    N     |
| `job_execution_timeout_seconds` | The maximum amount of time, in seconds, to wait for the underlying job to complete.                            |  int   |    N     |
| `job_retries`                   | The number of times to retry the underlying job if it fails. (Default: `1`)                                    |  int   |    N     |
| `priority`                      | The priority of the underlying job. (Default: `INTERACTIVE`)                                                   | string |    N     |
| `maximum_bytes_billed`          | The maximum number of bytes to be billed for the underlying job.                                               |  int   |    N     |

## Airflow Scheduler
**Engine Name:** `bigquery`

In order to share a common implementation across local and Airflow, SQLMesh BigQuery implements its own hook and operator.

### Installation

To enable support for this operator, the Airflow BigQuery provider package should be installed on the target Airflow cluster along with SQLMesh with the BigQuery extra:
```
pip install "apache-airflow-providers-google"
pip install "sqlmesh[bigquery]"
```

### Connection info

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target BigQuery account. Please see [GoogleBaseHook](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/common/hooks/base_google/index.html#airflow.providers.google.common.hooks.base_google.GoogleBaseHook) and [GCP connection](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html)for more details. Use the `sqlmesh_google_cloud_bigquery_default` (by default) connection ID instead of the `google_cloud_default` one in the Airflow guide.

By default, the connection ID is set to `sqlmesh_google_cloud_bigquery_default`, but it can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "bigquery",
    default_catalog="<project id>",
    engine_operator_args={
        "bigquery_conn_id": "<Connection ID>"
    },
)
```

#### Optional Arguments

* `location`: Sets the default location for datasets and tables. If not set, BigQuery defaults to US for new datasets. See `location` in [Connection options](#connection-options) for more details.

```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "bigquery",
    default_catalog="<project id>",
    engine_operator_args={
        "bigquery_conn_id": "<Connection ID>",
        "location": "<location>"
    },
)
```

## Connection Methods
- [oauth](https://google-auth.readthedocs.io/en/master/reference/google.auth.html#google.auth.default) (default)
    - Related Credential Configuration:
        - `scopes` (Optional)
- [oauth-secrets](https://google-auth.readthedocs.io/en/stable/reference/google.oauth2.credentials.html)
    - Related Credential Configuration:
        - `token` (Optional): Can be None if refresh information is provided.
        - `refresh_token` (Optional): If specified, credentials can be refreshed.
        - `client_id` (Optional): Must be specified for refresh, can be left as None if the token can not be refreshed.
        - `client_secret` (Optional): Must be specified for refresh, can be left as None if the token can not be refreshed.
        - `token_uri` (Optional): Must be specified for refresh, can be left as None if the token can not be refreshed.
        - `scopes` (Optional): OAuth 2.0 credentials can not request additional scopes after authorization. The scopes must be derivable from the refresh token if refresh information is provided (e.g. The refresh token scopes are a superset of this or contain a wild card scope like 'https://www.googleapis.com/auth/any-api')
- [service-account](https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html#google.oauth2.service_account.IDTokenCredentials.from_service_account_file)
    - Related Credential Configuration:
        - `keyfile` (Required)
        - `scopes` (Optional)
- [service-account-json](https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html#google.oauth2.service_account.IDTokenCredentials.from_service_account_info)
    - Related Credential Configuration:
        - `keyfile_json` (Required)
        - `scopes` (Optional)
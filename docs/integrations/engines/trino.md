# Trino

## Local/Built-in Scheduler
**Engine Adapter Type**: `trino`

NOTE: Trino may not be used for the SQLMesh [state connection](../../reference/configuration.md#connections).

## Installation
```
pip install "sqlmesh[trino]"
```

If you are using Oauth for Authentication, it is recommended to install keyring cache:
```
pip install "trino[external-authentication-token-cache]"
```

### Trino Connector Support

The trino engine adapter has been tested against the [Hive Connector](https://trino.io/docs/current/connector/hive.html), [Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html), and [Delta Lake Connector](https://trino.io/docs/current/connector/delta-lake.html).

Please let us know on [Slack](https://tobikodata.com/slack) if you are wanting to use another connector or have tried another connector.

#### Hive Connector Configuration

Recommended hive catalog properties configuration (`<catalog_name>.properties`):

```properties linenums="1"
hive.metastore-cache-ttl=0s
hive.metastore-refresh-interval=5s
hive.metastore-timeout=10s
hive.allow-drop-table=true
hive.allow-add-column=true
hive.allow-drop-column=true
hive.allow-rename-column=true
hive.allow-rename-table=true
```

#### Iceberg Connector Configuration

If you're using a hive metastore for the Iceberg catalog, the [properties](https://trino.io/docs/current/connector/metastores.html#general-metastore-configuration-properties) are mostly the same as the Hive connector.

```properties linenums="1"
iceberg.catalog.type=hive_metastore
# metastore properties as per the Hive Connector Configuration above
```

**Note**: The Trino Iceberg Connector must be configured with an `iceberg.catalog.type` that supports views. At the time of this writing, this is `hive_metastore`, `glue`, and `rest`.

The `jdbc` and `nessie` catalogs do not support views and are thus incompatible with SQLMesh.

#### Delta Lake Connector Configuration

The Trino adapter Delta Lake connector has only been tested with the Hive metastore catalog type.

The [properties file](https://trino.io/docs/current/connector/delta-lake.html#general-configuration) must include the Hive metastore URI and catalog name in addition to any other [general properties](https://trino.io/docs/current/object-storage/metastores.html#general-metastore-properties).

``` properties linenums="1"
hive.metastore.uri=thrift://example.net:9083
delta.hive-catalog-name=datalake_delta # example catalog name, can be any valid string
```

### Connection options

| Option               | Description                                                                                                                                                               |  Type  | Required |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:------:|:--------:|
| `type`               | Engine type name - must be `trino`                                                                                                                                        | string |    Y     |
| `user`               | The username (of the account) to log in to your cluster. When connecting to Starburst Galaxy clusters, you must include the role of the user as a suffix to the username. | string |    Y     |
| `host`               | The hostname of your cluster. Don't include the `http://` or `https://` prefix.                                                                                           | string |    Y     |
| `catalog`            | The name of a catalog in your cluster.                                                                                                                                    | string |    Y     |
| `http_scheme`        | The HTTP scheme to use when connecting to your cluster. By default, it's `https` and can only be `http` for no-auth or basic auth.                                        | string |    N     |
| `port`               | The port to connect to your cluster. By default, it's `443` for `https` scheme and `80` for `http`                                                                        |  int   |    N     |
| `roles`              | Mapping of catalog name to a role                                                                                                                                         |  dict  |    N     |
| `http_headers`       | Additional HTTP headers to send with each request.                                                                                                                        |  dict  |    N     |
| `session_properties` | Trino session properties. Run `SHOW SESSION` to see all options.                                                                                                          |  dict  |    N     |
| `retries`            | Number of retries to attempt when a request fails. Default: `3`                                                                                                           |  int   |    N     |
| `timezone`           | Timezone to use for the connection. Default: client-side local timezone                                                                                                   | string |    N     |

## Airflow Scheduler
**Engine Name:** `trino`

The SQLMesh Trino Operator is similar to the [TrinoOperator](https://airflow.apache.org/docs/apache-airflow-providers-trino/stable/operators/trino.html), and relies on the same [TrinoHook](https://airflow.apache.org/docs/apache-airflow-providers-trino/stable/_api/airflow/providers/trino/hooks/trino/index.html) implementation.

To enable support for this operator, the Airflow Trino provider package should be installed on the target Airflow cluster along with SQLMesh with the Trino extra:
```
pip install "apache-airflow-providers-trino"
pip install "sqlmesh[trino]"
```

The operator requires an [Airflow connection](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html) to determine the target Trino account. Refer to [Trino connection](https://airflow.apache.org/docs/apache-airflow-providers-trino/stable/connections.html) for more details.

By default, the connection ID is set to `trino_default`, but can be overridden using the `engine_operator_args` parameter to the `SQLMeshAirflow` instance as in the example below:
```python linenums="1"
sqlmesh_airflow = SQLMeshAirflow(
    "trino",
    default_catalog="<database name>",
    engine_operator_args={
        "trino_conn_id": "<Connection ID>"
    },
)
```
```yaml linenums="1"
gateway_name:
  connection:
    type: trino
    user: [user]
    host: [host]
    catalog: [catalog]
```

### Authentication

=== "No Auth"
    | Option     | Description                              |  Type  | Required |
    |------------|------------------------------------------|:------:|:--------:|
    | `method`   | `no-auth` (Default)                      | string |    N     |

    ```yaml linenums="1"
    gateway_name:
      connection:
        type: trino
        user: [user]
        host: [host]
        catalog: [catalog]
        # Most likely you will want http for scheme when not using auth
        http_scheme: http
    ```


=== "Basic Auth"

    | Option     | Description                                                                                                                                                                  |  Type  | Required |
    | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----: | :------: |
    | `method`   | `basic`                                                                                                                                                                      | string |    Y     |
    | `password` | The password to use when authenticating.                                                                                                                                     | string |    Y     |
    | `verify`   | Boolean flag for whether SSL verification should occur. Default: [trinodb Python client](https://github.com/trinodb/trino-python-client) default (`true` as of this writing) |  bool  |    N     |


    ```yaml linenums="1"
    gateway_name:
      connection:
        type: trino
        method: basic
        user: [user]
        password: [password]
        host: [host]
        catalog: [catalog]
    ```

    * [Trino Documentation on Basic Authentication](https://trino.io/docs/current/security/password-file.html)
    * [Python Client Basic Authentication](https://github.com/trinodb/trino-python-client#basic-authentication)

=== "LDAP"

    | Option               | Description                                                             |  Type  | Required |
    |----------------------|-------------------------------------------------------------------------|:------:|:--------:|
    | `method`             | `ldap`                                                                  | string |    Y     |
    | `password`           | The password to use when authenticating.                                | string |    Y     |
    | `impersonation_user` | Override the provided username. This lets you impersonate another user. | string |    N     |

    ```yaml linenums="1"
    gateway_name:
      connection:
        type: trino
        method: ldap
        user: [user]
        password: [password]
        host: [host]
        catalog: [catalog]
    ```

    * [Trino Documentation on LDAP Authentication](https://trino.io/docs/current/security/ldap.html)
    * [Python Client LDAP Authentication](https://github.com/trinodb/trino-python-client#basic-authentication)

=== "Kerberos"

    | Option                           | Description                                                                       |  Type  | Required |
    |----------------------------------|-----------------------------------------------------------------------------------|:------:|:--------:|
    | `method`                         | `kerberos`                                                                        | string |    Y     |
    | `keytab`                         | Path to keytab. Ex: `/tmp/trino.keytab`                                           | string |    Y     |
    | `krb5_config`                    | Path to config. Ex: `/tmp/krb5.conf`                                              | string |    Y     |
    | `principal`                      | Principal.  Ex: `user@company.com`                                                | string |    Y     |
    | `service_name`                   | Service name (default is `trino`)                                                 | string |    N     |
    | `hostname_override`              | Kerberos hostname for a host whose DNS name doesn't match                         | string |    N     |
    | `mutual_authentication`          | Boolean flag for mutual authentication. Default: `false`                          |  bool  |    N     |
    | `force_preemptive`               | Boolean flag to preemptively initiate the Kerberos GSS exchange. Default: `false` |  bool  |    N     |
    | `sanitize_mutual_error_response` | Boolean flag to strip content and headers from error responses. Default: `true`   |  bool  |    N     |
    | `delegate`                       | Boolean flag for credential delegation (`GSS_C_DELEG_FLAG`). Default: `false`     |  bool  |    N     |

    ```yaml linenums="1"
    gateway_name:
      connection:
        type: trino
        method: kerberos
        user: user
        keytab: /tmp/trino.keytab
        krb5_config: /tmp/krb5.conf
        principal: trino@company.com
        host: trino.company.com
        catalog: datalake
    ```

    * [Trino Documentation on Kerberos Authentication](https://trino.io/docs/current/security/kerberos.html)
    * [Python Client Kerberos Authentication](https://github.com/trinodb/trino-python-client#kerberos-authentication)

=== "JWT"

    | Option      | Description     |  Type  | Required |
    |-------------|-----------------|:------:|:--------:|
    | `method`    | `jwt`           | string |    Y     |
    | `jwt_token` | The JWT string. | string |    Y     |

    ```yaml linenums="1"
    gateway_name:
      connection:
        type: trino
        method: jwt
        user: [user]
        password: [password]
        host: [host]
        catalog: [catalog]
    ```

    * [Trino Documentation on JWT Authentication](https://trino.io/docs/current/security/jwt.html)
    * [Python Client JWT Authentication](https://github.com/trinodb/trino-python-client#jwt-authentication)

=== "Certificate"

    | Option               | Description                                       |  Type  | Required |
    |----------------------|---------------------------------------------------|:------:|:--------:|
    | `method`             | `certificate`                                     | string |    Y     |
    | `cert`               | The full path to a certificate file               | string |    Y     |
    | `client_certificate` | Path to client certificate. Ex: `/tmp/client.crt` | string |    Y     |
    | `client_private_key` | Path to client private key. Ex: `/tmp/client.key` | string |    Y     |


    ```yaml linenums="1"
    gateway_name:
      connection:
        type: trino
        method: certificate
        user: [user]
        password: [password]
        host: [host]
        catalog: [catalog]
        cert: [path/to/cert_file]
        client_certificate: [path/to/client/cert]
        client_private_key: [path/to/client/key]
    ```

=== "Oauth"

    | Option               | Description                                       |  Type  | Required |
    |----------------------|---------------------------------------------------|:------:|:--------:|
    | `method`             | `oauth`                                           | string |    Y     |

    ```yaml linenums="1"
    gateway_name:
      connection:
        type: trino
        method: oauth
        host: trino.company.com
        catalog: datalake
    ```

    * [Trino Documentation on Oauth Authentication](https://trino.io/docs/current/security/oauth2.html)
    * [Python Client Oauth Authentication](https://github.com/trinodb/trino-python-client#oauth2-authentication)

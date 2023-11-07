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

The trino engine adapter has been tested against the [Hive Connector](https://trino.io/docs/current/connector/hive.html).
Please let us know on [Slack](https://tobikodata.com/slack) if you are wanting to use another connector or have tried another connector.

### Hive Connector Configuration

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

    | Option     | Description                              |  Type  | Required |
    |------------|------------------------------------------|:------:|:--------:|
    | `method`   | `basic`                                  | string |    Y     |
    | `password` | The password to use when authenticating. | string |    Y     |


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

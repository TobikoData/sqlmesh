# Notifications guide

SQLMesh can be configured to send notifications via Slack or email when certain events occur. This is done by adding Notification Targets to users or [SQLMesh configurations](https://sqlmesh.readthedocs.io/en/stable/reference/configuration/).

When multiple targets are specified in a config, SQLMesh behaves in the following way:

- If no username is specified in a config, the config's notification targets are used. The most common use case is probably production environments.
- If a username is specified in a config, only that user gets notified. The most common use case is probably a user developing locally. There is currently one exception. See below:
- Audit model owners are notified their audits failed if there is a user with a notification target configured for an `audit_failure` event and the environment is prod.

## SQLMesh Events
- apply_start - Plan application start
- apply_end - Plan application end
- apply_failure - Plan application failure
- run_start - SQLMesh run start
- run_end - SQLMesh run end
- run_failure - SQLMesh run failure
- audit_failure - Audit failure

## Slack Notifications

SQLMesh supports notifications via Slack's webhooks and Web API.

### Webhook Configuration

Slack supports sending messages using Incoming Webhooks, i.e. a unique URL that accepts a JSON payload with the message you want to send.
Incoming Webhooks are tied to a specific Slack channel that is configured during the Webhook's creation and does not support direct messages to users.
For more information on creating Webhooks, please see [Slack's official documentation](https://api.slack.com/messaging/webhooks).

```python
SlackWebhookNotificationTarget(
    notify_on=["apply_start", "apply_failure", "run_start"],
    url=os.getenv("SLACK_WEBHOOK_URL"),
)
```

### API Configuration

If you want to notify users, you can use the Slack API notification target. To begin, you'll need a Slack API token. You can use this token for multiple notification targets with different channels or users. For more information on getting this token, please see [Slack's official documentation](https://api.slack.com/tutorials/tracks/getting-a-token).

```python
SlackApiNotificationTarget(
    notify_on=["apply_start", "apply_failure", "apply_end", "audit_failure"],
    token=os.getenv("SLACK_API_TOKEN"),
    channel="UXXXXXXXXX",  # Channel or a user's Slack member ID
)
```

## Email Notifications

SQLMesh supports notificatication via email.

### Basic SMTP Configuration

```python
BasicSMTPNotificationTarget(
    notify_on=["run_failure"],
    host=os.getenv("SMTP_HOST"),
    user=os.getenv("SMTP_USER"),
    password=os.getenv("SMTP_PASSWORD"),
    sender="notifications@example.com",
    recipients=[
        "team@example.com",
    ],
)
```

## Examples

```python
# config.py
config = Config(
    default_connection=DuckDBConnectionConfig(),
    users=[
        User(
            username="admin",
            roles=[UserRole.REQUIRED_APPROVER],
            notification_targets=[
                SlackApiNotificationTarget(
                    notify_on=["apply_start", "apply_failure", "apply_end", "audit_failure"],
                    token=os.getenv("ADMIN_SLACK_API_TOKEN"),
                    channel="UXXXXXXXXX",  # User's Slack member ID
                ),
            ],
        )
    ],
    notification_targets=[
        SlackWebhookNotificationTarget(
            notify_on=["apply_start", "apply_failure", "run_start"],
            url=os.getenv("SLACK_WEBHOOK_URL"),
        ),
        BasicSMTPNotificationTarget(
            notify_on=["run_failure"],
            host=os.getenv("SMTP_HOST"),
            user=os.getenv("SMTP_USER"),
            password=os.getenv("SMTP_PASSWORD"),
            sender="notifications@example.com",
            recipients=[
                "team@example.com",
            ],
        ),
    ],
)
```

## Advanced Usage

### Overriding Notification Targets

Notification Targets can be overridden to send custom messages.

```python
from sqlmesh.core.notification_target import SlackApiNotificationTarget


class CustomSMTPNotificationTarget(BasicSMTPNotificationTarget):
    def notify_run_failure(self, exc: str) -> None:
        with open("/home/sqlmesh/sqlmesh.log", "r", encoding="utf-8") as f:
            msg = f"{exc}\n\nLogs:\n{f.read()}"
        super().notify_run_failure(msg)
```

The above example appends the contents of a log file to the error stack trace.

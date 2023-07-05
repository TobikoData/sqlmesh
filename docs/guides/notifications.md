# Notifications guide

SQLMesh can send notifications via Slack or email when certain events occur. This page describes how to configure notifications and specify recipients.

## Notification targets

Notifications are configured with `notification targets`. Targets are specified in a project's [configuration](https://sqlmesh.readthedocs.io/en/stable/reference/configuration/) file (`config.yml` or `config.py`), and multiple targets can be specified for a project.

A project's notifications can be global *or* user-specific, but not both (with one exception). If both are present in a configuration file, SQLMesh will ignore the global notification targets. 

[Audit](../concepts/audits.md) failures are the exception to this rule if four conditions are met: 

1. A model's `owner` field is populated
2. The model executes one or more audits
3. The owner has a user-specific notification target configured
4. The notification target `notify_on` key includes audit failure events

When those conditions are met, global notification specifications will be honored and the audit owner will be notified if their audit failed in the `prod` environment.

There are three types of notification target, corresponding to the two [Slack notification methods](#slack-notifications) and [email notification](#email-notifications). They are specified in either a specific user's `notification_targets` key or the top-level `notification_targets` configuration key.

This example shows the location of both user-specific and global notification targets:

=== "YAML"

    ```yaml linenums="1"
    # User notification targets
    users:
      - username: User 1
        ...
        notification_targets:
          - notification_target_1
            ...
          - notification_target_2
            ...
      - username: User 2
        ...
        notification_targets:
          - notification_target_1
            ...
          - notification_target_2
            ...

    # Global notification targets
    notification_targets:
      - notification_target_1
        ...
      - notification_target_2
        ...
    ```

=== "Python"

    ```python linenums="1"
    config = Config(
        ...,
        # User notification targets
        users=[
            User(
                username="User 1",
                # User 1 notification targets
                notification_targets=[
                    notification_target_1(...),
                    notification_target_2(...),
                ],
            ),
            User(
                username="User 2",
                # User 2 notification targets
                notification_targets=[
                    notification_target_1(...),
                    notification_target_2(...),
                ],
            )
        ],

        # Global notification targets
        notification_targets=[
            notification_target_1(...),
            notification_target_2(...),
        ],
        ...
    )
    ```

## SQLMesh Events

SQLMesh notifications are triggered by events. The events that should trigger a notification are specified in the notification target's `notify_on` field.

Notifications are support for [`plan` application](../concepts/plans.md) start/end/failure, [`run`](../reference/cli.md#run) start/end/failure, and [`audit`](../concepts/audits.md) failures. 

For `plan` and `run` start/end, the target environment name is included in the notification message. For failures, the Python exception or error text is included in the notification message.

This table lists each event, its associated `notify_on` value, and its notification message:

| Event                         | `notify_on` Key Value  | Notification message                                     |
| ----------------------------- | ---------------------- | -------------------------------------------------------- |
| Plan application start        | apply_start            | "Plan apply started for environment `{environment}`."    |
| Plan application end          | apply_end              | "Plan apply finished for environment `{environment}`."   |
| Plan application failure      | apply_failure          | "Failed to apply plan.\n{exception}"                     |
| SQLMesh run start             | run_start              | "SQLMesh run started for environment `{environment}`."   |
| SQLMesh run end               | run_end                | "SQLMesh run finished for environment `{environment}`."  |
| SQLMesh run failure           | run_failure            | "Failed to run SQLMesh.\n{exception}"                    |
| Audit failure                 | audit_failure          | "{audit_error}"                                          |

Any combination of these events can be specified in a notification target's `notify_on` field.

## Slack Notifications

SQLMesh supports two types of Slack notification. Slack webhooks can notify a Slack channel, but they cannot message specific users. The Slack Web API can notify channels or users.

### Webhook Configuration

SQLMesh uses Slack's "Incoming Webhooks" for webhook notifications. When you [create an incoming webhook](https://api.slack.com/messaging/webhooks) in Slack, you will receive a unique URL associated with a specific Slack channel. SQLMesh transmits the notification message by submitting a JSON payload to that URL. 

This example shows a Slack webhook notification target. Notifications are triggered by plan application start, plan application failure, or SQLMesh run start. The specification uses an environment variable `SLACK_WEBHOOK_URL` instead of hard-coding the URL directly into the configuration file:

=== "YAML"

    ```yaml linenums="1"
    notification_targets:
      - notify_on:
        - apply_start
        - apply_failure
        - run_start
        url: "{{ env_var('SLACK_WEBHOOK_URL') }}"
    ```

=== "Python"

    ```python linenums="1"
    notification_targets=[
        SlackWebhookNotificationTarget(
            notify_on=["apply_start", "apply_failure", "run_start"],
            url=os.getenv("SLACK_WEBHOOK_URL"),
        )
    ]
    ```

### API Configuration

If you want to notify users, you can use the Slack API notification target. This requires a Slack API token, which can be used for multiple notification targets with different channels or users. See [Slack's official documentation](https://api.slack.com/tutorials/tracks/getting-a-token) for information on getting an API token.

This example shows a Slack API notification target. Notifications are triggered by plan application start, plan application end, or audit failure. The specification uses an environment variable `SLACK_API_TOKEN` instead of hard-coding the token directly into the configuration file:

=== "YAML"

    ```yaml linenums="1"
    notification_targets:
      - notify_on:
        - apply_start
        - apply_end
        - audit_failure
        token: "{{ env_var('SLACK_API_TOKEN') }}"
        channel: "UXXXXXXXXX"  # Channel or a user's Slack member ID
    ```

=== "Python"

    ```python linenums="1"
    notification_targets=[
        SlackApiNotificationTarget(
            notify_on=["apply_start", "apply_end", "audit_failure"],
            token=os.getenv("SLACK_API_TOKEN"),
            channel="UXXXXXXXXX",  # Channel or a user's Slack member ID
        )
    ]
    ```

## Email Notifications

SQLMesh supports notifications via email. The notification target specifies the SMTP host, user, password, and sender address. A target may notify multiple recipient email addresses.

This example shows an email notification target, where `sushi@example.com` emails `data-team@example.com` on SQLMesh run failure. The specification uses environment variables `SMTP_HOST`, `SMTP_USER`, and `SMTP_PASSWORD` instead of hard-coding the values directly into the configuration file:

=== "YAML"

    ```yaml linenums="1"
    notification_targets:
      - notify_on:
        - run_failure
        host: "{{ env_var('SMTP_HOST') }}"
        user: "{{ env_var('SMTP_USER') }}"
        password: "{{ env_var('SMTP_PASSWORD') }}"
        sender: sushi@example.com
        recipients:
          - data-team@example.com
    ```

=== "Python"

    ```python linenums="1"
    notification_targets=[
        BasicSMTPNotificationTarget(
            notify_on=["run_failure"],
            host=os.getenv("SMTP_HOST"),
            user=os.getenv("SMTP_USER"),
            password=os.getenv("SMTP_PASSWORD"),
            sender="notifications@example.com",
            recipients=[
                "data-team@example.com",
            ],
        )
    ]
    ```

## Advanced Usage

### Overriding Notification Targets

Notification Targets can be overridden to send custom messages. This example appends the contents of a log file to the error stack trace:

=== "Python"

```python
from sqlmesh.core.notification_target import BasicSMTPNotificationTarget

class CustomSMTPNotificationTarget(BasicSMTPNotificationTarget):
    def notify_run_failure(self, exc: str) -> None:
        with open("/home/sqlmesh/sqlmesh.log", "r", encoding="utf-8") as f:
            msg = f"{exc}\n\nLogs:\n{f.read()}"
        super().notify_run_failure(msg)
```

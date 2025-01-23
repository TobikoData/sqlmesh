# Alerts

Nobody likes learning about a data problem from stakeholders' angry messages about broken dashboards. If something goes wrong, you want to be the first to know!

Tobiko Cloud makes sure you hear about problems first, alerting the right people immediately when a problem occurs.

## Configuring Alerts

Configure alerts in the Tobiko Cloud Settings section.

To begin, navigate to Settings from the Home screen by clicking the `Settings` link in the top left navigation menu.

![Image highlighting location of the Setting section link](./alerts_notifications/settings_section_link.png)

In the Settings section, navigate to the Alerts page by clicking the `Alerts` link in the top left navigation menu.

Then add a new alert by clicking the `Add Alert` button in the top right.

![Image highlighting location of the Add Alert button](./alerts_notifications/add_alert_button.png)

This opens the Add New Alert configuration page.

Specify an informative name for the alert in the Name field, and click the drop downs for when you want this to run. This is a simple `event` alert, but we'll go into more options below.

After you're finished configuring the new alert, save it by clicking the `Save` button in the bottom right.

![Image showing the add Alert page](./alerts_notifications/add_alert_page.png)

## Alerts

Tobiko Cloud sends an alert based on a *trigger*. There are two types of triggers: [events](#event-triggers) and [measures](#measure-triggers).

Events are tied to steps in the SQLMesh `plan` and `run` processes. For example, you could alert whenever a `plan` succeeded or a `run` failed.

Measures are based on either [automatically calculated measures](../../guides/observer.md#measures) like run time or [custom measure you define](../../guides/observer.md#custom-measures) via SQL queries.

Choose whether the alert will be triggered by a Measure or Event in the alert's Trigger Type field.

![Image showing the add Alert page trigger type field](./alerts_notifications/add_alert_trigger_type.png)

### Event triggers

Tobiko Cloud Alerts can be triggered by the following events:

- Plan start
- Plan end
- Plan failure
- Run start
- Run end
- Run failure

Specify an event trigger by first choosing whether it is tied to a `plan` or `run` Artifact.

![Image showing the Artifact dropdown](./alerts_notifications/add_event_artifact.png)

Next, choose the notification Event type: Start, Failure, or End.

![Image showing the Event dropdown](./alerts_notifications/add_event_event.png)

Finally, choose a Notification Target where the alert should be sent (described [below](#notification-targets)) and click the Save button in the bottom right.

![Image showing the add event Alert page](./alerts_notifications/add_event_alert_page.png)

### Measure triggers

Tobiko Cloud Alerts can be triggered when a [measure](../../guides/observer.md#measures) exceeds a threshold or meets a condition.

To configure a measure alert, first build the condition that triggers the measure. Choose the measure of interest, the comparison operator, and a threshold value.

![Image showing the add measure Alert page condition section](./alerts_notifications/add_measure_alert_condition.png)

Now specify the alert Artifact field.

Some measures, like run time, are most useful when accumulated over an entire `plan` or `run`. For example, you might want to alert whenever a `run`'s total run time is longer than four hours.

Configure a cumulative measure alert by choosing an Artifact type of Plan or Run.

Other measure alerts are useful when applied to individual models or steps in a `plan` or `run`. For example, you might know that your `users` model should process around 1000 new users each day. You could [create a measure](../../guides/observer.md#custom-measures) to track the number of new users and an alert that notifies you if that number drops below 500.

Configure a non-cumulative measure alert by choosing an Artifact type of Measure.

![Image showing the add measure Alert page Artifact field](./alerts_notifications/add_measure_alert_artifact.png)

To prevent alert fatigue, you can limit measure-based alerts to a specific environment  or model in the optional Environment and Model fields.

![Image showing the add measure Alert page](./alerts_notifications/add_measure_alert_page.png)

## Notification Targets

Each alert is sent to one or more notification targets.

A notification target is a way for alerts to contact you. A target can be used in multiple alerts, so you only have to configure them once.

### Notification Target Configuration

Configure Notification targets in the Tobiko Cloud Settings section.

To add a new notification target, navigate to the Notification Targets page and click the Add Notification Target button in the top right.

![Image highlighting location of the Add Notification Target button](./alerts_notifications/add_notification_target_button.png)

Then enter a descriptive name for the new notification target, select its type (described [below](#notification-target-types)), fill in the configuration information, and click Save.

![Image showing the add Notification Target page](./alerts_notifications/add_notification_target_page.png)

### Notification target types

Tobiko Cloud supports the following notification target types, which require you to provide different pieces of configuration information.

- Slack API
    - API Token
        - Format: `xoxb-[13 digits]-[13 digits]-[24 alphanumeric characters]`
    - Channel ID
        - Format: `T[10 capital letter or numeric characters]`
        - Example: T139Z25G8F4
- Slack Webhook
    - Webhook URL
        - Format: Web URL
        - Example: https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
- PagerDuty
    - Routing Key
        - Format: `[32 alphanumeric characters]`
        - Example: j16lxprdvoy21paigybthal0llk51kh5k

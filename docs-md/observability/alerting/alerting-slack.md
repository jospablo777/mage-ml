# Alerting status updates in Slack

*Get pipeline status updates in your Slack channels.*

<img src="https://d34u8crftukxnk.cloudfront.net/slackpress/prod/sites/6/2019-01_BrandRefresh_Old-to-New-Final.gif" alt="Slack" width="300" />

## Overview of steps

1. Set up webhooks for Slack
2. Update the project settings
3. Customize message templates

---

## Set up webhooks for Slack

To send notifications to Slack, you need to configure an incoming webhook:

- Follow the official [Slack webhook setup guide](https://api.slack.com/messaging/webhooks).
- Create a webhook URL for your desired Slack channel.

---

## Update the project settings

Once you’ve set up webhooks for Slack, you should have a webhook URL that Slack
provides.

Here is an example webhook (yours may vary):

```
https://hooks.slack.com/services/T01XXXXRKMJ/B04XXXXGY67/1UgUkao8pXXXXPmsuXXXX2lk
```

Follow these steps to add that webhook URL to your project settings:

1. Open MageML in your browser (e.g. http://localhost:6789/).
2. Open a pipeline and start editing it (e.g.
   http://localhost:6789/pipelines/example_pipeline/edit).
3. In your left sidebar in the file browser, scroll all the way down and click
   on a file named `metadata.yaml`.
4. In the `metadata.yaml` file, add a section with the following
   configuration or update the existing `notification_config` section:

```yaml
    notification_config:
      alert_on:
        - trigger_failure
        - trigger_passed_sla
    ```

    If you omit the `alert_on` section it will default to `trigger_failure` and `trigger_passed_sla`.

    Options:

    - `trigger_failure`: alert when a run of a trigger fails
    - `trigger_success`: alert when a run of a trigger succeeds
    - `trigger_passed_sla`: alert when a SLA is missed.
      - In this context, SLA (Service Level Agreement) is an expected amount of time for the pipeline to complete.
      - If the pipeline runs _longer_ than the SLA, an alert will be sent.

5. In the `metadata.yaml` file, add the following values:

```yaml
    notification_config:
      alert_on:
        ...
      slack_config:
        webhook_url: ...
    ```

    Change the `webhook_url` value to be the webhook URL you created from Slack.

    `metadata.yaml` is rendered as a template when the project loads, so you can read the
    URL from an environment variable: `webhook_url: "{{ env_var('MAGE_SLACK_WEBHOOK_URL') }}"`.

## Customize message templates
You can customize message templates in `notification_config`. Here is an example config:
```yaml
notification_config:
  alert_on:
    ...
  slack_config:
    ...
  message_templates:
    failure:
      details: >
        Failed to execute pipeline {pipeline_run_url}.
        Pipeline uuid: {pipeline_uuid}. Trigger name: {pipeline_schedule_name}.
        Test custom message.
    success:
      details: ...
    passed_sla:
      details: ...
```
You can customize the message template for `success`, `failure`, `passed_sla` scenarios. For each message template,
you can specify `title`, `summary` and `details`.
* `details` is posted as the Slack message.
* Without `details`, the message is `summary` followed by the URL of the pipeline run page,
  or the default message when neither is set.

To interpolate variables in the message template, you can use `{variable_name}` syntax.
Here are the supported variables:
1. `execution_time`
1. `pipeline_run_url`
1. `pipeline_schedule_id`
1. `pipeline_schedule_name`
1. `pipeline_schedule_description`
1. `pipeline_uuid`
1. `error`
    * available only for the `failure` message template
1. `stacktrace`
    * available only for the `failure` message template

> **Note**: For `pipeline_run_url`, the default host is `http://localhost:6789`. You can specify `MAGE_PUBLIC_HOST` to be
the host url you want to use in the notification messages.

---

## What next?

Whenever a pipeline run is successfully completed or fails, a Slack message will
appear in the channel you configured the webhook URL for.

Here is an example of what that message could look like:

|                                                                                                        |
| ------------------------------------------------------------------------------------------------------ |
| ![Slack](https://mage-ai.github.io/assets/third-party/slack-message-run-alert.jpg) |

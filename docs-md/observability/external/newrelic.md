# Monitoring with New Relic

## Enable New Relic

You will need to add `ENABLE_NEW_RELIC` environment variable as `true` in order to use
New Relic in MageML.

## New Relic Configuration File

Following [New Relic Python integration guide](https://docs.newrelic.com/install/python/?python-non-web=non-web-yes),
 run command: `newrelic-admin generate-config YOUR_LICENSE_KEY newrelic.ini`
 to generate the `newrelic.ini` configuration file and edit your `newrelic.ini` and insert values for the following:
 ...
license_key = INSERT_YOUR_LICENSE_KEY
...
app_name = INSERT_YOUR_APP_NAME
...

An example `example_newrelic.ini` file is in the MageML repository under `integrations/newrelic`.

Add path of your newrelic configuration into environment variable `NEW_RELIC_CONFIG_PATH`.

## Using New Relic in MageML

Monitoring with New Relic will be enabled the next time you start the MageML server.

The following logs and errors will be reported to New Relic:

* Trigger runs.
* MageML scheduler.
* `mage run` CLI command.

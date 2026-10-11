# Power BI

![Power BI](https://icon-library.com/images/power-bi-icon/power-bi-icon-21.jpg)

<br/>

## Configuration

To set up the Power BI source and connect to your workspace, provide the following configuration parameters:

| Key             | Description                                                              | Sample Value        |
| --------------- | ------------------------------------------------------------------------ | ------------------- |
| `access_token`  | (REQUIRED) Access token used to authenticate with your Power BI account.  | `abcdefg123456`      |
| `request_timeout` | (OPTIONAL) Timeout (in seconds) for API requests (default: 300).         | `300`                |
| `user_agent`    | (OPTIONAL) User Agent string to send in the request header.                | `my-app-v1.0`        |
| `start_date`    | (OPTIONAL) Filter results to only sync records updated after this date. Format: `YYYY-MM-DD`. | `2023-01-01`  |

<br/>

## What is Power BI?

[Power BI](https://powerbi.microsoft.com/) is Microsoft’s business intelligence (BI) platform for dashboards and reports.
This source extracts metadata about Power BI apps, dashboards, datasets, workspaces, reports, and tiles.

<br/>

## How to Get Your `access_token`

Follow Power BI’s [official instructions](https://community.fabric.microsoft.com/t5/Developer/REST-API-Get-Access-Token/m-p/1895937/) to generate an OAuth `access_token` via the Azure Active Directory (Azure AD) app registration process.

<br/>

## Supported Streams from Power BI

The following Power BI streams are available:

- **apps**: Metadata about published Power BI apps in your organization.
- **dashboards**: Details of dashboards created and shared within workspaces.
- **datasets**: Information about datasets, including refresh schedules and models.
- **groups**: Workspaces and group information for collaborative projects.
- **reports**: Metadata on reports built and published in Power BI.
- **tiles**: Tiles on dashboards representing visuals and pinned content.

<br/>

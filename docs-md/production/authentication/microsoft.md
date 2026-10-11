# Sign in with Microsoft Entra ID aka Active Directory

*Enable signing in with a Microsoft account in MageML.*

### Pre-requisites

User authentication must be on (it is by default). You can find out more about
this in the [Overview page](overview.md).

### Enable Microsoft sign in

Microsoft sign in needs your own application registered in your organization's Microsoft Entra ID.
MageML has no default application, and no sign-in goes through Mage's servers.

**1. Log into your Azure Portal**

**2. Click into the Microsoft Entra ID service**

<img src="https://mage-ai.github.io/assets/production/authentication/ad.png" alt="Azure Active Directory" />

**3. Navigate to the tenant you want users to sign into**

**4. Click App registrations on the left sidebar**

<img src="https://mage-ai.github.io/assets/production/authentication/ad-app-registrations.png" alt="App registrations" />

**5. Register a new application for MageML**

Click the "New registration" button at the top of the page, and fill in the details for your application.

<img src="https://mage-ai.github.io/assets/production/authentication/ad-new-registration.png" alt="New registration" />

Make sure you add the following web redirect URI: `http(s)://<your-mageml-url>/oauth`.

**6. Create client secret for the application**

Click into the application you created, and click "Certificates & secrets" in the left sidebar.

<img src="https://mage-ai.github.io/assets/production/authentication/ad-certificates.png" alt="Certificates & secrets" />

Copy the "Application (client) ID" and the "Directory (tenant) ID" values.

<img src="https://mage-ai.github.io/assets/production/authentication/ad-application-fields.png" alt="Copy values" />

Create a new client secret and copy the value.

**7. Set environment variables for MageML**

Once you have registered your application, you will need to set the following environment variables
in your MageML setup.

| Name | Value |
| --- | --- |
| `ACTIVE_DIRECTORY_DIRECTORY_ID` | `<tenant_id>` |
| `ACTIVE_DIRECTORY_CLIENT_ID` | `<application_client_id>` |
| `ACTIVE_DIRECTORY_CLIENT_SECRET` | `<application_client_secret>` |

You should now see the "Sign in with Microsoft" button when you start MageML and attempt to sign in.
You may need to restart your MageML server for the changes to take effect.

<img src="https://mage-ai.github.io/assets/production/authentication/sign-in-with-microsoft.png" alt="Sign in with Microsoft" />

### Configurable environment variables
To configure the environment for Microsoft Azure Cloud and Microsoft Azure Gov Cloud sign-in,
you need to set the following environment variables:
1. You can set the `AZURE_HOST` environment variable to one of the following based on your environment:
```
Public cloud host url: "https://login.microsoftonline.com"
China host url: "https://login.chinacloudapi.cn"
Germany host url: "https://login.microsoftonline.de"
Government host url: "https://login.microsoftonline.us"
```
2. You can set the `MICROSOFT_GRAPH_API_HOST` environment variable to one of the following based on your environment:
```
Microsoft Graph global service: "https://graph.microsoft.com"
Microsoft Graph for US Government L4: "https://graph.microsoft.us"
Microsoft Graph for US Government L5 (DOD): "https://dod-graph.microsoft.us"
Microsoft Graph China operated by 21Vianet: "https://microsoftgraph.chinacloudapi.cn"
```

### Map AD role to MageML role

> [!NOTE]
>
> Your application needs app roles for this mapping to work.
>

You can map Active Directory roles to MageML roles by setting the `ACTIVE_DIRECTORY_ROLES_MAPPING` environment variable.
The value should be a JSON object with the AD role as the key and the name of the MageML role as the value. For example:

`export ACTIVE_DIRECTORY_ROLES_MAPPING='{"Mage.Edit": "Editor", "Mage.Admin": "Admin"}'`

You can find the AD role value in the App roles section of your application in the Azure Portal.

<img src="https://mage-ai.github.io/assets/production/authentication/ad-app-role-value.png" alt="App roles" />

When a user signs in for the first time with their Microsoft account, MageML uses this mapping to add
the appropriate roles to the user.

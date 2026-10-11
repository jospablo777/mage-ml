# Sign in with OIDC

*Enable signing in with OIDC in MageML.*

### Pre-requisites

User authentication must be on (it is by default). You can find out more about
this in the [Overview page](overview.md).

### Enable OIDC sign in

By default, users will not see the "Sign in with OIDC" option in their sign in page.

**1. Add Oauth application**

Register MageML as a client with your OIDC provider. The way to do this will
be different depending on your OIDC provider. The redirect URL should be `https://<your-mageml-url>/oauth`.

**2. Retrieve credentials**

Once you have registered MageML, you will need to retrieve the client ID, client secret,
and the OIDC discovery URL for your provider.

**3. Set environment variables**

Set the following environment variables in your MageML setup.

| Name | Value |
| --- | --- |
| `OIDC_DISCOVERY_URL` | `<oidc domain url>` |
| `OIDC_CLIENT_ID` | `<client id>` |
| `OIDC_CLIENT_SECRET` | `<client secret>` |

You should now see a "Sign in with OIDC" button when you start MageML and attempt to sign in.

MageML fetches the following OIDC endpoints from the discovery URL:

* authorization_endpoint
* token_endpoint
* userinfo_endpoint

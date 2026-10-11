# Sign in with Okta

*Enable signing in with Okta in MageML.*

### Pre-requisites

User authentication must be on (it is by default). You can find out more about
this in the [Overview page](overview.md).

### Enable Okta sign in

By default, users will not see the "Sign in with Okta" option in their sign in page.

**1. Add Oauth application**

Create an OAuth client for MageML in your Okta organization admin dashboard.
For more instructions on how to do this, please see the [Okta developer documentation](https://developer.okta.com/docs/guides/implement-oauth-for-okta/main/#create-an-oauth-2-0-app-in-okta).
Make sure you select the "Authorization Code" grant flow, and add `http(s)://<your-mageml-url>/oauth` as a Sign-in redirect URI.

**2. Retrieve credentials**

Once you have created the credentials, you will need to get the application's client ID and client secret.
Make sure you also have your Okta domain URL.

**3. Set environment variables**

Set the following environment variables in your MageML setup.

| Name | Value |
| --- | --- |
| `OKTA_DOMAIN_URL` | `<okta domain url>` |
| `OKTA_CLIENT_ID` | `<client id>` |
| `OKTA_CLIENT_SECRET` | `<client secret>` |

You should now see the "Sign in with Okta" button when you start MageML and attempt to sign in.

<img src="https://mage-ai.github.io/assets/production/authentication/sign-in-with-okta.png" alt="Sign in with Okta" />

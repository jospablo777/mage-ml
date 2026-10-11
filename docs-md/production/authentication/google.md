# Sign in with Google

*Enable signing in with a Google account in MageML.*

### Pre-requisites

User authentication must be on (it is by default). You can find out more about
this in the [Overview page](overview.md).

### Enable Google sign in

> [!NOTE]
>
> MageML requests access for the following scopes:
>
> ```
> https://www.googleapis.com/auth/userinfo.email
> https://www.googleapis.com/auth/userinfo.profile
> ```
>

By default, users will not see the "Sign in with Google" option in their sign in page.

**1. Add Oauth application**

Create an OAuth client for MageML in your Google Cloud Platform project.
For more instructions on how to do this, please see the [Google Cloud Platform documentation](https://developers.google.com/identity/protocols/oauth2/openid-connect#appsetup).

**2. Retrieve credentials**

Once you have created the credentials, you will need to get the application's client ID and client secret.
You will also need to add `http(s)://<your-mageml-url>/oauth` as an authorized redirect URI.

**3. Set environment variables**

Set the following environment variables in your MageML setup.

| Name | Value |
| --- | --- |
| `GOOGLE_CLIENT_ID` | `<client id>` |
| `GOOGLE_CLIENT_SECRET` | `<client secret>` |

You should now see the "Sign in with Google" button when you start MageML and attempt to sign in.

<img src="https://mage-ai.github.io/assets/production/authentication/sign-in-with-google.png" alt="Sign in with Google" />

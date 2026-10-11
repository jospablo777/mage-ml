# Zendesk

![Zendesk](https://upload.wikimedia.org/wikipedia/commons/thumb/c/c8/Zendesk_logo.svg/2560px-Zendesk_logo.svg.png)

<br />

## Config

You must enter the following credentials when configuring this source:

| Key | Description | Sample value
| --- | --- | --- |
| `access_token` | OAuth token to access API endpoints. | `abc123...` |
| `request_timeout` | Number of seconds until the API request should time out and raise an error. Defaults to `300`. | 300 |
| `start_date` | Fetch data that is newer than the `start_date`. | `2022-01-01T00:00:00Z` |
| `subdomain` | If you access Zendesk from a URL like this https://example.zendesk.com/, then the `subdomain` is `example`. | `example` |

<br />

## How to get your `access_token`

Follow [Zendesk’s documentation](https://support.zendesk.com/hc/en-us/articles/4408845965210)
to get your access token.

Example Admin Center URL: https://example.zendesk.com/admin/home
(subdomain in example is `example`).

### Example values for getting access token

<b>Zendesk authorization page</b>

https://example.zendesk.com/oauth/authorizations/new?client_id=my-app&response_type=code&redirect_uri=https://www.example.com&scope=read

<b>Authorize redirect URL with code</b>

https://www.example.com/?code=6b21972c79e12cbda37947df4ba22d4cb394da752e955c6dac81361d5f453695

<b>Example cURL command to exchange code for access token</b>

```curl
curl --request POST \
  --url https://example.zendesk.com/oauth/tokens \
  --header 'Content-Type: application/json' \
  --data '{
  "grant_type": "authorization_code",
  "code": "6b21972c79e12cbda37947df4ba22d4cb394da752e955c6dac81361d5f453695",
  "client_id": "my-app",
  "client_secret": "your_oauth_client_secret",
  "redirect_uri": "https://www.example.com",
  "scope": "read"
}'
```

<b>Example OAuth token exchange response</b>

```json
{
  "access_token": "cdaae5613efca304972cd8175adf51b560691f51e9a6331a986a772801a5d898",
  "token_type": "bearer",
  "scope": "read"
}
```

<br />

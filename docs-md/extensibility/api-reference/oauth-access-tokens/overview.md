# Overview

*Fetch OAuth access tokens.*

### OAuth access token object

**Response**

```json
{
  "oauth_access_tokens": [
    {
      "expires": "2023-08-18 23:41:52.480598+00:00",
      "token": "JS3Umh826UoZzbuGQUsj36PSBwruVCzQYlRFf3KmJWA",
      "user_id": 2
    }
  ],
  "metadata": {
    "count": 1,
    "next": false
  }
}
```

**`expires`** (string, required)

Datetime in UTC when the access token expires and is no longer valid to use.

**`token`** (string, required)

The token used to make authenticated API requests to MageML.

**`user_id`** (integer, required)

ID of user that the OAuth access token belongs to.

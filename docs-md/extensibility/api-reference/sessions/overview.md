# Overview

*Sign in and retrieve an OAuth token.*

### Session object

**Response**

```json
{
  "expires": "2023-04-30 03:02:14.781970+00:00",
  "token": "...",
  "user": {
    "avatar": null,
    "first_name": null,
    "id": 1,
    "last_name": null,
    "owner": true,
    "roles": null,
    "roles_display": "Owner",
    "username": "admin"
  }
}
```

**`expires`** (datetime, required)

The date and time the session is no longer valid.

**`token`** (string, required)

OAuth token used to authenticate each API request.

**`user`** (object, required)

The signed-in user.

**`id`** (integer, required)

User ID

**`owner`** (boolean)

If true, the current user has owner permissions.

**`roles`** (integer)

Roles a user has, represented as an integer.

**`roles_display`** (integer)

Human-readable role.

**`username`** (string)

Unique username for user.

To create a session, see [Create session](create-session.md).
To use the token, see [Authentication](../overview.md#authentication).

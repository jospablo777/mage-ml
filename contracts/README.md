# API contracts

Recorded from the Python server. They are what the Rust gateway and the authorization engine
must match when they take over each route and policy.

| File | What it holds | Source |
| --- | --- | --- |
| `openapi.json` | Every route and operation the server answers, with response schemas inferred from live responses of a seeded scratch project | `mage_ai/api/contracts/openapi.py` |
| `policy_matrix.json` | For each policy, action and attribute: the roles allowed, by `DISABLE_NOTEBOOK_EDIT_ACCESS` mode | `mage_ai/api/contracts/policy_matrix.py` |

Regenerate both after changing a resource, a policy, a presenter or a route (the recorder
also rewrites `docs/production/authentication/access.mdx`):

```bash
uv run python scripts/contracts/record.py
```

The tests in `mage_ai/tests/api/contracts` fail when a checked-in file no longer matches the
code.

## policy_matrix.json

Each check calls the policy's own `authorize_action`, `authorize_attribute` or `authorize_query`.
User authentication is on, and the record belongs to another user, at project level.

- `actions`: `[resource, action, [modes], [roles allowed]]`
- `attributes`: `[resource, read | write | query, action, [modes], {"role role": [names]}]`

Roles are `owner`, `admin`, `editor`, `viewer`, `none` (signed in with no role) and `anonymous`.
Modes 0, 1 and 2 are the values of `DISABLE_NOTEBOOK_EDIT_ACCESS`.

## openapi.json

Paths come from three places:

- the resource classes (`/api/{resource}`, `/api/{resource}/{id}`);
- the parent and child pairs the frontend calls (`frontend/api/index.ts`);
- the routes `make_app` registers with their own handlers.

A resource whose responses could not be recorded keeps its presenter's attribute names with no
type. It is listed under `x-mage-unrecorded` with the reason.

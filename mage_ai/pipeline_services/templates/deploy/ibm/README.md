# $name on IBM Cloud

## Deploy from your machine

Code Engine builds the image from this folder and runs it:

```bash
ibmcloud login --sso
ibmcloud plugin install code-engine
export REGION=us-south PROJECT=my-project
deploy/ibm/code-engine.sh
```

## Give the service its variables and secrets

Either way works; both can be combined.

**A Code Engine secret.** Code Engine passes its keys to the container as environment
variables:

```bash
ibmcloud ce secret create --name $name-env --from-env-file .env
ENV_SECRET=$name-env deploy/ibm/code-engine.sh
```

**IBM Cloud Secrets Manager, read by the service at start-up.** Nothing secret is stored
in Code Engine:

1. Create a trusted profile for compute resources, with the Code Engine project as the
   resource, and give it the `SecretsReader` role on the Secrets Manager instance.
2. Deploy with the profile, the instance and the secrets to read:

```bash
TRUSTED_PROFILE=Profile-1234 \
SECRETS_MANAGER_URL=https://<instance id>.<region>.secrets-manager.appdomain.cloud \
SECRETS='PGPASSWORD=ibm:<secret id>, PGUSER=ibm:<secret id>#username, ibm:<kv secret id>' \
deploy/ibm/code-engine.sh
```

Each entry is `[NAME=]ibm:<secret id>[#field]`. An arbitrary secret gives its payload, a
username and password secret its password (or `#username`), an IAM credentials secret its
API key, and a key-value secret without a name sets one variable per key. Instead of a
trusted profile, `IBM_CLOUD_API_KEY` (or `IBM_CLOUD_API_KEY_FILE`) can hold a service ID's
key. Values read this way are redacted from the logs.

## Continuous delivery

`.tekton/` holds a pipeline for IBM Cloud Continuous Delivery: it clones the repository,
builds the image with kaniko, pushes it to IBM Cloud Container Registry and deploys it to
Code Engine.

1. Push this folder to a Git repository (IBM Cloud Git, GitHub or GitLab).
2. Create a toolchain with a Delivery Pipeline of type Tekton. Under **Definitions**, add
   the repository with path `.tekton`.
3. Under **Environment properties**, add:

| Property | Value |
| --- | --- |
| `repository` | the repository's https URL |
| `branch` | `main` |
| `context-dir` | the folder of this export in the repository, `.` at the root |
| `apikey` | (secure) an API key that can push to the registry and deploy to Code Engine |
| `region`, `resource-group` | where the Code Engine project is |
| `registry-namespace` | an IBM Cloud Container Registry namespace |
| `code-engine-project` | the Code Engine project |
| `env-secret` | optional: the Code Engine secret with the variables |
| `trusted-profile` | optional: the trusted profile that reads Secrets Manager |
| `git-token` | (secure) optional: a token for a private repository |

4. Under **Triggers**, add a manual trigger and a Git trigger on the branch, both with the
   `$name-listener` event listener.

Each run tags the image with the commit and deploys it by digest, so a deployment always
runs the image that was built for it.

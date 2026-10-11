# One-way code sync using Git

*Enabling one-way Git sync in MageML ensures data will be synced in one direction with a specified git repository.*

Git sync is a Git feature that allows you to tie your local project to a remote repo. When Git sync is enabled, you will only be able to pull in changes from the remote repository. Git sync is useful for deployment, for example if you'd like a hosted MageML instance to stay up to date with a repo.

Running a Git sync will **_overwrite all local changes_** and replace them with the code in the specified remote branch. Git syncs can be run either from the Git settings page, or before every trigger run if you selected the `Sync before each trigger run` field.

To enable Git Sync, first [configure Git in MageML](../git/configure.md), then open **Settings** > **Git settings** and select the `One-way Git Sync` tab.

### Syncing git submodules

Repositories synced through MageML will only update 1 level of submodules. If your submodule contains other submodules, you will need to manually update them or reformat your submodules.

If your repo contains git submodules, you can enable syncing for them by ticking the `Include submodules` box. MageML will
use the same credentials that are provided for the main repository to update the submodules.

## Git sync settings as environment variables

> [!NOTE]
>
> MageML prioritizes git settings from environment variables when they are set. If you want to be able
> to change git settings from the UI, you will need to set the `GIT_OVERWRITE_WITH_PROJECT_SETTINGS` environment
> variable to `1`. Otherwise, edits to the git settings from the UI will not be applied.
>

You can choose to pass in Git settings as environment variables. MageML reads from these environment
variables to configure your Git repo.

| Setting                              | Variable                                   | Description                                                                                                                       | Required |
|--------------------------------------|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|----------|
| Git settings                         | `GIT_REPO_LINK`                            |  Corresponds to `Remote repo url`                                                                                                 | ✅       |
| Git settings                         | `GIT_REPO_PATH`                            |  Corresponds to `Local directory path`                                                                                            | ✅       |
| Git settings                         | `GIT_USERNAME`                             |  Corresponds to `Username`                                                                                                        | ✅       |
| Git settings                         | `GIT_EMAIL`                                |  Corresponds to `Email`                                                                                                           | ✅       |
| Git settings                         | `GIT_AUTH_TYPE`                            |  The method of authentication: `ssh`, `https` or `oauth` | ✅       |
| Git sync settings                    | `GIT_BRANCH`                               |  Corresponds to `Branch name`                                                                                                     | 🚫       |
| Git sync settings                    | `GIT_SYNC_ON_PIPELINE_RUN`                 |  Corresponds to `Sync before each trigger run`, options                                                                           | 🚫       |
| Git sync settings                    | `GIT_SYNC_ON_START`                        |  Corresponds to `Sync on server start up`, options                                                                                | 🚫       |
| Git sync settings                    | `GIT_SYNC_ON_EXECUTOR_START`               |  If set to "1", MageML runs a git sync every time an executor is started, options                                    | 🚫       |
| Git sync settings                    | `GIT_SYNC_SUBMODULES`                      |  Corresponds to `Include submodules`, options                                                                                     | 🚫       |
| Authentication environment variables | `GIT_SSH_PUBLIC_KEY`                       |  SSH public key encoded in base64, used for ssh auth type                                                                         | 🚫       |
| Authentication environment variables | `GIT_SSH_PRIVATE_KEY`                      |  SSH private key encoded in base64, used for ssh auth type                                                                        | 🚫       |
| Authentication environment variables | `GIT_ACCESS_TOKEN`                         |  Git access token, used for https auth type                                                                                       | 🚫       |
| Global git settings                  | `GIT_ENABLE_GIT_INTEGRATION`               |  Force git integration module to show in the UI. Primarily useful when the version control app does not work for your repository. | 🚫       |
| Global git settings                  | `GIT_OVERWRITE_WITH_PROJECT_SETTINGS`      |  Allow users to overwrite git settings set by environment variables through the Git Settings page in the UI.                      | 🚫       |

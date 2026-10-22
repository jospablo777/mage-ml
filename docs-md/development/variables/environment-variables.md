# MageML environment variables

*Configure MageML settings through environment variables.*

## Setting environment variables

Environment variables may be set in your Docker container to configure MageML settings. If you’re running MageML without Docker (`uv run mage start` or `mage start`), your local machine’s environment variables
are accessible within the running app.

Environment variables are different from [runtime variables](../../getting-started/runtime-variable.md) or [secrets](secrets.md).

If you’re running MageML using Docker, you must add the following command line flags:

```bash
-e SOME_VARIABLE_NAME_1=secret_value_1 -e SOME_VARIABLE_NAME_2=secret_value_2
```

The command to run MageML using Docker, with an image built from the repository (`docker build -t mageml .`), could look like this:

```bash
docker run -it -p 6789:6789 -v $(pwd):/home/src \
    -e SOME_VARIABLE_NAME_1=secret_value_1 -e SOME_VARIABLE_NAME_2=secret_value_2 \
    mageml /app/run_app.sh mage start [project_name]
```

## Setting the prefix for the MageML URL

You may need to add a prefix to the MageML url. You can configure the MageML server to start
at a prefix to the url by setting the `MAGE_BASE_PATH` environment variable.

Make sure to set the base path without the leading slash. For example, if you want to start MageML at
`http://localhost:6789/base/url/path`, you would configure your environment
variable like this:

`export MAGE_BASE_PATH=base/url/path`

You can also configure the base path separately for client requests and backend routes by setting
the `MAGE_REQUESTS_BASE_PATH` and `MAGE_ROUTES_BASE_PATH` environment variables respectively.

|                           |                                                                                     |
| ------------------------- | ----------------------------------------------------------------------------------- |
| `MAGE_BASE_PATH`          | Set base path for the entire app.                                                   |
| `MAGE_REQUESTS_BASE_PATH` | Set base path for frontend requests and urls, defaults to value of `MAGE_BASE_PATH` |
| `MAGE_ROUTES_BASE_PATH`   | Set base path for backend routes, defaults to value of `MAGE_BASE_PATH`             |

## Default variables

These are the environment variables used in MageML.

| Variable name                    | Description                                                                                                                                                     | Example values                                                           |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `ACTIVE_DIRECTORY_DIRECTORY_ID`  | Set this value to your organization's tenant id to enable signing in with Microsoft account.                                                                    | [More info](../../production/authentication/microsoft.md)                        |
| `AUTHENTICATION_MODE`            | Set the authentication mode in MageML. Omitting the variable will default to basic password authentication.                                                       | `ldap`                                                                   |
| `DATAFRAME_SAMPLE_COUNT_PREVIEW` | Number of lines to display in block sample output preview for dataframes. Not suggested to go over 10 lines (defaults to 10).                                   | 5                                                                        |
| `DEBUG`                          | Turn on "debug" mode. MageML will output more logs that could be useful for debugging issues.                                                                     | 1                                                                        |
| `DEFAULT_EXECUTOR_TYPE`          | The default executor type to run blocks                                                                                                                         | See enum types [here](../../production/configuring-production-settings/compute-resource.md#block-executor) |
| `DISABLE_NOTEBOOK_EDIT_ACCESS`   | Setting this variable will disable edit access based on the value. [More information](../../production/configuring-production-settings/overview.md#read-only-access)    | 1                                                                        |
| `DISABLE_TERMINAL`               | Setting this variable will disable access to the terminal.                                                                                                      | 1                                                                        |
| `ENV`                            | Set the environment of the MageML app. You can use different code or configurations in different environments.                                                    | `dev`, `staging`, `production`/`prod`, `test`                            |
| `GIT_REPO_LINK`                  | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_REPO_PATH`                  | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_USERNAME`                   | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_EMAIL`                      | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_AUTH_TYPE`                  | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_BRANCH`                     | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_SYNC_ON_PIPELINE_RUN`       | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_SYNC_ON_START`              | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_SSH_PUBLIC_KEY`             | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_SSH_PRIVATE_KEY`            | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `GIT_ACCESS_TOKEN`               | [More information](../../guides/data-sync/git-sync.md#git-sync-settings-as-environment-variables)                                                                   | See link                                                                 |
| `HIDE_ENV_VAR_VALUES`            | Filter out environment variables in notebook output.                                                                                                            | 1                                                                        |
| `JWT_SECRET`                     | Modify the json web token secret used for encoding and decoding access tokens. Defaults to `materia`.                                                           | `your_jwt_secret`                                                        |
| `JWT_DOWNLOAD_SECRET`            | Modify the json web token secret used for encoding and decoding download tokens. Defaults to randomly generated string value. Use an explicit value in horizontally scalable deployments, e.g.: kubernetes, cloud deployments | `your_jwt_secret`    |
| `LDAP_ADMIN_USERNAME`            | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LDAP_SERVER`                    | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LDAP_BIND_DN`                   | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LDAP_BIND_PASSWORD`             | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LDAP_BASE_DN`                   | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LDAP_AUTHENTICATION_FILTER`     | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LDAP_AUTHORIZATION_FILTER`      | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LDAP_GROUP_FIELD`               | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LDAP_ROLES_MAPPING`             | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LDAP_DEFAULT_ACCESS`            | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `LOGS_DIR_PATH`                  | Override the logs directory path. Default path is the variables_dir path (e.g. `/home/src/mage_data/`)                                                          | `/home/src/logs_dir`                                                     |
| `MAGE_ACCESS_TOKEN_EXPIRY_TIME`  | [More information](../../production/authentication/overview.md#ldap)                                                                                                    | See link                                                                 |
| `MAGE_BASE_PATH`                 | The base path or prefix of the MageML server url. [More information](#setting-the-prefix-for-the-mageml-url)                                          | `base/path`                                                              |
| `MAGE_GATEWAY`                   | `on` serves the web server through the Rust gateway (`mage-service gateway`, from `mage-ml[services]`): the server listens on a loopback port and the gateway on the port users open. Responses are the same; it adds under 1 ms. [More information](../gateway.md) | `on`                                                                     |
| `MAGE_GATEWAY_SHADOW`            | With `MAGE_GATEWAY=on`: the routes the gateway also answers from the metadata database and compares with the server's answers (default `block_runs,logs,pipeline_run,pipeline_runs,pipeline_schedules`), or `off`. [More information](../gateway.md#shadow-reads) | `off`                                                                    |
| `MAGE_GATEWAY_SERVE`             | With `MAGE_GATEWAY=on`: shadowed routes the gateway answers itself when it allows the read (`block_runs`, `logs`, `pipeline_run`, `pipeline_runs`, `pipeline_schedules`); off by default. [More information](../gateway.md#serving) | `block_runs,pipeline_run`                                               |
| `MAGE_LOG_QUERY_VALUES`          | SQL loaders log their queries with string literals masked as `'…'`, so values written in a query stay out of run logs. `on` logs the values, for local debugging. | `on`                                                                     |
| `MAGE_REQUESTS_BASE_PATH`        | [More information](#setting-the-prefix-for-the-mageml-url)                                                                                          | `base/path`                                                              |
| `MAGE_ROUTES_BASE_PATH`          | [More information](#setting-the-prefix-for-the-mageml-url)                                                                                          | `base/path`                                                              |
| `MAGE_CACHE_DIRECTORY`           | The directory MageML uses for storing and retrieving temporary data used in the UI.                                                                               | `/root/.mage_data/default_repo/.cache`                                   |
| `MAGE_DATABASE_CONNECTION_URL`   | Specify the database connection url for orchestration DB. Defaults to local sqlite db.                                                                          | [Example](../../production/configuring-production-settings/overview.md#postgres) |
| `MAGE_PUBLIC_HOST`               | The public host url that can be used to access the MageML app. This value will be used in emails or other notifications.                                          | `http://localhost:6789`                                                  |
| `MAGE_RESTART_STREAMING_PIPELINES_ON_REQUIREMENTS_CHANGE` | When true, running streaming pipelines restart after MageML installs the project's `requirements.txt` (a git pull or reset that installs requirements), so they load the new packages. An edit that is not installed, or an install that fails, restarts nothing. | 1                                                                        |
| `MAX_PRINT_OUTPUT_LINES`         | Caps the `stdout` lines, such as from `print` statements, of each output message from the websocket server. Not set by default: output is never cut.                         | `100`                                                                    |
| `REQUIRE_USER_AUTHENTICATION`    | Enable user authentication in MageML. [More information](../../production/authentication/overview.md)                                                                     | 1                                                                        |
| `RUN_PIPELINE_IN_ONE_PROCESS`    | When True, pipelines will run in one process, unless otherwise specified in the settings for a given pipeline.                                                  | 1
| `SERVER_VERBOSITY`               | [More information](../../observability/logging.md#server-logging)                                                                                           | See link                                                                 |
| `SHELL_COMMAND`                  | Set shell command to use for the MageML terminal. Default command is `bash` for macOS/Unix and `cmd` for Windows.                                                 | `bash`, `cmd`, ...                                                       |
| `ULIMIT_NO_FILE`                 | Override the maximum number of open files allowed in MageML processes.                                                                                            | 8192                                                                     |
| `USER_CODE_PATH`                 | Set this path to be `/home/src/{your_project_name}` so that MageML starts your project and installs the packages in requirements.txt when starting the container. | `/home/src/mage_project`                                                 |

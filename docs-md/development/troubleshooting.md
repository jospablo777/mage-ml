# Troubleshooting

*Debugging common issues in MageML and other frequently asked questions.*

## When a block fails

The traceback names the block's file and the line that failed, in the notebook, in the run's
logs and in `mage run`. The last lines name the block and, for common causes, what to do:

```text
ModuleNotFoundError: No module named 'sklearn'
Block train (transformers/train.py) failed.
scikit-learn is not installed in the Python environment MageML runs in (/opt/mage/bin/python).
Install it with `uv pip install --python /opt/mage/bin/python scikit-learn` and add it to the
project's requirements.txt, so deployments get it too.
```

| Message | What to do |
| --- | --- |
| `... is not installed in the Python environment MageML runs in` | Install the package with the command shown and add it to `requirements.txt` |
| `Profile 'prod' is not in .../io_config.yaml. Its profiles: ...` | Add the profile, or use one of those listed in the block |
| `Warning: POSTGRES_HOST in profile 'default' ... is None because env_var('...') is not set` | Set the variable in the environment MageML runs in, or the secret |
| `... is not in the project's R environment. Add it with rv add ...` | Run the `rv add` shown in the project's `r` folder; see [R packages](../guides/blocks/r-packages.md) |
| `Blocks can use the crates under [workspace.dependencies] ...` | Add the crate and version to the project's `rust/Cargo.toml`, as shown |
| `MageML could not reach the server` | Check host, port and credentials in the block's io_config profile, and that the server is reachable |
| `... was killed (SIGKILL), most often by the system when it runs out of memory` | Read fewer rows or columns, process in chunks, or give MageML more memory |
| `Block ... broke contract ...` | The output broke its [data contract](../guides/blocks/data-contracts.md); the lines above list each broken rule |

`mage run` exits with 1 when a block fails or breaks its contract.

## Pipeline edit page freezes browser

The data output of a block contains too much information for the browser to render.

To temporarily fix this, open the `/terminal` page in MageML.
Then type the following command and press enter to run it:

```bash
rm -rf mage_data/[project_name]/pipelines/[pipeline_name]/.variables
```

> [!NOTE]
> Change `[project_name]` to your MageML project name and change `[pipeline_name]` to the name of the pipeline that is freezing the browser.

The above command will remove the data output files for each block in your pipeline.

---

## The editor is slow or pages take seconds to load

The web server runs every request on one event loop; code that holds it makes every page,
API call and block output wait. To find it, start the server with:

```bash
MAGE_EVENT_LOOP_WATCH=1 mage start your_project
```

Each time the loop is held longer than 0.2 seconds (`MAGE_EVENT_LOOP_WATCH_SECONDS`), the
server log gets a warning with the duration and the code that held it, such as:

```
WARNING:mage_ai.server.loop_watch:The server event loop was blocked for 3.64 s; every request waited.
```

The stacks hold code paths only, no values. Report them with the MageML version. On
servers without internet access, also set `MAGE_UPDATE_CHECK_ENABLED=0`.

---

## Can’t connect to local database from within MageML via Docker

If you’re running MageML via Docker and you’re trying to connect to a local database outside of
Docker, you must use the following `host` or `IP` value when trying to connect to it:
`host.docker.internal`.

---

## Unpredictable Columns in an Upstream Dataframe

Sometimes, upstream data is out of our control. If we're expecting a column in a dataframe and writing SQL against it, we might see a `SQL compilation` error: `invalid identifier.`

Mage community member Josh Pachner shared this solution.

Say you have an upstream block that looks like:

```SQL
SELECT
  df."col_1",
  df."col_2",
  df."col_3",
FROM {{ df }} df
```

Where the underlying `df` might have all three columns, or just a subset. For example, `col_1` isn't always present.

For cases where expected columns _might_ not be in our dataframe, we can add a transformer block upstream of an exporter:

```python
# headers = [cols we need in our dataframe]
# df = our DataFrame()

for head in headers:
  if head not in df.columns:
       df[head] = None
```
This is especially helpful in databases like Snowflake, which don't have an `iferror()` function to check for missing columns.

---

## `InvalidToken` error when accessing data in S3

Accessing data located in S3, you may see an error like this:

```text
An error occurred (InvalidToken) when calling the GetObject operation:
The provided token is malformed or otherwise invalid.
```

This [Stack Overflow](https://stackoverflow.com/questions/54837248/an-error-occurred-invalidtoken-when-calling-the-listbuckets-operation-the-pr) article details the issue. Removing the `AWS_SESSION_TOKEN` line from the `io_config.yaml` should resolve the problem.

## MageML doesn't load properly when running locally

When attempting to connect to MageML with a port _other_ than `6789`, the browser may not render the page correctly with `localhost:port`.

Instead, use `127.0.0.1:port` to connect to MageML.

## Using credential files in Docker

If you have files containing secret credentials (e.g. GCP credentials JSON files), you’ll need to mount them as a volume so that those files appear in your running Docker container.

Use the `-v` command in your `docker run` command to mount the directory containing your files:

```bash
docker run -it -p 6789:6789 \
    -v /Users/dragon/secrets:/home/secrets \
    -v $(pwd):/home/src mageml \
    /app/run_app.sh mage start [project_name]
```

`mageml` is the image built from the MageML repository's Dockerfile (`docker build -t mageml .`).
Or, using `docker compose`, add a volume mount to your MageML service. The following is an example of mounting the `~/.aws` directory for authenticating via the AWS CLI:

```yaml
services:
  mage:
    image: mageml
    ...
    volumes:
      - ~/.aws:/root/.aws
```

## "Run block" button doesn't work when running MageML with Nginx

If the MageML terminal or "Run block" button doesn't work, it usually means there's a WebSocket connection issue. If the MageML server is deployed with Nginx, you can add the below config to Nginx config to enable WebSocket connection.

```
        location / {
          proxy_pass      http://127.0.0.1:6789;
          proxy_http_version 1.1;
          proxy_set_header Upgrade $http_upgrade;
          proxy_set_header Connection "Upgrade";
          proxy_set_header Host $host;
        }
```

## Block gets stuck in running status

If your block gets stuck in running status in the MageML editor, you can try the following things:
* [Restart the kernel](../getting-started/kernels.md#restart-kernel) and refresh the page.
* Check whether the CPU and memory usage of the instance is too high. Increase the CPU and memory if the usage is above 50%.

## Server fails to start or keeps restarting due to healthcheck failures

When deploying your MageML server on Cloud (Kubernetes, AWS, GCP, Azure, etc.) with Terraform scripts or Helm chart, there're health checks set up for the
cloud services. If you notice the server keeps restarting, one possible reason is the health check keeps failing due to timeout. You can try increasing the timeout or the initial delay of the health checks to resolve the issue.

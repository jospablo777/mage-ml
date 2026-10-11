# Configure your development environment

*Set up a development environment to contribute code to MageML.*

## Getting started

To get started, you'll need Docker installed on your machine. You can read more about Docker [here](https://www.docker.com/).

## Environment setup

Next, configure your development environment with the following commands.

**1. Configure your development environment**

Configure your coding environment for local development: uv, Python 3.12, pre-commit hooks, etc. Follow the instructions in [README_dev.md](https://github.com/jospablo777/mage-ml/blob/master/README_dev.md).

**2. Create a new project**

```bash
./scripts/init.sh [project_name]
```

**3. Build the dev docker image and run all services**

```bash
./scripts/dev.sh [project_name]
```
If you only work on backend, use the command below for better performance
```bash
./scripts/start.sh [project_name]
```

**4. Open MageML**

When developing frontend, use port 3000. If you only need to develop backend, you can use port 6789. Find these ports at [http://localhost:3000](http://localhost:3000) & [http://localhost:6789](http://localhost:6789).

## Push code changes

Once your environment is configured, you can make edits and see changes in real time! Now you're ready to contribute to the MageML repo. This is very similar to contributing to other open source projects on GitHub.

If you _don't_ have any experience contributing code and this is confusing, open an issue in [GitHub issues](https://github.com/jospablo777/mage-ml/issues).

Otherwise, follow the steps below to push your code changes to the MageML repo.

1. Fork [the repository](https://github.com/jospablo777/mage-ml) on GitHub and clone your fork.
2. Create a branch for the change: `git checkout -b fix-short-description`.
3. Make the change with tests next to it, in `mage_ai/tests` for the backend or
   `mage_ai/frontend/tests` for the frontend.
4. Run the checks the pre-commit hooks run, and the tests:

```bash
   uv run pre-commit run --all-files
   make test
   ```

5. Commit with a message that says what changed and why, push the branch to your fork, and
   open a pull request against `master`. Describe how you tested it.

## Troubleshoot

### Permission denied

If you get an error relating to permission denied when trying to execute a script,
it might have something to do with SELinux.

Two or more containers are trying to mount the same volume. With SELinux it’s not possible without
the `z` option.

For more information about using the `z` option,
read [Docker’s documentation](https://docs.docker.com/storage/bind-mounts/).

### ModuleNotFoundError: No module named 'x'

If new libraries were added, install the new dependencies. It can be done in 2 ways:

1. `docker compose build` from the repository root fully rebuilds the image with the new dependencies. It can take a long time.
2. `uv sync --locked` from inside the container installs the dependencies recorded in `uv.lock`. It is much faster.

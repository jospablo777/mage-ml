# Contributing to the backend server

*MageML backend code is written in Python and the server uses the Tornado framework. Guides on adding features to the MageML backend.*

## Guides

### API

- [Overview](../../extensibility/api-reference/overview.md)

### Data integrations

- [Add a new source to the data integration pipeline](../data-integrations/add-new-source.md)
- [Add a new destination to the data integration pipeline](../data-integrations/add-new-destination.md)

### Streaming pipelines

- [Add a new source or destination to the streaming pipeline](streaming/sources-and-destinations.md)

### IO classes

- [Add a new IO class](io/adding-a-class.md)

## Style guide

### Linter
Install `ruff` in your IDE to lint the Python code.

To run the linter locally, execute this script:

```bash
./scripts/server/lint.sh
```

## Testing

### Unit tests

Add unit tests for the feature in
[mage_ai/tests](https://github.com/jospablo777/mage-ml/tree/master/mage_ai/tests) directory.

To run the tests locally, execute this script:

```bash
./scripts/server/test.sh
```

It is also possible to run unit tests directly in a live docker instance, as given in the following steps.

1. Find out the backend server container name, with the command `docker container ls` in a terminal:
```bash
 % docker container ls

CONTAINER ID   IMAGE       COMMAND                  CREATED      STATUS          PORTS                    NAMES
8dbcfbe41755   mage/data   "./scripts/install_a…"   5 days ago   Up 38 minutes   0.0.0.0:3000->3000/tcp   mage-ml-app-1
b9d811e1e3e8   mage/data   "python mage_ai/serv…"   5 days ago   Up 38 minutes   0.0.0.0:6789->6789/tcp   mage-ml-server-1
```

2. Start an interactive `bash` session with the backend server container:
```bash
% docker exec -it mage-ml-server-1 /bin/bash
```

3. Run unit tests with the following command:
```bash
root@b9d811e1e3e8:/home/src# python3 -m pytest mage_ai -x
```

## Debugging

Run the server from the repository so your breakpoints apply:

```bash
DEBUG=1 uv run --no-sync python mage_ai/server/server.py --project default_repo
```

`DEBUG=1` turns on debug logging. To step through the code, start the same command from
your editor's debugger (a Python launch configuration with
`mage_ai/server/server.py` as the program and `--project default_repo` as its arguments), or
add `breakpoint()` where you want to stop. Blocks run in other processes in production runs,
so to debug a block's code, run it from the editor (one block at a time) or with
`uv run --no-sync mage run default_repo <pipeline>`.

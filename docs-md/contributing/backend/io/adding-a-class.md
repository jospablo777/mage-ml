# Adding an IO class

*IO classes power MageML sources and destinations. How to contribute an IO class to MageML.*

## What's an IO class?

IO classes are the code that lets MageML read and write data. When you configure your [`io_config`](../../../development/io_config.md) to use a source or destination, you're telling MageML to use a specific IO class.

IO classes are defined [here](https://github.com/jospablo777/mage-ml/tree/master/mage_ai/io) in the MageML repository. There are a few different types of IO classes: some are for reading data, some are for writing data, and some are for both.

## Why should I contribute?

If a source or destination you need is not supported, open an [issue](https://github.com/jospablo777/mage-ml/issues) or contribute the IO class to MageML.

## How do I contribute?

### Configure your development environment

The first step to contributing an IO class is to configure your development environment. You can find instructions for doing so [here](../../development-environment.md).

### Create a new IO class

Once configured, you'll want to create a new file in the `mage_ai/io` directory. The file should be named after the IO class you're contributing. For example, if you're contributing an IO class called `MyIOClass`, you should create a file called `my_io_class.py`.

Some IO classes are relatively straightforward, like [exporting to local files](https://github.com/jospablo777/mage-ml/blob/master/mage_ai/io/file.py) or [writing to Google Sheets](https://github.com/jospablo777/mage-ml/blob/master/mage_ai/io/google_sheets.py). Others, like full database integrations, can be pretty complex. [Google BigQuery](https://github.com/jospablo777/mage-ml/blob/master/mage_ai/io/bigquery.py) is a good example of a complex IO class.

Most classes are platform specific, but for a database, you might need the following methods:

- `alter_table`: Alter the table schema
- `load`: Load data from the database
- `export`: Export data to the database
- `execute`: Execute a query on the database
  - `execute_queries`: Execute multiple queries on the database

Additionally, every class will need to have the following methods:

- `__init__`: Initialize the class
- `with_config`: Initialize the database client from the configuration loader. You'll notice that this method is used in _all_ of our templates.
- `test_connection`: Validate credentials or reachability with a lightweight, side-effect-free check. Return normally (`None`) when the probe succeeds and let connection or probe failures raise so callers can surface the underlying error. This method should not load preview data, export records, or mutate remote resources. If the safest probe depends on a target resource (for example a bucket, index, or sheet), accept that target as an optional argument and document any broader no-target fallback permissions.

### Test your IO class

In order to test your class, you should perform all of the methods you defined in the class itself. Once you pass our linting checks (outlined in the development environment link above) _and_ your tests pass, you're ready to submit a pull request.

## Create a pull request

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

# Adapt an existing source

*MageML builds data integrations from [Singer taps](https://www.singer.io/#taps). If a Singer source you want to use isn't supported yet, you can adapt it yourself.*

## Getting started

MageML sources live in the `/mage_integrations/mage_integrations` folder. To add a new source, you'll need to create a new folder in the `/sources` subdirectory.

The folder name should be the name of the tap. For existing sources, that starts with pulling in the GitHub repo for the tap. For example, for the [GitHub tap](https://github.com/MeltanoLabs/tap-github), we would:

```bash
cd mage_integrations/mage_integrations
git clone https://github.com/MeltanoLabs/tap-github.git sources/github
```

The result:

```
.
├── LICENSE
├── README.md
├── config-sample.json
├── config.json
├── meltano.yml
├── poetry.lock
├── pyproject.toml
├── pytest.ini
├── tap-github.sh
├── tap_github
│   ├── __init__.py
│   ├── authenticator.py
│   ├── client.py
│   ├── organization_streams.py
│   ├── repository_streams.py
│   ├── schema_objects.py
│   ├── scraping.py
│   ├── streams.py
│   ├── tap.py
│   ├── tests
│   ├── user_streams.py
│   └── utils
└── tox.ini
```

## Configuring the source

We now have a folder with our tap and a bunch of miscellaneous files. Our first step is to clear out everything we _don't_ need. We can delete all files/folders other than `tap_your_source` and `tests`, but hold onto anything that looks like a `config.json` file or a sample config.

Next, we need to create a configuration file and an init file:

```bash
mkdir sources/github/templates \
&& touch sources/github/templates/config.json \
&& touch sources/github/__init__.py
```

We can now populate our `config.json` file with the sample from the tap. In the GitHub example, this is `config.sample.json`. It looks like this:

```json
{
    "access_token": "abcdefghijklmnopqrstuvwxyz1234567890ABCD",
    "repository": "singer-io/target-stitch",
    "start_date": "2021-01-01T00:00:00Z",
    "request_timeout": 300,
    "base_url": "https://api.github.com"
}
```

These are the parameters we'll need to configure our tap. After cleanup, our tree looks like:

```
.
├── __init__.py
├── tap_github
│   ├── __init__.py
│   ├── authenticator.py
│   ├── client.py
│   ├── organization_streams.py
│   ├── repository_streams.py
│   ├── schema_objects.py
│   ├── scraping.py
│   ├── streams.py
│   ├── tap.py
│   ├── tests
│   ├── user_streams.py
│   └── utils
└── templates
    └── config.json
```

Now, we need to override some methods in the class to make it work with MageML. Copy and paste the following template into `your_tap/__init__.py`:

```python
from typing import List

from mage_integrations.sources.base import Source, main
from mage_integrations.sources.catalog import Catalog

class YourSource(Source):
    def discover(self, streams: List[str] = None) -> Catalog:
        pass

    def sync(self, catalog: Catalog) -> None:
        pass

    def test_connection(self) -> None:
        client = YourSource(YOUR_CONFIG)
        client.close()

if __name__ == "__main__":
    main(YourSource, schemas_folder="tap_your_tap/schemas")
```

Replace YourSource with the name of your source, e.g. `Github` for Github, and `tap_your_tap` with the name of your tap, e.g. `tap_github`.

### Overwrite tap methods

Our goal is to overwrite the `discover` and `sync` methods to make them work with MageML. Discover is likely in `tap_your_tap/discover.py` and sync is likely in `tap_your_tap/sync.py`. It requires a bit of nuance, but our job is to look through the existing Singer tap, understand how the `sync` and `discover` methods were implemented, then execute them in the MageML `__init__.py` under the corresponding method. This is necessarily different for every tap, since each project is entirely independent.

Don't forget to use absolute imports for these files. [HubSpot](https://github.com/jospablo777/mage-ml/tree/master/mage_integrations/mage_integrations/sources/hubspot) is another good example of a completed tap.

### Add test connection method

We need a `test_connection` method to enable testing the tap in the MageML UI. This is a simple method that instantiates the tap and closes it. Replace `YourSource` with the source class, e.g. `GitHub`, and provide the configuration arguments to the tap. Here's an [example](https://github.com/jospablo777/mage-ml/blob/master/mage_integrations/mage_integrations/sources/sftp/__init__.py) from the SFTP tap:

```python
def test_connection(self) -> None:
        client = SFTPConnection(host=self.config['host'],
                                username=self.config['username'],
                                password=self.config['password'],
                                private_key_file=self.config.get(
                                    'private_key_file'),
                                port=self.config['port'])
        client.close()
```

### Overwrite `write_schema` method

Finally, we need to overwrite the `write_schema` method in the `tap_your_tap/sync.py` file. Add the following import:

`from mage_integrations.sources.messages import write_schema`

Now, switch out the default function (likely `singer.write_schema`) for the MageML function using the default arguments. Here's an example:

```python
  write_schema(
        stream_name=catalog['tap_stream_id'],
        schema=schema,
        key_properties=catalog.get('key_properties', ['id']),
        bookmark_properties=catalog.get('bookmark_properties'),
        disable_column_type_check=catalog.get('disable_column_type_check'),
        partition_keys=catalog.get('partition_keys'),
        replication_method=catalog.get('replication_method'),
        stream_alias=catalog.get('stream_alias'),
        unique_conflict_method=catalog.get('unique_conflict_method'),
        unique_constraints=catalog.get('unique_constraints'),
    )
```

### Add logging

To add verbose logging to MageML, add a logger argument to each tap file, `tap_github/sync.py`, `tap_github/discover.py`:

```python
def discover(client, logger=None):
    """
    Run the discovery mode, prepare the catalog file and return catalog.
    """
    mage_logger = logger or LOGGER
```

Then replace usage of `LOGGER` with `mage_logger`.

```python

for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            # These were LOGGER previously
            mage_logger.error(err)
            mage_logger.error('stream_name: %s', stream_name)
            mage_logger.error('type schema_dict: %s', type(schema_dict))
            raise err
```

This lets MageML log the Singer tap's messages.

## Add source to the MageML UI

Add the new source to the `SOURCES` list constant in [this](https://github.com/jospablo777/mage-ml/blob/master/mage_ai/data_integrations/sources/constants.py) file.

This will make your new source visible in the MageML UI.

## Test your source

Sources should be tested both in the console and via the UI. Follow the directions [here](https://github.com/jospablo777/mage-ml/blob/master/mage_integrations/README.md#testing-sync-locally) to configure a lightweight test of your source's tap via a bash terminal and the MageML UI in the dev Docker instance. This entails:

- Setting up a sample source and connection locally.
- Running the tap directly, via Python.
- Writing to an intermediate file to inspect the output.
- Performing an end-to-end sync.

## Populate `README.md`

Finally, populate a README file in the source folder, e.g. `/sources/github/README.md`. This should include:

* The name/logo of the tap.
* A configuration section.
* The method for accessing/creating an API key to use with the folder.
* See [HubSpot](https://github.com/jospablo777/mage-ml/tree/master/mage_integrations/mage_integrations/sources/hubspot) or [GitHub](https://github.com/jospablo777/mage-ml/tree/master/mage_integrations/mage_integrations/sources/github) for examples.

# Updating MageML

*How to update MageML to the latest version.*

MageML has no published Docker image or PyPI package. `docker pull mageai/mageai` and
`pip install -U mage-ai` update upstream Mage, not MageML. Update from the repository.

<details><summary>Source install (uv)</summary>

From the `mage-ml` folder, pull the latest code and synchronize the environment with the
same extras you installed:

```bash
git pull
uv sync --locked --no-default-groups --extra postgres
```

Then restart MageML with `uv run --no-sync mage start my_project`.

</details>

<details><summary>Docker</summary>

From the `mage-ml` folder, pull the latest code and rebuild the image:

```bash
git pull
docker build -t mageml .
```

Then restart your containers so they use the new image.

</details>

Changes between versions are listed in the [release notes](https://github.com/jospablo777/mage-ml/blob/master/docs/development/release-notes.md).

# Updating MageML

*How to update MageML to the latest version.*

`docker pull mageai/mageai` and `pip install -U mage-ai` update upstream Mage, not MageML.
Update MageML the way you installed it (see the [Quickstart](../getting-started/setup.md)).

<details><summary>uv project</summary>

From the project folder:

```bash
uv lock --upgrade-package mage-ml
uv sync
```

With an exact version in `pyproject.toml`, such as `mage-ml==0.1.0`, change the
version first. Then restart MageML with `uv run mage start my_project`.

</details>

<details><summary>uv tool</summary>

```bash
uv tool upgrade mage-ml
```

</details>

<details><summary>uv pip or pip</summary>

```bash
uv pip install --upgrade mage-ml --index https://jospablo777.github.io/mage-ml/simple/
```
```bash
.venv/bin/python -m pip install \
  -r https://jospablo777.github.io/mage-ml/install/<version>/base.txt
```

With pip, use the requirements file of the new release, such as `v0.1.0`.

</details>

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

For the small image built from the package index, change the version in its
Dockerfile and rebuild it. For the full image, pull the latest code in the `mage-ml`
folder and rebuild:

```bash
git pull
docker build -t mageml .
```

Then restart your containers so they use the new image.

</details>

Changes between versions are listed in the [release notes](https://github.com/jospablo777/mage-ml/blob/master/docs/development/release-notes.md).

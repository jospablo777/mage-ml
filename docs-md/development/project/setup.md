# Project setup

*How to set up a folder containing your MageML project code and other files.*

## Parent folder

Create a parent folder that will contain your MageML project code:

```bash
mkdir data_monorepo
```

Change directory into the parent folder:

```bash
cd data_monorepo
```

## Create a Dockerfile

Create an empty Dockerfile:

```bash
echo "" > Dockerfile
```

MageML has no published image, so this Dockerfile builds on a MageML image you built from the
[MageML repository's Dockerfile](https://github.com/jospablo777/mage-ml/blob/master/Dockerfile)
(`docker build -t mageml .` in the `mage-ml` folder). Paste the following into the Dockerfile you
just created:

```dockerfile
FROM mageml
COPY . /home/src
```

## Create a .gitignore file

Create an empty .gitignore file:

```bash
echo "" > .gitignore
```

Paste the following contents into the .gitignore file you just created:

```
.DS_Store
.file_versions
.gitkeep
.log
.logs/
.mage_temp_profiles
.preferences.yaml
.variables/
__pycache__/
docker-compose.override.yml
logs/
mage-ai.db
mage_data/
secrets/
```

## Create MageML project

Follow these [instructions](../../getting-started/setup.md) to create a MageML project in your
current directory. If you named your project `demo_project`, your current folder structure
will look like this:

```
data_monorepo/
|-- demo_project/
|-- .gitignore
|-- Dockerfile
```

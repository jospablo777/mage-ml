# Use a completed pipeline

## 1. Clone project

In your terminal, clone this [repository](https://github.com/mage-ai/demo_project)
by running the following command:

```bash
git clone https://github.com/mage-ai/demo_project.git
```

The demo project comes from upstream Mage.

## 2. Start MageML

<b>Docker</b>

From the folder that contains `demo_project`, with an image built from the MageML repository's
Dockerfile (`docker build -t mageml .`):

```bash
docker run -it -p 6789:6789 -v $(pwd):/home/src mageml /app/run_app.sh mage start demo_project
```

<b>Source checkout</b>

From your MageML checkout, after `uv sync`, pass the path to the project:

```bash
uv run --no-sync mage start /path/to/demo_project
```

Open [http://localhost:6789](http://localhost:6789) in your browser.

On the left side in the file browser underneath the folder `demo_project/pipelines`,
click the pipeline named `titanic_survivors`.

## 3. Follow the quick start tutorial

Go back to the [quick start tutorial](train-model.md),
start from step <b>3. Play around with scratchpad</b>,
and use the existing code to follow along.

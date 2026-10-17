# Pipeline sidekick

*The right panel of the pipeline editor: charts, variables, secrets and block settings.*

The Pipeline sidekick appears when you edit a pipeline.

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/sidekick/open-sidekick.gif" alt="The Pipeline sidekick appears when you edit a pipeline." />

In the top right corner of the page, click the `Chevron` or `Arrow` looking icon to expand
the right panel and reveal the Sidekick.

---

## Visualize block output data

Add 1 or multiple charts to visualize the data produced by a block.
Click the chart icon (<b>Add chart</b>) in a block header and pick a chart type. The block's
charts show in the Sidekick's <b>Charts</b> view.

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/sidekick/add-chart.gif" alt="Visualize block output data." />

---

## Configure pipeline variables

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/sidekick/variables.png" alt="Configure pipeline variables." />

Use variables in your code and interpolate them at runtime.
These variables can be defined at the moment the pipeline is triggered or
pre-configured from the Sidekick.

[Read more here](../../getting-started/runtime-variable.md)

---

## Store sensitive secrets

Store credentials as secrets instead of writing them in your code.

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/create-secrets.png" alt="Store sensitive secrets." />

[Read more here](../../development/variables/secrets.md)

---

## Customize settings for every individual block

1. Rename a block
1. Set what execution engine the block should use
1. Configure how the block should retry itself in case of failure
1. Set a timeout limit for how long the block can run before it should fail (in a pipeline
   that runs in one process, a block past its timeout stops the whole run)
1. Configure variables specific to a block
1. View which pipelines are currently using a block

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/sidekick/block-settings.png" alt="Customize settings for every individual block." />

---

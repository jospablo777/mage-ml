# Writing data pipeline code

*Add, arrange and edit blocks in the pipeline editor.*

## Adding blocks made easy

You can quickly add blocks of code to your pipeline with these methods:

<details><summary>Drag blocks from the file browser and drop it on the pipeline</summary>

> [!NOTE]
> **Supported block type directories:** `callbacks`, `conditionals`, `custom`, `data_exporters`, `data_loaders`, `dbt`,
> `dbts`, `markdowns`, `scratchpads`, `sensors`, and `transformers`

1. Open the file browser on the Pipeline Editor page.
1. Click a block file and keep holding down.
1. Drag the block file onto the center of the page (the Notebook area).
1. Release the button and it’ll be added to the pipeline.

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/code-editor/drag-block.gif" alt="Drag blocks from the file browser and drop it on the pipeline" />

</details>

<details><summary>Search blocks across entire projects</summary>

Find existing code and add it to the current pipeline.

> [!NOTE]
> You must first enable the feature named <b>Add new block v2</b> in your
> [project settings](http://localhost:6789/settings/workspace/preferences).
> This feature is available in standard (batch) pipelines.
>
> If you just recently enabled the feature <b>Add new block v2</b>, you must restart MageML
> so that your blocks can be indexed.

Instead of clicking a button to add a new block, type in the search bar to find an
existing block across all your projects.

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/code-editor/block-search.gif" alt="Search blocks across entire projects" />

> [!TIP]
> ⌘ + / (Command + Forward Slash) keyboard shortcut will instantly focus your typing on the
> search bar from anywhere on the page (as long as you aren’t already typing in a
> block of code).

> [!TIP]
> Use the arrow up and arrow down key to browse
> through the results.

</details>

<details><summary>Custom code templates for reducing boilerplate work</summary>

Create a template for a block or a pipeline that can be reused throughout your project.

[Video](https://github.com/mage-ai/assets/raw/main/custom-templates/blocks/overview.mp4)

[Read more here](../blocks/user-defined-templates.md)

</details>

---

## Notes on editing pipelines

> [!WARNING]
> Avoid opening and editing the **same pipeline in multiple browser tabs or windows** at the same time.
> Changes from one tab may overwrite changes from another.

Tips to stay safe:

- Edit a pipeline in **one tab only**.
- If you need to monitor the pipeline elsewhere, use another tab in **read-only mode**.
- Close extra editing tabs if opened by mistake.

---

### Collaboration best practices

If multiple users need to develop the **same pipeline**, have each person work in their own
copy of the project on a separate branch and merge the changes with
[Git](../../development/git/setting-up-git.md).

---

## Re-arrange and hide or show blocks

<details><summary>Re-arrange the order of blocks</summary>

Drag a block and move it up or down by dropping it on another block.

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/code-editor/rearrange-blocks.gif" alt="Drag a block and move it up or down by dropping it on another block." />

</details>

<details><summary>Current blocks tab</summary>

When editing a pipeline, open the <b>Current blocks</b> tab next to <b>All files</b> to list
the blocks in the pipeline. Click a block there to hide it in the notebook, and click it again
to show it. This can help optimize vertical screen space while writing code.

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/file-browser/blocks-in-pipeline.gif" alt="Hide and show blocks in the current pipeline" />

</details>

---

## Pre-built data integrations for 3rd party sources

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/batch-pipelines/data-integrations/configuration-credentials.png" alt="Pre-built data integrations for 3rd party sources" />

Sync data from 3rd party sources without writing boilerplate code by using pre-built
data integrations (Singer Taps and Targets) provided by the data engineering community.

> [!TIP]
> Data integrations to sync your data to 3rd party destinations are also available in the
> same feature set.

> [!TIP]
> These sources and destinations can be combined and chained together with
> your existing blocks.

[Read more here](../../data-integrations/batch-pipelines.md)

---

## Block upgrades

These block features change how a block runs:

<details><summary>Recursively dynamic code made simple</summary>

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/code-editor/dynamic-blocks.png" alt="Recursively dynamic code made simple." />

A dynamic block runs its downstream blocks once for each item it returns.

[Read more here](../../design/blocks/dynamic-blocks.md)

</details>

<details><summary>Replicate the code of an existing block</summary>

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/code-editor/replicate-block.gif" alt="Replicate the code of an existing block" />

[Read more here](../blocks/replicate-blocks.md)

</details>

<details><summary>Conditionally execute code</summary>

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/blocks/conditionals-example.png" alt="Conditionally execute code" />

[Read more here](../../design/blocks/conditionals.md)

</details>

<details><summary>Callbacks</summary>

A callback block is associated to another block.
When the parent block succeeds or fails, the callback block functions are executed.

[Read more here](../../design/blocks/callbacks.md)

</details>

---

## Add new block v2

An improved experience for adding new blocks to your pipeline in the
Pipeline Editor (`/pipelines/[pipeline_uuid]/edit`), which allows you to
search for existing block files and use data integration blocks inside of
a standard (batch) pipeline.

---

## Code and output side-by-side view

Run code and see its output right next to where you’re writing.

This makes it so that you don’t have to keep scrolling down every time you make a code change
and want to see its results.

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/code-editor/split-view.gif" alt="Code and output side-by-side view" />

> [!NOTE]
> This view is experimental. The `notebook_block_output_split_view` feature is not on the
> settings page; set it to `true` under `features` in the project's `metadata.yaml`. The toggle,
> <b>Show output next to code (beta)</b>, is in the block menu of the v2 code blocks.

<details><summary>Check out this demo for a quick overview</summary>

[YouTube video player](https://www.youtube.com/embed/x4VLyIG6fCc)

</details>

---

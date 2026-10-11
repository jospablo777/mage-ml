# Contributing

*Contributions to MageML are welcome, from small UI fixes to new features.*

<img src="https://media.giphy.com/media/5cZbRBLhW4tc4/giphy.gif" alt="Contributing" />

## Ways to contribute

There are many ways to contribute to MageML, and all contributions are welcome. To set up a
development environment, [read on](development-environment.md).

<details><summary>Report and fix bugs</summary>

The easiest way to contribute is to report bugs or suggest new features.

### Report bugs

Open an issue in [GitHub issues](https://github.com/jospablo777/mage-ml/issues) with what happened and how to reproduce it.

### Squash bugs

Take a look at the [GitHub issues](https://github.com/jospablo777/mage-ml/issues) for small to medium complexity projects and bugs that need fixing.

</details>

<details><summary>Features and docs</summary>

### Suggest new features
First take a look at the [roadmap](../about/roadmap.md) to see if your idea is already planned. You might also consider checking out open issues on [GitHub](https://github.com/jospablo777/mage-ml).

Otherwise, open an issue in [GitHub issues](https://github.com/jospablo777/mage-ml/issues) and tell us what you're thinking.

### Write documentation
The docs can always be improved. The easiest way to contribute is by scrolling to the bottom of the page you'd like to edit (maybe this one) and clicking the "Edit this page" link. You'll be taken to GitHub where you can make your changes and submit a pull request.

<img src="../../docs/media/edit-docs.png" />

If you like working in code, that's great too! All of our docs are written in markdown, so you can clone the repository from [GitHub](https://github.com/jospablo777/mage-ml) and edit the files in `docs/`.

</details>

<details><summary>Community support</summary>

### Community Support

If you know MageML well, help answer questions in [GitHub issues](https://github.com/jospablo777/mage-ml/issues).

There are also [open issues](https://github.com/jospablo777/mage-ml/issues) that need help.

</details>

<details><summary>Backend</summary>

### Backend development

Help with the backend can be refactoring code, adding new features, or fixing bugs. See
the [roadmap](../about/roadmap.md) for planned work.

### Data integration

MageML supports [data integration](../data-integrations/overview.md) pipelines via the Singer spec. Help adding or updating sources and destinations is welcome. See the [docs on adding a source](data-integrations/adapt-existing-source.md).

### IO operations (SQL block) and templates

Add IO classes under `mage_ai/io/` for new SQL block sources. For an example, see the upstream PR that added the [DuckDB IO class](https://github.com/mage-ai/mage-ai/pull/3463).

The simplest way to help is by adding data loader and exporter templates, as in the upstream PR that added [DuckDB templates](https://github.com/mage-ai/mage-ai/pull/3553).

</details>

<details><summary>Frontend</summary>

### Frontend development

Help with UI/UX, accessibility, and more is welcome. See the [frontend](frontend/overview.md) page.

</details>

Got questions? Ask in [GitHub issues](https://github.com/jospablo777/mage-ml/issues).

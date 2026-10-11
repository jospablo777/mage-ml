# Documentation

*MageML's documentation is open source. Learn how to edit pages or write new docs for MageML.*

<div align="center">
<img src="https://github.com/mage-ai/mage-ai/assets/59450879/769a5faf-37ea-453c-8bd6-83270366fe00" width="65%" />
</div>

## Overview

MageML uses [Mintlify](https://www.mintlify.com/) as its documentation framework. In this guide, we'll discuss different "levels" of contributing to the docs, starting from simplest to most advanced.

All of the docs are written in plain text as Markdown (MDX) files. If you're not familiar with Markdown, check out [this guide](https://www.markdownguide.org/basic-syntax/) to get started. The docs live in the [`docs` directory](https://github.com/jospablo777/mage-ml/tree/master/docs) of the repository on GitHub.

## Level 1: Suggesting edits

The simplest way to contribute to the docs is directly in the browser, using GitHub's file editing feature. This works for small edits to existing docs, but it's not the best way to add entirely new docs or complex formatting, images, etc.

Open the page's file in the [`docs` directory](https://github.com/jospablo777/mage-ml/tree/master/docs) on GitHub and click the edit (pencil) icon. Make your changes, then click "Commit changes" and open a pull request with your changes and a short description. Once the pull request is merged, your changes will be live on the site.

<img src="https://mage-ai.github.io/assets/contributing-docs.png" />

## Level 2: Add complex components

### Images

Images for the docs live in `docs/images` and `docs/media`. Add new images there and reference them with a path from the docs root, for example `/images/diagrams/compiled-chains.light.svg`.

Open an issue in [GitHub issues](https://github.com/jospablo777/mage-ml/issues) if you're editing documentation that needs additional assets.

When adding images, we use centered divs or the Mintlify frame component. You can see the code in this page for examples of both.

### Mintlify elements

To learn more about Mintlify-specific elements, visit the Mintlify docs [here](https://mintlify.com/docs/content/page). You can add things like accordion groups, interactive lists, html elements, and more. Anything you can do in Mintlify, you can do in the docs.

## Level 3: Add entirely new docs

### Adding a new page

Each page is an MDX file that should begin with the following metadata syntax with `---` at the start and end. Here are basics to get you started:

1. [Page basics](https://mintlify.com/docs/content/page)
2. [Headers and text](https://mintlify.com/docs/content/text)
3. [Images and embeds](https://mintlify.com/docs/content/image-embeds)

Once you add a new page in the appropriate folder in the `docs` directory, you'll need to add it to the site navigation. That is done in the `docs.json` file in the root of the `docs` directory.

Add the page's path to the `pages` array of the right group under `navigation.tabs`. The path is relative to the `docs` directory and does _not_ include the file extension, for example `guides/blocks/rust-blocks`.

The best way to understand the structure is to read about it in the [Mintlify docs](https://mintlify.com/docs/settings/global) and to look at the [`docs.json` file](https://github.com/jospablo777/mage-ml/blob/master/docs/docs.json).

### Developing locally in Mintlify

You can run the docs locally to see changes _live_ as you make them on your machine.

From the `docs` directory, run `./docs.sh`. It installs the Mintlify CLI with npm if it is missing and runs `mintlify dev --port 3333`, so the docs are served at `localhost:3333`.

### Adding icons

Some elements in Mintlify have icons. Those are sourced from [fontawesome](https://fontawesome.com/). Any icon you find there can be added as an icon to the docs, where appropriate. We only add icons to top-level items in the sidebar.

## Level 4: Adding entirely new sections

If you'd like to contribute a larger, more structured section of the documentation, open an issue in [GitHub issues](https://github.com/jospablo777/mage-ml/issues) first to discuss the plan.
